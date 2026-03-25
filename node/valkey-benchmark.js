/**
 * Valkey Benchmark Tool
 * A comprehensive performance testing utility for Valkey/Redis operations.
 *
 * ============================================================================
 * EXTENDED METRICS (--extended-metrics flag)
 * ============================================================================
 *
 * When enabled, the CSV output includes 12 additional columns (28 total):
 *
 * | Column           | Description                                          | How Measured                                    |
 * |------------------|------------------------------------------------------|------------------------------------------------|
 * | cache_hits       | Number of GET requests that returned a value         | Check if GET result !== null                   |
 * | cache_misses     | Number of GET requests that returned null            | Check if GET result === null                   |
 * | timeout_errors   | Requests that timed out                              | instanceof TimeoutError or "TIMEOUT" in error  |
 * | connection_errors| Connection failures (refused, reset, etc.)           | instanceof ConnectionError/ClosingError,       |
 * |                  |                                                      | or ECONNREFUSED/ECONNRESET in error            |
 * | busy_errors      | Server busy (Lua script running)                     | "BUSY" in error message                        |
 * | oom_errors       | Server out of memory                                 | "OOM" in error message                         |
 * | loading_errors   | Server loading dataset from disk                     | "LOADING" in error message                     |
 * | keyspace_count   | Current number of keys in database                   | DBSIZE command (every 5 seconds)               |
 * | replication_lag  | Replication lag in seconds                           | INFO REPLICATION command (every 5 seconds)     |
 * |                  | - Master: max lag of connected replicas              |                                                |
 * |                  | - Replica: seconds since master link down            |                                                |
 * | conn_est_p50     | Connection establishment time 50th percentile (ms)   | Measured during client pool creation           |
 * | conn_est_p99     | Connection establishment time 99th percentile (ms)   | Measured during client pool creation           |
 * | conn_est_p100    | Connection establishment time maximum (ms)           | Measured during client pool creation           |
 *
 * Notes:
 * - Connection time percentiles are measured at startup and remain constant
 * - Keyspace and replication monitoring run from orchestrator only (not workers)
 * - All error counters are per-interval (reset each CSV row)
 * - Cache hits/misses only tracked for GET commands
 *
 */

const path = require('path');
const fs = require('fs');
const os = require('os');
const cluster = require('cluster');
const { GlideClient, GlideClusterClient, TimeoutError, ConnectionError, ClosingError } = require('@valkey/valkey-glide');
const hdr = require('hdr-histogram-js');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// ============================================================================
// Logging System
// ============================================================================

/**
 * Log levels enum
 */
const LogLevel = {
    DEBUG: 10,
    INFO: 20,
    WARNING: 30,
    ERROR: 40,
    CRITICAL: 50,
    DISABLED: 100
};

/**
 * Logger class for configurable logging
 * By default, logging is disabled for performance
 */
class Logger {
    constructor() {
        this.level = LogLevel.DISABLED;
        this.csvMode = false;
    }

    /**
     * Setup logging configuration
     * @param {Object} options - Configuration options
     * @param {boolean} options.csvMode - If true, logs go to stderr
     * @param {string} options.logLevel - Log level string (DEBUG, INFO, WARNING, ERROR, CRITICAL)
     * @param {boolean} options.debug - If true, set level to DEBUG
     * @returns {boolean} True if logging is enabled
     */
    setup({ csvMode = false, logLevel = null, debug = false }) {
        this.csvMode = csvMode;

        // Debug flag overrides logLevel
        if (debug) {
            logLevel = 'DEBUG';
        }

        // If no log level specified, disable logging for performance
        if (!logLevel) {
            this.level = LogLevel.DISABLED;
            return false;
        }

        // Convert log level string to numeric value
        const levelUpper = logLevel.toUpperCase();
        if (LogLevel[levelUpper] !== undefined) {
            this.level = LogLevel[levelUpper];
        } else {
            this.level = LogLevel.WARNING;
            this._write('WARNING', `Invalid log level '${logLevel}', using WARNING`);
        }

        return true;
    }

    /**
     * Internal write method
     * @param {string} levelName - Level name for formatting
     * @param {string} message - Message to log
     */
    _write(levelName, message) {
        const timestamp = new Date().toISOString();
        const formatted = `${timestamp} [${levelName}] ${message}`;

        // In CSV mode, logs go to stderr to keep stdout clean for CSV data
        if (this.csvMode) {
            process.stderr.write(formatted + '\n');
        } else {
            console.log(formatted);
        }
    }

    /**
     * Log at DEBUG level
     * @param {string} message - Message to log
     */
    debug(message) {
        if (this.level <= LogLevel.DEBUG) {
            this._write('DEBUG', message);
        }
    }

    /**
     * Log at INFO level
     * @param {string} message - Message to log
     */
    info(message) {
        if (this.level <= LogLevel.INFO) {
            this._write('INFO', message);
        }
    }

    /**
     * Log at WARNING level
     * @param {string} message - Message to log
     */
    warning(message) {
        if (this.level <= LogLevel.WARNING) {
            this._write('WARNING', message);
        }
    }

    /**
     * Log at ERROR level
     * @param {string} message - Message to log
     */
    error(message) {
        if (this.level <= LogLevel.ERROR) {
            this._write('ERROR', message);
        }
    }

    /**
     * Log at CRITICAL level
     * @param {string} message - Message to log
     */
    critical(message) {
        if (this.level <= LogLevel.CRITICAL) {
            this._write('CRITICAL', message);
        }
    }

    /**
     * Check if logging is enabled
     * @returns {boolean} True if logging is enabled
     */
    isEnabled() {
        return this.level < LogLevel.DISABLED;
    }
}

// Global logger instance
const logger = new Logger();

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Loads custom commands from a specified file path
 * @param {string} filePath - Path to the custom commands implementation file
 * @param {string} args - Optional arguments to pass to custom commands
 * @returns {Object} An object containing the custom command implementation
 * @throws {Error} If the file cannot be loaded or doesn't exist
 */
function loadCustomCommands(filePath, args = null) {
    if (!filePath) {
        // Return default implementation if no file specified
        return {
            args: args,
            execute: async (client) => {
                try {
                    await client.set('custom:key', 'custom:value');
                    return true;
                } catch (error) {
                    logger.error(`Custom command error: ${error}`);
                    return false;
                }
            }
        };
    }

    try {
        const absolutePath = path.resolve(filePath);
        if (!fs.existsSync(absolutePath)) {
            console.error(`Custom command file not found: ${absolutePath}`);
            process.exit(1);
        }
        const customModule = require(absolutePath);

        // If the module exports a class or constructor, instantiate it with args
        if (typeof customModule === 'function') {
            return new customModule(args);
        }
        // If the module exports a createCustomCommands factory function, call it with args
        if (typeof customModule.createCustomCommands === 'function') {
            return customModule.createCustomCommands(args);
        }
        // If the module exports an object with an init method, call it with args
        if (customModule.init && typeof customModule.init === 'function') {
            customModule.init(args);
        }
        // Store args on the module for access if needed
        customModule.args = args;
        return customModule;
    } catch (error) {
        console.error('Error loading custom command file:', error);
        process.exit(1);
    }
}

/**
 * Generates random string data of specified size
 * @param {number} size - The size of random data to generate in bytes
 * @returns {string} Random string of specified length
 */
function generateRandomData(size) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    let result = '';
    for (let i = 0; i < size; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

/**
 * Generates a random key within the specified keyspace
 * @param {number} keyspace - The range for key generation
 * @param {number} offset - Starting point for keyspace range (default: 0)
 * @returns {string} Generated key in format 'key:{number}'
 */
function getRandomKey(keyspace, offset = 0) {
    return `key:${offset + Math.floor(Math.random() * keyspace)}`;
}


// ============================================================================
// QPS Controller Class
// ============================================================================

/**
 * Controls and manages the rate of requests (Queries Per Second)
 * Supports both static and dynamic QPS adjustments
 * Supports both linear and exponential ramp modes
 */
class QPSController {
    /**
     * @param {Object} config - Configuration object
     * @param {number} config.startQps - Initial QPS rate
     * @param {number} config.qps - Target QPS rate
     * @param {number} config.endQps - Final QPS rate for dynamic adjustment
     * @param {number} config.qpsChangeInterval - Interval for QPS changes
     * @param {number} config.qpsChange - Amount to change QPS by each interval (linear mode)
     * @param {string} config.qpsRampMode - 'linear' or 'exponential' (default: linear)
     */
    constructor(config) {
        this.config = config;
        this.lastUpdate = Date.now();
        this.requestsThisSecond = 0;
        this.secondStart = Date.now();
        this.exponentialMultiplier = 1.0;
        
        const qpsRampMode = config.qpsRampMode || 'linear';
        const startQps = config.startQps || 0;
        const endQps = config.endQps || 0;
        const qps = config.qps || 0;
        const qpsChangeInterval = config.qpsChangeInterval || 0;

        // Determine initial QPS: use startQps if set, otherwise fall back to qps or endQps
        if (startQps > 0) {
            this.currentQps = startQps;
        } else if (qps > 0) {
            this.currentQps = qps;
        } else if (endQps > 0) {
            // For ramp-up modes without startQps, use endQps as initial value
            this.currentQps = endQps;
        } else {
            this.currentQps = 0;
        }

        // Store effective startQps for later use in throttle
        this._effectiveStartQps = startQps > 0 ? startQps : endQps;
        
        // For exponential mode, set the multiplier (validation done in main())
        if (qpsRampMode === 'exponential') {
            this.exponentialMultiplier = config.qpsRampFactor || 1.0;
        }
    }

    /**
     * Throttles requests to maintain desired QPS rate
     * Implements dynamic QPS adjustment if configured
     * Supports both linear and exponential ramp modes
     * @returns {Promise<void>}
     */
    async throttle() {
        if (this.currentQps <= 0) return;

        const now = Date.now();
        const elapsedSinceLastUpdate = (now - this.lastUpdate) / 1000;
        
        const qpsRampMode = this.config.qpsRampMode || 'linear';
        const isExponential = qpsRampMode === 'exponential';
        
        let hasDynamicQps = this.config.startQps > 0 && this.config.endQps > 0 && 
                           this.config.qpsChangeInterval > 0;
        
        // For linear mode, also require qpsChange
        if (!isExponential) {
            hasDynamicQps = hasDynamicQps && this.config.qpsChange !== 0;
        }

        // Handle dynamic QPS adjustment
        if (hasDynamicQps) {
            if (elapsedSinceLastUpdate >= this.config.qpsChangeInterval) {
                if (isExponential) {
                    // Exponential mode: multiply by the computed multiplier
                    let newQps = Math.round(this.currentQps * this.exponentialMultiplier);
                    
                    // Clamp to endQps
                    if (this.config.endQps > this.config.startQps) {
                        // Increasing QPS
                        if (newQps > this.config.endQps) {
                            newQps = this.config.endQps;
                        }
                    } else {
                        // Decreasing QPS
                        if (newQps < this.config.endQps) {
                            newQps = this.config.endQps;
                        }
                    }
                    this.currentQps = newQps;
                } else {
                    // Linear mode: add qpsChange
                    const diff = this.config.endQps - this.currentQps;
                    if ((diff > 0 && this.config.qpsChange > 0) ||
                        (diff < 0 && this.config.qpsChange < 0)) {
                        this.currentQps += this.config.qpsChange;
                        if ((this.config.qpsChange > 0 && this.currentQps > this.config.endQps) ||
                            (this.config.qpsChange < 0 && this.currentQps < this.config.endQps)) {
                            this.currentQps = this.config.endQps;
                        }
                    }
                }
                this.lastUpdate = now;
            }
        }

        // Implement QPS throttling
        const elapsedThisSecond = (now - this.secondStart) / 1000;
        if (elapsedThisSecond >= 1) {
            this.requestsThisSecond = 0;
            this.secondStart = now;
        }

        if (this.requestsThisSecond >= this.currentQps) {
            const waitTime = 1000 - (now - this.secondStart);
            if (waitTime > 0) {
                await new Promise(resolve => setTimeout(resolve, waitTime));
            }
            this.requestsThisSecond = 0;
            this.secondStart = Date.now();
        }

        this.requestsThisSecond++;
    }
}

// ============================================================================
// Benchmark Statistics Class
// ============================================================================

/**
 * Tracks and manages benchmark statistics and metrics
 * Handles latency measurements, error tracking, and progress reporting
 */
class BenchmarkStats {
    /**
     * @param {number|null} csvIntervalSec - CSV emission interval in seconds
     * @param {number} workerId - Worker ID for multi-process mode (-1 for single process)
     * @param {boolean} isMultiProcess - Whether running in multi-process mode
     */
    constructor(csvIntervalSec = null, workerId = -1, isMultiProcess = false) {
        this.startTime = Date.now();
        this.requestsCompleted = 0;
        this.errors = 0;
        this.lastPrint = Date.now();
        this.lastRequests = 0;
        this.lastWindowTime = Date.now();
        this.windowSize = 1000; // 1 second window

        // Multi-process support
        this.workerId = workerId;
        this.isMultiProcess = isMultiProcess;

        // HDR Histogram configuration: track latencies in microseconds
        // Range: 1 microsecond to 60 seconds (60,000,000 microseconds)
        const histogramOptions = {
            lowestDiscernibleValue: 1,
            highestTrackableValue: 60000000,
            numberOfSignificantValueDigits: 3
        };

        // Overall latency histogram for final stats
        this.latencyHistogram = hdr.build(histogramOptions);
        // Current window histogram for real-time progress display
        this.windowHistogram = hdr.build(histogramOptions);

        // CSV interval metrics tracking
        this.csvIntervalSec = csvIntervalSec;
        this.csvMode = csvIntervalSec !== null && csvIntervalSec !== undefined;
        this.intervalStartTime = Date.now();
        this.intervalHistogram = hdr.build(histogramOptions);
        this.intervalErrors = 0;
        this.intervalMoved = 0;
        this.intervalClusterdown = 0;
        this.intervalDisconnects = 0;
        this.intervalRequests = 0;
        this.csvHeaderPrinted = false;

        // Extended metrics tracking (for --extended-metrics flag)
        this.extendedMetrics = false;

        // Error type counters (total and per-interval)
        this.timeoutErrors = 0;
        this.connectionErrors = 0;
        this.busyErrors = 0;      // Server busy (Lua script running)
        this.oomErrors = 0;       // Server out of memory
        this.loadingErrors = 0;   // Server loading dataset
        this.intervalTimeoutErrors = 0;
        this.intervalConnectionErrors = 0;
        this.intervalBusyErrors = 0;
        this.intervalOomErrors = 0;
        this.intervalLoadingErrors = 0;

        // Cache hit/miss tracking (for GET commands)
        this.cacheHits = 0;
        this.cacheMisses = 0;
        this.intervalCacheHits = 0;
        this.intervalCacheMisses = 0;

        // Connection establishment time tracking using HDR histogram (for percentiles)
        // Histogram stores values in microseconds for precision
        this.connectionTimeHistogram = hdr.build({
            lowestDiscernibleValue: 1,
            highestTrackableValue: 60000000,  // Up to 60 seconds
            numberOfSignificantValueDigits: 3
        });
    }
    /**
     * Records a latency measurement and updates statistics
     * @param {number} latency - Latency measurement in milliseconds
     */
    addLatency(latency) {
        // Convert milliseconds to microseconds for HDR histogram
        const latencyUsec = Math.max(1, Math.floor(latency * 1000));
        this.latencyHistogram.recordValue(latencyUsec);
        this.windowHistogram.recordValue(latencyUsec);
        this.requestsCompleted++;

        if (this.csvMode) {
            this.intervalHistogram.recordValue(latencyUsec);
            this.intervalRequests++;
            this.checkCsvInterval();
        } else if (this.isMultiProcess) {
            this.sendProgressMetrics();
        } else {
            this.printProgress();
        }
    }
    
        /**
         * Increments the error counter
         */
        addError() {
            this.errors++;
            if (this.csvMode) {
                this.intervalErrors++;
            }
        }
        
        /**
         * Increments the MOVED response counter
         */
        addMoved() {
            if (this.csvMode) {
                this.intervalMoved++;
            }
        }
        
        /**
         * Increments the CLUSTERDOWN response counter
         */
        addClusterdown() {
            if (this.csvMode) {
                this.intervalClusterdown++;
            }
        }
        
        /**
         * Increments the client disconnect counter
         */
        addDisconnect() {
            if (this.csvMode) {
                this.intervalDisconnects++;
            }
        }

        /**
         * Increments the timeout error counter
         */
        addTimeoutError() {
            this.timeoutErrors++;
            if (this.csvMode) {
                this.intervalTimeoutErrors++;
            }
        }

        /**
         * Increments the connection error counter
         */
        addConnectionError() {
            this.connectionErrors++;
            if (this.csvMode) {
                this.intervalConnectionErrors++;
            }
        }

        /**
         * Increments the BUSY error counter (server busy with Lua script)
         */
        addBusyError() {
            this.busyErrors++;
            if (this.csvMode) {
                this.intervalBusyErrors++;
            }
        }

        /**
         * Increments the OOM error counter (server out of memory)
         */
        addOomError() {
            this.oomErrors++;
            if (this.csvMode) {
                this.intervalOomErrors++;
            }
        }

        /**
         * Increments the LOADING error counter (server loading dataset)
         */
        addLoadingError() {
            this.loadingErrors++;
            if (this.csvMode) {
                this.intervalLoadingErrors++;
            }
        }

        /**
         * Increments the cache hit counter (GET returned a value)
         */
        addCacheHit() {
            this.cacheHits++;
            if (this.csvMode) {
                this.intervalCacheHits++;
            }
        }

        /**
         * Increments the cache miss counter (GET returned null)
         */
        addCacheMiss() {
            this.cacheMisses++;
            if (this.csvMode) {
                this.intervalCacheMisses++;
            }
        }

        /**
         * Records a connection establishment time using HDR histogram
         * @param {number} timeMs - Connection time in milliseconds
         */
        addConnectionTime(timeMs) {
            // Store in microseconds for precision
            const timeUsec = Math.max(1, Math.floor(timeMs * 1000));
            this.connectionTimeHistogram.recordValue(timeUsec);
        }

        /**
         * Get connection time percentiles in milliseconds
         * @returns {Object} {p50, p99, p100} in milliseconds
         */
        getConnectionTimePercentiles() {
            if (this.connectionTimeHistogram.totalCount === 0) {
                return { p50: 0, p99: 0, p100: 0 };
            }
            return {
                p50: this.connectionTimeHistogram.getValueAtPercentile(50) / 1000,
                p99: this.connectionTimeHistogram.getValueAtPercentile(99) / 1000,
                p100: this.connectionTimeHistogram.maxValue / 1000
            };
        }

        /**
         * Prints CSV header line (once at start)
         * Base: 16 columns
         * Extended (+12): cache_hits, cache_misses, timeout_errors, connection_errors,
         *                 busy_errors, oom_errors, loading_errors, keyspace_count,
         *                 replication_lag, conn_est_p50, conn_est_p99, conn_est_p100
         */
        printCsvHeader() {
            if (!this.csvHeaderPrinted) {
                let header = "timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,request_finished,requests_total_failed,requests_moved,requests_clusterdown,client_disconnects";
                if (this.extendedMetrics) {
                    // 12 additional columns for extended metrics
                    header += ",cache_hits,cache_misses,timeout_errors,connection_errors,busy_errors,oom_errors,loading_errors,keyspace_count,replication_lag,conn_est_p50,conn_est_p99,conn_est_p100";
                }
                console.log(header);
                this.csvHeaderPrinted = true;
            }
        }

        /**
         * Emit a CSV data line for the current interval
         */
        emitCsvLine() {
            const now = Date.now();
            const intervalDuration = (now - this.intervalStartTime) / 1000; // in seconds

            // Calculate timestamp (Unix epoch seconds)
            const timestamp = Math.floor(now / 1000);

            // Calculate request_sec for this interval
            const requestSec = intervalDuration > 0 ? this.intervalRequests / intervalDuration : 0.0;

            // Get percentiles from HDR histogram (already in microseconds)
            let p50, p90, p95, p99, p99_9, p99_99, p99_999, p100, avg;
            if (this.intervalHistogram.totalCount > 0) {
                p50 = Math.floor(this.intervalHistogram.getValueAtPercentile(50));
                p90 = Math.floor(this.intervalHistogram.getValueAtPercentile(90));
                p95 = Math.floor(this.intervalHistogram.getValueAtPercentile(95));
                p99 = Math.floor(this.intervalHistogram.getValueAtPercentile(99));
                p99_9 = Math.floor(this.intervalHistogram.getValueAtPercentile(99.9));
                p99_99 = Math.floor(this.intervalHistogram.getValueAtPercentile(99.99));
                p99_999 = Math.floor(this.intervalHistogram.getValueAtPercentile(99.999));
                p100 = Math.floor(this.intervalHistogram.maxValue);
                avg = Math.floor(this.intervalHistogram.mean);
            } else {
                p50 = p90 = p95 = p99 = p99_9 = p99_99 = p99_999 = p100 = avg = 0;
            }

            // Output CSV line (16 base fields + optional 12 extended metrics)
            let csvLine = `${timestamp},${requestSec.toFixed(6)},${p50},${p90},${p95},${p99},${p99_9},${p99_99},${p99_999},${p100},${avg},${this.intervalRequests},${this.intervalErrors},${this.intervalMoved},${this.intervalClusterdown},${this.intervalDisconnects}`;
            if (this.extendedMetrics) {
                const connTime = this.getConnectionTimePercentiles();
                // 12 extended columns: cache_hits, cache_misses, timeout_errors, connection_errors,
                // busy_errors, oom_errors, loading_errors, keyspace_count, replication_lag,
                // conn_est_p50, conn_est_p99, conn_est_p100
                // Note: keyspace_count and replication_lag are 0 in single-process mode (monitoring runs in orchestrator)
                csvLine += `,${this.intervalCacheHits},${this.intervalCacheMisses},${this.intervalTimeoutErrors},${this.intervalConnectionErrors},${this.intervalBusyErrors},${this.intervalOomErrors},${this.intervalLoadingErrors},0,0,${connTime.p50.toFixed(2)},${connTime.p99.toFixed(2)},${connTime.p100.toFixed(2)}`;
            }
            console.log(csvLine);

            // Reset interval counters and histogram
            this.intervalStartTime = now;
            this.intervalHistogram.reset();
            this.intervalErrors = 0;
            this.intervalMoved = 0;
            this.intervalClusterdown = 0;
            this.intervalDisconnects = 0;
            this.intervalRequests = 0;
            this.intervalTimeoutErrors = 0;
            this.intervalConnectionErrors = 0;
            this.intervalBusyErrors = 0;
            this.intervalOomErrors = 0;
            this.intervalLoadingErrors = 0;
            this.intervalCacheHits = 0;
            this.intervalCacheMisses = 0;
        }
        
    /**
     * Check if it's time to emit a CSV line
     */
    checkCsvInterval() {
        if (this.csvMode) {
            const now = Date.now();
            if ((now - this.intervalStartTime) / 1000 >= this.csvIntervalSec) {
                if (this.isMultiProcess) {
                    this.sendCsvIntervalMetrics();
                } else {
                    this.emitCsvLine();
                }
            }
        }
    }

    /**
     * Send progress metrics to orchestrator in multi-process mode
     */
    sendProgressMetrics() {
        if (!this.isMultiProcess) return;

        const now = Date.now();
        if (now - this.lastPrint >= 1000) { // Send every second
            // Encode window histogram for IPC
            const windowHistogramData = hdr.encodeIntoCompressedBase64(this.windowHistogram);

            process.send({
                type: 'progress',
                workerId: this.workerId,
                requestsCompleted: this.requestsCompleted,
                errors: this.errors,
                windowHistogramData: windowHistogramData,
                windowCount: this.windowHistogram.totalCount,
                timestamp: now
            });

            // Reset window histogram
            this.windowHistogram.reset();
            this.lastPrint = now;
            this.lastRequests = this.requestsCompleted;
        }
    }

    /**
     * Send CSV interval metrics to orchestrator in multi-process mode
     */
    sendCsvIntervalMetrics() {
        if (!this.isMultiProcess) return;

        const now = Date.now();
        const intervalDuration = (now - this.intervalStartTime) / 1000;

        // Encode interval histogram for IPC
        const intervalHistogramData = hdr.encodeIntoCompressedBase64(this.intervalHistogram);

        // Encode connection time histogram for IPC
        const connTimeHistogramData = hdr.encodeIntoCompressedBase64(this.connectionTimeHistogram);

        process.send({
            type: 'csv_interval',
            workerId: this.workerId,
            timestamp: Math.floor(now / 1000),
            intervalDuration: intervalDuration,
            intervalHistogramData: intervalHistogramData,
            intervalRequests: this.intervalRequests,
            intervalErrors: this.intervalErrors,
            intervalMoved: this.intervalMoved,
            intervalClusterdown: this.intervalClusterdown,
            intervalDisconnects: this.intervalDisconnects,
            // Extended metrics
            intervalTimeoutErrors: this.intervalTimeoutErrors,
            intervalConnectionErrors: this.intervalConnectionErrors,
            intervalBusyErrors: this.intervalBusyErrors,
            intervalOomErrors: this.intervalOomErrors,
            intervalLoadingErrors: this.intervalLoadingErrors,
            intervalCacheHits: this.intervalCacheHits,
            intervalCacheMisses: this.intervalCacheMisses,
            connTimeHistogramData: connTimeHistogramData
        });

        // Reset interval counters and histogram
        this.intervalStartTime = now;
        this.intervalHistogram.reset();
        this.intervalErrors = 0;
        this.intervalMoved = 0;
        this.intervalClusterdown = 0;
        this.intervalDisconnects = 0;
        this.intervalRequests = 0;
        this.intervalTimeoutErrors = 0;
        this.intervalConnectionErrors = 0;
        this.intervalBusyErrors = 0;
        this.intervalOomErrors = 0;
        this.intervalLoadingErrors = 0;
        this.intervalCacheHits = 0;
        this.intervalCacheMisses = 0;
    }

    /**
     * Send final metrics to orchestrator at the end of benchmark
     */
    sendFinalMetrics() {
        if (!this.isMultiProcess) return;

        // Export histogram data for aggregation
        const histogramData = hdr.encodeIntoCompressedBase64(this.latencyHistogram);
        const connTimeHistogramData = hdr.encodeIntoCompressedBase64(this.connectionTimeHistogram);

        process.send({
            type: 'final',
            workerId: this.workerId,
            requestsCompleted: this.requestsCompleted,
            errors: this.errors,
            histogramData: histogramData,
            totalTime: (Date.now() - this.startTime) / 1000,
            // Extended metrics for aggregation
            timeoutErrors: this.timeoutErrors,
            connectionErrors: this.connectionErrors,
            busyErrors: this.busyErrors,
            oomErrors: this.oomErrors,
            loadingErrors: this.loadingErrors,
            cacheHits: this.cacheHits,
            cacheMisses: this.cacheMisses,
            connTimeHistogramData: connTimeHistogramData
        });
    }

    /**
     * Calculates statistical metrics from an HDR histogram
     * @param {Object} histogram - HDR histogram instance
     * @returns {Object|null} Statistical metrics including min, max, avg, and percentiles (in ms)
     */
    calculateLatencyStats(histogram) {
        if (histogram.totalCount === 0) return null;

        // Convert from microseconds back to milliseconds for display
        return {
            min: histogram.minNonZeroValue / 1000,
            max: histogram.maxValue / 1000,
            avg: histogram.mean / 1000,
            p50: histogram.getValueAtPercentile(50) / 1000,
            p95: histogram.getValueAtPercentile(95) / 1000,
            p99: histogram.getValueAtPercentile(99) / 1000
        };
    }

    /**
     * Prints current progress and real-time statistics
     * Updates once per second
     */
    printProgress() {
        const now = Date.now();
        if (now - this.lastPrint >= 1000) {
            const intervalRequests = this.requestsCompleted - this.lastRequests;
            const currentRps = intervalRequests;
            const overallRps = this.requestsCompleted / ((now - this.startTime) / 1000);

            const windowStats = this.calculateLatencyStats(this.windowHistogram);

            process.stdout.write('\r\x1b[K');

            let output = `Progress: ${this.requestsCompleted} requests, ` +
                        `Current RPS: ${currentRps.toFixed(2)}, ` +
                        `Overall RPS: ${overallRps.toFixed(2)}, ` +
                        `Errors: ${this.errors}`;

            if (windowStats) {
                output += ` | Latencies (ms) - ` +
                         `Avg: ${windowStats.avg.toFixed(4)}, ` +
                         `p50: ${windowStats.p50.toFixed(4)}, ` +
                         `p99: ${windowStats.p99.toFixed(4)}`;
            }

            process.stdout.write(output);

            // Reset window histogram for next interval
            this.windowHistogram.reset();
            this.lastPrint = now;
            this.lastRequests = this.requestsCompleted;
        }
    }

    /**
     * Prints final benchmark results and detailed statistics
     */
    printFinalStats() {
        const totalTime = (Date.now() - this.startTime) / 1000;
        const finalRps = this.requestsCompleted / totalTime;

        // Calculate final latency stats from histogram
        const finalStats = this.calculateLatencyStats(this.latencyHistogram);

        console.log('\n\nFinal Results:');
        console.log('=============');
        console.log(`Total time: ${totalTime.toFixed(2)} seconds`);
        console.log(`Requests completed: ${this.requestsCompleted}`);
        console.log(`Requests per second: ${finalRps.toFixed(2)}`);
        console.log(`Total errors: ${this.errors}`);

        // Error breakdown (always shown if there are errors)
        if (this.errors > 0) {
            console.log('\nError Breakdown:');
            console.log('================');
            console.log(`Timeout errors: ${this.timeoutErrors}`);
            console.log(`Connection errors: ${this.connectionErrors}`);
            console.log(`BUSY errors: ${this.busyErrors}`);
            console.log(`OOM errors: ${this.oomErrors}`);
            console.log(`LOADING errors: ${this.loadingErrors}`);
        }

        // Cache hit rate (shown when GET command was used with extended metrics)
        const totalCacheOps = this.cacheHits + this.cacheMisses;
        if (totalCacheOps > 0) {
            const hitRate = (this.cacheHits / totalCacheOps * 100).toFixed(2);
            console.log('\nCache Statistics:');
            console.log('================');
            console.log(`Cache hits: ${this.cacheHits}`);
            console.log(`Cache misses: ${this.cacheMisses}`);
            console.log(`Hit rate: ${hitRate}%`);
        }

        // Connection establishment time (shown when extended metrics enabled)
        if (this.connectionTimeHistogram.totalCount > 0) {
            const connTime = this.getConnectionTimePercentiles();
            console.log('\nConnection Establishment Time (ms):');
            console.log('==================================');
            console.log(`p50: ${connTime.p50.toFixed(2)}`);
            console.log(`p99: ${connTime.p99.toFixed(2)}`);
            console.log(`p100 (max): ${connTime.p100.toFixed(2)}`);
            console.log(`Connections: ${this.connectionTimeHistogram.totalCount}`);
        }

        if (finalStats) {
            console.log('\nLatency Statistics (ms):');
            console.log('=====================');
            console.log(`Minimum: ${finalStats.min.toFixed(3)}`);
            console.log(`Average: ${finalStats.avg.toFixed(3)}`);
            console.log(`Maximum: ${finalStats.max.toFixed(3)}`);
            console.log(`Median (p50): ${finalStats.p50.toFixed(3)}`);
            console.log(`95th percentile: ${finalStats.p95.toFixed(3)}`);
            console.log(`99th percentile: ${finalStats.p99.toFixed(3)}`);

            // Latency distribution using percentile buckets
            console.log('\nLatency Distribution:');
            console.log('====================');
            const percentiles = [50, 75, 90, 95, 99, 99.9, 99.99, 100];
            for (const p of percentiles) {
                const valueMs = this.latencyHistogram.getValueAtPercentile(p) / 1000;
                console.log(`${p}% <= ${valueMs.toFixed(3)} ms`);
            }
        }
    }
}

// ============================================================================
// Main Benchmark Function
// ============================================================================

/**
 * Executes the benchmark with specified configuration
 * @param {Object} config - Benchmark configuration parameters
 * @param {number} workerId - Worker ID for multi-process mode (-1 for single process)
 * @param {boolean} isMultiProcess - Whether running in multi-process mode
 * @returns {Promise<void>}
 */
async function runBenchmark(config, workerId = -1, isMultiProcess = false) {
    const stats = new BenchmarkStats(config.csvIntervalSec, workerId, isMultiProcess);
    stats.extendedMetrics = config.extendedMetrics || false;
    const qpsController = new QPSController(config);

    const workerPrefix = isMultiProcess ? `[Worker ${workerId}] ` : '';
    logger.debug(`${workerPrefix}Starting benchmark run`);
    logger.debug(`${workerPrefix}Config: command=${config.command}, requests=${config.totalRequests}, poolSize=${config.poolSize}`);

    // Shutdown flag for graceful termination
    let shutdownRequested = false;
    if (isMultiProcess) {
        process.on('message', (msg) => {
            if (msg.type === 'shutdown') {
                logger.info(`${workerPrefix}Received shutdown signal`);
                shutdownRequested = true;
            }
        });
    }

    // Only print banner if not in CSV mode and not in multi-process worker mode
    if (!stats.csvMode && !isMultiProcess) {
        console.log('Valkey Benchmark');
        console.log(`Host: ${config.host}`);
        console.log(`Port: ${config.port}`);
        console.log(`Threads: ${config.numThreads}`);
        console.log(`Total Requests: ${config.totalRequests}`);
        console.log(`Data Size: ${config.dataSize}`);
        console.log(`Command: ${config.command}`);
        console.log(`Is Cluster: ${config.isCluster}`);
        console.log(`Read from Replica: ${config.readFromReplica}`);
        console.log(`Use TLS: ${config.useTls}`);
        if (config.clientsRampStart > 0 && config.clientsRampEnd > 0) {
            if (config.clientRampMode === 'exponential') {
                console.log(`Client Ramp: ${config.clientsRampStart} to ${config.clientsRampEnd} clients (exponential, factor ${config.clientRampFactor}x every ${config.clientRampInterval}s)`);
            } else {
                console.log(`Client Ramp: ${config.clientsRampStart} to ${config.clientsRampEnd} clients (linear, +${config.clientsPerRamp} every ${config.clientRampInterval}s)`);
            }
        }
        console.log();
    } else if (stats.csvMode && !isMultiProcess) {
        // In CSV mode (single-process), print header to stdout
        stats.printCsvHeader();
    }

    // Helper function to create a single client
    async function createClient() {
        const clientConfig = {
            addresses: [{
                host: config.host,
                port: config.port
            }],
            useTLS: config.useTls,
            readFrom: config.readFromReplica ? 'preferReplica' : 'primary'
        };

        if (config.requestTimeout !== undefined) {
            clientConfig.requestTimeout = config.requestTimeout;
        }

        // Add advanced configuration for connection timeout
        if (config.connectionTimeout !== undefined) {
            clientConfig.advancedConfiguration = {
                connectionTimeout: config.connectionTimeout
            };
        }

        return config.isCluster
            ? await GlideClusterClient.createClient(clientConfig)
            : await GlideClient.createClient(clientConfig);
    }

    // Client ramp-up configuration
    const rampEnabled = config.clientsRampStart > 0 && config.clientsRampEnd > 0;
    const initialClientCount = rampEnabled ? config.clientsRampStart : config.poolSize;

    // Create initial client pool
    logger.info(`${workerPrefix}Creating ${initialClientCount} client connections to ${config.host}:${config.port}`);
    const clientPool = [];
    for (let i = 0; i < initialClientCount; i++) {
        const connStart = performance.now();
        const client = await createClient();
        const connTime = performance.now() - connStart;
        if (stats.extendedMetrics) {
            stats.addConnectionTime(connTime);
        }
        clientPool.push(client);
        logger.debug(`${workerPrefix}Created client ${i + 1}/${initialClientCount} in ${connTime.toFixed(2)}ms`);
    }
    logger.info(`${workerPrefix}Client pool created successfully`);

    // Create worker promises
    const workers = Array(config.numThreads).fill().map(async (_, threadId) => {
        const data = config.command === 'set' ? generateRandomData(config.dataSize) : null;
        let running = true;

        // Generate random starting offset if sequential-random-start is enabled
        let sequentialOffset = 0;
        if (config.useSequential && config.sequentialRandomStart) {
            sequentialOffset = Math.floor(Math.random() * config.sequentialKeyspacelen);
        }

        if (config.testDuration > 0) {
            setTimeout(() => {
                running = false;
            }, config.testDuration * 1000);
        }

        while (running && !shutdownRequested && (config.testDuration > 0 || stats.requestsCompleted < config.totalRequests)) {
            // Use current pool size to handle growing pool during ramp-up
            const poolSize = clientPool.length;
            if (poolSize === 0) {
                // Safety check: wait for initial clients
                await new Promise(resolve => setTimeout(resolve, 10));
                continue;
            }
            const clientIndex = stats.requestsCompleted % poolSize;
            const client = clientPool[clientIndex];
            await qpsController.throttle();

            const start = performance.now();
            try {
                if (config.command === 'set') {
                    const key = config.useSequential
                        ? `key:${config.keyspaceOffset + (sequentialOffset + stats.requestsCompleted) % config.sequentialKeyspacelen}`
                        : config.randomKeyspace > 0
                            ? getRandomKey(config.randomKeyspace, config.keyspaceOffset)
                            : `key:${threadId}:${stats.requestsCompleted}`;

                    await client.set(key, data);
                } else if (config.command === 'get') {
                    const key = config.useSequential
                        ? `key:${config.keyspaceOffset + (sequentialOffset + stats.requestsCompleted) % config.sequentialKeyspacelen}`
                        : config.randomKeyspace > 0
                            ? getRandomKey(config.randomKeyspace, config.keyspaceOffset)
                            : `key:${threadId}:${stats.requestsCompleted}`;
                    const result = await client.get(key);
                    if (stats.extendedMetrics) {
                        if (result !== null) {
                            stats.addCacheHit();
                        } else {
                            stats.addCacheMiss();
                        }
                    }
                } else if (config.command === 'custom') {
                    await config.customCommands.execute(client);
                }
                
                const latency = performance.now() - start;
                stats.addLatency(latency);
            } catch (error) {
                const errorMsg = error.toString().toUpperCase();

                // Timeout error detection
                if (error instanceof TimeoutError || errorMsg.includes('TIMEOUT')) {
                    stats.addTimeoutError();
                }

                // Connection error detection
                if (error instanceof ConnectionError || error instanceof ClosingError ||
                    errorMsg.includes('ECONNREFUSED') || errorMsg.includes('ECONNRESET') ||
                    errorMsg.includes('ETIMEDOUT') || errorMsg.includes('CONNECTION')) {
                    stats.addConnectionError();
                    stats.addDisconnect();
                }

                // Server state error detection (separate counters for each type)
                if (errorMsg.includes('BUSY')) {
                    stats.addBusyError();
                }
                if (errorMsg.includes('OOM')) {
                    stats.addOomError();
                }
                if (errorMsg.includes('LOADING')) {
                    stats.addLoadingError();
                }

                // Existing MOVED/CLUSTERDOWN detection
                if (errorMsg.includes('MOVED')) {
                    stats.addMoved();
                } else if (errorMsg.includes('CLUSTERDOWN')) {
                    stats.addClusterdown();
                }

                stats.addError();

                // In CSV mode, we still need to check if it's time to emit a line
                // even when there are only errors
                if (stats.csvMode) {
                    stats.checkCsvInterval();
                } else {
                    console.error(`Error in thread ${threadId}:`, error);
                }
            }
        }
    });

    // Client ramp-up function
    async function rampUpClients() {
        if (!rampEnabled) return;

        let currentClients = config.clientsRampStart;
        const targetClients = config.clientsRampEnd;
        const rampInterval = config.clientRampInterval * 1000; // Convert to ms
        const isExponential = config.clientRampMode === 'exponential';

        while (currentClients < targetClients && !shutdownRequested) {
            await new Promise(resolve => setTimeout(resolve, rampInterval));

            if (shutdownRequested) break;

            // Calculate new client target based on mode
            let newClientTarget;
            if (isExponential) {
                // Exponential mode: multiply by factor
                newClientTarget = Math.round(currentClients * config.clientRampFactor);
            } else {
                // Linear mode: add fixed amount
                newClientTarget = currentClients + config.clientsPerRamp;
            }
            // Clamp to target
            newClientTarget = Math.min(newClientTarget, targetClients);
            const batchSize = newClientTarget - currentClients;

            // Create batch of clients
            for (let i = 0; i < batchSize && !shutdownRequested; i++) {
                const connStart = performance.now();
                const client = await createClient();
                const connTime = performance.now() - connStart;
                if (stats.extendedMetrics) {
                    stats.addConnectionTime(connTime);
                }
                clientPool.push(client);
                currentClients++;
            }
        }
    }

    // Run workers and client ramp-up concurrently
    if (rampEnabled) {
        await Promise.all([...workers, rampUpClients()]);
    } else {
        await Promise.all(workers);
    }

    // Handle end-of-benchmark metrics
    if (isMultiProcess) {
        // Send any remaining CSV interval metrics
        if (stats.csvMode && (stats.intervalRequests > 0 || stats.intervalErrors > 0)) {
            stats.sendCsvIntervalMetrics();
        }
        // Send final metrics to orchestrator
        stats.sendFinalMetrics();
    } else {
        // Single-process mode
        // Emit final CSV line if in CSV mode and there's any data or errors
        if (stats.csvMode && (stats.intervalHistogram.totalCount > 0 || stats.intervalErrors > 0 ||
                              stats.intervalMoved > 0 || stats.intervalClusterdown > 0)) {
            stats.emitCsvLine();
        }

        // Only print final stats if not in CSV mode
        if (!stats.csvMode) {
            stats.printFinalStats();
        }
    }

    // Close all clients
    logger.info(`${workerPrefix}Closing ${clientPool.length} client connections`);
    for (const client of clientPool) {
        await client.close();
    }
    logger.debug(`${workerPrefix}Benchmark run complete`);
}

// ============================================================================
// Command Line Parser
// ============================================================================

/**
 * Parses and validates command line arguments
 * @returns {Object} Parsed command line arguments
 */
function parseCommandLine() {
    return yargs(hideBin(process.argv))
        .option('h', {
            alias: 'host',
            describe: 'Server hostname',
            default: '127.0.0.1',
            type: 'string'
        })
        .option('p', {
            alias: 'port',
            describe: 'Server port',
            default: 6379,
            type: 'number'
        })
        .option('c', {
            alias: 'clients',
            describe: 'Number of parallel connections',
            default: 50,
            type: 'number'
        })
        .option('n', {
            alias: 'requests',
            describe: 'Total number of requests',
            default: 100000,
            type: 'number'
        })
        .option('d', {
            alias: 'datasize',
            describe: 'Data size of value in bytes for SET',
            default: 3,
            type: 'number'
        })
        .option('t', {
            alias: 'type',
            describe: 'Command to benchmark, set, get, or custom',
            default: 'set',
            type: 'string'
        })
        .option('r', {
            alias: 'random',
            describe: 'Use random keys from 0 to keyspacelen-1',
            default: 0,
            type: 'number'
        })
        .option('threads', {
            describe: 'Number of worker threads',
            default: 1,
            type: 'number'
        })
        .option('test-duration', {
            describe: 'Test duration in seconds',
            type: 'number'
        })
        .option('sequential', {
            describe: 'Use sequential keys',
            type: 'number'
        })
        .option('sequential-random-start', {
            describe: 'Start each process/thread at a random offset in sequential keyspace (requires --sequential)',
            type: 'boolean',
            default: false
        })
        .option('keyspace-offset', {
            describe: 'Starting point for keyspace range (default: 0). Works with both -r/--random and --sequential',
            type: 'number',
            default: 0
        })
        .option('qps', {
            describe: 'Queries per second limit',
            type: 'number'
        })
        .option('start-qps', {
            describe: 'Starting QPS for dynamic rate',
            type: 'number'
        })
        .option('end-qps', {
            describe: 'Ending QPS for dynamic rate',
            type: 'number'
        })
        .option('qps-change-interval', {
            describe: 'Interval for QPS changes in seconds',
            type: 'number'
        })
        .option('qps-change', {
            describe: 'QPS change amount per interval (linear mode only)',
            type: 'number'
        })
        .option('qps-ramp-mode', {
            describe: 'QPS ramp mode: linear or exponential (default: linear)',
            type: 'string',
            default: 'linear',
            choices: ['linear', 'exponential']
        })
        .option('qps-ramp-factor', {
            describe: 'Explicit multiplier for exponential QPS ramp (e.g., 2.0 to double QPS each interval). If not provided, factor is auto-calculated.',
            type: 'number'
        })
        .option('tls', {
            describe: 'Use TLS connection',
            type: 'boolean',
            default: false
        })
        .option('cluster', {
            describe: 'Use cluster client',
            type: 'boolean',
            default: false
        })
        .option('read-from-replica', {
            describe: 'Read from replica nodes',
            type: 'boolean',
            default: false
        })
        .option('request-timeout', {
            describe: 'Request timeout in milliseconds',
            type: 'number'
        })
        .option('connection-timeout', {
            describe: 'Connection timeout in milliseconds (TCP/TLS establishment)',
            type: 'number'
        })
        .option('clients-ramp-start', {
            describe: 'Initial number of clients per process for ramp-up. Must be used with --clients-ramp-end, --clients-per-ramp, and --client-ramp-interval. Mutually exclusive with --clients',
            type: 'number'
        })
        .option('clients-ramp-end', {
            describe: 'Target number of clients per process at end of ramp-up. Must be used with --clients-ramp-start, --clients-per-ramp, and --client-ramp-interval. Mutually exclusive with --clients',
            type: 'number'
        })
        .option('clients-per-ramp', {
            describe: 'Number of clients to add per ramp step. Must be used with --clients-ramp-start, --clients-ramp-end, and --client-ramp-interval. Mutually exclusive with --clients',
            type: 'number'
        })
        .option('client-ramp-interval', {
            describe: 'Time interval in seconds between client ramp steps. Must be used with --clients-ramp-start, --clients-ramp-end, and --clients-per-ramp (linear) or --client-ramp-factor (exponential). Mutually exclusive with --clients',
            type: 'number'
        })
        .option('client-ramp-mode', {
            describe: 'Client ramp mode: linear or exponential (default: linear)',
            type: 'string',
            default: 'linear',
            choices: ['linear', 'exponential']
        })
        .option('client-ramp-factor', {
            describe: 'Multiplier for exponential client ramp (e.g., 2.0 to double clients each interval). Required for exponential mode.',
            type: 'number'
        })
        .option('custom-command-file', {
            describe: 'Path to custom command implementation file',
            type: 'string'
        })
        .option('custom-command-args', {
            describe: 'Arguments to pass to custom command as a single string (e.g., "key1=value1,key2=value2")',
            type: 'string'
        })
        .option('debug', {
            describe: 'Enable debug logging (equivalent to --log-level DEBUG)',
            type: 'boolean',
            default: false
        })
        .option('log-level', {
            describe: 'Set logging level (default: logging disabled for performance)',
            type: 'string',
            choices: ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        })
        .option('interval-metrics-interval-duration-sec', {
            describe: 'Emit CSV metrics every N seconds (enables CSV output mode)',
            type: 'number'
        })
        .option('extended-metrics', {
            describe: 'Enable extended metrics: 12 additional CSV columns including cache hits/misses, error types, keyspace count, replication lag, and connection time percentiles',
            type: 'boolean',
            default: false
        })
        .option('monitoring-interval', {
            describe: 'Interval in seconds for keyspace and replication monitoring (default: 5). Only used with --extended-metrics in multi-process mode.',
            type: 'number',
            default: 5
        })
        .option('processes', {
            describe: 'Number of worker processes (auto = CPU count)',
            type: 'string',
            default: 'auto'
        })
        .option('single-process', {
            describe: 'Force single-process mode (legacy behavior)',
            type: 'boolean',
            default: false
        })
        .help()
        .argv;
}

// ============================================================================
// Multi-Process Orchestration
// ============================================================================

/**
 * Calculate latency statistics from an HDR histogram
 * @param {Object} histogram - HDR histogram instance
 * @returns {Object|null} Statistics object or null if empty (values in ms)
 */
function calculateLatencyStatsFromHistogram(histogram) {
    if (!histogram || histogram.totalCount === 0) return null;

    // Values are stored in microseconds, convert to milliseconds for display
    return {
        min: histogram.minNonZeroValue / 1000,
        max: histogram.maxValue / 1000,
        avg: histogram.mean / 1000,
        p50: histogram.getValueAtPercentile(50) / 1000,
        p95: histogram.getValueAtPercentile(95) / 1000,
        p99: histogram.getValueAtPercentile(99) / 1000
    };
}

/**
 * Aggregate CSV metrics from multiple workers by merging HDR histograms
 * @param {Object[]} workerMetrics - Array of metrics from workers
 * @returns {Object|null} Aggregated metrics with merged histogram or null if empty
 */
function aggregateCsvMetrics(workerMetrics) {
    if (!workerMetrics || workerMetrics.length === 0) return null;

    // Create merged histograms
    const histogramOptions = {
        lowestDiscernibleValue: 1,
        highestTrackableValue: 60000000,
        numberOfSignificantValueDigits: 3
    };
    const mergedHistogram = hdr.build(histogramOptions);
    const mergedConnTimeHistogram = hdr.build({
        lowestDiscernibleValue: 1,
        highestTrackableValue: 60000000,
        numberOfSignificantValueDigits: 3
    });

    let totalRequests = 0;
    let totalErrors = 0;
    let totalMoved = 0;
    let totalClusterdown = 0;
    let totalDisconnects = 0;
    let totalDuration = 0;
    // Extended metrics
    let totalTimeoutErrors = 0;
    let totalConnectionErrors = 0;
    let totalBusyErrors = 0;
    let totalOomErrors = 0;
    let totalLoadingErrors = 0;
    let totalCacheHits = 0;
    let totalCacheMisses = 0;

    let decodeFailures = 0;
    for (const metrics of workerMetrics) {
        // Decode and merge latency histogram from worker
        if (metrics.intervalHistogramData) {
            try {
                const workerHistogram = hdr.decodeFromCompressedBase64(metrics.intervalHistogramData);
                mergedHistogram.add(workerHistogram);
            } catch (e) {
                decodeFailures++;
                process.stderr.write(`Warning: Failed to decode histogram from worker ${metrics.workerId}: ${e.message}\n`);
            }
        }
        // Decode and merge connection time histogram from worker
        if (metrics.connTimeHistogramData) {
            try {
                const workerConnTimeHist = hdr.decodeFromCompressedBase64(metrics.connTimeHistogramData);
                mergedConnTimeHistogram.add(workerConnTimeHist);
            } catch (e) {
                // Silently ignore connection time histogram decode failures
            }
        }
        totalRequests += metrics.intervalRequests;
        totalErrors += metrics.intervalErrors;
        totalMoved += metrics.intervalMoved;
        totalClusterdown += metrics.intervalClusterdown;
        totalDisconnects += metrics.intervalDisconnects;
        totalDuration += metrics.intervalDuration;
        // Extended metrics
        totalTimeoutErrors += metrics.intervalTimeoutErrors || 0;
        totalConnectionErrors += metrics.intervalConnectionErrors || 0;
        totalBusyErrors += metrics.intervalBusyErrors || 0;
        totalOomErrors += metrics.intervalOomErrors || 0;
        totalLoadingErrors += metrics.intervalLoadingErrors || 0;
        totalCacheHits += metrics.intervalCacheHits || 0;
        totalCacheMisses += metrics.intervalCacheMisses || 0;
    }

    const avgDuration = totalDuration / workerMetrics.length;

    // Get connection time percentiles
    let connTimeP50 = 0, connTimeP99 = 0, connTimeP100 = 0;
    if (mergedConnTimeHistogram.totalCount > 0) {
        connTimeP50 = mergedConnTimeHistogram.getValueAtPercentile(50) / 1000;
        connTimeP99 = mergedConnTimeHistogram.getValueAtPercentile(99) / 1000;
        connTimeP100 = mergedConnTimeHistogram.maxValue / 1000;
    }

    return {
        histogram: mergedHistogram,
        requests: totalRequests,
        errors: totalErrors,
        moved: totalMoved,
        clusterdown: totalClusterdown,
        disconnects: totalDisconnects,
        duration: avgDuration,
        // Extended metrics
        timeoutErrors: totalTimeoutErrors,
        connectionErrors: totalConnectionErrors,
        busyErrors: totalBusyErrors,
        oomErrors: totalOomErrors,
        loadingErrors: totalLoadingErrors,
        cacheHits: totalCacheHits,
        cacheMisses: totalCacheMisses,
        connTimeP50: connTimeP50,
        connTimeP99: connTimeP99,
        connTimeP100: connTimeP100
    };
}

/**
 * Emit a CSV line from aggregated metrics using merged HDR histogram
 * @param {number} timestamp - Unix timestamp
 * @param {Object} aggregated - Aggregated metrics with histogram
 * @param {Object} options - CSV output options
 * @param {boolean} options.extendedMetrics - Whether to include 12 extended metrics columns
 * @param {Object} options.monitoringData - Current monitoring data {keyspaceCount, replicationLag}
 */
function emitAggregatedCsvLine(timestamp, aggregated, options = {}) {
    const { extendedMetrics = false, monitoringData = {} } = options;
    const requestSec = aggregated.duration > 0 ? aggregated.requests / aggregated.duration : 0;

    let p50, p90, p95, p99, p99_9, p99_99, p99_999, p100, avg;
    if (aggregated.histogram && aggregated.histogram.totalCount > 0) {
        // Get percentiles directly from merged histogram (already in microseconds)
        p50 = Math.floor(aggregated.histogram.getValueAtPercentile(50));
        p90 = Math.floor(aggregated.histogram.getValueAtPercentile(90));
        p95 = Math.floor(aggregated.histogram.getValueAtPercentile(95));
        p99 = Math.floor(aggregated.histogram.getValueAtPercentile(99));
        p99_9 = Math.floor(aggregated.histogram.getValueAtPercentile(99.9));
        p99_99 = Math.floor(aggregated.histogram.getValueAtPercentile(99.99));
        p99_999 = Math.floor(aggregated.histogram.getValueAtPercentile(99.999));
        p100 = Math.floor(aggregated.histogram.maxValue);
        avg = Math.floor(aggregated.histogram.mean);
    } else {
        p50 = p90 = p95 = p99 = p99_9 = p99_99 = p99_999 = p100 = avg = 0;
    }

    let csvLine = `${timestamp},${requestSec.toFixed(6)},${p50},${p90},${p95},${p99},${p99_9},${p99_99},${p99_999},${p100},${avg},${aggregated.requests},${aggregated.errors},${aggregated.moved},${aggregated.clusterdown},${aggregated.disconnects}`;
    if (extendedMetrics) {
        // 12 extended columns: cache_hits, cache_misses, timeout_errors, connection_errors,
        // busy_errors, oom_errors, loading_errors, keyspace_count, replication_lag,
        // conn_est_p50, conn_est_p99, conn_est_p100
        csvLine += `,${aggregated.cacheHits || 0},${aggregated.cacheMisses || 0},${aggregated.timeoutErrors || 0},${aggregated.connectionErrors || 0},${aggregated.busyErrors || 0},${aggregated.oomErrors || 0},${aggregated.loadingErrors || 0},${monitoringData.keyspaceCount || 0},${monitoringData.replicationLag || 0},${(aggregated.connTimeP50 || 0).toFixed(2)},${(aggregated.connTimeP99 || 0).toFixed(2)},${(aggregated.connTimeP100 || 0).toFixed(2)}`;
    }
    console.log(csvLine);
}

/**
 * Orchestrator process that manages worker processes and aggregates metrics
 * @param {Object} config - Base benchmark configuration
 * @param {number} numProcesses - Number of worker processes to spawn
 */
function orchestrator(config, numProcesses) {
    const csvMode = config.csvIntervalSec !== null && config.csvIntervalSec !== undefined;

    logger.info(`Starting orchestrator with ${numProcesses} worker processes`);
    logger.debug(`Orchestrator config: host=${config.host}:${config.port}, command=${config.command}`);

    // Calculate per-worker configuration
    const totalRequests = config.totalRequests;
    const requestsPerWorker = Math.floor(totalRequests / numProcesses);
    const remainder = totalRequests % numProcesses;

    logger.debug(`Distributing ${totalRequests} requests: ${requestsPerWorker} per worker (${remainder} extra for first workers)`);

    const totalQps = config.qps || 0;
    const startQps = config.startQps || 0;
    const endQps = config.endQps || 0;

    // Print banner if not in CSV mode
    if (!csvMode) {
        console.log('Valkey Benchmark (Multi-Process Mode)');
        console.log(`Host: ${config.host}`);
        console.log(`Port: ${config.port}`);
        console.log(`Processes: ${numProcesses}`);
        console.log(`Threads per process: ${config.numThreads}`);
        console.log(`Clients per process: ${config.poolSize}`);
        console.log(`Total Requests: ${totalRequests}`);
        console.log(`Data Size: ${config.dataSize}`);
        console.log(`Command: ${config.command}`);
        console.log(`Is Cluster: ${config.isCluster}`);
        console.log(`Read from Replica: ${config.readFromReplica}`);
        console.log(`Use TLS: ${config.useTls}`);
        if (config.clientsRampStart > 0 && config.clientsRampEnd > 0) {
            if (config.clientRampMode === 'exponential') {
                console.log(`Client Ramp: ${config.clientsRampStart} to ${config.clientsRampEnd} clients per process (exponential, factor ${config.clientRampFactor}x every ${config.clientRampInterval}s)`);
            } else {
                console.log(`Client Ramp: ${config.clientsRampStart} to ${config.clientsRampEnd} clients per process (linear, +${config.clientsPerRamp} every ${config.clientRampInterval}s)`);
            }
        }
        console.log();
    } else {
        // Print CSV header (16 base columns + 12 extended when --extended-metrics)
        let header = "timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,request_finished,requests_total_failed,requests_moved,requests_clusterdown,client_disconnects";
        if (config.extendedMetrics) {
            header += ",cache_hits,cache_misses,timeout_errors,connection_errors,busy_errors,oom_errors,loading_errors,keyspace_count,replication_lag,conn_est_p50,conn_est_p99,conn_est_p100";
        }
        console.log(header);
    }

    // State tracking
    const startTime = Date.now();
    let lastPrint = Date.now();
    const workerState = {}; // workerId -> {requestsCompleted, errors}
    const finalHistograms = []; // Store histogram data from workers

    // HDR histogram for aggregating progress metrics from workers
    const histogramOptions = {
        lowestDiscernibleValue: 1,
        highestTrackableValue: 60000000,
        numberOfSignificantValueDigits: 3
    };
    let currentWindowHistogram = hdr.build(histogramOptions);
    let currentWindowCount = 0;
    let histogramDecodeFailures = 0;

    // CSV mode state
    const csvIntervalSec = config.csvIntervalSec || 0;
    let intervalStart = Date.now();
    const intervalWorkerMetrics = {}; // workerId -> metrics

    // Monitoring state (defined here so accessible in CSV emission)
    const keyspaceHistory = []; // Array of {timestamp, keyCount}
    let keyspaceDropDetected = false;
    let keyspaceMaxDrop = 0;
    let keyspaceMaxDropPercent = 0;
    const replicationHistory = []; // Array of {timestamp, role, masterOffset, replicaLag, connectedSlaves}
    let maxReplicaLag = 0;

    // Spawn workers
    logger.info(`Spawning ${numProcesses} worker processes`);
    cluster.setupPrimary({
        exec: __filename,
        args: ['--worker-mode'],
        silent: false
    });

    const workers = [];
    for (let i = 0; i < numProcesses; i++) {
        const workerConfig = { ...config };

        // Distribute requests
        workerConfig.totalRequests = requestsPerWorker + (i < remainder ? 1 : 0);

        // Distribute QPS proportionally
        if (totalQps > 0) {
            workerConfig.qps = Math.floor(totalQps / numProcesses);
        }
        if (startQps > 0) {
            workerConfig.startQps = Math.floor(startQps / numProcesses);
        }
        if (endQps > 0) {
            workerConfig.endQps = Math.floor(endQps / numProcesses);
        }
        // Also distribute qpsChange proportionally for linear ramp mode
        if (config.qpsChange) {
            workerConfig.qpsChange = Math.floor(config.qpsChange / numProcesses);
        }

        const worker = cluster.fork({
            WORKER_CONFIG: JSON.stringify(workerConfig),
            WORKER_ID: i.toString()
        });
        workers.push(worker);
        logger.debug(`Spawned worker ${i} (PID: ${worker.process.pid}), requests: ${workerConfig.totalRequests}`);

        // Handle messages from worker
        worker.on('message', (metrics) => {
            if (metrics.type === 'progress') {
                const workerId = metrics.workerId;
                workerState[workerId] = {
                    requestsCompleted: metrics.requestsCompleted,
                    errors: metrics.errors
                };

                // Merge worker's window histogram into aggregated histogram
                if (metrics.windowHistogramData) {
                    try {
                        const workerHistogram = hdr.decodeFromCompressedBase64(metrics.windowHistogramData);
                        currentWindowHistogram.add(workerHistogram);
                        currentWindowCount += metrics.windowCount || 0;
                    } catch (e) {
                        histogramDecodeFailures++;
                        process.stderr.write(`Warning: Failed to decode progress histogram from worker ${workerId}: ${e.message}\n`);
                    }
                }

                // Print progress periodically (non-CSV mode)
                const now = Date.now();
                if (!csvMode && now - lastPrint >= 1000) {
                    const elapsed = (now - startTime) / 1000;

                    const totalCompleted = Object.values(workerState).reduce((sum, w) => sum + w.requestsCompleted, 0);
                    const totalErrors = Object.values(workerState).reduce((sum, w) => sum + w.errors, 0);

                    const currentRps = currentWindowCount;
                    const overallRps = totalCompleted / elapsed;

                    const windowStats = calculateLatencyStatsFromHistogram(currentWindowHistogram);

                    let output = `\r\x1b[K[${elapsed.toFixed(1)}s] ` +
                        `Progress: ${totalCompleted.toLocaleString()}/${totalRequests.toLocaleString()} ` +
                        `(${(totalCompleted / totalRequests * 100).toFixed(1)}%), ` +
                        `RPS: current=${currentRps.toLocaleString()} avg=${overallRps.toFixed(1)}, ` +
                        `Errors: ${totalErrors}`;

                    if (windowStats) {
                        output += ` | Latency (ms): avg=${windowStats.avg.toFixed(2)} ` +
                            `p50=${windowStats.p50.toFixed(2)} p95=${windowStats.p95.toFixed(2)} p99=${windowStats.p99.toFixed(2)}`;
                    }

                    process.stdout.write(output);
                    // Reset window histogram for next interval
                    currentWindowHistogram.reset();
                    currentWindowCount = 0;
                    lastPrint = now;
                }
            } else if (metrics.type === 'csv_interval') {
                const workerId = metrics.workerId;
                intervalWorkerMetrics[workerId] = metrics;

                // Check if we have metrics from all workers or if interval has passed
                const now = Date.now();
                if (Object.keys(intervalWorkerMetrics).length === numProcesses ||
                    (now - intervalStart) / 1000 >= csvIntervalSec) {
                    const workerList = Object.values(intervalWorkerMetrics);
                    const aggregated = aggregateCsvMetrics(workerList);
                    if (aggregated) {
                        // Get latest monitoring data for CSV
                        const latestKeyspace = keyspaceHistory.length > 0 ? keyspaceHistory[keyspaceHistory.length - 1] : null;
                        const latestReplication = replicationHistory.length > 0 ? replicationHistory[replicationHistory.length - 1] : null;
                        // Calculate replication lag from latest replication info
                        let replicationLag = 0;
                        if (latestReplication) {
                            if (latestReplication.role === 'slave') {
                                replicationLag = latestReplication.replicaLag || 0;
                            } else if (latestReplication.role === 'master' && latestReplication.maxSlaveLag !== undefined) {
                                replicationLag = latestReplication.maxSlaveLag;
                            }
                        }
                        emitAggregatedCsvLine(Math.floor(now / 1000), aggregated, {
                            extendedMetrics: config.extendedMetrics,
                            monitoringData: {
                                keyspaceCount: latestKeyspace ? latestKeyspace.keyCount : 0,
                                replicationLag: replicationLag
                            }
                        });
                    }

                    // Reset for next interval
                    for (const key in intervalWorkerMetrics) {
                        delete intervalWorkerMetrics[key];
                    }
                    intervalStart = now;
                }
            } else if (metrics.type === 'final') {
                finalHistograms.push(metrics);
            }
        });
    }

    // Keyspace monitoring setup (orchestrator only, runs when extendedMetrics is enabled)
    let keyspaceMonitoringClient = null;
    let keyspaceMonitoringInterval = null;

    if (config.extendedMetrics) {
        (async () => {
            try {
                // Create a separate client for monitoring
                const clientConfig = {
                    addresses: [{
                        host: config.host,
                        port: config.port
                    }],
                    useTLS: config.useTls
                };

                keyspaceMonitoringClient = config.isCluster
                    ? await GlideClusterClient.createClient(clientConfig)
                    : await GlideClient.createClient(clientConfig);

                logger.info('Keyspace monitoring client created');

                // Initial DBSIZE check
                const initialSize = await keyspaceMonitoringClient.dbsize();
                keyspaceHistory.push({ timestamp: Date.now(), keyCount: Number(initialSize) });
                if (!csvMode) {
                    logger.info(`Initial keyspace size: ${initialSize} keys`);
                }

                // Set up periodic monitoring
                keyspaceMonitoringInterval = setInterval(async () => {
                    try {
                        const currentSize = await keyspaceMonitoringClient.dbsize();
                        const currentCount = Number(currentSize);
                        const now = Date.now();
                        keyspaceHistory.push({ timestamp: now, keyCount: currentCount });

                        // Check for significant drops
                        if (keyspaceHistory.length >= 2) {
                            const prevEntry = keyspaceHistory[keyspaceHistory.length - 2];
                            const drop = prevEntry.keyCount - currentCount;
                            if (drop > 0 && prevEntry.keyCount > 0) {
                                const dropPercent = (drop / prevEntry.keyCount) * 100;
                                if (drop > keyspaceMaxDrop) {
                                    keyspaceMaxDrop = drop;
                                    keyspaceMaxDropPercent = dropPercent;
                                }
                                if (dropPercent >= 1) { // 1% threshold for "significant" drop
                                    keyspaceDropDetected = true;
                                    if (!csvMode) {
                                        process.stderr.write(`\nWarning: Keyspace dropped by ${drop} keys (${dropPercent.toFixed(2)}%)\n`);
                                    }
                                }
                            }
                        }
                    } catch (e) {
                        logger.warn(`Keyspace monitoring error: ${e.message}`);
                    }
                }, config.monitoringInterval * 1000);

            } catch (e) {
                logger.error(`Failed to set up keyspace monitoring: ${e.message}`);
            }
        })();
    }

    // Replication lag monitoring setup (orchestrator only, runs when extendedMetrics is enabled)
    let replicationMonitoringClient = null;
    let replicationMonitoringInterval = null;

    if (config.extendedMetrics) {
        (async () => {
            try {
                // Create a separate client for monitoring
                const clientConfig = {
                    addresses: [{
                        host: config.host,
                        port: config.port
                    }],
                    useTLS: config.useTls
                };

                replicationMonitoringClient = config.isCluster
                    ? await GlideClusterClient.createClient(clientConfig)
                    : await GlideClient.createClient(clientConfig);

                logger.info('Replication monitoring client created');

                // Function to parse INFO REPLICATION output
                const parseReplicationInfo = (info) => {
                    const result = {
                        role: 'unknown',
                        masterOffset: 0,
                        replicaLag: 0,
                        connectedSlaves: 0,
                        maxSlaveLag: 0
                    };
                    const lines = info.split('\n');
                    for (const line of lines) {
                        const [key, value] = line.split(':').map(s => s.trim());
                        if (key === 'role') result.role = value;
                        if (key === 'master_repl_offset') result.masterOffset = parseInt(value, 10) || 0;
                        if (key === 'slave_repl_offset') result.replicaOffset = parseInt(value, 10) || 0;
                        if (key === 'master_link_down_since_seconds') result.replicaLag = parseInt(value, 10) || 0;
                        if (key === 'connected_slaves') result.connectedSlaves = parseInt(value, 10) || 0;
                        // Parse slave lag for master nodes (format: slave0:ip=...,lag=N)
                        if (key && key.startsWith('slave') && value) {
                            const lagMatch = value.match(/lag=(\d+)/);
                            if (lagMatch) {
                                const slaveLag = parseInt(lagMatch[1], 10) || 0;
                                if (slaveLag > result.maxSlaveLag) {
                                    result.maxSlaveLag = slaveLag;
                                }
                            }
                        }
                    }
                    return result;
                };

                // Initial check
                const initialInfo = await replicationMonitoringClient.customCommand(['INFO', 'REPLICATION']);
                const initialData = parseReplicationInfo(initialInfo.toString());
                replicationHistory.push({ timestamp: Date.now(), ...initialData });
                if (!csvMode) {
                    logger.info(`Initial replication status: role=${initialData.role}, offset=${initialData.masterOffset}`);
                }

                // Set up periodic monitoring
                replicationMonitoringInterval = setInterval(async () => {
                    try {
                        const info = await replicationMonitoringClient.customCommand(['INFO', 'REPLICATION']);
                        const data = parseReplicationInfo(info.toString());
                        const now = Date.now();
                        replicationHistory.push({ timestamp: now, ...data });

                        // Track max replica lag
                        if (data.replicaLag > maxReplicaLag) {
                            maxReplicaLag = data.replicaLag;
                        }

                        // Calculate offset lag if we're a replica
                        if (data.role === 'slave' && replicationHistory.length >= 2) {
                            const prevEntry = replicationHistory[replicationHistory.length - 2];
                            if (prevEntry.masterOffset && data.replicaOffset) {
                                const offsetLag = prevEntry.masterOffset - data.replicaOffset;
                                if (offsetLag > maxReplicaLag) {
                                    maxReplicaLag = offsetLag;
                                }
                            }
                        }
                    } catch (e) {
                        logger.warn(`Replication monitoring error: ${e.message}`);
                    }
                }, config.monitoringInterval * 1000);

            } catch (e) {
                logger.error(`Failed to set up replication monitoring: ${e.message}`);
            }
        })();
    }

    // Handle graceful shutdown
    let shuttingDown = false;
    const shutdown = () => {
        if (shuttingDown) return;
        shuttingDown = true;

        // Clean up keyspace monitoring
        if (keyspaceMonitoringInterval) {
            clearInterval(keyspaceMonitoringInterval);
        }
        if (keyspaceMonitoringClient) {
            try {
                const closeResult = keyspaceMonitoringClient.close();
                if (closeResult && typeof closeResult.catch === 'function') {
                    closeResult.catch(() => {});
                }
            } catch (e) {
                // Ignore close errors
            }
        }

        // Clean up replication monitoring
        if (replicationMonitoringInterval) {
            clearInterval(replicationMonitoringInterval);
        }
        if (replicationMonitoringClient) {
            try {
                const closeResult = replicationMonitoringClient.close();
                if (closeResult && typeof closeResult.catch === 'function') {
                    closeResult.catch(() => {});
                }
            } catch (e) {
                // Ignore close errors
            }
        }

        if (!csvMode) {
            process.stderr.write('\n\nShutting down workers...\n');
        }

        for (const worker of workers) {
            worker.send({ type: 'shutdown' });
        }

        // Force kill after timeout
        setTimeout(() => {
            for (const worker of workers) {
                if (!worker.isDead()) {
                    worker.kill();
                }
            }
        }, 5000);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    // Wait for all workers to exit
    let exitedCount = 0;
    cluster.on('exit', (worker, code, signal) => {
        exitedCount++;

        if (exitedCount === numProcesses) {
            // All workers done - emit final CSV if needed
            if (csvMode && Object.keys(intervalWorkerMetrics).length > 0) {
                const workerList = Object.values(intervalWorkerMetrics);
                const aggregated = aggregateCsvMetrics(workerList);
                if (aggregated) {
                    // Get latest monitoring data for CSV
                    const latestKeyspace = keyspaceHistory.length > 0 ? keyspaceHistory[keyspaceHistory.length - 1] : null;
                    const latestReplication = replicationHistory.length > 0 ? replicationHistory[replicationHistory.length - 1] : null;
                    // Calculate replication lag from latest replication info
                    let replicationLag = 0;
                    if (latestReplication) {
                        if (latestReplication.role === 'slave') {
                            replicationLag = latestReplication.replicaLag || 0;
                        } else if (latestReplication.role === 'master' && latestReplication.maxSlaveLag !== undefined) {
                            replicationLag = latestReplication.maxSlaveLag;
                        }
                    }
                    emitAggregatedCsvLine(Math.floor(Date.now() / 1000), aggregated, {
                        extendedMetrics: config.extendedMetrics,
                        monitoringData: {
                            keyspaceCount: latestKeyspace ? latestKeyspace.keyCount : 0,
                            replicationLag: replicationLag
                        }
                    });
                }
            }

            // Print final stats if not in CSV mode
            if (!csvMode) {
                const totalTime = (Date.now() - startTime) / 1000;
                const totalCompleted = Object.values(workerState).reduce((sum, w) => sum + w.requestsCompleted, 0);
                const totalErrors = Object.values(workerState).reduce((sum, w) => sum + w.errors, 0);
                const finalRps = totalCompleted / totalTime;

                // Merge histograms from all workers
                const histogramOptions = {
                    lowestDiscernibleValue: 1,
                    highestTrackableValue: 60000000,
                    numberOfSignificantValueDigits: 3
                };
                const mergedHistogram = hdr.build(histogramOptions);

                for (const final of finalHistograms) {
                    if (final.histogramData) {
                        try {
                            const workerHist = hdr.decodeFromCompressedBase64(final.histogramData);
                            mergedHistogram.add(workerHist);
                        } catch (e) {
                            histogramDecodeFailures++;
                            process.stderr.write(`Warning: Failed to decode final histogram from worker ${final.workerId}: ${e.message}\n`);
                        }
                    }
                }

                // Report decode failures if any occurred
                if (histogramDecodeFailures > 0) {
                    process.stderr.write(`\nWarning: ${histogramDecodeFailures} histogram decode failure(s) occurred during benchmark\n`);
                }

                console.log('\n\nFinal Results:');
                console.log('=============');
                console.log(`Total time: ${totalTime.toFixed(2)} seconds`);
                console.log(`Requests completed: ${totalCompleted}`);
                console.log(`Requests per second: ${finalRps.toFixed(2)}`);
                console.log(`Total errors: ${totalErrors}`);

                // Aggregate extended metrics from all workers
                let totalTimeoutErrors = 0;
                let totalConnectionErrors = 0;
                let totalBusyErrors = 0;
                let totalOomErrors = 0;
                let totalLoadingErrors = 0;
                let totalCacheHits = 0;
                let totalCacheMisses = 0;
                // Merge connection time histograms
                const connTimeHistogram = hdr.build({
                    lowestDiscernibleValue: 1,
                    highestTrackableValue: 60000000,
                    numberOfSignificantValueDigits: 3
                });

                for (const final of finalHistograms) {
                    totalTimeoutErrors += final.timeoutErrors || 0;
                    totalConnectionErrors += final.connectionErrors || 0;
                    totalBusyErrors += final.busyErrors || 0;
                    totalOomErrors += final.oomErrors || 0;
                    totalLoadingErrors += final.loadingErrors || 0;
                    totalCacheHits += final.cacheHits || 0;
                    totalCacheMisses += final.cacheMisses || 0;
                    if (final.connTimeHistogramData) {
                        try {
                            const workerConnTimeHist = hdr.decodeFromCompressedBase64(final.connTimeHistogramData);
                            connTimeHistogram.add(workerConnTimeHist);
                        } catch (e) {
                            // Silently ignore connection time histogram decode failures
                        }
                    }
                }

                // Error breakdown
                if (totalErrors > 0) {
                    console.log('\nError Breakdown:');
                    console.log('================');
                    console.log(`Timeout errors: ${totalTimeoutErrors}`);
                    console.log(`Connection errors: ${totalConnectionErrors}`);
                    console.log(`BUSY errors: ${totalBusyErrors}`);
                    console.log(`OOM errors: ${totalOomErrors}`);
                    console.log(`LOADING errors: ${totalLoadingErrors}`);
                }

                // Cache statistics
                const totalCacheOps = totalCacheHits + totalCacheMisses;
                if (totalCacheOps > 0) {
                    const hitRate = (totalCacheHits / totalCacheOps * 100).toFixed(2);
                    console.log('\nCache Statistics:');
                    console.log('================');
                    console.log(`Cache hits: ${totalCacheHits}`);
                    console.log(`Cache misses: ${totalCacheMisses}`);
                    console.log(`Hit rate: ${hitRate}%`);
                }

                // Connection establishment time
                if (connTimeHistogram.totalCount > 0) {
                    console.log('\nConnection Establishment Time (ms):');
                    console.log('==================================');
                    console.log(`p50: ${(connTimeHistogram.getValueAtPercentile(50) / 1000).toFixed(2)}`);
                    console.log(`p99: ${(connTimeHistogram.getValueAtPercentile(99) / 1000).toFixed(2)}`);
                    console.log(`p100 (max): ${(connTimeHistogram.maxValue / 1000).toFixed(2)}`);
                    console.log(`Connections: ${connTimeHistogram.totalCount}`);
                }

                // Keyspace monitoring results (when extended metrics enabled)
                if (config.extendedMetrics && keyspaceHistory.length > 0) {
                    console.log('\nKeyspace Monitoring:');
                    console.log('===================');
                    const initialCount = keyspaceHistory[0].keyCount;
                    const finalCount = keyspaceHistory[keyspaceHistory.length - 1].keyCount;
                    const netChange = finalCount - initialCount;
                    console.log(`Initial key count: ${initialCount}`);
                    console.log(`Final key count: ${finalCount}`);
                    console.log(`Net change: ${netChange >= 0 ? '+' : ''}${netChange}`);
                    console.log(`Samples collected: ${keyspaceHistory.length}`);
                    if (keyspaceDropDetected) {
                        console.log(`Max drop detected: ${keyspaceMaxDrop} keys (${keyspaceMaxDropPercent.toFixed(2)}%)`);
                    }
                }

                // Replication monitoring results (when extended metrics enabled)
                if (config.extendedMetrics && replicationHistory.length > 0) {
                    console.log('\nReplication Monitoring:');
                    console.log('======================');
                    const latest = replicationHistory[replicationHistory.length - 1];
                    console.log(`Role: ${latest.role}`);
                    if (latest.role === 'master') {
                        console.log(`Master replication offset: ${latest.masterOffset}`);
                        console.log(`Connected slaves: ${latest.connectedSlaves}`);
                    } else if (latest.role === 'slave') {
                        console.log(`Replica offset: ${latest.replicaOffset || 0}`);
                        if (maxReplicaLag > 0) {
                            console.log(`Max replication lag detected: ${maxReplicaLag}`);
                        }
                    }
                    console.log(`Samples collected: ${replicationHistory.length}`);
                }

                if (mergedHistogram.totalCount > 0) {
                    console.log('\nLatency Statistics (ms):');
                    console.log('=====================');
                    console.log(`Minimum: ${(mergedHistogram.minNonZeroValue / 1000).toFixed(3)}`);
                    console.log(`Average: ${(mergedHistogram.mean / 1000).toFixed(3)}`);
                    console.log(`Maximum: ${(mergedHistogram.maxValue / 1000).toFixed(3)}`);
                    console.log(`Median (p50): ${(mergedHistogram.getValueAtPercentile(50) / 1000).toFixed(3)}`);
                    console.log(`95th percentile: ${(mergedHistogram.getValueAtPercentile(95) / 1000).toFixed(3)}`);
                    console.log(`99th percentile: ${(mergedHistogram.getValueAtPercentile(99) / 1000).toFixed(3)}`);

                    console.log('\nLatency Distribution:');
                    console.log('====================');
                    const percentiles = [50, 75, 90, 95, 99, 99.9, 99.99, 100];
                    for (const p of percentiles) {
                        const valueMs = mergedHistogram.getValueAtPercentile(p) / 1000;
                        console.log(`${p}% <= ${valueMs.toFixed(3)} ms`);
                    }
                }
            }

            // Clean up keyspace monitoring before exit
            if (keyspaceMonitoringInterval) {
                clearInterval(keyspaceMonitoringInterval);
            }
            if (keyspaceMonitoringClient) {
                try {
                    const closeResult = keyspaceMonitoringClient.close();
                    if (closeResult && typeof closeResult.catch === 'function') {
                        closeResult.catch(() => {});
                    }
                } catch (e) {
                    // Ignore close errors
                }
            }

            // Clean up replication monitoring before exit
            if (replicationMonitoringInterval) {
                clearInterval(replicationMonitoringInterval);
            }
            if (replicationMonitoringClient) {
                try {
                    const closeResult = replicationMonitoringClient.close();
                    if (closeResult && typeof closeResult.catch === 'function') {
                        closeResult.catch(() => {});
                    }
                } catch (e) {
                    // Ignore close errors
                }
            }

            process.exit(0);
        }
    });
}

/**
 * Worker process entry point
 * @param {Object} config - Worker-specific configuration
 * @param {number} workerId - Worker ID
 */
async function workerMain(config, workerId) {
    try {
        await runBenchmark(config, workerId, true);
    } catch (error) {
        process.stderr.write(`Worker ${workerId} error: ${error.message}\n`);
        process.exit(1);
    }
    process.exit(0);
}

// ============================================================================
// Main Application Entry Point
// ============================================================================

/**
 * Main application entry point
 * Initializes and runs the benchmark based on provided configuration
 */
async function main() {
    // Check if we're running as a worker process
    if (process.env.WORKER_CONFIG && process.env.WORKER_ID) {
        const config = JSON.parse(process.env.WORKER_CONFIG);
        const workerId = parseInt(process.env.WORKER_ID, 10);

        // Setup logging in worker process
        logger.setup({
            csvMode: config.csvIntervalSec !== null && config.csvIntervalSec !== undefined,
            logLevel: config.logLevel,
            debug: config.debug
        });

        // Re-load custom commands in worker process
        if (config.customCommandFile) {
            config.customCommands = loadCustomCommands(config.customCommandFile, config.customCommandArgs);
            // Initialize custom commands with benchmark config (if init method exists)
            if (config.customCommands && typeof config.customCommands.init === 'function') {
                config.customCommands.init(config);
            }
        }

        await workerMain(config, workerId);
        return;
    }

    const args = parseCommandLine();

    // Setup logging early
    const csvMode = args['interval-metrics-interval-duration-sec'] !== undefined;
    logger.setup({
        csvMode: csvMode,
        logLevel: args['log-level'],
        debug: args.debug
    });

    logger.info(`Starting Valkey benchmark with command: ${args.type}`);
    logger.debug(`Host: ${args.host}:${args.port}, Clients: ${args.clients}, Requests: ${args.requests}`);

    const CustomCommands = loadCustomCommands(args['custom-command-file'], args['custom-command-args']);

    const config = {
        host: args.host,
        port: args.port,
        poolSize: args.clients,
        totalRequests: args.requests,
        dataSize: args.datasize,
        command: args.type,
        randomKeyspace: args.random,
        numThreads: args.threads,
        testDuration: args['test-duration'],
        useSequential: args.sequential !== undefined,
        sequentialKeyspacelen: args.sequential,
        sequentialRandomStart: args['sequential-random-start'],
        keyspaceOffset: args['keyspace-offset'] || 0,
        qps: args.qps,
        startQps: args['start-qps'],
        endQps: args['end-qps'],
        qpsChangeInterval: args['qps-change-interval'],
        qpsChange: args['qps-change'],
        qpsRampMode: args['qps-ramp-mode'] || 'linear',
        qpsRampFactor: args['qps-ramp-factor'] || 0,
        useTls: args.tls,
        isCluster: args.cluster,
        readFromReplica: args['read-from-replica'],
        customCommands: CustomCommands,
        customCommandFile: args['custom-command-file'], // Store for worker processes
        customCommandArgs: args['custom-command-args'], // Store for worker processes
        debug: args.debug,
        logLevel: args['log-level'],
        csvIntervalSec: args['interval-metrics-interval-duration-sec'],
        extendedMetrics: args['extended-metrics'] || false,
        requestTimeout: args['request-timeout'],
        connectionTimeout: args['connection-timeout'],
        clientsRampStart: args['clients-ramp-start'] || 0,
        clientsRampEnd: args['clients-ramp-end'] || 0,
        clientsPerRamp: args['clients-per-ramp'] || 0,
        clientRampInterval: args['client-ramp-interval'] || 0,
        clientRampMode: args['client-ramp-mode'] || 'linear',
        clientRampFactor: args['client-ramp-factor'] || 0,
        monitoringInterval: args['monitoring-interval'] || 5
    };

    // Validate QPS configuration before spawning workers
    if (config.qpsRampMode === 'exponential') {
        const missing = [];
        if (!config.startQps || config.startQps <= 0) missing.push('--start-qps');
        if (!config.endQps || config.endQps <= 0) missing.push('--end-qps');
        if (!config.qpsChangeInterval || config.qpsChangeInterval <= 0) missing.push('--qps-change-interval');
        if (!config.qpsRampFactor || config.qpsRampFactor <= 0) missing.push('--qps-ramp-factor');

        if (missing.length > 0) {
            console.error(`Error: exponential mode requires all of: --start-qps, --end-qps, --qps-change-interval, --qps-ramp-factor`);
            console.error(`Missing: ${missing.join(', ')}`);
            process.exit(1);
        }

        if (config.qpsRampFactor < 1) {
            console.error('Warning: qpsRampFactor < 1 will cause QPS to decrease (ramp-down) each interval');
        }
    }

    // Validate client ramp-up configuration
    const rampStartSpecified = config.clientsRampStart > 0;
    const rampEndSpecified = config.clientsRampEnd > 0;
    const perRampSpecified = config.clientsPerRamp > 0;
    const rampIntervalSpecified = config.clientRampInterval > 0;
    const rampFactorSpecified = config.clientRampFactor > 0;
    const isExponentialClientRamp = config.clientRampMode === 'exponential';
    const anyRampSpecified = rampStartSpecified || rampEndSpecified || perRampSpecified || rampIntervalSpecified || rampFactorSpecified;

    if (anyRampSpecified) {
        // Check required params based on mode
        const missing = [];
        if (!rampStartSpecified) missing.push('--clients-ramp-start');
        if (!rampEndSpecified) missing.push('--clients-ramp-end');
        if (!rampIntervalSpecified) missing.push('--client-ramp-interval');

        if (isExponentialClientRamp) {
            // Exponential mode requires --client-ramp-factor
            if (!rampFactorSpecified) missing.push('--client-ramp-factor');
        } else {
            // Linear mode requires --clients-per-ramp
            if (!perRampSpecified) missing.push('--clients-per-ramp');
        }

        if (missing.length > 0) {
            if (isExponentialClientRamp) {
                console.error(`Error: Exponential client ramp-up requires: --clients-ramp-start, --clients-ramp-end, --client-ramp-interval, --client-ramp-factor`);
            } else {
                console.error(`Error: Linear client ramp-up requires: --clients-ramp-start, --clients-ramp-end, --clients-per-ramp, --client-ramp-interval`);
            }
            console.error(`Missing: ${missing.join(', ')}`);
            process.exit(1);
        }

        // Check mutual exclusivity with --clients
        if (args.clients !== 50) { // 50 is the default
            console.error('Error: Client ramp-up parameters are mutually exclusive with --clients/-c');
            process.exit(1);
        }

        // Validate start < end
        if (config.clientsRampStart >= config.clientsRampEnd) {
            console.error(`Error: --clients-ramp-start (${config.clientsRampStart}) must be less than --clients-ramp-end (${config.clientsRampEnd})`);
            process.exit(1);
        }

        // Validate exponential factor
        if (isExponentialClientRamp && config.clientRampFactor <= 1) {
            console.error(`Error: --client-ramp-factor must be greater than 1 for ramp-up (got ${config.clientRampFactor})`);
            process.exit(1);
        }

        // Set pool size to ramp end (max clients)
        config.poolSize = config.clientsRampEnd;
    }

    // Validate keyspace configuration
    if (config.sequentialRandomStart && !config.useSequential) {
        console.error('Error: --sequential-random-start requires --sequential to be set');
        process.exit(1);
    }

    if (config.keyspaceOffset !== 0 && !(config.randomKeyspace > 0 || config.useSequential)) {
        console.error('Error: --keyspace-offset requires either -r/--random or --sequential to be set');
        process.exit(1);
    }

    // Initialize custom commands with benchmark config (if init method exists)
    if (config.customCommands && typeof config.customCommands.init === 'function') {
        config.customCommands.init(config);
    }

    // Determine number of processes
    let numProcesses = 1;
    if (!args['single-process']) {
        if (args.processes.toLowerCase() === 'auto') {
            numProcesses = os.cpus().length;
        } else {
            numProcesses = parseInt(args.processes, 10);
            if (isNaN(numProcesses) || numProcesses < 1) {
                console.error(`Error: Invalid value for --processes: ${args.processes}`);
                process.exit(1);
            }
        }
    }

    // Run benchmark
    if (numProcesses === 1 || args['single-process']) {
        // Single-process mode (legacy behavior)
        await runBenchmark(config);
    } else {
        // Multi-process mode
        orchestrator(config, numProcesses);
    }
}

// Start the application
main().catch(console.error);

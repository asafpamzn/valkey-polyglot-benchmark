/**
 * Valkey Benchmark Tool
 * A comprehensive performance testing utility for Valkey/Redis operations.
 * 
 */

const path = require('path');
const fs = require('fs');
const os = require('os');
const cluster = require('cluster');
const { GlideClient, GlideClusterClient } = require('@valkey/valkey-glide');
const hdr = require('hdr-histogram-js');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Loads custom commands from a specified file path
 * @param {string} filePath - Path to the custom commands implementation file
 * @returns {Object} An object containing the custom command implementation
 * @throws {Error} If the file cannot be loaded or doesn't exist
 */
function loadCustomCommands(filePath) {
    if (!filePath) {
        // Return default implementation if no file specified
        return {
            execute: async (client) => {
                try {
                    await client.set('custom:key', 'custom:value');
                    return true;
                } catch (error) {
                    console.error('Custom command error:', error);
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
        return require(absolutePath);
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
 * @returns {string} Generated key in format 'key:{number}'
 */
function getRandomKey(keyspace) {
    return `key:${Math.floor(Math.random() * keyspace)}`;
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
        let startQps = config.startQps || 0;
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
            console.error('Warning: startQps not set for ramp mode, using endQps as initial QPS');
        } else {
            this.currentQps = 0;
        }
        
        // Validate startQps if ramp mode is configured
        if (qpsChangeInterval > 0 && endQps > 0) {
            if (startQps <= 0) {
                console.error('Warning: startQps must be positive for QPS ramping. Using endQps as fallback.');
                // Use local effective startQps instead of modifying config
                startQps = endQps;
            }
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
            lowestDiscernibleValue: 10,
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
    }
    /**
     * Records a latency measurement and updates statistics
     * @param {number} latency - Latency measurement in milliseconds
     */
    addLatency(latency) {
        // Convert milliseconds to microseconds for HDR histogram
        const latencyUsec = Math.max(10, Math.floor(latency * 1000));
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
         * Prints CSV header line (once at start)
         */
        printCsvHeader() {
            if (!this.csvHeaderPrinted) {
                console.log("timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,request_finished,requests_total_failed,requests_moved,requests_clusterdown,client_disconnects");
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

            // Output CSV line with exactly 16 fields (includes request_finished)
            console.log(`${timestamp},${requestSec.toFixed(6)},${p50},${p90},${p95},${p99},${p99_9},${p99_99},${p99_999},${p100},${avg},${this.intervalRequests},${this.intervalErrors},${this.intervalMoved},${this.intervalClusterdown},${this.intervalDisconnects}`);

            // Reset interval counters and histogram
            this.intervalStartTime = now;
            this.intervalHistogram.reset();
            this.intervalErrors = 0;
            this.intervalMoved = 0;
            this.intervalClusterdown = 0;
            this.intervalDisconnects = 0;
            this.intervalRequests = 0;
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
            intervalDisconnects: this.intervalDisconnects
        });

        // Reset interval counters and histogram
        this.intervalStartTime = now;
        this.intervalHistogram.reset();
        this.intervalErrors = 0;
        this.intervalMoved = 0;
        this.intervalClusterdown = 0;
        this.intervalDisconnects = 0;
        this.intervalRequests = 0;
    }

    /**
     * Send final metrics to orchestrator at the end of benchmark
     */
    sendFinalMetrics() {
        if (!this.isMultiProcess) return;

        // Export histogram data for aggregation
        // We'll send the encoded histogram so it can be merged
        const histogramData = hdr.encodeIntoCompressedBase64(this.latencyHistogram);

        process.send({
            type: 'final',
            workerId: this.workerId,
            requestsCompleted: this.requestsCompleted,
            errors: this.errors,
            histogramData: histogramData,
            totalTime: (Date.now() - this.startTime) / 1000
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
    const qpsController = new QPSController(config);

    // Shutdown flag for graceful termination
    let shutdownRequested = false;
    if (isMultiProcess) {
        process.on('message', (msg) => {
            if (msg.type === 'shutdown') {
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
        console.log();
    } else if (stats.csvMode && !isMultiProcess) {
        // In CSV mode (single-process), print header to stdout
        stats.printCsvHeader();
    }

    // Create client pool
    const clientPool = [];
    for (let i = 0; i < config.poolSize; i++) {
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

        const client = config.isCluster 
            ? await GlideClusterClient.createClient(clientConfig)
            : await GlideClient.createClient(clientConfig);

        clientPool.push(client);
    }

    // Create worker promises
    const workers = Array(config.numThreads).fill().map(async (_, threadId) => {
        const data = config.command === 'set' ? generateRandomData(config.dataSize) : null;
        let running = true;

        if (config.testDuration > 0) {
            setTimeout(() => {
                running = false;
            }, config.testDuration * 1000);
        }

        while (running && !shutdownRequested && (config.testDuration > 0 || stats.requestsCompleted < config.totalRequests)) {
            const clientIndex = stats.requestsCompleted % config.poolSize;
            const client = clientPool[clientIndex];
            await qpsController.throttle();

            const start = Date.now();
            try {
                if (config.command === 'set') {
                    const key = config.useSequential 
                        ? `key:${stats.requestsCompleted % config.sequentialKeyspacelen}`
                        : config.randomKeyspace > 0 
                            ? getRandomKey(config.randomKeyspace)
                            : `key:${threadId}:${stats.requestsCompleted}`;
 
                    await client.set(key, data);
                } else if (config.command === 'get') {
                    const key = config.randomKeyspace > 0 
                        ? getRandomKey(config.randomKeyspace)
                        : `key:${threadId}:${stats.requestsCompleted}`;
                    await client.get(key);
                } else if (config.command === 'custom') {
                    await config.customCommands.execute(client);
                }
                
                const latency = Date.now() - start;
                stats.addLatency(latency);
            } catch (error) {
                const errorMsg = error.toString().toUpperCase();
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

    await Promise.all(workers);

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
    for (const client of clientPool) {
        await client.close();
    }
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
        .option('custom-command-file', {
            describe: 'Path to custom command implementation file',
            type: 'string'
        })
        .option('interval-metrics-interval-duration-sec', {
            describe: 'Emit CSV metrics every N seconds (enables CSV output mode)',
            type: 'number'
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

    // Create merged histogram
    const histogramOptions = {
        lowestDiscernibleValue: 10,
        highestTrackableValue: 60000000,
        numberOfSignificantValueDigits: 3
    };
    const mergedHistogram = hdr.build(histogramOptions);

    let totalRequests = 0;
    let totalErrors = 0;
    let totalMoved = 0;
    let totalClusterdown = 0;
    let totalDisconnects = 0;
    let totalDuration = 0;

    let decodeFailures = 0;
    for (const metrics of workerMetrics) {
        // Decode and merge histogram from worker
        if (metrics.intervalHistogramData) {
            try {
                const workerHistogram = hdr.decodeFromCompressedBase64(metrics.intervalHistogramData);
                mergedHistogram.add(workerHistogram);
            } catch (e) {
                decodeFailures++;
                process.stderr.write(`Warning: Failed to decode histogram from worker ${metrics.workerId}: ${e.message}\n`);
            }
        }
        totalRequests += metrics.intervalRequests;
        totalErrors += metrics.intervalErrors;
        totalMoved += metrics.intervalMoved;
        totalClusterdown += metrics.intervalClusterdown;
        totalDisconnects += metrics.intervalDisconnects;
        totalDuration += metrics.intervalDuration;
    }

    const avgDuration = totalDuration / workerMetrics.length;

    return {
        histogram: mergedHistogram,
        requests: totalRequests,
        errors: totalErrors,
        moved: totalMoved,
        clusterdown: totalClusterdown,
        disconnects: totalDisconnects,
        duration: avgDuration
    };
}

/**
 * Emit a CSV line from aggregated metrics using merged HDR histogram
 * @param {number} timestamp - Unix timestamp
 * @param {Object} aggregated - Aggregated metrics with histogram
 */
function emitAggregatedCsvLine(timestamp, aggregated) {
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

    console.log(`${timestamp},${requestSec.toFixed(6)},${p50},${p90},${p95},${p99},${p99_9},${p99_99},${p99_999},${p100},${avg},${aggregated.requests},${aggregated.errors},${aggregated.moved},${aggregated.clusterdown},${aggregated.disconnects}`);
}

/**
 * Orchestrator process that manages worker processes and aggregates metrics
 * @param {Object} config - Base benchmark configuration
 * @param {number} numProcesses - Number of worker processes to spawn
 */
function orchestrator(config, numProcesses) {
    const csvMode = config.csvIntervalSec !== null && config.csvIntervalSec !== undefined;

    // Calculate per-worker configuration
    const totalRequests = config.totalRequests;
    const requestsPerWorker = Math.floor(totalRequests / numProcesses);
    const remainder = totalRequests % numProcesses;

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
        console.log();
    } else {
        // Print CSV header
        console.log("timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,request_finished,requests_total_failed,requests_moved,requests_clusterdown,client_disconnects");
    }

    // State tracking
    const startTime = Date.now();
    let lastPrint = Date.now();
    const workerState = {}; // workerId -> {requestsCompleted, errors}
    const finalHistograms = []; // Store histogram data from workers

    // HDR histogram for aggregating progress metrics from workers
    const histogramOptions = {
        lowestDiscernibleValue: 10,
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

    // Spawn workers
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
                        emitAggregatedCsvLine(Math.floor(now / 1000), aggregated);
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

    // Handle graceful shutdown
    let shuttingDown = false;
    const shutdown = () => {
        if (shuttingDown) return;
        shuttingDown = true;

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
                    emitAggregatedCsvLine(Math.floor(Date.now() / 1000), aggregated);
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
                    lowestDiscernibleValue: 10,
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

        // Re-load custom commands in worker process
        if (config.customCommandFile) {
            config.customCommands = loadCustomCommands(config.customCommandFile);
        }

        await workerMain(config, workerId);
        return;
    }

    const args = parseCommandLine();
    const CustomCommands = loadCustomCommands(args['custom-command-file']);

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
        csvIntervalSec: args['interval-metrics-interval-duration-sec'],
        requestTimeout: args['request-timeout']
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

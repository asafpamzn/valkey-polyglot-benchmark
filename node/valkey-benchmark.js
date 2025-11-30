/**
 * Valkey Benchmark Tool
 * A comprehensive performance testing utility for Valkey/Redis operations.
 * 
 */

const path = require('path');
const fs = require('fs');
const { GlideClient, GlideClusterClient } = require('@valkey/valkey-glide');
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
        this.currentQps = config.startQps || config.qps;
        this.lastUpdate = Date.now();
        this.requestsThisSecond = 0;
        this.secondStart = Date.now();
        this.exponentialMultiplier = 1.0;
        
        // For exponential mode, compute the multiplier
        const qpsRampMode = config.qpsRampMode || 'linear';
        if (qpsRampMode === 'exponential' && 
            config.startQps > 0 && config.endQps > 0 && 
            config.qpsChangeInterval > 0) {
            
            // Use explicit factor if provided, otherwise auto-calculate
            if (config.qpsRampFactor > 0) {
                this.exponentialMultiplier = config.qpsRampFactor;
            } else if (config.testDuration > 0) {
                const numIntervals = Math.floor(config.testDuration / config.qpsChangeInterval);
                if (numIntervals > 0) {
                    // multiplier = (endQps / startQps) ^ (1 / numIntervals)
                    this.exponentialMultiplier = Math.pow(config.endQps / config.startQps, 1.0 / numIntervals);
                } else {
                    console.error('Warning: test-duration is less than qps-change-interval, exponential mode will not ramp QPS');
                }
            } else {
                console.error('Warning: exponential mode requires either --qps-ramp-factor or --test-duration');
            }
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
    constructor(csvIntervalSec = null) {
        this.startTime = Date.now();
        this.requestsCompleted = 0;
        this.latencies = [];
        this.errors = 0;
        this.lastPrint = Date.now();
        this.lastRequests = 0;
        this.currentWindowLatencies = [];
        this.lastWindowTime = Date.now();
        this.windowSize = 1000; // 1 second window
        
        // CSV interval metrics tracking
        this.csvIntervalSec = csvIntervalSec;
        this.csvMode = csvIntervalSec !== null && csvIntervalSec !== undefined;
        this.intervalStartTime = Date.now();
        this.intervalLatencies = [];
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
            this.latencies.push(latency);
            this.currentWindowLatencies.push(latency);
            this.requestsCompleted++;
            
            if (this.csvMode) {
                this.intervalLatencies.push(latency);
                this.intervalRequests++;
                this.checkCsvInterval();
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
                console.log("timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,requests_total_failed,requests_moved,requests_clusterdown,client_disconnects");
                this.csvHeaderPrinted = true;
            }
        }
        
        /**
         * Calculate percentile from sorted latencies in microseconds (truncated)
         * @param {number[]} sortedLatencies - Sorted array of latencies in milliseconds
         * @param {number} percentile - Percentile value (0-100)
         * @returns {number} Percentile value in microseconds (truncated)
         */
        calculatePercentileUsec(sortedLatencies, percentile) {
            if (sortedLatencies.length === 0) {
                return 0;
            }
            
            let idx = Math.floor(sortedLatencies.length * percentile / 100.0);
            if (idx >= sortedLatencies.length) {
                idx = sortedLatencies.length - 1;
            }
            
            // Convert milliseconds to microseconds and truncate (not round)
            return Math.floor(sortedLatencies[idx] * 1000);
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
            
            // Calculate percentiles from interval latencies
            let p50, p90, p95, p99, p99_9, p99_99, p99_999, p100, avg;
            if (this.intervalLatencies.length > 0) {
                const sortedLats = [...this.intervalLatencies].sort((a, b) => a - b);
                p50 = this.calculatePercentileUsec(sortedLats, 50);
                p90 = this.calculatePercentileUsec(sortedLats, 90);
                p95 = this.calculatePercentileUsec(sortedLats, 95);
                p99 = this.calculatePercentileUsec(sortedLats, 99);
                p99_9 = this.calculatePercentileUsec(sortedLats, 99.9);
                p99_99 = this.calculatePercentileUsec(sortedLats, 99.99);
                p99_999 = this.calculatePercentileUsec(sortedLats, 99.999);
                p100 = Math.floor(sortedLats[sortedLats.length - 1] * 1000); // max in microseconds
                avg = Math.floor(sortedLats.reduce((a, b) => a + b, 0) / sortedLats.length * 1000); // avg in microseconds
            } else {
                p50 = p90 = p95 = p99 = p99_9 = p99_99 = p99_999 = p100 = avg = 0;
            }
            
            // Output CSV line with exactly 15 fields
            console.log(`${timestamp},${requestSec.toFixed(6)},${p50},${p90},${p95},${p99},${p99_9},${p99_99},${p99_999},${p100},${avg},${this.intervalErrors},${this.intervalMoved},${this.intervalClusterdown},${this.intervalDisconnects}`);
            
            // Reset interval counters
            this.intervalStartTime = now;
            this.intervalLatencies = [];
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
                    this.emitCsvLine();
                }
            }
        }

    /**
     * Calculates statistical metrics for latency measurements
     * @param {number[]} latencies - Array of latency measurements
     * @returns {Object|null} Statistical metrics including min, max, avg, and percentiles
     */
    calculateLatencyStats(latencies) {
        if (latencies.length === 0) return null;
        
        const sorted = [...latencies].sort((a, b) => a - b);
        
        const getPercentile = (p) => {
            const index = Math.ceil((p / 100) * sorted.length) - 1;
            return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
        };

        const sum = sorted.reduce((a, b) => a + Number(b), 0);
        const mean = sum / sorted.length;

        const p50 = Math.max(0.001, getPercentile(50));
        const p95 = Math.max(p50, getPercentile(95));
        const p99 = Math.max(p95, getPercentile(99));

        return {
            min: sorted[0],
            max: sorted[sorted.length - 1],
            avg: mean,
            p50: p50,
            p95: p95,
            p99: p99
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
            
            const windowStats = this.calculateLatencyStats(this.currentWindowLatencies);
            
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
    
            this.currentWindowLatencies = [];
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

        // Calculate final latency stats
        const finalStats = this.calculateLatencyStats(this.latencies);

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

            // Add latency distribution
            console.log('\nLatency Distribution:');
            console.log('====================');
            const ranges = [0.1, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000];
            let current = 0;
            for (const range of ranges) {
                const count = this.latencies.filter(l => l <= range).length - current;
                const percentage = (count / this.latencies.length * 100).toFixed(2);
                console.log(`<= ${range.toFixed(1)} ms: ${percentage}% (${count} requests)`);
                current += count;
            }
            const remaining = this.latencies.length - current;
            if (remaining > 0) {
                const percentage = (remaining / this.latencies.length * 100).toFixed(2);
                console.log(`> 1000 ms: ${percentage}% (${remaining} requests)`);
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
 * @returns {Promise<void>}
 */
async function runBenchmark(config) {
    const stats = new BenchmarkStats(config.csvIntervalSec);
    const qpsController = new QPSController(config);

    // Only print banner if not in CSV mode
    if (!stats.csvMode) {
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
    } else {
        // In CSV mode, print header to stdout
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

        while (running && (config.testDuration > 0 || stats.requestsCompleted < config.totalRequests)) {
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
    
    // Emit final CSV line if in CSV mode and there's any data or errors
    if (stats.csvMode && (stats.intervalLatencies.length > 0 || stats.intervalErrors > 0 ||
                          stats.intervalMoved > 0 || stats.intervalClusterdown > 0)) {
        stats.emitCsvLine();
    }
    
    // Only print final stats if not in CSV mode
    if (!stats.csvMode) {
        stats.printFinalStats();
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
        .option('custom-command-file', {
            describe: 'Path to custom command implementation file',
            type: 'string'
        })
        .option('interval-metrics-interval-duration-sec', {
            describe: 'Emit CSV metrics every N seconds (enables CSV output mode)',
            type: 'number'
        })
        .help()
        .argv;
}

// ============================================================================
// Main Application Entry Point
// ============================================================================

/**
 * Main application entry point
 * Initializes and runs the benchmark based on provided configuration
 */
async function main() {
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
        csvIntervalSec: args['interval-metrics-interval-duration-sec']
    };
    
    if (config.useSequential && config.testDuration > 0) {
        console.error('Error: --sequential and --test-duration are mutually exclusive');
        process.exit(1);
    }

    await runBenchmark(config);
}

// Start the application
main().catch(console.error);

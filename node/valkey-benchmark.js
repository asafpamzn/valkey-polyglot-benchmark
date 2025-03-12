const path = require('path');
const fs = require('fs');
const { GlideClient, GlideClusterClient } = require('@valkey/valkey-glide');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

// Configuration class
class BenchmarkConfig {
    constructor() {
        this.host = '127.0.0.1';
        this.port = 6379;
        this.numThreads = 1;
        this.totalRequests = 100000;
        this.dataSize = 3;
        this.command = 'set';
        this.showHelp = false;
        this.randomKeyspace = 0;
        this.useSequential = false;
        this.sequentialKeyspacelen = 0;
        this.poolSize = 1;
        this.qps = 0;
        this.startQps = 0;
        this.endQps = 0;
        this.qpsChangeInterval = 0;
        this.qpsChange = 0;
        this.testDuration = 0;
        this.useTls = false;
        this.isCluster = false;
        this.readFromReplica = false;
    }
}

// Helper functions

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

function generateRandomData(size) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ';
    let result = '';
    for (let i = 0; i < size; i++) {
        result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
}

function getRandomKey(keyspace) {
    return `key:${Math.floor(Math.random() * keyspace)}`;
}

// QPS Controller class
class QPSController {
    constructor(config) {
        this.config = config;
        this.currentQps = config.startQps || config.qps;
        this.lastUpdate = Date.now();
        this.requestsThisSecond = 0;
        this.secondStart = Date.now();
    }

    async throttle() {
        if (this.currentQps <= 0) return;

        const now = Date.now();
        const elapsedSinceLastUpdate = (now - this.lastUpdate) / 1000;

        if (this.config.startQps > 0 && this.config.endQps > 0) {
            if (elapsedSinceLastUpdate >= this.config.qpsChangeInterval) {
                const diff = this.config.endQps - this.currentQps;
                if ((diff > 0 && this.config.qpsChange > 0) ||
                    (diff < 0 && this.config.qpsChange < 0)) {
                    this.currentQps += this.config.qpsChange;
                    if ((this.config.qpsChange > 0 && this.currentQps > this.config.endQps) ||
                        (this.config.qpsChange < 0 && this.currentQps < this.config.endQps)) {
                        this.currentQps = this.config.endQps;
                    }
                }
                this.lastUpdate = now;
            }
        }

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

class BenchmarkStats {
    constructor() {
        this.startTime = Date.now();
        this.requestsCompleted = 0;
        this.latencies = [];
        this.errors = 0;
        this.lastPrint = Date.now();
        this.lastRequests = 0;
        
        // Add running statistics
        this.currentWindowLatencies = [];
        this.lastWindowTime = Date.now();
        this.windowSize = 1000; // 1 second window
    }

    addLatency(latency) {
        this.latencies.push(latency);
        this.currentWindowLatencies.push(latency);
        this.requestsCompleted++;
        this.printProgress();
    }

    addError() {
        this.errors++;
    }

    calculateLatencyStats(latencies) {
        if (latencies.length === 0) return null;
        
        const sorted = [...latencies].sort((a, b) => a - b);
        return {
            min: sorted[0],
            max: sorted[sorted.length - 1],
            avg: latencies.reduce((a, b) => a + b, 0) / latencies.length,
            p50: sorted[Math.floor(sorted.length * 0.5)],
            p95: sorted[Math.floor(sorted.length * 0.95)],
            p99: sorted[Math.floor(sorted.length * 0.99)]
        };
    }

    printProgress() {
        const now = Date.now();
        if (now - this.lastPrint >= 1000) { // Print every second
            const intervalRequests = this.requestsCompleted - this.lastRequests;
            const currentRps = intervalRequests;
            const overallRps = this.requestsCompleted / ((now - this.startTime) / 1000);
            
            // Calculate latency stats for current window
            const windowStats = this.calculateLatencyStats(this.currentWindowLatencies);
            
            // Clear console line and print new stats
            process.stdout.write('\r\x1b[K'); // Clear line
            
            let output = `Progress: ${this.requestsCompleted} requests, ` +
                        `Current RPS: ${currentRps.toFixed(2)}, ` +
                        `Overall RPS: ${overallRps.toFixed(2)}, ` +
                        `Errors: ${this.errors}`;
    
            if (windowStats) {
                output += ` | Latencies (ms) - ` +
                         `Avg: ${windowStats.avg.toFixed(2)}, ` +                    
                         `p50: ${windowStats.p50.toFixed(2)}, ` +                         
                         `p99: ${windowStats.p99.toFixed(2)}`;
            }
    
            process.stdout.write(output);
    
            // Reset window stats
            this.currentWindowLatencies = [];
            this.lastPrint = now;
            this.lastRequests = this.requestsCompleted;
        }
    }

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

// Main benchmark function
async function runBenchmark(config) {
    const stats = new BenchmarkStats();
    const qpsController = new QPSController(config);

    console.log('Valkey-GLIDE Benchmark');
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

    // Create client pool
    const clientPool = [];
    for (let i = 0; i < config.poolSize; i++) {
        const clientConfig = {
            addresses: [{
                host: config.host,
                port: config.port
            }],
            useTLS: config.useTls,
            readFrom: config.readFromReplica ? 'PREFER_REPLICA' : 'PRIMARY'
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
                            expect(await client.set(key, value)).toEqual("OK");
                } else if (config.command === 'get') {
                    const key = config.randomKeyspace > 0 
                        ? getRandomKey(config.randomKeyspace)
                        : 'somekey';
                    await client.get(key);
                } else if (config.command === 'custom') {
                    await config.customCommands.execute(client);
                }
                
                const latency = Date.now() - start;
                stats.addLatency(latency);
            } catch (error) {
                stats.addError();
                console.error(`Error in thread ${threadId}:`, error);
            }
        }
    });

    await Promise.all(workers);
    stats.printFinalStats();

    // Close all clients
    for (const client of clientPool) {
        await client.close();
    }
}

// Command line parsing
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
            describe: 'Command to benchmark',
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
            describe: 'QPS change amount per interval',
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
        .help()
        .argv;
}

// Main function
async function main() {
    const args = parseCommandLine();
    const config = new BenchmarkConfig();

    const CustomCommands = loadCustomCommands(args['custom-command-file']);
  
    Object.assign(config, {
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
        useTls: args.tls,
        isCluster: args.cluster,
        readFromReplica: args['read-from-replica'],
        customCommands: CustomCommands
    });

    if (config.useSequential && config.testDuration > 0) {
        console.error('Error: --sequential and --test-duration are mutually exclusive');
        process.exit(1);
    }

    await runBenchmark(config);
}

main().catch(console.error);
# Valkey Polyglot Benchmark

The node.js implementation of the Valkey Polyglot Benchmark provides a robust performance testing framework using the [valkey-GLIDE](https://github.com/valkey-io/valkey-glide) client library. This tool enables developers to conduct comprehensive benchmarks of Valkey operations, including throughput testing, latency measurements, and custom command evaluation. 

## Installation

1. Clone the repository:
    ```bash
    git clone <repository-url>
    cd valkey-glide-benchmark/node
    ```

2. Install dependencies:
    ```bash
    npm install
    ```

## Dependencies

This tool requires the following Node.js packages:
- `@valkey/valkey-glide`: Valkey GLIDE client library
- `hdr-histogram-js`: High Dynamic Range histogram for latency tracking
- `yargs`: Command-line argument parsing

Install them using:
```bash
npm install
```

## Basic Usage

Run a basic benchmark:
```bash
node valkey-benchmark.js -h localhost -p 6379
```

Common usage patterns:
```bash
# Run SET benchmark with 50 parallel clients
node valkey-benchmark.js -c 50 -t set

# Run GET benchmark with rate limiting
node valkey-benchmark.js -t get --qps 1000

# Run benchmark for specific duration
node valkey-benchmark.js --test-duration 60

# Run benchmark with sequential keys
node valkey-benchmark.js --sequential 1000000
```

## Configuration Options

### Basic Options
- `-h, --host <hostname>`: Server hostname (default: "127.0.0.1")
- `-p, --port <port>`: Server port (default: 6379)
- `-c, --clients <num>`: Number of parallel connections (default: 50)
- `-n, --requests <num>`: Total number of requests (default: 100000)
- `-d, --datasize <bytes>`: Data size for SET operations (default: 3)
- `-t, --type <command>`: Command to benchmark (e.g., SET, GET)

### Advanced Options
- `--threads <num>`: Number of worker threads (default: 1)
- `--test-duration <seconds>`: Run test for specified duration
- `--sequential <keyspace>`: Use sequential keys
- `--sequential-random-start`: Start each process/thread at a random offset in sequential keyspace (requires --sequential)
- `-r, --random <keyspace>`: Use random keys from keyspace (0 to keyspace)
- `--keyspace-offset <num>`: Starting point for keyspace range (default: 0). Works with both -r/--random and --sequential

### Rate Limiting Options
- `--qps <num>`: Limit queries per second
- `--start-qps <num>`: Starting QPS for dynamic rate
- `--end-qps <num>`: Target QPS for dynamic rate
- `--qps-change-interval <seconds>`: Interval for QPS changes
- `--qps-change <num>`: QPS change amount per interval (required for linear mode)
- `--qps-ramp-mode <mode>`: QPS ramp mode - `linear` (default) or `exponential`
  - In linear mode, QPS changes by a fixed amount each interval
  - In exponential mode, QPS grows/decays by the multiplier each interval
- `--qps-ramp-factor <factor>`: Multiplier for exponential QPS ramp (required for exponential mode)
  - E.g., 2.0 to double QPS each interval
  - QPS caps at end-qps and stays there for remaining duration

### Security Options
- `--tls`: Enable TLS connection

### Cluster Options
- `--cluster`: Use cluster client
- `--read-from-replica`: Read from replica nodes

### Timeout Options
- `--request-timeout <milliseconds>`: Request timeout in milliseconds
- `--connection-timeout <milliseconds>`: Connection timeout in milliseconds (TCP/TLS establishment)

### Multi-Process Options
- `--processes <num|auto>`: Number of worker processes (default: "auto" uses CPU count)
- `--single-process`: Force single-process mode (legacy behavior)

### Client Ramp-Up Options
These options allow gradual increase of client connections during the benchmark. All four must be specified together and are mutually exclusive with `-c/--clients`.
- `--clients-ramp-start <num>`: Initial number of clients per process for ramp-up
- `--clients-ramp-end <num>`: Target number of clients per process at end of ramp-up
- `--clients-per-ramp <num>`: Number of clients to add per ramp step
- `--client-ramp-interval <seconds>`: Time interval between client ramp steps

### CSV Output Options
- `--interval-metrics-interval-duration-sec <seconds>`: Emit CSV metrics every N seconds (enables CSV output mode)

### Logging Options
- `--debug`: Enable debug logging (equivalent to `--log-level DEBUG`)
- `--log-level <level>`: Set logging level - `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`
  - **By default**: Logging is **disabled** for performance (no overhead)
  - **DEBUG**: Detailed diagnostic information, including connection details, worker state, and all operations
  - **INFO**: General informational messages about benchmark progress and major operations
  - **WARNING**: Warning messages for potential issues
  - **ERROR**: Error messages for failures
  - **CRITICAL**: Critical errors only

### Custom Command Options
- `--custom-command-file <path>`: Path to custom command implementation file
- `--custom-command-args <string>`: Arguments to pass to custom command as a single string (e.g., "key1=value1,key2=value2")

## Test Scenarios

### Throughput Testing
```bash
# Maximum throughput test
node valkey-benchmark.js -c 100 -n 1000000

# Rate-limited test
node valkey-benchmark.js -c 50 --qps 5000
```

### Latency Testing
```bash
# Low-concurrency latency test
node valkey-benchmark.js -c 1 -n 10000

# High-concurrency latency test
node valkey-benchmark.js -c 200 -n 100000
```

### Duration-based Testing
```bash
# Run test for 5 minutes
node valkey-benchmark.js --test-duration 300
```

### Dynamic QPS Ramp-Up Testing
```bash
# Linear ramp-up: increase QPS from 1000 to 10000 over 60 seconds
# (QPS increases by 500 every 5 seconds)
node valkey-benchmark.js --test-duration 60 --start-qps 1000 --end-qps 10000 --qps-change-interval 5 --qps-change 500

# Exponential ramp-up: double QPS every 5 seconds until hitting 10000, then sustain
# Requires --qps-ramp-factor along with --qps-change-interval
node valkey-benchmark.js --test-duration 120 --start-qps 100 --end-qps 10000 --qps-change-interval 5 --qps-ramp-mode exponential --qps-ramp-factor 2.0
```

### Multi-Process Testing
```bash
# Use all available CPU cores (default)
node valkey-benchmark.js -n 1000000

# Use specific number of processes
node valkey-benchmark.js --processes 4 -n 1000000

# Force single-process mode (legacy behavior)
node valkey-benchmark.js --single-process -n 1000000
```

### Client Ramp-Up Testing
```bash
# Gradually increase clients from 10 to 100, adding 10 clients every 5 seconds
node valkey-benchmark.js --test-duration 60 --clients-ramp-start 10 --clients-ramp-end 100 --clients-per-ramp 10 --client-ramp-interval 5
```

### CSV Output
```bash
# Emit CSV metrics every 1 second
node valkey-benchmark.js --test-duration 60 --interval-metrics-interval-duration-sec 1

# CSV output with client ramp-up for load testing analysis
node valkey-benchmark.js --test-duration 120 --clients-ramp-start 10 --clients-ramp-end 200 --clients-per-ramp 10 --client-ramp-interval 5 --interval-metrics-interval-duration-sec 1
```

### Key Space Testing
```bash
# Sequential keys (generates keys from 0 to 999999)
node valkey-benchmark.js --sequential 1000000

# Sequential keys with random starting offset per worker/process
# (helps distribute load more evenly across clustered nodes)
node valkey-benchmark.js --sequential 1000000 --sequential-random-start

# Sequential keys with offset (generates keys from 2000001 to 3000000)
node valkey-benchmark.js --sequential 1000000 --keyspace-offset 2000001

# Random keys (generates keys from 0 to 1000000)
node valkey-benchmark.js -r 1000000

# Random keys with offset (generates keys from 2000001 to 4000001)
node valkey-benchmark.js -r 2000000 --keyspace-offset 2000001
```

### Logging and Debugging
```bash
# Enable debug logging for detailed diagnostics
node valkey-benchmark.js --debug -c 50 -n 10000

# Run with INFO level logging for less verbose output
node valkey-benchmark.js --log-level INFO -c 50 -n 10000

# CSV mode with debug logging (logs go to stderr, CSV to stdout)
node valkey-benchmark.js --interval-metrics-interval-duration-sec 5 --debug > metrics.csv 2> debug.log

# Both CSV and logs to separate files
node valkey-benchmark.js --interval-metrics-interval-duration-sec 5 --log-level INFO > metrics.csv 2> benchmark.log
```

## Output and Statistics
The benchmark tool provides real-time and final statistics including:

### Real-time Metrics
- Current throughput (requests/second)
- Overall throughput
- Average latency

### Final Report
- Total execution time
- Total requests completed
- Average throughput
- Latency percentiles (P50, P95, P99)
- Min/Max/Avg latencies

Example output:
```plaintext
Final Results:
=============
Total time: 20.01 seconds
Requests completed: 1365377
Requests per second: 68234.73
Total errors: 0

Latency Statistics (ms):
=====================
Minimum: 0.000
Average: 0.116
Maximum: 11.000
Median (p50): 0.000
95th percentile: 1.000
99th percentile: 1.000

Latency Distribution:
====================
<= 0.1 ms: 88.47% (1208014 requests)
<= 0.5 ms: 0.00% (0 requests)
<= 1.0 ms: 11.50% (157005 requests)
<= 2.0 ms: 0.00% (40 requests)
<= 5.0 ms: 0.01% (174 requests)
<= 10.0 ms: 0.01% (135 requests)
<= 20.0 ms: 0.00% (9 requests)
<= 50.0 ms: 0.00% (0 requests)
<= 100.0 ms: 0.00% (0 requests)
<= 200.0 ms: 0.00% (0 requests)
<= 500.0 ms: 0.00% (0 requests)
<= 1000.0 ms: 0.00% (0 requests)
```

## Custom Commands
To implement custom commands, create a custom command file. The module can export:
1. A class/constructor that receives args in constructor
2. An object with `createCustomCommands(args)` factory function
3. An object with `init(args)` method and `execute(client)` method

### Basic Custom Command (no arguments)
```javascript
// custom-commands.js - Simple object export
module.exports = {
    async execute(client) {
        try {
            await client.set('custom:key', 'custom:value');
            return true;
        } catch (error) {
            console.error('Custom command error:', error);
            return false;
        }
    }
};
```

### Custom Command with Arguments
```javascript
// custom-commands-with-args.js - Class export with argument parsing
class CustomCommands {
    constructor(args) {
        // Default configuration
        this.keyPrefix = 'myapp';
        this.batchSize = 1;
        this.counter = 0;

        // Parse arguments if provided (format: "key1=value1,key2=value2")
        if (args) {
            const pairs = args.split(',');
            for (const pair of pairs) {
                const [key, value] = pair.split('=');
                if (key === 'key_prefix') this.keyPrefix = value;
                if (key === 'batch_size') this.batchSize = parseInt(value, 10);
            }
        }

        console.log(`CustomCommands initialized: prefix=${this.keyPrefix}, batchSize=${this.batchSize}`);
    }

    async execute(client) {
        try {
            const key = `${this.keyPrefix}:key:${this.counter++}`;
            await client.set(key, `value:${this.counter}`);
            return true;
        } catch (error) {
            console.error('Custom command error:', error);
            return false;
        }
    }
}

module.exports = CustomCommands;
```

### Running Custom Commands
```bash
# Basic custom command
node valkey-benchmark.js -t custom --custom-command-file custom-commands.js

# Custom command with arguments
node valkey-benchmark.js -t custom --custom-command-file custom-commands-with-args.js --custom-command-args "key_prefix=test,batch_size=5"
```

### Sample Custom Commands
A complete example is provided in `sample_custom_commands.js`:

```bash
# Run with SET operation (default)
node valkey-benchmark.js -t custom --custom-command-file sample_custom_commands.js

# Run with MSET operation and batch size of 5
node valkey-benchmark.js -t custom \
    --custom-command-file sample_custom_commands.js \
    --custom-command-args "operation=mset,batch_size=5,key_prefix=test"

# Run with HSET operation
node valkey-benchmark.js -t custom \
    --custom-command-file sample_custom_commands.js \
    --custom-command-args "operation=hset,key_prefix=myhash"
```

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

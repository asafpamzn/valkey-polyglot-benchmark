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
To implement custom commands, create a custom command file:

```javascript
// Custom commands implementation
const CustomCommands = {
    async execute(client) {
        try {
            await client.set('custom:key', 'custom:value', {
                conditionalSet: "onlyIfDoesNotExist",
                returnOldValue: true});
            return true;
        } catch (error) {
            console.error('Custom command error:', error);
            return false;
        }
    }
};
```

Run custom command benchmark:
```bash
node valkey-benchmark.js -t custom --custom-command-file custom-commands.js
```

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

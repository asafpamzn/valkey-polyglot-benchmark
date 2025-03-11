
# Valkey GLIDE Benchmark Tool

A high-performance benchmark tool for Valkey using the [valkey-GLIDE](https://github.com/valkey-io/valkey-glide) client library. This tool supports various testing scenarios including throughput testing, latency measurements, and custom command benchmarking.


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
- `yargs`: Command-line argument parsing

Install them using:
```bash
npm install @valkey/valkey-glide yargs
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
- `--random <keyspace>`: Use random keys from keyspace

### Rate Limiting Options
- `--qps <num>`: Limit queries per second
- `--start-qps <num>`: Starting QPS for dynamic rate
- `--end-qps <num>`: Target QPS for dynamic rate
- `--qps-change-interval <seconds>`: Interval for QPS changes
- `--qps-change <num>`: QPS change amount per interval

### Security Options
- `--tls`: Enable TLS connection

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

### Key Space Testing
```bash
# Sequential keys
node valkey-benchmark.js --sequential 1000000

# Random keys
node valkey-benchmark.js -r 1000000
```

## Output and Statistics
The benchmark tool provides real-time and final statistics including:

### Real-time Metrics
- Current throughput (requests/second)
- Overall throughput
- Error count
- Progress percentage

### Final Report
- Total execution time
- Total requests completed
- Average throughput
- Latency percentiles (P50, P95, P99)
- Min/Max/Avg latencies
- Error count and rate

Example output:

```plaintext
Final Results:
Total time: 10.50 seconds
Requests completed: 50000
Requests per second: 4761.90
Total errors: 0

Latency (ms):
Min: 0.50
Avg: 2.10
Max: 15.30
P50: 1.90
P95: 4.20
P99: 8.10
```

## Custom Commands
To implement custom commands, modify the `CustomCommands` class in the benchmark code:

```javascript
const CustomCommands = {
    async execute(client) {
        // Implement your custom command logic here
        try {
            // Example custom command
            await client.set('custom:key', 'custom:value');
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
node valkey-benchmark.js -t custom --custom-command-file customem.js
```

## Error Handling
The tool provides error handling and reporting for:

- Connection failures
- Command execution errors
- Rate limiting violations
- Invalid configurations

Errors are logged to stderr and counted in the final statistics.

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

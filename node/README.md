
# Valkey GLIDE Benchmark Tool

A high-performance benchmark tool for Valkey using the GLIDE client library. This tool supports various testing scenarios including throughput testing, latency measurements, and custom command benchmarking.

## Table of Contents
- [Installation](#installation)
- [Dependencies](#dependencies)
- [Basic Usage](#basic-usage)
- [Configuration Options](#configuration-options)
- [Test Scenarios](#test-scenarios)
- [Output and Statistics](#output-and-statistics)
- [Custom Commands](#custom-commands)
- [Error Handling](#error-handling)
- [Contributing](#contributing)
- [License](#license)

## Installation

1. Clone the repository:
    ```bash
    git clone <repository-url>
    cd valkey-glide-benchmark
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
node benchmark.js -h localhost -p 6379
```

Common usage patterns:

```bash
# Run SET benchmark with 50 parallel clients
node benchmark.js -c 50 -t set

# Run GET benchmark with rate limiting
node benchmark.js -t get --qps 1000

# Run benchmark for specific duration
node benchmark.js --test-duration 60

# Run benchmark with sequential keys
node benchmark.js --sequential 1000000
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
node benchmark.js -c 100 -n 1000000

# Rate-limited test
node benchmark.js -c 50 --qps 5000
```

### Latency Testing
```bash
# Low-concurrency latency test
node benchmark.js -c 1 -n 10000

# High-concurrency latency test
node benchmark.js -c 200 -n 100000
```

### Duration-based Testing
```bash
# Run test for 5 minutes
node benchmark.js --test-duration 300
```

### Key Space Testing
```bash
# Sequential keys
node benchmark.js --sequential 1000000

# Random keys
node benchmark.js -r 1000000
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
node benchmark.js -t custom --custom-command-file customem.js
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

## License
MIT License

Copyright (c) 2024

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.


`.gitignore`:

node_modules/
.env
.DS_Store
*.log


Directory structure should look like:

valkey-benchmark/
├── README.md
├── package.json
├── .gitignore
└── benchmark.js


To use:
1. Create each file with the content provided
2. Run `npm install` to install dependencies
3. Run the benchmark tool using the commands described in the README

The benchmark.js file would be the one from the previous response. Would you like me to provide that again as well?
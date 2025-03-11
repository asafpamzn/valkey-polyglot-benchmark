# Valkey GLIDE Benchmark Tool

A high-performance benchmark tool for Valkey using the (#[valkey-GLIDE](https://github.com/valkey-io/valkey-glide)) client library. This tool supports various testing scenarios including throughput testing, latency measurements, and custom command benchmarking.

## Installation

1. Clone the repository:
    ```bash
    git clone <repository-url>
    cd valkey-glide-benchmark/java
    ```

2. Build the project:
    ```bash
    ./gradlew build
    ```

## Dependencies

This tool requires the following Java dependencies:
- `glide-api`: Valkey GLIDE client library

These dependencies are managed in the `build.gradle` file.

## Basic Usage

Run a basic benchmark:
```bash
./gradlew :runBenchmark --args="-h localhost -p 6379"
```
Common usage patterns:

```bash
# Run SET benchmark with 50 parallel clients
./gradlew :runBenchmark --args="-c 50 -t set"

# Run GET benchmark with rate limiting
./gradlew :runBenchmark --args="-t get --qps 1000"

# Run benchmark for specific duration
./gradlew :runBenchmark --args="--test-duration 60"

# Run benchmark with sequential keys
./gradlew :runBenchmark --args="--sequential 1000000"

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
./gradlew :runBenchmark --args="-c 100 -n 1000000"

# Rate-limited test
./gradlew :runBenchmark --args="-c 50 --qps 5000"
```

### Latency Testing
```bash
# Low-concurrency latency test
./gradlew :runBenchmark --args="-c 1 -n 10000"

# High-concurrency latency test
./gradlew :runBenchmark --args="-c 200 -n 100000"
```

### Duration-based Testing
```bash
# Run test for 5 minutes
./gradlew :runBenchmark --args="--test-duration 300"
```

### Key Space Testing
```bash
# Sequential keys
./gradlew :runBenchmark --args="--sequential 1000000"

# Random keys
./gradlew :runBenchmark --args="-r 1000000"
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
```java
static class CustomCommand {
    static boolean execute(GlideClient client) {
        // Implement your custom command logic here
        try {
            // Example custom command
            String result = client.set("custom:key", "custom:value").get();
            return "OK".equalsIgnoreCase(result);
        } catch (Exception e) {
            log(ERROR, "glide", "Custom command error: " + e.getMessage());
            return false;
        }
    }
}
```

Run custom command benchmark:

```bash
./gradlew :runBenchmark --args="-t custom"
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
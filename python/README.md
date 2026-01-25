# Valkey Polyglot Benchmark

The Python implementation of the Valkey Polyglot Benchmark provides a robust performance testing framework using the [valkey-GLIDE](https://github.com/valkey-io/valkey-glide) client library. This tool enables developers to conduct comprehensive benchmarks of Valkey operations, including throughput testing, latency measurements, and custom command evaluation. 

## Installation

1. Clone the repository:
    ```bash
    git clone <repository-url>
    cd valkey-glide-benchmark/python
    ```

2. Create and activate virtual environment (recommended):
    ```bash
    python -m venv myenv
    source myenv/bin/activate  # On Linux/Mac
    ```

3. Install dependencies:
    ```bash
    pip install valkey-glide
    ```

## Dependencies

This tool requires the following Python packages:
- `valkey-glide`: Valkey GLIDE client library

## Basic Usage

Run a basic benchmark:
```bash
python valkey-benchmark.py -H localhost -p 6379
```

Common usage patterns:
```bash
# Run SET benchmark with 50 parallel clients
python valkey-benchmark.py -c 50 -t set

# Run GET benchmark with rate limiting
python valkey-benchmark.py -t get --qps 1000

# Run benchmark for specific duration
python valkey-benchmark.py --test-duration 60

# Run benchmark with sequential keys
python valkey-benchmark.py --sequential 1000000
```

## Configuration Options

### Basic Options
- `-H, --host <hostname>`: Server hostname (default: "127.0.0.1")
- `-p, --port <port>`: Server port (default: 6379)
- `-c, --clients <num>`: Number of parallel connections (default: 50)
- `-n, --requests <num>`: Total number of requests (default: 100000)
- `-d, --datasize <bytes>`: Data size for SET operations (default: 3)
- `-t, --type <command>`: Command to benchmark (e.g., SET, GET)

### Advanced Options
- `--threads <num>`: Number of worker threads (default: 1)
- `--test-duration <seconds>`: Run test for specified duration
- `--sequential <keyspace>`: Use sequential keys
- `--sequential-random-start`: Start each process/client at a random offset in sequential keyspace (requires --sequential)
- `-r, --random <keyspace>`: Use random keys from keyspace (0 to keyspace)
- `--keyspace-offset <num>`: Starting point for keyspace range (default: 0). Must be used with either `-r`/`--random` or `--sequential`. Keys will be generated from offset to offset+keyspace

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
- `--request-timeout <milliseconds>`: Request timeout in milliseconds (time to wait for a request to complete, including sending the request, awaiting response, and any retries)
- `--connection-timeout <milliseconds>`: Connection timeout in milliseconds (time to wait for a TCP/TLS connection to establish during initial client creation or reconnection)

### Logging Options
- `--debug`: Enable debug logging (equivalent to `--log-level DEBUG`)
- `--log-level <level>`: Set logging level - `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`
  - **By default**: Logging is **disabled** for performance (no overhead)
  - **DEBUG**: Detailed diagnostic information, including connection details, worker state, and all operations
  - **INFO**: General informational messages about benchmark progress and major operations
  - **WARNING**: Warning messages and non-critical errors
  - **ERROR**: Error messages only
  - **CRITICAL**: Critical errors only

**Performance Note:** Logging is completely disabled by default to avoid any performance overhead. Only enable logging when you need visibility for debugging or monitoring purposes.

**Note:** In CSV output mode (`--interval-metrics-interval-duration-sec`), all logs are written to **stderr** to keep stdout clean for CSV data parsing.

### Client Ramp-Up Options
**Important: Client ramp-up operates at the per-process level.** Each worker process independently ramps up its own clients over time to prevent connection storms.

**Note:** This feature controls the number of **clients**, not connections directly. Each client opens 2 connections to each node in the topology, so the actual connection count is: `clients × 2 × number_of_nodes`.

**All four parameters must be specified together and are mutually exclusive with `--clients`:**

- `--clients-ramp-start <num>`: Initial number of clients per process at the beginning of ramp-up
- `--clients-ramp-end <num>`: Target number of clients per process at the end of ramp-up
- `--clients-per-ramp <num>`: Number of clients to add per ramp step (per process)
- `--client-ramp-interval <seconds>`: Time interval in seconds between client ramp steps

#### Understanding Connection Multipliers

**Critical Detail:** Each client opens **2 connections to each node** in the topology.

When using multi-process mode:
- Each process ramps from `--clients-ramp-start` to `--clients-ramp-end`
- **Total connections = clients × processes × 2 × number_of_nodes**

**Example Calculation (3-node cluster):**
- Configuration: `--processes=10 --clients-ramp-start=1 --clients-ramp-end=100`
- Initial state: 10 processes × 1 client × 2 connections × 3 nodes = **60 connections**
- Final state: 10 processes × 100 clients × 2 connections × 3 nodes = **6,000 connections**

#### Ramp-Up Behavior

Client ramp-up creates clients in batches at the process level:
1. Each process starts with `--clients-ramp-start` clients
2. The process adds `--clients-per-ramp` clients every `--client-ramp-interval` seconds
3. The ramp continues until reaching `--clients-ramp-end` clients
4. Workers start processing requests immediately with the initial clients
5. No inter-process coordination is required

### Multi-Process Options (NEW)
- `--processes <num|auto>`: Number of worker processes (default: auto = CPU cores). Overcomes Python's GIL limitation for multi-core utilization. Note: May have overhead on small instances; use `--single-process` for smaller workloads.
- `--single-process`: Force single-process mode (legacy behavior)

## Test Scenarios

### Throughput Testing
```bash
# Maximum throughput test
python valkey-benchmark.py -c 100 -n 1000000

# Rate-limited test
python valkey-benchmark.py -c 50 --qps 5000
```

### Latency Testing
```bash
# Low-concurrency latency test
python valkey-benchmark.py -c 1 -n 10000

# High-concurrency latency test
python valkey-benchmark.py -c 200 -n 100000
```

### Duration-based Testing
```bash
# Run test for 5 minutes
python valkey-benchmark.py --test-duration 300
```

### Dynamic QPS Ramp-Up Testing
```bash
# Linear ramp-up: increase QPS from 1000 to 10000 over 60 seconds
# (QPS increases by 500 every 5 seconds)
python valkey-benchmark.py --test-duration 60 --start-qps 1000 --end-qps 10000 --qps-change-interval 5 --qps-change 500

# Exponential ramp-up: double QPS every 5 seconds until hitting 10000, then sustain
# Requires --qps-ramp-factor along with --qps-change-interval
python valkey-benchmark.py --test-duration 120 --start-qps 100 --end-qps 10000 --qps-change-interval 5 --qps-ramp-mode exponential --qps-ramp-factor 2.0
```

### Client Ramp-Up Testing
**Purpose:** Prevent connection storms by gradually ramping up clients over time.

**Note:** All four ramp-up parameters must be specified together and are mutually exclusive with `--clients`.

```bash
# Single-process: Ramp from 1 to 100 clients, adding 10 clients every 2 seconds
# Total ramp time: ((100 - 1) / 10) * 2 = 19.8 seconds (10 ramp steps after initial batch)
python valkey-benchmark.py --single-process \
  --clients-ramp-start 1 \
  --clients-ramp-end 100 \
  --clients-per-ramp 10 \
  --client-ramp-interval 2

# Multi-process example: 10 processes, each ramping up clients from 1 to 100
# Each process adds 10 clients every 5 seconds
# For a 3-node cluster:
#   - Initial: 10 processes × 1 client × 2 connections × 3 nodes = 60 connections
#   - After first ramp: 10 processes × 11 clients × 2 × 3 = 660 connections  
#   - Final: 10 processes × 100 clients × 2 × 3 = 6,000 connections
#   - Total ramp time per process: ((100 - 1) / 10) * 5 = 49.5 seconds (10 ramp steps after initial batch)
python valkey-benchmark.py \
  --processes 10 \
  --clients-ramp-start 1 \
  --clients-ramp-end 100 \
  --clients-per-ramp 10 \
  --client-ramp-interval 5 \
  --test-duration 120

# Starting from higher baseline: Ramp from 20 to 200 clients
# Useful when you want to avoid very low client counts
python valkey-benchmark.py \
  --processes 4 \
  --clients-ramp-start 20 \
  --clients-ramp-end 200 \
  --clients-per-ramp 10 \
  --client-ramp-interval 5

# Slower, more gradual ramp: 5 clients every 10 seconds
# Useful for very large client counts or sensitive environments
python valkey-benchmark.py \
  --processes 4 \
  --clients-ramp-start 10 \
  --clients-ramp-end 200 \
  --clients-per-ramp 5 \
  --client-ramp-interval 10
```

### Key Space Testing
```bash
# Sequential keys (generates keys from 0 to 999999)
python valkey-benchmark.py --sequential 1000000

# Sequential keys with random starting offset per worker/process
# (helps distribute load more evenly across clustered nodes)
python valkey-benchmark.py --sequential 1000000 --sequential-random-start

# Sequential keys with offset (generates keys from 2000001 to 3000000)
python valkey-benchmark.py --sequential 1000000 --keyspace-offset 2000001

# Random keys (generates keys from 0 to 1000000)
python valkey-benchmark.py -r 1000000

# Random keys with offset (generates keys from 2000001 to 4000001)
python valkey-benchmark.py -r 2000000 --keyspace-offset 2000001
```

### Multi-Process Testing (NEW)
```bash
# Use all available CPU cores (default)
python valkey-benchmark.py -c 100 -n 1000000

# Use specific number of processes
python valkey-benchmark.py --processes 4 -c 100 -n 1000000

# Force single-process mode (for small instances or smaller workloads)
python valkey-benchmark.py --single-process -c 100 -n 1000000
```

### Debug and Logging Testing
```bash
# Run with debug logging to see detailed execution
python valkey-benchmark.py --debug -c 50 -n 10000

# Run with INFO level logging for less verbose output
python valkey-benchmark.py --log-level INFO -c 50 -n 10000

# CSV mode with debug logging (logs go to stderr)
python valkey-benchmark.py --interval-metrics-interval-duration-sec 5 --debug > metrics.csv 2> debug.log

# Monitor errors in real-time during long test
python valkey-benchmark.py --test-duration 300 --log-level WARNING 2>&1 | grep -i error
```

## Output and Statistics

The benchmark tool provides real-time and final statistics including:

### Real-time Metrics
```
[1.5s] Progress: 1,234/100,000 (1.2%), RPS: current=1,234 avg=1,230.5, Errors: 0 | Latency (ms): avg=0.12 p50=0.11 p95=0.15 p99=0.18
```

### Final Report
```
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
...
```

## Debugging and Troubleshooting

### Enable Debug Logging

**By default, logging is disabled for performance.** To enable logging for debugging:

```bash
# Enable debug logging
python valkey-benchmark.py --debug -c 50 -n 1000

# Or set specific log level
python valkey-benchmark.py --log-level INFO -c 50 -n 1000
```

Debug logging includes:
- Connection establishment details
- Client creation and pool management
- Worker thread startup and completion
- Client ramp-up progress
- QPS controller state changes
- Detailed error information with context

**Performance Impact:** Logging is completely disabled by default (no handlers, no overhead). Only enable it when needed for troubleshooting.

### CSV Mode with Logging

When using CSV output mode, logs are automatically redirected to stderr to keep CSV data clean:

```bash
# CSV output to file, logs to console
python valkey-benchmark.py --interval-metrics-interval-duration-sec 5 --debug > metrics.csv

# Both CSV and logs to separate files
python valkey-benchmark.py --interval-metrics-interval-duration-sec 5 --log-level INFO > metrics.csv 2> benchmark.log
```

### Understanding Error Types

The benchmark categorizes errors for better diagnostics:

- **MOVED errors**: Cluster topology changes, keys moved to different nodes
- **CLUSTERDOWN errors**: Cluster is unavailable or in failure state  
- **Generic errors**: Connection issues, timeouts, or command failures

All error types are tracked and reported in:
- CSV output (`requests_total_failed`, `requests_moved`, `requests_clusterdown`)
- Final statistics summary
- Log output (with ERROR or WARNING level)

### Common Issues

**Connection refused errors:**
```bash
# Check if server is running and reachable
redis-cli -h 127.0.0.1 -p 6379 ping

# Enable debug logging to see connection details
python valkey-benchmark.py --debug -H 127.0.0.1 -p 6379
```

**High error rates:**
```bash
# Monitor errors in real-time with CSV mode
python valkey-benchmark.py --interval-metrics-interval-duration-sec 1 --log-level WARNING

# Check logs for error patterns
python valkey-benchmark.py --log-level INFO 2>&1 | grep -i error
```

**No output in CSV mode:**
- Previously, there was no output when using `--interval-metrics-interval-duration-sec`. This has been fixed.
- CSV header and metrics are now printed to stdout
- Application logs (when enabled with `--debug` or `--log-level`) are printed to stderr
- By default, logging is disabled for performance. Enable it explicitly if you need visibility during execution.

## Custom Commands

To implement custom commands, create a Python file with your custom command implementation:

```python
# custom_commands.py
from typing import Any

class CustomCommands:
    def __init__(self, args=None):
        """Initialize with optional command-line arguments.
        
        Args:
            args (str, optional): Command-line arguments passed as a single string
        """
        self.counter = 0
        self.args = args
        if args:
            # Parse the arguments string as needed for your use case
            # Example: args could be "key_prefix=myapp,batch_size=10"
            print(f'Custom command initialized with args: {args}')

    async def execute(self, client: Any) -> bool:
        """Execute custom command with the client"""
        try:
            key = f'custom:key:{self.counter}'
            self.counter += 1
            
            await client.set(key, 'custom:value')
            return True
        except Exception as e:
            print(f'Custom command error: {str(e)}')
            return False
```

Run custom command benchmark:
```bash
# Without arguments
python valkey-benchmark.py -t custom --custom-command-file custom_commands.py

# With arguments
python valkey-benchmark.py -t custom --custom-command-file custom_commands.py --custom-command-args "key_prefix=myapp,batch_size=10"
```

### Sample Custom Commands

A complete example is available in `sample_custom_commands.py` which demonstrates:
- Parsing command-line arguments
- Different operation types (SET, MSET, HSET)
- Configurable batch sizes and key prefixes

Example usage:
```bash
# Run with MSET operation and batch size of 5
python valkey-benchmark.py -t custom \
    --custom-command-file sample_custom_commands.py \
    --custom-command-args "operation=mset,batch_size=5,key_prefix=test"
```


## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

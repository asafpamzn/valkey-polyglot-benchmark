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
- `-r, --random <keyspace>`: Use random keys from keyspace

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

### Multi-Process Options (NEW)
- `--processes <num|auto>`: Number of worker processes to use (default: auto = CPU cores)
  - Use `auto` to automatically use all available CPU cores
  - Specify a number (e.g., `--processes 4`) to use a specific number of processes
  - Multi-process mode overcomes Python's GIL limitation for true multi-core utilization
- `--single-process`: Force single-process mode (legacy behavior)
  - Use this flag to disable multi-process mode and run in the original single-process mode

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

### Key Space Testing
```bash
# Sequential keys
python valkey-benchmark.py --sequential 1000000

# Random keys
python valkey-benchmark.py -r 1000000
```

### Multi-Process Testing (NEW)
```bash
# Use all available CPU cores (default)
python valkey-benchmark.py -c 100 -n 1000000

# Use specific number of processes
python valkey-benchmark.py -c 100 -n 1000000 --processes 4

# Force single-process mode (legacy behavior)
python valkey-benchmark.py -c 100 -n 1000000 --single-process

# High-throughput test with multiple processes
python valkey-benchmark.py -c 200 -n 10000000 --processes 8 -t set

# Multi-process with QPS ramping
python valkey-benchmark.py --processes 4 --test-duration 120 --start-qps 1000 --end-qps 50000 --qps-change-interval 10 --qps-change 5000
```

## Multi-Process Architecture

The Python benchmark now supports a multi-process architecture to overcome Python's Global Interpreter Lock (GIL) limitation and achieve true multi-core utilization.

### Why Multi-Process?

Python's GIL prevents multiple native threads from executing Python bytecode simultaneously within a single process. This means that even with async workers or threading, a single Python process can only effectively utilize 1-2 CPU cores. For high-throughput benchmarking scenarios (e.g., targeting millions of QPS), this becomes a significant bottleneck.

The multi-process architecture solves this by:
- **True Parallelism**: Each process has its own Python interpreter and GIL, allowing N processes to fully utilize N CPU cores
- **Linear Scaling**: Throughput scales approximately linearly with the number of CPU cores
- **High Throughput**: Capable of generating millions of QPS on modern multi-core hardware
- **Transparent**: Workload and metrics are automatically distributed and aggregated

### How It Works

1. **Orchestrator Process**: Main process that:
   - Parses command-line arguments
   - Determines number of worker processes (default: CPU count)
   - Distributes workload across workers (requests, QPS, clients, threads)
   - Spawns worker processes
   - Aggregates metrics in real-time
   - Produces unified output

2. **Worker Processes**: Each worker:
   - Runs the existing async benchmark logic on a portion of the workload
   - Sends metrics to the orchestrator via a multiprocessing Queue
   - Operates independently with its own connections and state

3. **Metrics Aggregation**:
   - Progress metrics: Aggregated and displayed in real-time
   - CSV metrics: Per-interval metrics aggregated across all workers
   - Latency percentiles: Calculated from combined latency data
   - Final statistics: Complete view across all workers

### Usage Modes

- **Auto Mode (Default)**: `--processes auto` - Uses all available CPU cores
- **Specific Count**: `--processes 4` - Uses exactly 4 processes
- **Single-Process**: `--single-process` - Legacy mode, bypasses multi-process architecture

### Performance Impact

On a system with 8 CPU cores:
- **Single-process mode**: ~1-2 cores utilized, ~500K-1M QPS max
- **Multi-process mode (8 processes)**: All 8 cores utilized, 4M-8M QPS achievable

### Performance Considerations

**When to use single-process mode:**
- **Small instances or limited CPU resources**: Multi-process mode introduces context-switching overhead that can hurt performance on small instances (e.g., 2-4 cores or constrained environments)
- **Smaller workloads**: For benchmarks with low request counts or low target QPS, the overhead of spawning and coordinating multiple processes may outweigh the benefits
- **Recommendation**: Use `--single-process` flag for smaller workloads or when running on instances with limited CPU resources

**When to use multi-process mode:**
- **Large instances with many CPU cores**: Multi-process mode scales well on systems with 8+ cores
- **High-throughput testing**: When targeting millions of QPS or testing multi-core Valkey/Redis deployments
- **Long-running benchmarks**: The overhead is amortized over longer test durations

### Compatibility

- All existing CLI options work in multi-process mode
- Output format (including CSV) is preserved
- QPS ramping (linear/exponential) works correctly
- Graceful shutdown (Ctrl+C) supported

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

## Custom Commands

To implement custom commands, create a Python file with your custom command implementation:

```python
# custom_commands.py
from typing import Any

class CustomCommands:
    def __init__(self):
        self.counter = 0

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
python valkey-benchmark.py -t custom --custom-command-file custom_commands.py
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

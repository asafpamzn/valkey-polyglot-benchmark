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
- `--request-timeout <milliseconds>`: Request timeout in milliseconds

### Connection Ramp-Up Options
**Important: Connection ramp-up operates at the per-process level.** Each worker process independently ramps up its own connections over time to prevent connection storms.

- `--connections-per-ramp <num>`: Number of connections to add per ramp step (per process). Must be used with `--connection-ramp-interval`
- `--connection-ramp-interval <seconds>`: Time interval in seconds between connection ramp steps. Must be used with `--connections-per-ramp`

#### Understanding Connection Multipliers

**Critical Detail:** Each client (specified by `--clients`) opens **2 connections to each node** in the topology.

When using multi-process mode:
- Each process starts with at least 1 client
- Minimum total clients = number of processes
- **Total connections = clients × processes × 2 × number_of_nodes**

**Example Calculation (3-node cluster):**
- Configuration: `--processes=10 --clients=100`
- Initial state: 10 processes × 1 client × 2 connections × 3 nodes = **60 connections**
- Final state: 10 processes × 100 clients × 2 connections × 3 nodes = **6,000 connections**

#### Ramp-Up Behavior

Connection ramp-up creates connections in batches at the process level:
1. Each process independently creates its connections in batches
2. The process waits `--connection-ramp-interval` seconds between batches
3. Workers start processing requests as soon as the first batch of connections is available
4. No inter-process coordination is required

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

### Connection Ramp-Up Testing
**Purpose:** Prevent connection storms by gradually creating connections over time.

```bash
# Single-process: Ramp from 1 to 100 clients, adding 10 clients every 2 seconds
# Total ramp time: (100 / 10) * 2 = 20 seconds
python valkey-benchmark.py --single-process --clients 100 --connections-per-ramp 10 --connection-ramp-interval 2

# Multi-process example: 10 processes ramping up connections
# Each process creates 100 clients (10 at a time, every 5 seconds)
# For a 3-node cluster:
#   - Initial: 10 processes × 1 client × 2 connections × 3 nodes = 60 connections
#   - After first ramp: 10 processes × 10 clients × 2 × 3 = 600 connections  
#   - Final: 10 processes × 100 clients × 2 × 3 = 6,000 connections
#   - Total ramp time per process: (100 / 10) * 5 = 50 seconds
python valkey-benchmark.py \
  --processes 10 \
  --clients 100 \
  --connections-per-ramp 10 \
  --connection-ramp-interval 5 \
  --test-duration 120

# Slower, more gradual ramp: 5 connections every 10 seconds
# Useful for very large connection counts or sensitive environments
python valkey-benchmark.py \
  --processes 4 \
  --clients 200 \
  --connections-per-ramp 5 \
  --connection-ramp-interval 10
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

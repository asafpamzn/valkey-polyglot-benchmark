"""
Valkey-Polyglot Benchmark Tool
===========================

A comprehensive performance testing utility for Valkey/Redis operations using the Valkey GLIDE client.

"""

import os
import sys
import time
import random
import string
import argparse
from typing import List, Dict, Optional, Any
import asyncio
from glide import (
    GlideClient,
    GlideClientConfiguration,
    GlideClusterClient,
    GlideClusterClientConfiguration,
    NodeAddress,
    ReadFrom
)

class QPSController:
    """
    Controls and manages the rate of requests (Queries Per Second).
    
    This class implements both static and dynamic QPS control mechanisms,
    allowing for gradual QPS changes over time.

    Attributes:
        config (Dict): Configuration dictionary containing QPS settings
        current_qps (float): Current target QPS rate
        last_update (float): Timestamp of last QPS update
        requests_this_second (int): Counter for requests in current second
        second_start (float): Timestamp of current second start
    """

    def __init__(self, config: Dict):
        """
        Initialize the QPS controller.

        Args:
            config (Dict): Configuration dictionary containing:
                - start_qps: Initial QPS rate
                - qps: Target QPS rate
                - end_qps: Final QPS rate for dynamic adjustment
                - qps_change_interval: Interval for QPS changes
                - qps_change: Amount to change QPS by each interval
        """
        self.config = config
        self.current_qps = config.get('start_qps') or config.get('qps', 0)
        self.last_update = time.time()
        self.requests_this_second = 0
        self.second_start = time.time()

    async def throttle(self):
        """
        Throttles requests to maintain desired QPS rate.
        
        Implements dynamic QPS adjustment if configured and ensures
        request rate doesn't exceed the current QPS target.
        """
        if self.current_qps <= 0:
            return

        now = time.time()
        elapsed_since_last_update = now - self.last_update

        if self.config.get('start_qps') and self.config.get('end_qps'):
            if elapsed_since_last_update >= self.config['qps_change_interval']:
                diff = self.config['end_qps'] - self.current_qps
                if ((diff > 0 and self.config['qps_change'] > 0) or
                    (diff < 0 and self.config['qps_change'] < 0)):
                    self.current_qps += self.config['qps_change']
                    if ((self.config['qps_change'] > 0 and self.current_qps > self.config['end_qps']) or
                        (self.config['qps_change'] < 0 and self.current_qps < self.config['end_qps'])):
                        self.current_qps = self.config['end_qps']
                self.last_update = now

        elapsed_this_second = now - self.second_start
        if elapsed_this_second >= 1:
            self.requests_this_second = 0
            self.second_start = now

        if self.requests_this_second >= self.current_qps:
            wait_time = 1.0 - (now - self.second_start)
            if wait_time > 0:
                await asyncio.sleep(wait_time)
            self.requests_this_second = 0
            self.second_start = time.time()

        self.requests_this_second += 1

class BenchmarkStats:
    """
    Tracks and manages benchmark statistics and metrics.

    This class handles:
    - Latency measurements
    - Request counting
    - Error tracking
    - Real-time progress reporting
    - Statistical calculations
    
    Attributes:
        start_time (float): Benchmark start timestamp
        requests_completed (int): Total completed requests
        latencies (List[float]): List of all latency measurements
        errors (int): Total error count
        last_print (float): Last progress print timestamp
        last_requests (int): Request count at last print
        current_window_latencies (List[float]): Latencies in current window
        window_size (float): Size of measurement window in seconds
        total_requests (int): Total number of requests to perform
        test_start_time (float): Test start timestamp
    """

    def __init__(self, csv_file: Optional[str] = None):
        """
        Initialize the statistics tracker.
        
        Args:
            csv_file (Optional[str]): Path to CSV file for interval statistics output
        """
        self.start_time = time.time()
        self.requests_completed = 0
        self.latencies = []
        self.errors = 0
        self.last_print = time.time()
        self.last_requests = 0
        self.current_window_latencies = []
        self.window_size = 1.0  # 1 second window
        self.total_requests = 0
        self.test_start_time = time.time()
        self.csv_file = csv_file
        self.csv_handle = None
        self.csv_writer = None
        
        # Initialize CSV file if specified
        if self.csv_file:
            import csv
            try:
                self.csv_handle = open(self.csv_file, 'w', newline='')
                self.csv_writer = csv.writer(self.csv_handle)
                # Write header
                self.csv_writer.writerow(['timestamp', 'elapsed_seconds', 'qps', 'p50_ms', 'p90_ms', 'p99_ms', 'errors'])
                self.csv_handle.flush()
            except Exception as e:
                print(f'Warning: Could not open CSV file {self.csv_file}: {str(e)}', file=sys.stderr)
                self.csv_file = None
                self.csv_handle = None
                self.csv_writer = None

    def add_latency(self, latency: float):
        """
        Record a latency measurement and update statistics.

        Args:
            latency (float): Latency measurement in milliseconds
        """
        self.latencies.append(latency)
        self.current_window_latencies.append(latency)
        self.requests_completed += 1
        self.print_progress()

    def add_error(self):
        """Increment the error counter."""
        self.errors += 1

    @staticmethod
    def calculate_latency_stats(latencies: List[float]) -> Optional[Dict]:
        """
        Calculate statistical metrics for a set of latency measurements.

        Args:
            latencies (List[float]): List of latency measurements

        Returns:
            Optional[Dict]: Dictionary containing statistical metrics:
                - min: Minimum latency
                - max: Maximum latency
                - avg: Average latency
                - p50: 50th percentile (median)
                - p90: 90th percentile
                - p95: 95th percentile
                - p99: 99th percentile
            Returns None if input list is empty
        """
        if not latencies:
            return None

        sorted_latencies = sorted(latencies)
        return {
            'min': sorted_latencies[0],
            'max': sorted_latencies[-1],
            'avg': sum(latencies) / len(latencies),
            'p50': sorted_latencies[len(sorted_latencies) // 2],
            'p90': sorted_latencies[int(len(sorted_latencies) * 0.90)],
            'p95': sorted_latencies[int(len(sorted_latencies) * 0.95)],
            'p99': sorted_latencies[int(len(sorted_latencies) * 0.99)]
        }

    def print_progress(self):
        """
        Print real-time progress and statistics.
        
        Displays:
        - Elapsed time
        - Progress percentage
        - Current and average RPS
        - Error count
        - Recent latency statistics
        
        Also writes interval statistics to CSV file if configured.
        """
        now = time.time()
        if now - self.last_print >= 1:  # Print every second
            interval_requests = self.requests_completed - self.last_requests
            current_rps = interval_requests
            overall_rps = self.requests_completed / (now - self.start_time)
            elapsed_time = now - self.test_start_time

            window_stats = self.calculate_latency_stats(self.current_window_latencies)

            # Calculate progress percentage
            progress_pct = (self.requests_completed / self.total_requests * 100) if self.total_requests > 0 else 0

            # Format the output string
            output = (
                f"\r[{elapsed_time:.1f}s] "
                f"Progress: {self.requests_completed:,}/{self.total_requests:,} ({progress_pct:.1f}%), "
                f"RPS: current={current_rps:,} avg={overall_rps:,.1f}, "
                f"Errors: {self.errors}"
            )

            if window_stats:
                output += (
                    f" | Latency (ms): "
                    f"avg={window_stats['avg']:.2f} "
                    f"p50={window_stats['p50']:.2f} "
                    f"p95={window_stats['p95']:.2f} "
                    f"p99={window_stats['p99']:.2f}"
                )

            print(output, end='', flush=True)

            # Write to CSV if configured
            if self.csv_writer and window_stats:
                try:
                    self.csv_writer.writerow([
                        int(now),  # timestamp
                        round(elapsed_time, 2),  # elapsed_seconds
                        current_rps,  # qps
                        round(window_stats['p50'], 3),  # p50_ms
                        round(window_stats['p90'], 3),  # p90_ms
                        round(window_stats['p99'], 3),  # p99_ms
                        self.errors  # errors
                    ])
                    self.csv_handle.flush()
                except Exception as e:
                    print(f'\nWarning: Could not write to CSV: {str(e)}', file=sys.stderr)

            # Reset window stats
            self.current_window_latencies = []
            self.last_print = now
            self.last_requests = self.requests_completed

    def print_final_stats(self):
        """
        Print final benchmark results and detailed statistics.
        
        Displays:
        - Total execution time
        - Total requests completed
        - Final RPS
        - Error count
        - Detailed latency statistics
        - Latency distribution
        """
        total_time = time.time() - self.start_time
        final_rps = self.requests_completed / total_time

        final_stats = self.calculate_latency_stats(self.latencies)

        print('\n\nFinal Results:')
        print('=============')
        print(f'Total time: {total_time:.2f} seconds')
        print(f'Requests completed: {self.requests_completed}')
        print(f'Requests per second: {final_rps:.2f}')
        print(f'Total errors: {self.errors}')

        if final_stats:
            print('\nLatency Statistics (ms):')
            print('=====================')
            print(f"Minimum: {final_stats['min']:.3f}")
            print(f"Average: {final_stats['avg']:.3f}")
            print(f"Maximum: {final_stats['max']:.3f}")
            print(f"Median (p50): {final_stats['p50']:.3f}")
            print(f"95th percentile: {final_stats['p95']:.3f}")
            print(f"99th percentile: {final_stats['p99']:.3f}")

            print('\nLatency Distribution:')
            print('====================')
            ranges = [0.1, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000]
            current = 0
            for range_value in ranges:
                count = sum(1 for l in self.latencies if l <= range_value) - current
                percentage = (count / len(self.latencies) * 100)
                print(f'<= {range_value:.1f} ms: {percentage:.2f}% ({count} requests)')
                current += count

            remaining = len(self.latencies) - current
            if remaining > 0:
                percentage = (remaining / len(self.latencies) * 100)
                print(f'> 1000 ms: {percentage:.2f}% ({remaining} requests)')

    def set_total_requests(self, total: int):
        """
        Set the total number of requests to be performed.

        Args:
            total (int): Total number of requests
        """
        self.total_requests = total

    def close(self):
        """Close the CSV file if it's open."""
        if self.csv_handle:
            try:
                self.csv_handle.close()
            except Exception as e:
                print(f'Warning: Error closing CSV file: {str(e)}', file=sys.stderr)

def generate_random_data(size: int) -> str:
    """
    Generate random string data of specified size.

    Args:
        size (int): Size of random string to generate

    Returns:
        str: Random string of specified length
    """
    return ''.join(random.choices(string.ascii_uppercase, k=size))

def get_random_key(keyspace: int) -> str:
    """
    Generate a random key within the specified keyspace.

    Args:
        keyspace (int): Range for key generation

    Returns:
        str: Generated key in format 'key:{number}'
    """
    return f'key:{random.randint(0, keyspace - 1)}'

class RunningState:
    """
    Simple class to hold the running state of the benchmark.
    
    Attributes:
        value (bool): Current running state
    """
    def __init__(self, initial=True):
        """
        Initialize running state.

        Args:
            initial (bool): Initial state value
        """
        self.value = initial

async def run_benchmark(config: Dict):
    """
    Execute the benchmark with specified configuration.

    Args:
        config (Dict): Benchmark configuration parameters including:
            - host: Server hostname
            - port: Server port
            - pool_size: Connection pool size
            - num_threads: Number of worker threads
            - command: Benchmark command (set/get/custom)
            - data_size: Size of data for SET operations
            - output_csv: Optional CSV file path for interval statistics
            - And other configuration parameters
    """
    stats = BenchmarkStats(csv_file=config.get('output_csv'))
    stats.set_total_requests(config['total_requests'])
    qps_controller = QPSController(config)

    print('Valkey Benchmark')
    print(f"Host: {config['host']}")
    print(f"Port: {config['port']}")
    print(f"Threads: {config['num_threads']}")
    print(f"Total Requests: {config['total_requests']}")
    print(f"Data Size: {config['data_size']}")
    print(f"Command: {config['command']}")
    print(f"Is Cluster: {config['is_cluster']}")
    print(f"Read from Replica: {config['read_from_replica']}")
    print(f"Use TLS: {config['use_tls']}")
    print()

    # Create client pool
    client_pool = []
    for _ in range(config['pool_size']):
        addresses = [NodeAddress(host=config['host'], port=config['port'])]
        
        if config['is_cluster']:
            client_config = GlideClusterClientConfiguration(
                addresses=addresses,
                use_tls=config['use_tls'],
                read_from=ReadFrom.PREFER_REPLICA if config['read_from_replica'] else ReadFrom.PRIMARY
            )
            client = await GlideClusterClient.create(client_config)
        else:
            client_config = GlideClientConfiguration(
                addresses=addresses,
                use_tls=config['use_tls'],
                read_from=ReadFrom.PREFER_REPLICA if config['read_from_replica'] else ReadFrom.PRIMARY
            )
            client = await GlideClient.create(client_config)

        client_pool.append(client)

    async def worker(thread_id: int):
        """Worker function that executes benchmark operations."""
        data = generate_random_data(config['data_size']) if config['command'] == 'set' else None
        running = RunningState(True)

        test_duration = config.get('test_duration', 0)
        if test_duration:
            asyncio.create_task(
                asyncio.sleep(test_duration)
            ).add_done_callback(lambda _: setattr(running, 'value', False))

        while running.value and (test_duration > 0 or 
                         stats.requests_completed < config['total_requests']):
            client_index = stats.requests_completed % config['pool_size']
            client = client_pool[client_index]

            await qps_controller.throttle()

            start = time.time()
            try:
                if config['command'] == 'set':
                    key = (f"key:{stats.requests_completed % config['sequential_keyspacelen']}"
                          if config.get('use_sequential')
                          else get_random_key(config.get('random_keyspace', 0))
                          if config.get('random_keyspace', 0) > 0
                          else f"key:{thread_id}:{stats.requests_completed}")
                    await client.set(key, data)
                elif config['command'] == 'get':
                    key = (get_random_key(config.get('random_keyspace', 0))
                          if config.get('random_keyspace', 0) > 0
                          else "key:{thread_id}:{stats.requests_completed}")
                    await client.get(key)
                elif config['command'] == 'custom':
                    await config['custom_commands'].execute(client)

                latency = (time.time() - start) * 1000  # Convert to milliseconds
                stats.add_latency(latency)
            except Exception as e:
                stats.add_error()
                print(f'Error in thread {thread_id}: {str(e)}', file=sys.stderr)

    workers = [worker(i) for i in range(config['num_threads'])]
    await asyncio.gather(*workers)
    stats.print_final_stats()

    # Close all clients
    for client in client_pool:
        await client.close()
    
    # Close CSV file if open
    stats.close()

def parse_arguments() -> argparse.Namespace:
    """
    Parse and validate command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Valkey-Python Benchmark', 
                                   add_help=False)
    
    parser.add_argument('--help', action='help', default=argparse.SUPPRESS,
                       help='Show this help message and exit')
    
    # Basic options
    basic_group = parser.add_argument_group('Basic options')
    basic_group.add_argument('-H', '--host', default='127.0.0.1', 
                           help='Server hostname')
    basic_group.add_argument('-p', '--port', type=int, default=6379, 
                           help='Server port')
    basic_group.add_argument('-c', '--clients', type=int, default=50, 
                           help='Number of parallel connections')
    basic_group.add_argument('-n', '--requests', type=int, default=100000, 
                           help='Total number of requests')
    basic_group.add_argument('-d', '--datasize', type=int, default=3, 
                           help='Data size of value in bytes for SET')
    basic_group.add_argument('-t', '--type', default='set', 
                           help='Command to benchmark set, get, or custom')
    
    # Advanced options
    advanced_group = parser.add_argument_group('Advanced options')
    advanced_group.add_argument('-r', '--random', type=int, default=0, 
                              help='Use random keys from 0 to keyspacelen-1')
    advanced_group.add_argument('--threads', type=int, default=1, 
                              help='Number of worker threads')
    advanced_group.add_argument('--test-duration', type=int, 
                              help='Test duration in seconds')
    advanced_group.add_argument('--sequential', type=int, 
                              help='Use sequential keys')
    
    # QPS options
    qps_group = parser.add_argument_group('QPS options')
    qps_group.add_argument('--qps', type=int, 
                          help='Queries per second limit')
    qps_group.add_argument('--start-qps', type=int, 
                          help='Starting QPS for dynamic rate')
    qps_group.add_argument('--end-qps', type=int, 
                          help='Ending QPS for dynamic rate')
    qps_group.add_argument('--qps-change-interval', type=int, 
                          help='Interval for QPS changes in seconds')
    qps_group.add_argument('--qps-change', type=int, 
                          help='QPS change amount per interval')
    
    # Connection options
    conn_group = parser.add_argument_group('Connection options')
    conn_group.add_argument('--tls', action='store_true', 
                          help='Use TLS connection')
    conn_group.add_argument('--cluster', action='store_true', 
                          help='Use cluster client')
    conn_group.add_argument('--read-from-replica', action='store_true', 
                          help='Read from replica nodes')
    
    # Custom options
    custom_group = parser.add_argument_group('Custom options')
    custom_group.add_argument('--custom-command-file', 
                            help='Path to custom command implementation file')
    
    # Output options
    output_group = parser.add_argument_group('Output options')
    output_group.add_argument('--output-csv', 
                            help='Output interval statistics to CSV file (p50, p90, p99 latency and QPS per second)')
    
    return parser.parse_args()

def load_custom_commands(filepath: str = None) -> Any:
    """
    Load custom commands from file or return default implementation.

    Args:
        filepath (str, optional): Path to custom command implementation file

    Returns:
        Any: Object containing custom command implementation

    Raises:
        SystemExit: If custom command file cannot be loaded
    """
    if not filepath:
        class DefaultCommands:
            async def execute(self, client):
                try:
                    await client.set('default:key', 'default:value')
                    return True
                except Exception as e:
                    print(f'Default command error: {str(e)}')
                    return False
        return DefaultCommands()

    try:
        import importlib.util
        import os
        import sys

        abs_path = os.path.abspath(filepath)
        if not os.path.exists(abs_path):
            print(f"Custom command file not found: {abs_path}")
            sys.exit(1)

        module_name = "custom_commands"
        spec = importlib.util.spec_from_file_location(module_name, abs_path)
        if not spec or not spec.loader:
            raise ImportError(f"Could not load {filepath}")
            
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, 'CustomCommands'):
            raise AttributeError("Module must contain CustomCommands class")

        return module.CustomCommands()

    except Exception as e:
        print(f"Error loading custom commands: {str(e)}")
        sys.exit(1)

async def main():
    """
    Main application entry point.
    
    Initializes and runs the benchmark based on provided configuration.
    Handles command line parsing and benchmark execution.
    """
    args = parse_arguments()
    custom_commands = load_custom_commands(args.custom_command_file)
    
    config = {
        'host': args.host,
        'port': args.port,
        'pool_size': args.clients,
        'total_requests': args.requests,
        'data_size': args.datasize,
        'command': args.type,
        'random_keyspace': args.random or 0,
        'num_threads': args.threads,
        'test_duration': args.test_duration or 0,
        'use_sequential': bool(args.sequential),
        'sequential_keyspacelen': args.sequential or 0,
        'qps': args.qps or 0,
        'start_qps': args.start_qps or 0,
        'end_qps': args.end_qps or 0,
        'qps_change_interval': args.qps_change_interval or 0,
        'qps_change': args.qps_change or 0,
        'use_tls': bool(args.tls),
        'is_cluster': bool(args.cluster),
        'read_from_replica': bool(args.read_from_replica),
        'custom_commands': custom_commands,
        'output_csv': args.output_csv
    }

    if config['use_sequential'] and config['test_duration']:
        print('Error: --sequential and --test-duration are mutually exclusive', file=sys.stderr)
        sys.exit(1)

    if config['command'] == 'custom' and not config['custom_commands']:
        print("Error: Custom commands required but not provided")
        sys.exit(1)

    await run_benchmark(config)

if __name__ == '__main__':
    asyncio.run(main())

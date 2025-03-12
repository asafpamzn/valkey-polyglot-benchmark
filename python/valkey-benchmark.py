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
    def __init__(self, config: Dict):
        self.config = config
        self.current_qps = config.get('start_qps') or config.get('qps', 0)
        self.last_update = time.time()
        self.requests_this_second = 0
        self.second_start = time.time()

    async def throttle(self):
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
    def __init__(self):
        self.start_time = time.time()
        self.requests_completed = 0
        self.latencies = []
        self.errors = 0
        self.last_print = time.time()
        self.last_requests = 0
        self.current_window_latencies = []
        self.window_size = 1.0  # 1 second window
        self.total_requests = 0  # Add this line
        self.test_start_time = time.time()

    def add_latency(self, latency: float):
        self.latencies.append(latency)
        self.current_window_latencies.append(latency)
        self.requests_completed += 1
        self.print_progress()

    def add_error(self):
        self.errors += 1

    @staticmethod
    def calculate_latency_stats(latencies: List[float]):
        if not latencies:
            return None

        sorted_latencies = sorted(latencies)
        return {
            'min': sorted_latencies[0],
            'max': sorted_latencies[-1],
            'avg': sum(latencies) / len(latencies),
            'p50': sorted_latencies[len(sorted_latencies) // 2],
            'p95': sorted_latencies[int(len(sorted_latencies) * 0.95)],
            'p99': sorted_latencies[int(len(sorted_latencies) * 0.99)]
        }

    def print_progress(self):
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

            # Print with carriage return and flush
            print(output, end='', flush=True)

            # Reset window stats
            self.current_window_latencies = []
            self.last_print = now
            self.last_requests = self.requests_completed

    def print_final_stats(self):
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

    def set_total_requests(self, total):
        self.total_requests = total

def generate_random_data(size: int) -> str:
    return ''.join(random.choices(string.ascii_uppercase, k=size))

def get_random_key(keyspace: int) -> str:
    return f'key:{random.randint(0, keyspace - 1)}'

# Create a simple class to hold the running state
class RunningState:
    def __init__(self, initial=True):
        self.value = initial

async def run_benchmark(config: Dict):
    stats = BenchmarkStats()
    stats.set_total_requests(config['total_requests'])  # Set total requests
    qps_controller = QPSController(config)

    print('Valkey-GLIDE Benchmark')
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
        # Create proper configuration objects
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

    # Create worker tasks
    async def worker(thread_id: int):
        data = generate_random_data(config['data_size']) if config['command'] == 'set' else None
        running = RunningState(True)  # Use the RunningState class instead of bool

        # Fix the test duration check
        test_duration = config.get('test_duration', 0)
        if test_duration:
            asyncio.create_task(
                asyncio.sleep(test_duration)
            ).add_done_callback(lambda _: setattr(running, 'value', False))

        while running.value and (test_duration > 0 or  # Use running.value instead of running
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
                          else 'somekey')
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

def parse_arguments():
    # Create parser without the default help argument
    parser = argparse.ArgumentParser(description='Valkey-GLIDE-Python Benchmark', 
                                   add_help=False)
    
    # Add a custom help argument
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
                           help='Command to benchmark')
    
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
    
    return parser.parse_args()

def load_custom_commands(filepath: str = None) -> Any:
    """Load custom commands from file or return default implementation"""
    if not filepath:
        # Return default implementation
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

        # Get absolute path
        abs_path = os.path.abspath(filepath)
        if not os.path.exists(abs_path):
            print(f"Custom command file not found: {abs_path}")
            sys.exit(1)

        # Load module dynamically
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
        'custom_commands': custom_commands
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
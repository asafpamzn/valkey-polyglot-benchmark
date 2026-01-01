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
import multiprocessing
from multiprocessing import Queue, Event, Process
import queue
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
    allowing for gradual QPS changes over time. Supports both linear and
    exponential ramp modes.

    Attributes:
        config (Dict): Configuration dictionary containing QPS settings
        current_qps (float): Current target QPS rate
        last_update (float): Timestamp of last QPS update
        requests_this_second (int): Counter for requests in current second
        second_start (float): Timestamp of current second start
        exponential_multiplier (float): Multiplier for exponential ramp mode
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
                - qps_change: Amount to change QPS by each interval (for linear mode)
                - qps_ramp_mode: 'linear' or 'exponential'
        """
        self.config = config
        self.last_update = time.time()
        self.requests_this_second = 0
        self.second_start = time.time()
        self.exponential_multiplier = 1.0
        
        qps_ramp_mode = config.get('qps_ramp_mode', 'linear')
        start_qps = config.get('start_qps', 0)
        end_qps = config.get('end_qps', 0)
        qps = config.get('qps', 0)
        qps_change_interval = config.get('qps_change_interval', 0)
        
        # Determine initial QPS: use start_qps if set, otherwise fall back to qps or end_qps
        if start_qps > 0:
            self.current_qps = start_qps
        elif qps > 0:
            self.current_qps = qps
        elif end_qps > 0:
            # For ramp-up modes without start_qps, use end_qps as initial value
            self.current_qps = end_qps
            print("Warning: start_qps not set for ramp mode, using end_qps as initial QPS", file=sys.stderr)
        else:
            self.current_qps = 0
        
        # Validate start_qps if ramp mode is configured
        if qps_change_interval > 0 and end_qps > 0:
            if start_qps <= 0:
                print("Warning: start_qps must be positive for QPS ramping. Using end_qps as fallback.", file=sys.stderr)
                # Use a local effective_start_qps instead of modifying config
                start_qps = end_qps
        
        # Store effective start_qps for later use in throttle
        self._effective_start_qps = start_qps if start_qps > 0 else end_qps
        
        # For exponential mode, use the provided multiplier
        if qps_ramp_mode == 'exponential' and \
           start_qps > 0 and end_qps > 0 and \
           qps_change_interval > 0:
            
            # Exponential mode requires --qps-ramp-factor
            qps_ramp_factor = config.get('qps_ramp_factor', 0)
            if qps_ramp_factor > 0:
                self.exponential_multiplier = qps_ramp_factor
                # Warn if factor < 1 (causes ramp-down instead of ramp-up)
                if qps_ramp_factor < 1:
                    print("Warning: qps_ramp_factor < 1 will cause QPS to decrease (ramp-down) each interval", file=sys.stderr)
            else:
                print("Error: exponential mode requires --qps-ramp-factor to be specified", file=sys.stderr)
                sys.exit(1)

    async def throttle(self):
        """
        Throttles requests to maintain desired QPS rate.
        
        Implements dynamic QPS adjustment if configured and ensures
        request rate doesn't exceed the current QPS target.
        Supports both linear and exponential ramp modes.
        """
        if self.current_qps <= 0:
            return

        now = time.time()
        elapsed_since_last_update = now - self.last_update
        
        qps_ramp_mode = self.config.get('qps_ramp_mode', 'linear')
        is_exponential = qps_ramp_mode == 'exponential'
        
        has_dynamic_qps = self.config.get('start_qps') and self.config.get('end_qps') and \
                          self.config.get('qps_change_interval', 0) > 0
        
        # For linear mode, also require qps_change
        if not is_exponential:
            has_dynamic_qps = has_dynamic_qps and self.config.get('qps_change', 0) != 0

        if has_dynamic_qps:
            if elapsed_since_last_update >= self.config['qps_change_interval']:
                if is_exponential:
                    # Exponential mode: multiply by the computed multiplier
                    new_qps = int(round(self.current_qps * self.exponential_multiplier))
                    
                    # Clamp to end_qps
                    if self.config['end_qps'] > self.config['start_qps']:
                        # Increasing QPS
                        if new_qps > self.config['end_qps']:
                            new_qps = self.config['end_qps']
                    else:
                        # Decreasing QPS
                        if new_qps < self.config['end_qps']:
                            new_qps = self.config['end_qps']
                    self.current_qps = new_qps
                else:
                    # Linear mode: add qps_change
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
        metrics_queue (Queue): Queue for sending metrics to orchestrator (multi-process mode)
        worker_id (int): Worker ID for multi-process mode
    """

    def __init__(self, csv_interval_sec=None, metrics_queue=None, worker_id=0):
        """Initialize the statistics tracker.
        
        Args:
            csv_interval_sec (int, optional): If set, enables CSV output mode
            metrics_queue (Queue, optional): Queue for sending metrics to orchestrator
            worker_id (int): Worker ID for identification in multi-process mode
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
        
        # Multi-process mode attributes
        self.metrics_queue = metrics_queue
        self.worker_id = worker_id
        
        # CSV interval metrics tracking
        self.csv_interval_sec = csv_interval_sec
        self.csv_mode = csv_interval_sec is not None
        self.interval_start_time = time.time()
        self.interval_latencies = []
        self.interval_errors = 0
        self.interval_moved = 0
        self.interval_clusterdown = 0
        self.interval_disconnects = 0
        self.interval_requests = 0
        self.csv_header_printed = False

    def add_latency(self, latency: float):
        """
        Record a latency measurement and update statistics.

        Args:
            latency (float): Latency measurement in milliseconds
        """
        self.latencies.append(latency)
        self.current_window_latencies.append(latency)
        self.requests_completed += 1
        
        if self.csv_mode:
            self.interval_latencies.append(latency)
            self.interval_requests += 1
            
            # In multi-process mode, send interval metrics to orchestrator
            if self.metrics_queue is not None:
                self.check_csv_interval_multiprocess()
            else:
                self.check_csv_interval()
        else:
            # In multi-process mode, send progress metrics periodically
            if self.metrics_queue is not None:
                self.send_progress_metrics()
            else:
                self.print_progress()

    def add_error(self):
        """Increment the error counter."""
        self.errors += 1
        if self.csv_mode:
            self.interval_errors += 1
    
    def add_moved(self):
        """Increment the MOVED response counter."""
        if self.csv_mode:
            self.interval_moved += 1
    
    def add_clusterdown(self):
        """Increment the CLUSTERDOWN response counter."""
        if self.csv_mode:
            self.interval_clusterdown += 1
    
    def add_disconnect(self):
        """Increment the client disconnect counter."""
        if self.csv_mode:
            self.interval_disconnects += 1
    
    def print_csv_header(self):
        """Print CSV header line (once at start)."""
        if not self.csv_header_printed:
            print("timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,requests_total_failed,requests_moved,requests_clusterdown,client_disconnects", flush=True)
            self.csv_header_printed = True
    
    def calculate_percentile_usec(self, sorted_latencies: List[float], percentile: float) -> int:
        """
        Calculate percentile from sorted latencies in microseconds (truncated).
        
        Args:
            sorted_latencies: List of latencies in milliseconds (sorted)
            percentile: Percentile value (0-100)
        
        Returns:
            int: Percentile value in microseconds (truncated)
        """
        if not sorted_latencies:
            return 0
        
        idx = int(len(sorted_latencies) * percentile / 100.0)
        if idx >= len(sorted_latencies):
            idx = len(sorted_latencies) - 1
        
        # Convert milliseconds to microseconds and truncate (not round)
        return int(sorted_latencies[idx] * 1000)
    
    def emit_csv_line(self):
        """Emit a CSV data line for the current interval."""
        now = time.time()
        interval_duration = now - self.interval_start_time
        
        # Calculate timestamp (Unix epoch seconds)
        timestamp = int(now)
        
        # Calculate request_sec for this interval
        if interval_duration > 0:
            request_sec = self.interval_requests / interval_duration
        else:
            request_sec = 0.0
        
        # Calculate percentiles from interval latencies
        if self.interval_latencies:
            sorted_lats = sorted(self.interval_latencies)
            p50 = self.calculate_percentile_usec(sorted_lats, 50)
            p90 = self.calculate_percentile_usec(sorted_lats, 90)
            p95 = self.calculate_percentile_usec(sorted_lats, 95)
            p99 = self.calculate_percentile_usec(sorted_lats, 99)
            p99_9 = self.calculate_percentile_usec(sorted_lats, 99.9)
            p99_99 = self.calculate_percentile_usec(sorted_lats, 99.99)
            p99_999 = self.calculate_percentile_usec(sorted_lats, 99.999)
            p100 = int(sorted_lats[-1] * 1000)  # max in microseconds
            avg = int(sum(sorted_lats) / len(sorted_lats) * 1000)  # avg in microseconds
        else:
            p50 = p90 = p95 = p99 = p99_9 = p99_99 = p99_999 = p100 = avg = 0
        
        # Output CSV line with exactly 15 fields
        print(f"{timestamp},{request_sec:.6f},{p50},{p90},{p95},{p99},{p99_9},{p99_99},{p99_999},{p100},{avg},{self.interval_errors},{self.interval_moved},{self.interval_clusterdown},{self.interval_disconnects}", flush=True)
        
        # Reset interval counters
        self.interval_start_time = now
        self.interval_latencies = []
        self.interval_errors = 0
        self.interval_moved = 0
        self.interval_clusterdown = 0
        self.interval_disconnects = 0
        self.interval_requests = 0
    
    def check_csv_interval(self):
        """Check if it's time to emit a CSV line."""
        if self.csv_mode:
            now = time.time()
            if now - self.interval_start_time >= self.csv_interval_sec:
                self.emit_csv_line()
    
    def check_csv_interval_multiprocess(self):
        """Check if it's time to send CSV metrics to orchestrator in multi-process mode."""
        if self.csv_mode and self.metrics_queue is not None:
            now = time.time()
            if now - self.interval_start_time >= self.csv_interval_sec:
                self.send_csv_metrics()
    
    def send_csv_metrics(self):
        """Send CSV interval metrics to orchestrator via queue."""
        if self.metrics_queue is None:
            return
        
        now = time.time()
        interval_duration = now - self.interval_start_time
        
        # Send metrics to orchestrator
        metrics = {
            'type': 'csv_interval',
            'worker_id': self.worker_id,
            'timestamp': int(now),
            'interval_duration': interval_duration,
            'interval_latencies': self.interval_latencies.copy(),
            'interval_requests': self.interval_requests,
            'interval_errors': self.interval_errors,
            'interval_moved': self.interval_moved,
            'interval_clusterdown': self.interval_clusterdown,
            'interval_disconnects': self.interval_disconnects
        }
        
        try:
            self.metrics_queue.put(metrics, block=False)
        except (queue.Full, Exception):
            pass  # Queue full, skip this metric
        
        # Reset interval counters
        self.interval_start_time = now
        self.interval_latencies = []
        self.interval_errors = 0
        self.interval_moved = 0
        self.interval_clusterdown = 0
        self.interval_disconnects = 0
        self.interval_requests = 0
    
    def send_progress_metrics(self):
        """Send progress metrics to orchestrator in multi-process mode."""
        if self.metrics_queue is None:
            return
        
        now = time.time()
        if now - self.last_print >= 1:  # Send every second
            metrics = {
                'type': 'progress',
                'worker_id': self.worker_id,
                'requests_completed': self.requests_completed,
                'errors': self.errors,
                'current_window_latencies': self.current_window_latencies.copy(),
                'timestamp': now
            }
            
            try:
                self.metrics_queue.put(metrics, block=False)
            except (queue.Full, Exception):
                pass  # Queue full, skip this metric
            
            # Reset window stats
            self.current_window_latencies = []
            self.last_print = now
            self.last_requests = self.requests_completed
    
    def send_final_metrics(self):
        """Send final metrics to orchestrator at the end of benchmark."""
        if self.metrics_queue is None:
            return
        
        metrics = {
            'type': 'final',
            'worker_id': self.worker_id,
            'requests_completed': self.requests_completed,
            'errors': self.errors,
            'latencies': self.latencies.copy(),
            'total_time': time.time() - self.start_time
        }
        
        try:
            self.metrics_queue.put(metrics, block=True, timeout=5)
        except (queue.Full, Exception):
            pass  # Timeout or queue full, but we tried

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

async def run_benchmark(config: Dict, metrics_queue=None, shutdown_event=None, worker_id=0):
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
            - And other configuration parameters
        metrics_queue (Queue, optional): Queue for sending metrics to orchestrator
        shutdown_event (Event, optional): Event to signal shutdown
        worker_id (int): Worker ID for identification
    """
    stats = BenchmarkStats(
        csv_interval_sec=config.get('csv_interval_sec'),
        metrics_queue=metrics_queue,
        worker_id=worker_id
    )
    stats.set_total_requests(config['total_requests'])
    qps_controller = QPSController(config)

    # Only print banner if not in CSV mode and not in multi-process mode
    if not stats.csv_mode and metrics_queue is None:
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
    elif stats.csv_mode and metrics_queue is None:
        # In CSV mode, print header to stdout (only in single-process mode)
        stats.print_csv_header()

    # Create client pool
    client_pool = []
    for _ in range(config['pool_size']):
        addresses = [NodeAddress(host=config['host'], port=config['port'])]
        
        if config['is_cluster']:
            client_config = GlideClusterClientConfiguration(
                addresses=addresses,
                use_tls=config['use_tls'],
                read_from=ReadFrom.PREFER_REPLICA if config['read_from_replica'] else ReadFrom.PRIMARY,
                request_timeout=config['request_timeout']
            )
            client = await GlideClusterClient.create(client_config)
        else:
            client_config = GlideClientConfiguration(
                addresses=addresses,
                use_tls=config['use_tls'],
                read_from=ReadFrom.PREFER_REPLICA if config['read_from_replica'] else ReadFrom.PRIMARY,
                request_timeout=config['request_timeout']
            )
            client = await GlideClient.create(client_config)

        client_pool.append(client)

    async def worker(thread_id: int):
        """Worker function that executes benchmark operations."""
        data = generate_random_data(config['data_size']) if config['command'] == 'set' else None
        running = RunningState(True)
        
        # Generate random starting offset if sequential-random-start is enabled
        sequential_offset = 0
        if config.get('use_sequential') and config.get('sequential_random_start'):
            sequential_offset = random.randint(0, config['sequential_keyspacelen'] - 1)

        test_duration = config.get('test_duration', 0)
        if test_duration:
            asyncio.create_task(
                asyncio.sleep(test_duration)
            ).add_done_callback(lambda _: setattr(running, 'value', False))

        while running.value and (test_duration > 0 or 
                         stats.requests_completed < config['total_requests']):
            # Check for shutdown signal from orchestrator
            if shutdown_event is not None and shutdown_event.is_set():
                break
            
            client_index = stats.requests_completed % config['pool_size']
            client = client_pool[client_index]

            await qps_controller.throttle()

            start = time.time()
            try:
                if config['command'] == 'set':
                    key = (f"key:{(sequential_offset + stats.requests_completed) % config['sequential_keyspacelen']}"
                          if config.get('use_sequential')
                          else get_random_key(config.get('random_keyspace', 0))
                          if config.get('random_keyspace', 0) > 0
                          else f"key:{thread_id}:{stats.requests_completed}")
                    await client.set(key, data)
                elif config['command'] == 'get':
                    key = (f"key:{(sequential_offset + stats.requests_completed) % config['sequential_keyspacelen']}"
                          if config.get('use_sequential')
                          else get_random_key(config.get('random_keyspace', 0))
                          if config.get('random_keyspace', 0) > 0
                          else f"key:{thread_id}:{stats.requests_completed}")
                    await client.get(key)
                elif config['command'] == 'custom':
                    await config['custom_commands'].execute(client)

                latency = (time.time() - start) * 1000  # Convert to milliseconds
                stats.add_latency(latency)
            except Exception as e:
                error_msg = str(e).upper()
                if 'MOVED' in error_msg:
                    stats.add_moved()
                elif 'CLUSTERDOWN' in error_msg:
                    stats.add_clusterdown()
                stats.add_error()
                
                # In CSV mode, we still need to check if it's time to emit a line
                # even when there are only errors
                if stats.csv_mode:
                    if metrics_queue is not None:
                        stats.check_csv_interval_multiprocess()
                    else:
                        stats.check_csv_interval()
                elif metrics_queue is None:
                    print(f'Error in thread {thread_id}: {str(e)}', file=sys.stderr)

    workers = [worker(i) for i in range(config['num_threads'])]
    await asyncio.gather(*workers)
    
    # Send final CSV metrics or emit final CSV line
    if stats.csv_mode:
        if metrics_queue is not None:
            # Multi-process mode: send any remaining interval data
            if stats.interval_latencies or stats.interval_errors > 0 or \
               stats.interval_moved > 0 or stats.interval_clusterdown > 0:
                stats.send_csv_metrics()
        else:
            # Single-process mode: emit final CSV line if there's any data
            if stats.interval_latencies or stats.interval_errors > 0 or \
               stats.interval_moved > 0 or stats.interval_clusterdown > 0:
                stats.emit_csv_line()
    
    # Send final metrics in multi-process mode
    if metrics_queue is not None:
        stats.send_final_metrics()
    
    # Only print final stats if not in CSV mode and not in multi-process mode
    if not stats.csv_mode and metrics_queue is None:
        stats.print_final_stats()

    # Close all clients
    for client in client_pool:
        await client.close()

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
    advanced_group.add_argument('--sequential-random-start', action='store_true',
                              help='Start each process/client at a random offset in sequential keyspace (requires --sequential)')
    
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
                          help='QPS change amount per interval (linear mode only)')
    qps_group.add_argument('--qps-ramp-mode', type=str, default='linear',
                          choices=['linear', 'exponential'],
                          help='QPS ramp mode: linear or exponential (default: linear)')
    qps_group.add_argument('--qps-ramp-factor', type=float,
                          help='Explicit multiplier for exponential QPS ramp (e.g., 2.0 to double QPS each interval). If not provided, factor is auto-calculated.')
    
    # Connection options
    conn_group = parser.add_argument_group('Connection options')
    conn_group.add_argument('--tls', action='store_true', 
                          help='Use TLS connection')
    conn_group.add_argument('--cluster', action='store_true', 
                          help='Use cluster client')
    conn_group.add_argument('--read-from-replica', action='store_true', 
                          help='Read from replica nodes')
    conn_group.add_argument('--request-timeout', type=int, default=None,
                          help='Request timeout in milliseconds')
    
    # Custom options
    custom_group = parser.add_argument_group('Custom options')
    custom_group.add_argument('--custom-command-file', 
                            help='Path to custom command implementation file')
    custom_group.add_argument('--custom-command-args',
                            help='Arguments to pass to custom command as a single string')
    
    # Metrics options
    metrics_group = parser.add_argument_group('Metrics options')
    metrics_group.add_argument('--interval-metrics-interval-duration-sec', type=int,
                             help='Emit CSV metrics every N seconds (enables CSV output mode)')
    
    # Multi-process options
    multiprocess_group = parser.add_argument_group('Multi-process options')
    multiprocess_group.add_argument('--processes', type=str, default='auto',
                                   help='Number of processes to use (default: auto = CPU cores, or specify a number)')
    multiprocess_group.add_argument('--single-process', action='store_true',
                                   help='Force single-process mode (legacy behavior)')
    
    return parser.parse_args()

def load_custom_commands(filepath: str = None, args: str = None) -> Any:
    """
    Load custom commands from file or return default implementation.

    Args:
        filepath (str, optional): Path to custom command implementation file
        args (str, optional): Arguments to pass to custom command initializer

    Returns:
        Any: Object containing custom command implementation

    Raises:
        SystemExit: If custom command file cannot be loaded
    """
    if not filepath:
        class DefaultCommands:
            def __init__(self, args=None):
                self.args = args
            
            async def execute(self, client):
                try:
                    await client.set('default:key', 'default:value')
                    return True
                except Exception as e:
                    print(f'Default command error: {str(e)}')
                    return False
        return DefaultCommands(args)

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

        return module.CustomCommands(args)

    except Exception as e:
        print(f"Error loading custom commands: {str(e)}")
        sys.exit(1)

def worker_process_entry(config: Dict, metrics_queue: Queue, shutdown_event: Event, worker_id: int):
    """
    Entry point for worker processes.
    
    Args:
        config (Dict): Benchmark configuration
        metrics_queue (Queue): Queue for sending metrics to orchestrator
        shutdown_event (Event): Event to signal shutdown
        worker_id (int): Worker ID for identification
    """
    try:
        asyncio.run(run_benchmark(config, metrics_queue, shutdown_event, worker_id))
    except KeyboardInterrupt:
        pass
    except Exception as e:
        print(f"Worker {worker_id} error: {str(e)}", file=sys.stderr)

def aggregate_csv_metrics(worker_metrics: List[Dict]) -> Optional[Dict]:
    """
    Aggregate CSV metrics from multiple workers for a single interval.
    
    Args:
        worker_metrics (List[Dict]): List of metric dictionaries from workers
    
    Returns:
        Optional[Dict]: Aggregated metrics or None if no data
    """
    if not worker_metrics:
        return None
    
    # Aggregate latencies and counters
    all_latencies = []
    total_requests = 0
    total_errors = 0
    total_moved = 0
    total_clusterdown = 0
    total_disconnects = 0
    total_duration = 0
    
    for metrics in worker_metrics:
        all_latencies.extend(metrics['interval_latencies'])
        total_requests += metrics['interval_requests']
        total_errors += metrics['interval_errors']
        total_moved += metrics['interval_moved']
        total_clusterdown += metrics['interval_clusterdown']
        total_disconnects += metrics['interval_disconnects']
        total_duration += metrics['interval_duration']
    
    # Use average duration
    avg_duration = total_duration / len(worker_metrics) if worker_metrics else 0
    
    return {
        'latencies': all_latencies,
        'requests': total_requests,
        'errors': total_errors,
        'moved': total_moved,
        'clusterdown': total_clusterdown,
        'disconnects': total_disconnects,
        'duration': avg_duration
    }

def emit_aggregated_csv_line(timestamp: int, aggregated: Dict):
    """
    Emit a CSV line from aggregated metrics.
    
    Args:
        timestamp (int): Unix timestamp
        aggregated (Dict): Aggregated metrics
    """
    # Calculate request_sec
    if aggregated['duration'] > 0:
        request_sec = aggregated['requests'] / aggregated['duration']
    else:
        request_sec = 0.0
    
    # Calculate percentiles
    if aggregated['latencies']:
        sorted_lats = sorted(aggregated['latencies'])
        # Create a temporary BenchmarkStats instance for percentile calculation
        temp_stats = BenchmarkStats()
        p50 = temp_stats.calculate_percentile_usec(sorted_lats, 50)
        p90 = temp_stats.calculate_percentile_usec(sorted_lats, 90)
        p95 = temp_stats.calculate_percentile_usec(sorted_lats, 95)
        p99 = temp_stats.calculate_percentile_usec(sorted_lats, 99)
        p99_9 = temp_stats.calculate_percentile_usec(sorted_lats, 99.9)
        p99_99 = temp_stats.calculate_percentile_usec(sorted_lats, 99.99)
        p99_999 = temp_stats.calculate_percentile_usec(sorted_lats, 99.999)
        p100 = int(sorted_lats[-1] * 1000)
        avg = int(sum(sorted_lats) / len(sorted_lats) * 1000)
    else:
        p50 = p90 = p95 = p99 = p99_9 = p99_99 = p99_999 = p100 = avg = 0
    
    # Output CSV line
    print(f"{timestamp},{request_sec:.6f},{p50},{p90},{p95},{p99},{p99_9},{p99_99},{p99_999},{p100},{avg},{aggregated['errors']},{aggregated['moved']},{aggregated['clusterdown']},{aggregated['disconnects']}", flush=True)

def orchestrator(config: Dict, num_processes: int):
    """
    Orchestrator process that manages worker processes and aggregates metrics.
    
    Args:
        config (Dict): Base benchmark configuration
        num_processes (int): Number of worker processes to spawn
    """
    # Create inter-process communication objects
    metrics_queue = Queue(maxsize=1000)
    shutdown_event = Event()
    
    # Calculate per-worker configuration
    total_requests = config['total_requests']
    requests_per_worker = total_requests // num_processes
    remainder = total_requests % num_processes
    
    total_qps = config.get('qps', 0)
    start_qps = config.get('start_qps', 0)
    end_qps = config.get('end_qps', 0)

    # Keep clients and threads the same per process (don't divide them!)
    clients_per_worker = config['pool_size']
    threads_per_worker = config['num_threads']
    
    # Print banner if not in CSV mode
    csv_mode = config.get('csv_interval_sec') is not None
    if not csv_mode:
        print('Valkey Benchmark (Multi-Process Mode)')
        print(f"Host: {config['host']}")
        print(f"Port: {config['port']}")
        print(f"Processes: {num_processes}")
        print(f"Threads per process: {threads_per_worker}")
        print(f"Clients per process: {clients_per_worker}")
        print(f"Total Requests: {total_requests}")
        print(f"Data Size: {config['data_size']}")
        print(f"Command: {config['command']}")
        print(f"Is Cluster: {config['is_cluster']}")
        print(f"Read from Replica: {config['read_from_replica']}")
        print(f"Use TLS: {config['use_tls']}")
        print()
    else:
        # Print CSV header
        print("timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,requests_total_failed,requests_moved,requests_clusterdown,client_disconnects", flush=True)
    
    # Spawn worker processes
    workers = []
    for i in range(num_processes):
        worker_config = config.copy()
        
        # Distribute requests
        worker_requests = requests_per_worker + (1 if i < remainder else 0)
        worker_config['total_requests'] = worker_requests
        
        # Distribute QPS (proportionally)
        if total_qps > 0:
            worker_config['qps'] = total_qps // num_processes
        if start_qps > 0:
            worker_config['start_qps'] = start_qps // num_processes
        if end_qps > 0:
            worker_config['end_qps'] = end_qps // num_processes
        
        # Distribute clients and threads
        worker_config['pool_size'] = clients_per_worker
        worker_config['num_threads'] = threads_per_worker
        
        # Create worker process
        p = Process(
            target=worker_process_entry,
            args=(worker_config, metrics_queue, shutdown_event, i)
        )
        p.start()
        workers.append(p)
    
    # Aggregate metrics
    start_time = time.time()
    last_print = time.time()
    worker_state = {}  # worker_id -> {requests_completed, errors}
    all_latencies = []
    current_window_latencies = []
    
    # For CSV mode
    csv_interval_sec = config.get('csv_interval_sec', 0)
    interval_start = time.time()
    interval_worker_metrics = {}  # worker_id -> metrics
    
    try:
        while any(p.is_alive() for p in workers):
            try:
                metrics = metrics_queue.get(timeout=0.1)
                
                if metrics['type'] == 'progress':
                    # Track per-worker state
                    worker_id = metrics['worker_id']
                    prev_completed = worker_state.get(worker_id, {}).get('requests_completed', 0)
                    prev_errors = worker_state.get(worker_id, {}).get('errors', 0)
                    
                    worker_state[worker_id] = {
                        'requests_completed': metrics.get('requests_completed', 0),
                        'errors': metrics.get('errors', 0)
                    }
                    
                    current_window_latencies.extend(metrics.get('current_window_latencies', []))
                    
                    # Print progress periodically
                    now = time.time()
                    if not csv_mode and now - last_print >= 1:
                        elapsed = now - start_time
                        
                        # Calculate total from all workers
                        total_completed = sum(w['requests_completed'] for w in worker_state.values())
                        total_errors = sum(w['errors'] for w in worker_state.values())
                        
                        current_rps = len(current_window_latencies)
                        overall_rps = total_completed / elapsed if elapsed > 0 else 0
                        
                        window_stats = BenchmarkStats.calculate_latency_stats(current_window_latencies)
                        
                        output = (
                            f"\r[{elapsed:.1f}s] "
                            f"Progress: {total_completed:,}/{total_requests:,} ({total_completed/total_requests*100:.1f}%), "
                            f"RPS: current={current_rps:,} avg={overall_rps:,.1f}, "
                            f"Errors: {total_errors}"
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
                        current_window_latencies = []
                        last_print = now
                
                elif metrics['type'] == 'csv_interval':
                    # Store interval metrics by worker
                    worker_id = metrics['worker_id']
                    interval_worker_metrics[worker_id] = metrics
                    
                    # Check if we have metrics from all workers or if interval has passed
                    now = time.time()
                    if len(interval_worker_metrics) == num_processes or \
                       now - interval_start >= csv_interval_sec:
                        # Aggregate and emit
                        worker_list = list(interval_worker_metrics.values())
                        aggregated = aggregate_csv_metrics(worker_list)
                        if aggregated:
                            emit_aggregated_csv_line(int(now), aggregated)
                        
                        # Reset for next interval
                        interval_worker_metrics = {}
                        interval_start = now
                
                elif metrics['type'] == 'final':
                    # Accumulate final metrics
                    all_latencies.extend(metrics.get('latencies', []))
            
            except (queue.Empty, Exception):
                # Timeout or queue error, continue polling
                continue
        
        # Wait for all workers to finish
        for p in workers:
            p.join()
        
        # Drain remaining metrics from queue
        while not metrics_queue.empty():
            try:
                metrics = metrics_queue.get_nowait()
                if metrics['type'] == 'final':
                    all_latencies.extend(metrics.get('latencies', []))
                elif metrics['type'] == 'csv_interval':
                    worker_id = metrics['worker_id']
                    interval_worker_metrics[worker_id] = metrics
            except (queue.Empty, Exception):
                # Queue empty or error
                break
        
        # Emit final CSV interval if there's data
        if csv_mode and interval_worker_metrics:
            worker_list = list(interval_worker_metrics.values())
            aggregated = aggregate_csv_metrics(worker_list)
            if aggregated:
                emit_aggregated_csv_line(int(time.time()), aggregated)
        
        # Print final stats if not in CSV mode
        if not csv_mode:
            total_time = time.time() - start_time
            
            # Calculate totals from worker state
            total_completed = sum(w['requests_completed'] for w in worker_state.values())
            total_errors = sum(w['errors'] for w in worker_state.values())
            
            final_rps = total_completed / total_time if total_time > 0 else 0
            
            final_stats = BenchmarkStats.calculate_latency_stats(all_latencies)
            
            print('\n\nFinal Results:')
            print('=============')
            print(f'Total time: {total_time:.2f} seconds')
            print(f'Requests completed: {total_completed}')
            print(f'Requests per second: {final_rps:.2f}')
            print(f'Total errors: {total_errors}')
            
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
                    count = sum(1 for l in all_latencies if l <= range_value) - current
                    percentage = (count / len(all_latencies) * 100) if all_latencies else 0
                    print(f'<= {range_value:.1f} ms: {percentage:.2f}% ({count} requests)')
                    current += count
                
                remaining = len(all_latencies) - current
                if remaining > 0:
                    percentage = (remaining / len(all_latencies) * 100)
                    print(f'> 1000 ms: {percentage:.2f}% ({remaining} requests)')
    
    except KeyboardInterrupt:
        print("\n\nShutting down workers...", file=sys.stderr)
        shutdown_event.set()
        for p in workers:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
                p.join()
    except Exception as e:
        print(f"\nOrchestrator error: {str(e)}", file=sys.stderr)
        shutdown_event.set()
        for p in workers:
            p.terminate()
            p.join()
        raise

def main():
    """
    Main application entry point.
    
    Initializes and runs the benchmark based on provided configuration.
    Handles command line parsing and benchmark execution.
    """
    args = parse_arguments()
    custom_commands = load_custom_commands(args.custom_command_file, args.custom_command_args)
    
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
        'sequential_random_start': bool(args.sequential_random_start),
        'qps': args.qps or 0,
        'start_qps': args.start_qps or 0,
        'end_qps': args.end_qps or 0,
        'qps_change_interval': args.qps_change_interval or 0,
        'qps_change': args.qps_change or 0,
        'qps_ramp_mode': args.qps_ramp_mode or 'linear',
        'qps_ramp_factor': args.qps_ramp_factor or 0,
        'use_tls': bool(args.tls),
        'is_cluster': bool(args.cluster),
        'read_from_replica': bool(args.read_from_replica),
        'custom_commands': custom_commands,
        'csv_interval_sec': args.interval_metrics_interval_duration_sec,
        'request_timeout': args.request_timeout
    }

    if config['command'] == 'custom' and not config['custom_commands']:
        print("Error: Custom commands required but not provided", file=sys.stderr)
        sys.exit(1)
    
    if config['sequential_random_start'] and not config['use_sequential']:
        print("Error: --sequential-random-start requires --sequential to be set", file=sys.stderr)
        sys.exit(1)

    # Determine number of processes
    num_processes = 1
    if not args.single_process:
        if args.processes.lower() == 'auto':
            num_processes = multiprocessing.cpu_count()
        else:
            try:
                num_processes = int(args.processes)
                if num_processes < 1:
                    print("Error: --processes must be at least 1", file=sys.stderr)
                    sys.exit(1)
            except ValueError:
                print(f"Error: Invalid value for --processes: {args.processes}", file=sys.stderr)
                sys.exit(1)
    
    # Run benchmark
    if num_processes == 1 or args.single_process:
        # Single-process mode (legacy behavior)
        asyncio.run(run_benchmark(config))
    else:
        # Multi-process mode
        orchestrator(config, num_processes)

if __name__ == '__main__':
    main()

#!/usr/bin/env python3
"""
Valkey Benchmark Real-Time Visualizer
=====================================

A real-time visualization tool for monitoring Valkey benchmark performance metrics.
Displays live graphs for QPS, P50/P90/P99 latencies, and errors.

Usage:
    python valkey-benchmark-visualizer.py <csv_file>

Example:
    python valkey-benchmark-visualizer.py results.csv
"""

import sys
import time
import argparse
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from matplotlib.gridspec import GridSpec
import warnings

warnings.filterwarnings('ignore')


class BenchmarkVisualizer:
    """
    Real-time visualizer for Valkey benchmark results.
    
    Monitors a CSV file and updates graphs with new data as it arrives.
    """
    
    def __init__(self, csv_file: str, update_interval: int = 1000, window_size: int = 200):
        """
        Initialize the visualizer.
        
        Args:
            csv_file (str): Path to the CSV file to monitor
            update_interval (int): Update interval in milliseconds (default: 1000ms)
            window_size (int): Time window to display in seconds (default: 200s)
        """
        self.csv_file = Path(csv_file)
        self.update_interval = update_interval
        self.window_size = window_size
        self.last_row_count = 0
        self.data = pd.DataFrame()
        self.previous_errors = 0
        
        # Create figure with subplots
        self.fig = plt.figure(figsize=(14, 10))
        self.fig.suptitle(f'Valkey Benchmark Monitor - {self.csv_file.name}', 
                         fontsize=16, fontweight='bold')
        
        # Create grid layout for subplots - 4 rows, 2 columns
        gs = GridSpec(4, 2, figure=self.fig, hspace=0.3, wspace=0.3)
        
        # Create subplots
        self.ax_qps = self.fig.add_subplot(gs[0, :])  # TPS spans full width
        self.ax_cow_peak = self.fig.add_subplot(gs[1, :])  # COW Peak spans full width
        self.ax_p50 = self.fig.add_subplot(gs[2, 0])
        self.ax_replicas = self.fig.add_subplot(gs[2, 1])
        self.ax_p99 = self.fig.add_subplot(gs[3, 0])
        self.ax_errors = self.fig.add_subplot(gs[3, 1])
        
        # Configure subplots
        self._setup_plots()
        
    def _setup_plots(self):
        """Configure the appearance of all subplots."""
        # TPS plot
        self.ax_qps.set_title('Server TPS', fontweight='bold', fontsize=12)
        self.ax_qps.set_xlabel('Elapsed Time (seconds)')
        self.ax_qps.set_ylabel('TPS')
        self.ax_qps.grid(True, alpha=0.3)
        
        # COW Peak plot
        self.ax_cow_peak.set_title('Copy-on-Write Peak Memory', fontweight='bold', fontsize=12)
        self.ax_cow_peak.set_xlabel('Elapsed Time (seconds)')
        self.ax_cow_peak.set_ylabel('Memory (MB)')
        self.ax_cow_peak.grid(True, alpha=0.3)
        
        # P50 plot
        self.ax_p50.set_title('P50 Latency (Median)', fontweight='bold', fontsize=12)
        self.ax_p50.set_xlabel('Elapsed Time (seconds)')
        self.ax_p50.set_ylabel('Latency (ms)')
        self.ax_p50.grid(True, alpha=0.3)
        
        # Connected Replicas plot
        self.ax_replicas.set_title('Connected Replicas', fontweight='bold', fontsize=12)
        self.ax_replicas.set_xlabel('Elapsed Time (seconds)')
        self.ax_replicas.set_ylabel('Replica Count')
        self.ax_replicas.grid(True, alpha=0.3)
        
        # P99 plot
        self.ax_p99.set_title('P99 Latency', fontweight='bold', fontsize=12)
        self.ax_p99.set_xlabel('Elapsed Time (seconds)')
        self.ax_p99.set_ylabel('Latency (ms)')
        self.ax_p99.grid(True, alpha=0.3)
        
        # Errors plot
        self.ax_errors.set_title('Cumulative Errors', fontweight='bold', fontsize=12)
        self.ax_errors.set_xlabel('Elapsed Time (seconds)')
        self.ax_errors.set_ylabel('Error Count')
        self.ax_errors.grid(True, alpha=0.3)
        
    def _read_csv_data(self):
        """
        Read new data from the CSV file.
        
        Returns:
            bool: True if new data was read, False otherwise
        """
        if not self.csv_file.exists():
            return False
            
        try:
            # Read the entire CSV file
            df = pd.read_csv(self.csv_file)
            
            # Check if there's new data
            if len(df) > self.last_row_count:
                self.data = df
                self.last_row_count = len(df)
                return True
                
        except (pd.errors.EmptyDataError, pd.errors.ParserError):
            # File might be empty or being written to
            pass
        except Exception as e:
            print(f"Error reading CSV: {e}", file=sys.stderr)
            
        return False
        
    def _get_filtered_data(self):
        """
        Get filtered data for the last window_size seconds.
        
        Returns:
            tuple: (elapsed, qps, p50, connected_replicas, p99, errors, server_tps, cow_peak) filtered to the last window_size seconds
        """
        if self.data.empty:
            return None
        
        elapsed = self.data['elapsed_seconds']
        max_time = elapsed.max()
        
        # Show last window_size seconds
        window_start = max(0, max_time - self.window_size)
        window_end = max_time
        
        # Filter data to the window
        mask = (elapsed >= window_start) & (elapsed <= window_end)
        
        # Check if server_tps column exists
        server_tps = self.data['server_tps'][mask] if 'server_tps' in self.data.columns else pd.Series([0] * sum(mask))
        
        # Check if connected_replicas column exists
        connected_replicas = self.data['connected_replicas'][mask] if 'connected_replicas' in self.data.columns else pd.Series([0] * sum(mask))
        
        # Check if current_cow_peak column exists
        cow_peak = self.data['current_cow_peak'][mask] if 'current_cow_peak' in self.data.columns else pd.Series([0] * sum(mask))
        
        return (
            elapsed[mask],
            self.data['qps'][mask],
            self.data['p50_ms'][mask],
            connected_replicas,
            self.data['p99_ms'][mask],
            self.data['errors'][mask],
            server_tps,
            cow_peak
        )
    
    def _update_plots(self, frame):
        """
        Update all plots with new data.
        
        Args:
            frame: Frame number (required by FuncAnimation, not used)
        """
        # Try to read new data
        has_new_data = self._read_csv_data()
        
        if not has_new_data or self.data.empty:
            return
        
        # Get filtered data
        filtered = self._get_filtered_data()
        if filtered is None:
            return
        
        elapsed, qps, p50, connected_replicas, p99, errors, server_tps, cow_peak = filtered
        
        if len(elapsed) == 0:
            return
        
        # Calculate per-second timeouts (non-cumulative)
        current_errors = errors.iloc[-1] if len(errors) > 0 else 0
        errors_per_second = current_errors - self.previous_errors
        self.previous_errors = current_errors
        
        # Calculate timeouts per second for the entire window
        timeouts_per_sec = []
        prev_err = 0
        for err in errors:
            timeouts_per_sec.append(max(0, err - prev_err))
            prev_err = err
            
        # Clear all axes
        self.ax_qps.clear()
        self.ax_cow_peak.clear()
        self.ax_p50.clear()
        self.ax_replicas.clear()
        self.ax_p99.clear()
        self.ax_errors.clear()
        
        # Re-setup plots after clearing
        self._setup_plots()
        
        # Update QPS plot title to Server TPS
        self.ax_qps.set_title('Server TPS (Transactions Per Second)', fontweight='bold', fontsize=12)
        self.ax_qps.set_ylabel('TPS')
        
        # Update Errors plot title to Timeouts/Second
        self.ax_errors.set_title('Timeouts Per Second', fontweight='bold', fontsize=12)
        self.ax_errors.set_ylabel('Timeouts/sec')
        
        # Plot Server TPS from CSV
        if len(server_tps) > 0 and server_tps.sum() > 0:
            self.ax_qps.plot(elapsed, server_tps, 'b-', 
                           linewidth=2, label='Server TPS')
            self.ax_qps.fill_between(elapsed, server_tps, alpha=0.3)
            avg_tps = server_tps.mean()
            self.ax_qps.axhline(y=avg_tps, color='r', linestyle='--', 
                               linewidth=1, label=f'Avg: {avg_tps:.0f}')
            self.ax_qps.legend(loc='upper right')
        else:
            self.ax_qps.text(0.5, 0.5, 'Waiting for server TPS data...', 
                           transform=self.ax_qps.transAxes,
                           ha='center', va='center', fontsize=12)
        
        # Plot COW Peak Memory (convert bytes to MB)
        if len(cow_peak) > 0 and cow_peak.sum() > 0:
            cow_peak_mb = cow_peak / (1024 * 1024)  # Convert bytes to MB
            self.ax_cow_peak.plot(elapsed, cow_peak_mb, 'purple', 
                                 linewidth=2, label='COW Peak')
            self.ax_cow_peak.fill_between(elapsed, cow_peak_mb, alpha=0.3, color='purple')
            avg_cow = cow_peak_mb.mean()
            max_cow = cow_peak_mb.max()
            self.ax_cow_peak.axhline(y=avg_cow, color='r', linestyle='--', 
                                    linewidth=1, label=f'Avg: {avg_cow:.1f} MB')
            self.ax_cow_peak.text(0.02, 0.98, f'Max: {max_cow:.1f} MB', 
                                 transform=self.ax_cow_peak.transAxes,
                                 verticalalignment='top',
                                 bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            self.ax_cow_peak.legend(loc='upper right')
        else:
            self.ax_cow_peak.text(0.5, 0.5, 'Waiting for COW peak data...', 
                                 transform=self.ax_cow_peak.transAxes,
                                 ha='center', va='center', fontsize=12)
        
        # Plot P50
        self.ax_p50.plot(elapsed, p50, 'g-', linewidth=2, label='P50')
        self.ax_p50.fill_between(elapsed, p50, alpha=0.3, color='g')
        if len(p50) > 0:
            avg_p50 = p50.mean()
            self.ax_p50.axhline(y=avg_p50, color='r', linestyle='--', 
                               linewidth=1, label=f'Avg: {avg_p50:.2f}ms')
            self.ax_p50.legend(loc='upper right')
        
        # Plot Connected Replicas
        self.ax_replicas.plot(elapsed, connected_replicas, 'orange', linewidth=2, label='Connected Replicas')
        self.ax_replicas.fill_between(elapsed, connected_replicas, alpha=0.3, color='orange')
        if len(connected_replicas) > 0:
            avg_replicas = connected_replicas.mean()
            self.ax_replicas.axhline(y=avg_replicas, color='r', linestyle='--', 
                               linewidth=1, label=f'Avg: {avg_replicas:.1f}')
            self.ax_replicas.legend(loc='upper right')
        
        # Plot P99
        self.ax_p99.plot(elapsed, p99, 'r-', linewidth=2, label='P99')
        self.ax_p99.fill_between(elapsed, p99, alpha=0.3, color='r')
        if len(p99) > 0:
            avg_p99 = p99.mean()
            self.ax_p99.axhline(y=avg_p99, color='darkred', linestyle='--', 
                               linewidth=1, label=f'Avg: {avg_p99:.2f}ms')
            self.ax_p99.legend(loc='upper right')
        
        # Plot Timeouts Per Second (non-cumulative)
        self.ax_errors.plot(elapsed, timeouts_per_sec, 'purple', linewidth=2, label='Timeouts/sec')
        self.ax_errors.fill_between(elapsed, timeouts_per_sec, alpha=0.3, color='purple')
        if len(timeouts_per_sec) > 0:
            self.ax_errors.text(0.02, 0.98, f'Current: {errors_per_second}', 
                              transform=self.ax_errors.transAxes,
                              verticalalignment='top',
                              bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        # Adjust layout
        self.fig.tight_layout()
        
    def run(self):
        """Start the visualization and display the window."""
        print(f"Monitoring CSV file: {self.csv_file}")
        print("Waiting for data...")
        
        # Wait for file to exist
        while not self.csv_file.exists():
            time.sleep(0.5)
            
        print("CSV file found. Starting visualization...")
        print("Close the window to exit.")
        
        # Create animation
        anim = FuncAnimation(
            self.fig, 
            self._update_plots,
            interval=self.update_interval,
            cache_frame_data=False
        )
        
        # Show the plot
        plt.show()


def parse_arguments():
    """
    Parse command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Real-time visualizer for Valkey benchmark results',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python valkey-benchmark-visualizer.py results.csv
  python valkey-benchmark-visualizer.py results.csv --interval 500
  python valkey-benchmark-visualizer.py results.csv --window 300
        """
    )
    
    parser.add_argument(
        'csv_file',
        help='Path to the CSV file to monitor'
    )
    
    parser.add_argument(
        '--interval',
        type=int,
        default=1000,
        help='Update interval in milliseconds (default: 1000)'
    )
    
    parser.add_argument(
        '--window',
        type=int,
        default=200,
        help='Time window to display in seconds (default: 200)'
    )
    
    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Check if matplotlib is available
    try:
        import matplotlib
    except ImportError:
        print("Error: matplotlib is required but not installed.", file=sys.stderr)
        print("Install it with: pip install matplotlib", file=sys.stderr)
        sys.exit(1)
    
    # Check if pandas is available
    try:
        import pandas
    except ImportError:
        print("Error: pandas is required but not installed.", file=sys.stderr)
        print("Install it with: pip install pandas", file=sys.stderr)
        sys.exit(1)
    
    # Create and run visualizer
    visualizer = BenchmarkVisualizer(args.csv_file, args.interval, args.window)
    
    try:
        visualizer.run()
    except KeyboardInterrupt:
        print("\nVisualization stopped by user.")
        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()

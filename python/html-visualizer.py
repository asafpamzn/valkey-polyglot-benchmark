#!/usr/bin/env python3
"""
Valkey Benchmark HTML Visualizer
================================

Fetches results.csv from remote Ubuntu machine via SCP and generates
an auto-refreshing HTML visualization locally.

Usage:
    python html-visualizer.py [--interval SECONDS] [--window SECONDS]
"""

import subprocess
import time
import argparse
import sys
from pathlib import Path

# Remote server configuration
SSH_KEY = "~/.ssh/mac.pem"
SSH_USER = "ubuntu"
SSH_HOST = "ec2-23-22-167-26.compute-1.amazonaws.com"
REMOTE_CSV_PATH = "/home/ubuntu/valkey-polyglot-benchmark/python/results.csv"
LOCAL_CSV_PATH = Path(__file__).parent / "results.csv"
HTML_OUTPUT_PATH = Path(__file__).parent / "benchmark.html"


def fetch_csv(ssh_host=SSH_HOST, remote_csv_path=REMOTE_CSV_PATH):
    """Fetch results.csv from remote server via SCP."""
    ssh_key_expanded = str(Path(SSH_KEY).expanduser())
    cmd = [
        "scp",
        "-i", ssh_key_expanded,
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=5",
        f"{SSH_USER}@{ssh_host}:{remote_csv_path}",
        str(LOCAL_CSV_PATH)
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, timeout=10)
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        return False
    except Exception as e:
        print(f"SCP error: {e}", file=sys.stderr)
        return False


def parse_csv():
    """Parse the CSV file and return data arrays."""
    if not LOCAL_CSV_PATH.exists():
        return None

    try:
        with open(LOCAL_CSV_PATH, 'r') as f:
            lines = f.readlines()

        if len(lines) < 2:
            return None

        header = lines[0].strip().split(',')
        data = {col: [] for col in header}

        for line in lines[1:]:
            values = line.strip().split(',')
            if len(values) == len(header):
                for i, col in enumerate(header):
                    try:
                        data[col].append(float(values[i]))
                    except ValueError:
                        data[col].append(0)

        return data
    except Exception as e:
        print(f"CSV parse error: {e}", file=sys.stderr)
        return None


def generate_html(data, window_size):
    """Generate the HTML visualization file."""
    if not data or 'elapsed_seconds' not in data:
        return

    elapsed = data.get('elapsed_seconds', [])
    if not elapsed:
        return

    max_time = max(elapsed)
    window_start = max(0, max_time - window_size)

    # Filter data to window
    indices = [i for i, t in enumerate(elapsed) if t >= window_start]

    def filter_data(arr):
        return [arr[i] for i in indices] if arr else []

    elapsed_filtered = filter_data(elapsed)
    server_tps = filter_data(data.get('server_tps', []))
    p50 = filter_data(data.get('p50_ms', []))
    p99 = filter_data(data.get('p99_ms', []))
    errors = filter_data(data.get('errors', []))
    connected_replicas = filter_data(data.get('connected_replicas', []))

    # Calculate timeouts per second (delta of errors)
    timeouts_per_sec = [0]
    for i in range(1, len(errors)):
        delta = errors[i] - errors[i-1]
        timeouts_per_sec.append(max(0, delta))

    # Calculate averages
    avg_tps = sum(server_tps) / len(server_tps) if server_tps else 0
    avg_p50 = sum(p50) / len(p50) if p50 else 0
    avg_p99 = sum(p99) / len(p99) if p99 else 0
    avg_replicas = sum(connected_replicas) / len(connected_replicas) if connected_replicas else 0

    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="2">
    <title>Valkey Benchmark Monitor</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #1a1a2e;
            color: #eee;
            padding: 20px;
        }}
        h1 {{
            text-align: center;
            margin-bottom: 20px;
            color: #00d4ff;
        }}
        .stats-bar {{
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-bottom: 20px;
            flex-wrap: wrap;
        }}
        .stat {{
            background: #16213e;
            padding: 15px 25px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-value {{
            font-size: 24px;
            font-weight: bold;
            color: #00d4ff;
        }}
        .stat-label {{
            font-size: 12px;
            color: #888;
            margin-top: 5px;
        }}
        .chart-grid {{
            display: grid;
            grid-template-columns: 1fr;
            gap: 20px;
        }}
        .chart-row {{
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 20px;
        }}
        .chart-container {{
            background: #16213e;
            border-radius: 8px;
            padding: 15px;
        }}
        .chart-container.full-width {{
            grid-column: 1 / -1;
        }}
        .chart-title {{
            font-size: 14px;
            font-weight: bold;
            margin-bottom: 10px;
            color: #fff;
        }}
        canvas {{
            width: 100% !important;
            height: 200px !important;
        }}
        .full-width canvas {{
            height: 250px !important;
        }}
        .timestamp {{
            text-align: center;
            color: #666;
            margin-top: 20px;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <h1>Valkey Benchmark Monitor</h1>

    <div class="stats-bar">
        <div class="stat">
            <div class="stat-value">{avg_tps:,.0f}</div>
            <div class="stat-label">Avg TPS</div>
        </div>
        <div class="stat">
            <div class="stat-value">{avg_p50:.2f}ms</div>
            <div class="stat-label">Avg P50</div>
        </div>
        <div class="stat">
            <div class="stat-value">{avg_p99:.2f}ms</div>
            <div class="stat-label">Avg P99</div>
        </div>
        <div class="stat">
            <div class="stat-value">{avg_replicas:.1f}</div>
            <div class="stat-label">Avg Replicas</div>
        </div>
        <div class="stat">
            <div class="stat-value">{sum(timeouts_per_sec):,.0f}</div>
            <div class="stat-label">Total Timeouts</div>
        </div>
    </div>

    <div class="chart-grid">
        <div class="chart-container full-width">
            <div class="chart-title">Server TPS (Transactions Per Second)</div>
            <canvas id="tpsChart"></canvas>
        </div>

        <div class="chart-row">
            <div class="chart-container">
                <div class="chart-title">P50 Latency (Median)</div>
                <canvas id="p50Chart"></canvas>
            </div>
            <div class="chart-container">
                <div class="chart-title">Connected Replicas</div>
                <canvas id="replicasChart"></canvas>
            </div>
        </div>

        <div class="chart-row">
            <div class="chart-container">
                <div class="chart-title">P99 Latency</div>
                <canvas id="p99Chart"></canvas>
            </div>
            <div class="chart-container">
                <div class="chart-title">Timeouts Per Second</div>
                <canvas id="timeoutsChart"></canvas>
            </div>
        </div>
    </div>

    <div class="timestamp">Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}</div>

    <script>
        const elapsed = {elapsed_filtered};
        const serverTps = {server_tps};
        const p50 = {p50};
        const p99 = {p99};
        const replicas = {connected_replicas};
        const timeouts = {timeouts_per_sec};

        const chartOptions = {{
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            plugins: {{
                legend: {{
                    display: false
                }}
            }},
            scales: {{
                x: {{
                    ticks: {{
                        color: '#888',
                        maxTicksLimit: 20
                    }},
                    grid: {{
                        color: '#333'
                    }}
                }},
                y: {{
                    ticks: {{
                        color: '#888'
                    }},
                    grid: {{
                        color: '#333'
                    }},
                    beginAtZero: true
                }}
            }}
        }};

        function createChart(id, data, color, label) {{
            new Chart(document.getElementById(id), {{
                type: 'line',
                data: {{
                    labels: elapsed.map(t => t.toFixed(0) + 's'),
                    datasets: [{{
                        label: label,
                        data: data,
                        borderColor: color,
                        backgroundColor: color + '33',
                        fill: true,
                        tension: 0.1,
                        pointRadius: 0,
                        borderWidth: 2
                    }}]
                }},
                options: chartOptions
            }});
        }}

        createChart('tpsChart', serverTps, '#00d4ff', 'TPS');
        createChart('p50Chart', p50, '#00ff88', 'P50');
        createChart('replicasChart', replicas, '#ff9500', 'Replicas');
        createChart('p99Chart', p99, '#ff4444', 'P99');
        createChart('timeoutsChart', timeouts, '#aa44ff', 'Timeouts/sec');
    </script>
</body>
</html>
'''

    with open(HTML_OUTPUT_PATH, 'w') as f:
        f.write(html_content)


def main():
    parser = argparse.ArgumentParser(description='HTML visualizer for Valkey benchmark')
    parser.add_argument('--interval', type=int, default=1, help='Fetch interval in seconds (default: 1)')
    parser.add_argument('--window', type=int, default=100, help='Time window to display in seconds (default: 100)')
    parser.add_argument('--host', type=str, default=SSH_HOST, help=f'Remote host (default: {SSH_HOST})')
    parser.add_argument('--remote-path', type=str, default=REMOTE_CSV_PATH, help=f'Remote CSV path (default: {REMOTE_CSV_PATH})')
    args = parser.parse_args()

    ssh_host = args.host
    remote_csv_path = args.remote_path

    print(f"Valkey Benchmark HTML Visualizer")
    print(f"================================")
    print(f"Remote: {SSH_USER}@{SSH_HOST}:{REMOTE_CSV_PATH}")
    print(f"Local CSV: {LOCAL_CSV_PATH}")
    print(f"HTML Output: {HTML_OUTPUT_PATH}")
    print(f"Fetch interval: {args.interval}s")
    print(f"Window size: {args.window}s")
    print()
    print(f"Open in browser: file://{HTML_OUTPUT_PATH}")
    print()
    print("Press Ctrl+C to stop...")

    # Open browser on first run
    try:
        subprocess.run(["open", str(HTML_OUTPUT_PATH)], check=False)
    except Exception:
        pass

    while True:
        try:
            if fetch_csv(ssh_host, remote_csv_path):
                data = parse_csv()
                if data:
                    generate_html(data, args.window)
                    print(f"\r[{time.strftime('%H:%M:%S')}] Updated - {len(data.get('elapsed_seconds', []))} data points", end='', flush=True)
                else:
                    print(f"\r[{time.strftime('%H:%M:%S')}] Waiting for data...", end='', flush=True)
            else:
                print(f"\r[{time.strftime('%H:%M:%S')}] Fetch failed - retrying...", end='', flush=True)

            time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\n\nStopped.")
            break


if __name__ == '__main__':
    main()

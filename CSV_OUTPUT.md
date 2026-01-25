# CSV Output Mode for Redis-Benchmark Compatibility

## Overview

The Valkey Polyglot Benchmark tools now support a CSV output mode that is compatible with the existing redis-benchmark parser. This allows the same tooling and analysis pipelines to consume output from both redis-benchmark and valkey-polyglot-benchmark without modification.

## Enabling CSV Output

To enable CSV output mode, use the `--interval-metrics-interval-duration-sec` option with the desired interval in seconds:

### Python
```bash
python valkey-benchmark.py --interval-metrics-interval-duration-sec 5 -n 10000
```

### Node.js
```bash
node valkey-benchmark.js --interval-metrics-interval-duration-sec 5 -n 10000
```

### Java
```bash
java -jar valkey-benchmark.jar --interval-metrics-interval-duration-sec 5 -n 10000
```

## Output Format

### CSV Header
The first line is always the header (printed exactly once):
```
timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,request_finished,requests_total_failed,requests_moved,requests_clusterdown,client_disconnects
```

### Data Lines
Each subsequent line represents metrics for one interval, with exactly 16 comma-separated values:

1. **timestamp**: Unix epoch seconds at interval end
2. **request_sec**: Per-interval throughput (requests/second) in decimal notation
3. **p50_usec**: 50th percentile latency in microseconds (truncated)
4. **p90_usec**: 90th percentile latency in microseconds (truncated)
5. **p95_usec**: 95th percentile latency in microseconds (truncated)
6. **p99_usec**: 99th percentile latency in microseconds (truncated)
7. **p99_9_usec**: 99.9th percentile latency in microseconds (truncated)
8. **p99_99_usec**: 99.99th percentile latency in microseconds (truncated)
9. **p99_999_usec**: 99.999th percentile latency in microseconds (truncated)
10. **p100_usec**: Maximum latency in microseconds (truncated)
11. **avg_usec**: Average latency in microseconds (truncated)
12. **request_finished**: Successfully completed requests during this interval
13. **requests_total_failed**: Failed requests during this interval only
14. **requests_moved**: MOVED responses during this interval only
15. **requests_clusterdown**: CLUSTERDOWN responses during this interval only
16. **client_disconnects**: Disconnect events during this interval only

### Example Output
```
timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,request_finished,requests_total_failed,requests_moved,requests_clusterdown,client_disconnects
1696500000,12345.678900,100,200,250,300,350,380,390,400,150,61729,0,0,0,0
1696500005,12350.123456,105,205,255,305,355,385,395,405,155,61751,1,0,0,0
1696500010,12340.987654,102,202,252,302,352,382,392,402,152,61702,0,0,0,0
```

## Key Specifications

### Interval Behavior
- One data line is emitted every N seconds (as specified by `--interval-metrics-interval-duration-sec`)
- All counters (fields 12-16) are **per-interval deltas**, not cumulative
- `request_sec` is the **per-interval rate**, not cumulative average
- The final interval is emitted at benchmark end if it contains any data

### Data Formats
- **Percentiles and avg**: Integer microseconds (truncated, not rounded)
- **request_sec**: Decimal number with up to 6 decimal places (no scientific notation)
- **Counters**: Non-negative integers
- **Timestamp**: Unix epoch seconds (integer)

### Formatting Rules
- No trailing commas
- Decimal separator is '.'
- No whitespace padding
- No scientific notation
- Percentiles are non-decreasing: p50 ≤ p90 ≤ p95 ≤ p99 ≤ p99.9 ≤ p99.99 ≤ p99.999 ≤ p100

### CSV Mode Behavior
When CSV mode is enabled:
- Only the CSV header and data lines are written to stdout
- Progress messages and banners are suppressed
- Error messages are sent to stderr (if needed)
- No blank lines or comments are emitted

### Edge Cases
- **Zero throughput**: If an interval has zero successful requests, `request_sec=0`, `request_finished=0`, and all latency fields are `0`
- **Single sample**: If very few samples in an interval, percentile values may be the same
- **Cluster errors**: MOVED and CLUSTERDOWN counters remain 0 when not in cluster mode
- **No failures**: Failed/moved/clusterdown/disconnect counters use literal `0` when no events occur

## Use Cases

### Real-time Monitoring
Monitor benchmark progress in real-time by streaming CSV output:
```bash
python valkey-benchmark.py --interval-metrics-interval-duration-sec 1 -n 100000 | tail -f
```

### Long-running Tests
Capture metrics from extended duration tests:
```bash
node valkey-benchmark.js --interval-metrics-interval-duration-sec 10 --test-duration 3600 > metrics.csv
```

### Analysis and Visualization
Import CSV data into analysis tools:
```python
import pandas as pd
df = pd.read_csv('metrics.csv')
df.plot(x='timestamp', y='request_sec')
```

### Cluster Monitoring
Track cluster-specific metrics:
```bash
java -jar valkey-benchmark.jar --cluster --interval-metrics-interval-duration-sec 5 > cluster_metrics.csv
```

## Compatibility

The CSV output format is designed to be compatible with existing redis-benchmark parsers and analysis tools. The format follows the same structure and semantics, ensuring seamless integration with existing workflows.

## Implementation Notes

### Percentile Calculation
Percentiles are calculated using the standard method:
- Sort latencies in ascending order
- Calculate index: `floor(percentile/100 * count)`
- Truncate (not round) when converting from milliseconds to microseconds

### Thread Safety
All implementations use thread-safe mechanisms for tracking interval metrics:
- Python: Standard thread-safe operations
- Node.js: Single-threaded event loop
- Java: Atomic counters and synchronized collections

### Performance Impact
CSV mode has minimal performance impact:
- Interval metrics are tracked in memory
- CSV output occurs only once per interval
- No disk I/O during individual operations

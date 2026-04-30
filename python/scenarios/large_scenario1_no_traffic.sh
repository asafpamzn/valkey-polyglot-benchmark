#!/bin/bash
# Large Machine - Scenario 1: No traffic (Python SET only for results.csv)
# Host: ec2-13-218-147-29.compute-1.amazonaws.com
# VB_GET_CONCURRENCY=0, VB_SET_CONCURRENCY=0

set -euo pipefail

cleanup() {
    echo ""
    echo "Cleaning up: Killing all benchmark processes..."
    pkill -9 python3 || true
    exit 0
}
trap cleanup INT TERM EXIT

HOST="ec2-13-218-147-29.compute-1.amazonaws.com"

# Config matching set_benchmark.py for large machine
VB_DATA_SIZE=512
VB_KEYSPACE=450000000

# No valkey-benchmark traffic
VB_GET_CONCURRENCY=0
VB_SET_CONCURRENCY=0

# Python SET for latency stats (1 QPS to same key) - this generates results.csv
PYTHON_QPS=1
PYTHON_NREQ=8500000000
PYTHON_THREADS=4

OUTPUT="results.csv"
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"
rm -f "$OUTPUT"

echo "=========================================="
echo "Large Machine - Scenario 1: No Traffic"
echo "=========================================="
echo "Host: $HOST"
echo "Data Size: $VB_DATA_SIZE bytes"
echo "Keyspace: $VB_KEYSPACE"
echo "GET Concurrency: $VB_GET_CONCURRENCY (no background traffic)"
echo "SET Concurrency: $VB_SET_CONCURRENCY (no background traffic)"
echo "SET (Python): $PYTHON_QPS TPS to fixed key (for latency measurement + CSV)"
echo "=========================================="
echo ""

# Launch Python SET stats process - this generates results.csv
echo "--- Launching Python SET stats process ---"
LOG_FILE="$LOG_DIR/python_set_stats.log"
echo "Launching Python SET @ $PYTHON_QPS TPS to fixed key (logging to $LOG_FILE)"
python3 valkey-benchmark.py -c 1 --threads 1 -t custom \
     --custom-command-file "scenarios/set_benchmark_no_traffic_large.py" \
     -H "$HOST" \
     --qps $PYTHON_QPS -n $PYTHON_NREQ --timeout 50 \
     --output-csv "$OUTPUT" >"$LOG_FILE" 2>&1 &

echo ""
echo "Waiting for benchmark process..."
wait

echo ""
echo "Done! Results in $OUTPUT"

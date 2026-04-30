#!/bin/bash
# Small Machine - Scenario 1: No traffic (Python SET only for results.csv)
# Host: ec2-18-215-169-82.compute-1.amazonaws.com
# VB_GET_CONCURRENCY=0, VB_SET_CONCURRENCY=0

set -euo pipefail

# Get script directory and python directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PYTHON_DIR"

cleanup() {
    echo ""
    echo "Cleaning up: Killing all benchmark processes..."
    pkill -9 python3 || true
    exit 0
}
trap cleanup INT TERM EXIT

# Parse arguments
SKIP_WARMUP=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-warmup)
            SKIP_WARMUP=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

HOST="ec2-18-215-169-82.compute-1.amazonaws.com"

# Config matching set_benchmark.py for small machine
VB_DATA_SIZE=50
VB_KEYSPACE=220000000

# No valkey-benchmark traffic
VB_GET_CONCURRENCY=0
VB_SET_CONCURRENCY=0

# Python SET for latency stats (1 QPS to same key) - this generates results.csv
PYTHON_QPS=1
PYTHON_NREQ=8500000000

VB_CMD="valkey-benchmark"

OUTPUT="$PYTHON_DIR/results.csv"
LOG_DIR="$PYTHON_DIR/logs"
mkdir -p "$LOG_DIR"
rm -f "$OUTPUT"

echo "=========================================="
echo "Small Machine - Scenario 1: No Traffic"
echo "=========================================="
echo "Host: $HOST"
echo "Data Size: $VB_DATA_SIZE bytes"
echo "Keyspace: $VB_KEYSPACE"
echo "GET Concurrency: $VB_GET_CONCURRENCY (no background traffic)"
echo "SET Concurrency: $VB_SET_CONCURRENCY (no background traffic)"
echo "SET (Python): $PYTHON_QPS TPS to fixed key (for latency measurement + CSV)"
echo "Skip Warmup: $SKIP_WARMUP"
echo "Output: $OUTPUT"
echo "=========================================="
echo ""

# === Warmup Phase ===
if [ "$SKIP_WARMUP" = false ]; then
    echo "🔥 Phase 1: Warmup - Populating $VB_KEYSPACE keys using native valkey-benchmark"
    echo "   Command: SET key:__rand_int__ __data__ (sequential, pipelined)"
    echo "   Data size: $VB_DATA_SIZE bytes"
    echo ""

    WARMUP_LOG="$LOG_DIR/warmup_vb.log"
    echo "Launching valkey-benchmark warmup (logging to $WARMUP_LOG)"
    $VB_CMD -h "$HOST" \
            -c 50 --threads 4 \
            -r $VB_KEYSPACE -d $VB_DATA_SIZE \
            -n $VB_KEYSPACE \
            -P 16 \
            --sequential \
            -- SET "key:__rand_int__" __data__ \
            > "$WARMUP_LOG" 2>&1

    echo "Warmup completed!"
    echo ""
else
    echo "Skipping warmup phase"
    echo ""
fi

# === Benchmark Phase ===
echo "--- Launching Python SET stats process ---"
LOG_FILE="$LOG_DIR/python_set_stats.log"
echo "Launching Python SET @ $PYTHON_QPS TPS to fixed key (logging to $LOG_FILE)"
python3 valkey-benchmark.py -c 1 --threads 1 -t custom \
     --custom-command-file "scenarios/set_benchmark_no_traffic_small.py" \
     -H "$HOST" \
     --qps $PYTHON_QPS -n $PYTHON_NREQ --timeout 50 \
     --output-csv "$OUTPUT" >"$LOG_FILE" 2>&1 &

echo ""
echo "Waiting for benchmark process..."
wait

echo ""
echo "Done! Results in $OUTPUT"

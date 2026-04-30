#!/bin/bash
# Large Machine - Scenario 3: 400K GET + 100K SET (valkey-benchmark) + 1500 SET (Python)
# Host: ec2-13-218-147-29.compute-1.amazonaws.com
# VB_GET_CONCURRENCY=20, VB_SET_CONCURRENCY=5

set -euo pipefail

# Get script directory and python directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PYTHON_DIR"

# Track all child PIDs
PIDS=()

cleanup() {
    echo ""
    echo "Cleaning up: Killing all benchmark processes..."
    for pid in "${PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    pkill -9 -P $$ 2>/dev/null || true
    exit 0
}
trap cleanup INT TERM EXIT

# Parse arguments
SKIP_WARMUP=false
REPLICA_HOST=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-warmup)
            SKIP_WARMUP=true
            shift
            ;;
        --replica)
            REPLICA_HOST="$2"
            shift 2
            ;;
        *)
            shift
            ;;
    esac
done

HOST="ec2-13-218-147-29.compute-1.amazonaws.com"

# Config matching set_benchmark.py
VB_DATA_SIZE=512
VB_KEYSPACE=450000000

# 400K GET total = 20 processes x 20K RPS each
VB_GET_CONCURRENCY=20
VB_GET_RPS=20000
VB_GET_NREQ=288000000

# 100K SET total = 5 processes x 20K RPS each
VB_SET_CONCURRENCY=5
VB_SET_RPS=20000
VB_SET_NREQ=144000000

# Replica settings
VB_REPLICA_GET_CONCURRENCY=10
VB_REPLICA_GET_RPS=40000
VB_REPLICA_GET_NREQ=144000000

VB_CLIENTS=50
VB_THREADS=4
VB_CMD="valkey-benchmark"

# Python SET for latency stats (1500 QPS)
PYTHON_QPS=1500
PYTHON_NREQ=8500000000
PYTHON_THREADS=4

OUTPUT="$PYTHON_DIR/results.csv"
LOG_DIR="$PYTHON_DIR/logs"
mkdir -p "$LOG_DIR"
rm -f "$OUTPUT"

echo "=========================================="
echo "Large Machine - Scenario 3: Mixed Traffic"
echo "=========================================="
echo "Host: $HOST"
echo "Data Size: $VB_DATA_SIZE bytes"
echo "Keyspace: $VB_KEYSPACE"
echo "GET: $VB_GET_CONCURRENCY processes x $VB_GET_RPS RPS = $((VB_GET_CONCURRENCY * VB_GET_RPS)) TPS"
echo "SET (valkey-benchmark): $VB_SET_CONCURRENCY processes x $VB_SET_RPS RPS = $((VB_SET_CONCURRENCY * VB_SET_RPS)) TPS"
echo "SET (Python): $PYTHON_QPS QPS (for latency measurement)"
if [ -n "$REPLICA_HOST" ]; then
    echo "Replica Host: $REPLICA_HOST"
    echo "Replica GET: $VB_REPLICA_GET_CONCURRENCY x $VB_REPLICA_GET_RPS RPS"
fi
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
# Launch Python SET stats process
echo "--- Launching Python SET stats process ---"
LOG_FILE="$LOG_DIR/python_set_stats.log"
python3 valkey-benchmark.py -c $PYTHON_THREADS --threads $PYTHON_THREADS -t custom \
     --custom-command-file "scenarios/set_benchmark_large.py" \
     -H "$HOST" \
     --qps $PYTHON_QPS -n $PYTHON_NREQ --timeout 50 \
     --output-csv "$OUTPUT" >"$LOG_FILE" 2>&1 &
PIDS+=($!)

# Launch valkey-benchmark GET workers
echo ""
echo "--- Launching valkey-benchmark GET workers ---"
for i in $(seq 1 $VB_GET_CONCURRENCY); do
    LOG_FILE="$LOG_DIR/vb_get_$i.log"
    echo "Launching GET worker $i @ $VB_GET_RPS RPS"
    $VB_CMD -h "$HOST" \
            -c $VB_CLIENTS --threads $VB_THREADS \
            -r $VB_KEYSPACE -d $VB_DATA_SIZE \
            -n $VB_GET_NREQ --rps $VB_GET_RPS \
            -- GET "key:__rand_int__" \
            >"$LOG_FILE" 2>&1 &
    PIDS+=($!)
done

# Launch valkey-benchmark SET workers
echo ""
echo "--- Launching valkey-benchmark SET workers ---"
for i in $(seq 1 $VB_SET_CONCURRENCY); do
    LOG_FILE="$LOG_DIR/vb_set_$i.log"
    echo "Launching SET worker $i @ $VB_SET_RPS RPS"
    $VB_CMD -h "$HOST" \
            -c $VB_CLIENTS --threads $VB_THREADS \
            -r $VB_KEYSPACE -d $VB_DATA_SIZE \
            -n $VB_SET_NREQ --rps $VB_SET_RPS \
            -- SET "key:__rand_int__" __data__ \
            >"$LOG_FILE" 2>&1 &
    PIDS+=($!)
done

# Launch replica GET workers if specified
if [ -n "$REPLICA_HOST" ]; then
    echo ""
    echo "--- Launching replica GET workers ---"
    for i in $(seq 1 $VB_REPLICA_GET_CONCURRENCY); do
        LOG_FILE="$LOG_DIR/replica_get_$i.log"
        echo "Launching replica GET worker $i @ $VB_REPLICA_GET_RPS RPS"
        $VB_CMD -h "$REPLICA_HOST" \
                -c $VB_CLIENTS --threads $VB_THREADS \
                -r $VB_KEYSPACE -d $VB_DATA_SIZE \
                -n $VB_REPLICA_GET_NREQ --rps $VB_REPLICA_GET_RPS \
                -- GET "key:__rand_int__" \
                >"$LOG_FILE" 2>&1 &
        PIDS+=($!)
    done
fi

echo ""
echo "Waiting for all processes... (Ctrl+C to stop)"
wait

echo ""
echo "Done! Results in $OUTPUT"

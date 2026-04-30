#!/bin/bash
# Small Machine - Scenario 2: 80K GET + 1500 SET (from set_benchmark.py only)
# Host: ec2-18-215-169-82.compute-1.amazonaws.com
# VB_GET_CONCURRENCY=4, VB_SET_CONCURRENCY=0 (no valkey-benchmark SET)

set -euo pipefail

cleanup() {
    echo ""
    echo "Cleaning up: Killing all benchmark processes..."
    pkill -9 python3 || true
    pkill -9 valkey-benchmark || true
    exit 0
}
trap cleanup INT TERM EXIT

HOST="ec2-18-215-169-82.compute-1.amazonaws.com"
REPLICA_HOST="${1:-}"

# Config matching set_benchmark.py for small machine
VB_DATA_SIZE=50
VB_KEYSPACE=220000000

# 80K GET total = 4 processes x 20K RPS each
VB_GET_CONCURRENCY=4
VB_GET_RPS=20000
VB_GET_NREQ=288000000

# No valkey-benchmark SET - only Python SET
VB_SET_CONCURRENCY=0

# Replica settings
VB_REPLICA_GET_CONCURRENCY=10
VB_REPLICA_GET_RPS=20000
VB_REPLICA_GET_NREQ=144000000

VB_CLIENTS=50
VB_THREADS=4
VB_CMD="valkey-benchmark"

# Python SET for latency stats (1500 QPS)
PYTHON_QPS=1500
PYTHON_NREQ=8500000000
PYTHON_THREADS=4

OUTPUT="results.csv"
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"
rm -f "$OUTPUT"

echo "=========================================="
echo "Small Machine - Scenario 2: 80K GET + 1500 SET"
echo "=========================================="
echo "Host: $HOST"
echo "Data Size: $VB_DATA_SIZE bytes"
echo "Keyspace: $VB_KEYSPACE"
echo "GET: $VB_GET_CONCURRENCY processes x $VB_GET_RPS RPS = $((VB_GET_CONCURRENCY * VB_GET_RPS)) TPS"
echo "SET: Python only @ $PYTHON_QPS QPS (for latency measurement)"
if [ -n "$REPLICA_HOST" ]; then
    echo "Replica Host: $REPLICA_HOST"
    echo "Replica GET: $VB_REPLICA_GET_CONCURRENCY x $VB_REPLICA_GET_RPS RPS"
fi
echo "=========================================="
echo ""

# Launch Python SET stats process
echo "--- Launching Python SET stats process ---"
LOG_FILE="$LOG_DIR/python_set_stats.log"
python3 valkey-benchmark.py -c $PYTHON_THREADS --threads $PYTHON_THREADS -t custom \
     --custom-command-file "scenarios/set_benchmark_small.py" \
     -H "$HOST" \
     --qps $PYTHON_QPS -n $PYTHON_NREQ --timeout 50 \
     --output-csv "$OUTPUT" >"$LOG_FILE" 2>&1 &

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
    done
fi

echo ""
echo "Waiting for all processes..."
wait

echo ""
echo "Done! Results in $OUTPUT"

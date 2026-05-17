#!/bin/bash
# Large Machine - Scenario 4: 400K GET + 100K SET via MSET(20) (valkey-benchmark) + MSET(20) (Python)
# Host: ec2-98-80-5-25.compute-1.amazonaws.com
# MSET of 20 keys = 100K SET ops / 20 = 5K MSET commands per second

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
USE_TLS=true
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
        --no-tls)
            USE_TLS=false
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# TLS configuration for native valkey-benchmark
TLS_CERT="${VB_TLS_CERT:-/home/ubuntu/valkey-polyglot-benchmark/python/tls/client.crt}"
TLS_KEY="${VB_TLS_KEY:-/home/ubuntu/valkey-polyglot-benchmark/python/tls/client.key}"
TLS_CACERT="${VB_TLS_CACERT:-/home/ubuntu/valkey-polyglot-benchmark/python/tls/ca.crt}"
TLS_ARGS=""
PYTHON_TLS_ARGS=""
if [ "$USE_TLS" = true ]; then
    TLS_ARGS="--tls --cert $TLS_CERT --key $TLS_KEY --cacert $TLS_CACERT"
    PYTHON_TLS_ARGS="--tls-cert $TLS_CERT --tls-key $TLS_KEY --tls-cacert $TLS_CACERT"
else
    PYTHON_TLS_ARGS="--no-tls"
fi

HOST="ec2-98-80-5-25.compute-1.amazonaws.com"
REPLICA_HOST="${REPLICA_HOST:-ec2-54-160-129-85.compute-1.amazonaws.com}"

# Config matching set_benchmark.py
VB_DATA_SIZE=512
VB_KEYSPACE=450000000
MSET_KEYS=20  # Number of keys per MSET command

# 400K GET total = 20 processes x 20K RPS each
VB_GET_CONCURRENCY=20
VB_GET_RPS=20000
VB_GET_NREQ=288000000

# 100K SET total via MSET(20) = 5K MSET commands/sec = 5 processes x 1K RPS each
VB_MSET_CONCURRENCY=5
VB_MSET_RPS=1000
VB_MSET_NREQ=14400000  # 1K RPS x 4 hours

# Replica settings
VB_REPLICA_GET_CONCURRENCY=10
VB_REPLICA_GET_RPS=40000
VB_REPLICA_GET_NREQ=144000000

VB_CLIENTS=50
VB_THREADS=4
VB_CMD="valkey-benchmark"

# Python MSET for latency stats
# 100 MSET/sec x 20 keys = 2000 SET ops/sec from Python (for latency measurement)
PYTHON_QPS=100
PYTHON_NREQ=8500000000
PYTHON_THREADS=4

OUTPUT="$PYTHON_DIR/results.csv"
LOG_DIR="$PYTHON_DIR/logs"
mkdir -p "$LOG_DIR"
rm -f "$OUTPUT"

echo "=========================================="
echo "Large Machine - Scenario 4: MSET Traffic"
echo "=========================================="
echo "Host: $HOST"
echo "Data Size: $VB_DATA_SIZE bytes"
echo "Keyspace: $VB_KEYSPACE"
echo "MSET Keys: $MSET_KEYS keys per MSET command"
echo "GET: $VB_GET_CONCURRENCY processes x $VB_GET_RPS RPS = $((VB_GET_CONCURRENCY * VB_GET_RPS)) TPS"
echo "MSET (valkey-benchmark): $VB_MSET_CONCURRENCY processes x $VB_MSET_RPS RPS = $((VB_MSET_CONCURRENCY * VB_MSET_RPS)) MSET/sec = $((VB_MSET_CONCURRENCY * VB_MSET_RPS * MSET_KEYS)) SET ops/sec"
echo "MSET (Python): $PYTHON_QPS MSET/sec x $MSET_KEYS = $((PYTHON_QPS * MSET_KEYS)) SET ops/sec (for latency measurement)"
if [ -n "$REPLICA_HOST" ]; then
    echo "Replica Host: $REPLICA_HOST"
    echo "Replica GET: $VB_REPLICA_GET_CONCURRENCY x $VB_REPLICA_GET_RPS RPS"
fi
echo "Skip Warmup: $SKIP_WARMUP"
echo "TLS: $USE_TLS"
echo "Output: $OUTPUT"
echo "=========================================="
echo ""

# === Warmup Phase ===
if [ "$SKIP_WARMUP" = false ]; then
    echo "Phase 1: Warmup - Populating $VB_KEYSPACE keys using native valkey-benchmark"
    echo "   Command: SET key:__rand_int__ __data__ (sequential, pipelined)"
    echo "   Data size: $VB_DATA_SIZE bytes"
    echo ""

    WARMUP_LOG="$LOG_DIR/warmup_vb.log"
    echo "Launching valkey-benchmark warmup (logging to $WARMUP_LOG)"
    $VB_CMD -h "$HOST" $TLS_ARGS \
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
# Launch Python MSET stats process
echo "--- Launching Python MSET stats process ---"
LOG_FILE="$LOG_DIR/python_mset_stats.log"
python3 valkey-benchmark.py -c $PYTHON_THREADS --threads $PYTHON_THREADS -t custom \
     --custom-command-file "scenarios/mset_benchmark_large.py" \
     -H "$HOST" $PYTHON_TLS_ARGS \
     --qps $PYTHON_QPS -n $PYTHON_NREQ --timeout 50 \
     --get-probe-keyspace $VB_KEYSPACE \
     --output-csv "$OUTPUT" >"$LOG_FILE" 2>&1 &
PIDS+=($!)

# Launch valkey-benchmark GET workers
echo ""
echo "--- Launching valkey-benchmark GET workers ---"
for i in $(seq 1 $VB_GET_CONCURRENCY); do
    LOG_FILE="$LOG_DIR/vb_get_$i.log"
    echo "Launching GET worker $i @ $VB_GET_RPS RPS"
    $VB_CMD -h "$HOST" $TLS_ARGS \
            -c $VB_CLIENTS --threads $VB_THREADS \
            -r $VB_KEYSPACE -d $VB_DATA_SIZE \
            -n $VB_GET_NREQ --rps $VB_GET_RPS \
            -- GET "key:__rand_int__" \
            >"$LOG_FILE" 2>&1 &
    PIDS+=($!)
done

# Launch valkey-benchmark MSET workers
# MSET with 20 random keys: MSET key1 val1 key2 val2 ... key20 val20
echo ""
echo "--- Launching valkey-benchmark MSET workers ---"
for i in $(seq 1 $VB_MSET_CONCURRENCY); do
    LOG_FILE="$LOG_DIR/vb_mset_$i.log"
    echo "Launching MSET worker $i @ $VB_MSET_RPS RPS ($MSET_KEYS keys per MSET)"
    $VB_CMD -h "$HOST" $TLS_ARGS \
            -c $VB_CLIENTS --threads $VB_THREADS \
            -r $VB_KEYSPACE -d $VB_DATA_SIZE \
            -n $VB_MSET_NREQ --rps $VB_MSET_RPS \
            -- MSET "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
                    "key:__rand_int__" __data__ \
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
        $VB_CMD -h "$REPLICA_HOST" $TLS_ARGS \
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

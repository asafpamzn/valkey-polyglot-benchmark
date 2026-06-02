#!/bin/bash
# Large Machine - Scenario 5: Large Hash Tables (300GB)
# 3000 hash tables × 100,000 fields × 1000 bytes = 300GB total
# Each hash table is ~100MB
# Traffic: 400K HGET + 100K HSET (Python) + Python latency measurement

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
USE_EC=false
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
        --ec)
            USE_EC=true
            shift
            ;;
        *)
            shift
            ;;
    esac
done

# TLS configuration
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

if [ "$USE_EC" = true ]; then
    HOST="criu-try.dpingq.ng.0001.use1.cache.amazonaws.com"
else
    HOST="ec2-54-173-41-193.compute-1.amazonaws.com"
fi
REPLICA_HOST="${REPLICA_HOST:-}"

# Hash table configuration
NUM_HASH_TABLES=3000
FIELDS_PER_HASH=100000
VALUE_SIZE=1000
TOTAL_DATA_GB=300

# Warmup configuration
WARMUP_PROCESSES=30
# Each process handles 100 hash tables, 10 concurrent at a time = 10 invocations
WARMUP_INVOCATIONS=10

# Benchmark traffic configuration
# 400K HGET total = 20 processes x 20K QPS each
HGET_CONCURRENCY=20
HGET_QPS=20000
HGET_NREQ=288000000

# 100K HSET total = 5 processes x 20K QPS each
HSET_CONCURRENCY=5
HSET_QPS=20000
HSET_NREQ=144000000

# Python latency measurement (same pattern as scenario 3)
PYTHON_HSET_QPS=1500
PYTHON_HGET_QPS=1500
PYTHON_NREQ=8500000000
PYTHON_THREADS=4

# Replica settings
REPLICA_HGET_CONCURRENCY=10
REPLICA_HGET_QPS=40000
REPLICA_HGET_NREQ=144000000

OUTPUT="$PYTHON_DIR/results.csv"
LOG_DIR="$PYTHON_DIR/logs"
mkdir -p "$LOG_DIR"
rm -f "$OUTPUT"

echo "=========================================="
echo "Large Machine - Scenario 5: Large Hash Tables"
echo "=========================================="
echo "Host: $HOST"
echo "Hash Tables: $NUM_HASH_TABLES"
echo "Fields per Hash: $FIELDS_PER_HASH"
echo "Value Size: $VALUE_SIZE bytes (70% compressible)"
echo "Total Data: ~${TOTAL_DATA_GB}GB"
echo "HGET: $HGET_CONCURRENCY processes x $HGET_QPS QPS = $((HGET_CONCURRENCY * HGET_QPS)) TPS"
echo "HSET: $HSET_CONCURRENCY processes x $HSET_QPS QPS = $((HSET_CONCURRENCY * HSET_QPS)) TPS"
echo "Python HSET latency: $PYTHON_HSET_QPS QPS (for latency measurement)"
echo "Python HGET latency: $PYTHON_HGET_QPS QPS (for latency measurement)"
if [ -n "$REPLICA_HOST" ]; then
    echo "Replica Host: $REPLICA_HOST"
    echo "Replica HGET: $REPLICA_HGET_CONCURRENCY x $REPLICA_HGET_QPS QPS"
fi
echo "Skip Warmup: $SKIP_WARMUP"
echo "TLS: $USE_TLS"
echo "Output: $OUTPUT"
echo "=========================================="
echo ""

# === Warmup Phase ===
if [ "$SKIP_WARMUP" = false ]; then
    echo "Phase 1: Warmup - Populating $NUM_HASH_TABLES hash tables ($WARMUP_PROCESSES parallel processes)"
    echo "   Each hash: $FIELDS_PER_HASH fields x $VALUE_SIZE bytes = ~100MB"
    echo "   Total: ~${TOTAL_DATA_GB}GB"
    echo ""

    export HSET_WARMUP_MODE=1

    for i in $(seq 0 $((WARMUP_PROCESSES - 1))); do
        WARMUP_LOG="$LOG_DIR/warmup_$i.log"
        echo "Launching warmup process $i (logging to $WARMUP_LOG)"

        WARMUP_PROCESS_ID=$i WARMUP_TOTAL_PROCESSES=$WARMUP_PROCESSES \
        python3 valkey-benchmark.py -c 1 --threads 1 -t custom \
             --custom-command-file "scenarios/hset_benchmark_scenario5.py" \
             -H "$HOST" $PYTHON_TLS_ARGS \
             -n $WARMUP_INVOCATIONS \
             --timeout 50000 \
             > "$WARMUP_LOG" 2>&1 &
        PIDS+=($!)
    done

    echo ""
    echo "Waiting for all warmup processes to complete..."
    wait
    PIDS=()

    unset HSET_WARMUP_MODE

    echo "Warmup completed!"
    echo ""
else
    echo "Skipping warmup phase"
    echo ""
fi

# === Benchmark Phase ===
echo "Phase 2: Benchmark"
echo ""

# Launch Python HSET latency stats process
echo "--- Launching Python HSET latency stats process ---"
LOG_FILE="$LOG_DIR/python_hset_stats.log"
python3 valkey-benchmark.py -c $PYTHON_THREADS --threads $PYTHON_THREADS -t custom \
     --custom-command-file "scenarios/hset_benchmark_scenario5.py" \
     -H "$HOST" $PYTHON_TLS_ARGS \
     --qps $PYTHON_HSET_QPS -n $PYTHON_NREQ --timeout 50 \
     --output-csv "$OUTPUT" >"$LOG_FILE" 2>&1 &
PIDS+=($!)

# Launch Python HGET latency stats process
echo "--- Launching Python HGET latency stats process ---"
LOG_FILE="$LOG_DIR/python_hget_stats.log"
python3 valkey-benchmark.py -c $PYTHON_THREADS --threads $PYTHON_THREADS -t custom \
     --custom-command-file "scenarios/hget_benchmark_scenario5.py" \
     -H "$HOST" $PYTHON_TLS_ARGS \
     --qps $PYTHON_HGET_QPS -n $PYTHON_NREQ --timeout 50 \
     --output-csv "${OUTPUT%.csv}_hget.csv" >"$LOG_FILE" 2>&1 &
PIDS+=($!)

# Launch HGET bulk traffic workers
echo ""
echo "--- Launching HGET workers ---"
for i in $(seq 1 $HGET_CONCURRENCY); do
    LOG_FILE="$LOG_DIR/hget_$i.log"
    echo "Launching HGET worker $i @ $HGET_QPS QPS"
    python3 valkey-benchmark.py -c $PYTHON_THREADS --threads $PYTHON_THREADS -t custom \
         --custom-command-file "scenarios/hget_benchmark_scenario5.py" \
         -H "$HOST" $PYTHON_TLS_ARGS \
         --qps $HGET_QPS -n $HGET_NREQ --timeout 50 \
         >"$LOG_FILE" 2>&1 &
    PIDS+=($!)
done

# Launch HSET bulk traffic workers
echo ""
echo "--- Launching HSET workers ---"
for i in $(seq 1 $HSET_CONCURRENCY); do
    LOG_FILE="$LOG_DIR/hset_$i.log"
    echo "Launching HSET worker $i @ $HSET_QPS QPS"
    python3 valkey-benchmark.py -c $PYTHON_THREADS --threads $PYTHON_THREADS -t custom \
         --custom-command-file "scenarios/hset_benchmark_scenario5.py" \
         -H "$HOST" $PYTHON_TLS_ARGS \
         --qps $HSET_QPS -n $HSET_NREQ --timeout 50 \
         >"$LOG_FILE" 2>&1 &
    PIDS+=($!)
done

# Launch replica HGET workers if specified
if [ -n "$REPLICA_HOST" ]; then
    echo ""
    echo "--- Launching replica HGET workers ---"
    for i in $(seq 1 $REPLICA_HGET_CONCURRENCY); do
        LOG_FILE="$LOG_DIR/replica_hget_$i.log"
        echo "Launching replica HGET worker $i @ $REPLICA_HGET_QPS QPS"
        python3 valkey-benchmark.py -c $PYTHON_THREADS --threads $PYTHON_THREADS -t custom \
             --custom-command-file "scenarios/hget_benchmark_scenario5.py" \
             -H "$REPLICA_HOST" $PYTHON_TLS_ARGS \
             --qps $REPLICA_HGET_QPS -n $REPLICA_HGET_NREQ --timeout 50 \
             >"$LOG_FILE" 2>&1 &
        PIDS+=($!)
    done
fi

echo ""
echo "Waiting for all processes... (Ctrl+C to stop)"
wait

echo ""
echo "Done! Results in $OUTPUT"

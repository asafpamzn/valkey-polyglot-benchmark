#!/bin/bash
set -euo pipefail

# Cleanup function to kill all python3 processes
cleanup() {
    echo ""
    echo "🛑 Cleaning up: Killing all python3 processes..."
    pkill -9 python3 || true
    exit 0
}

# Set trap to call cleanup on INT (Ctrl+C), TERM, or EXIT
trap cleanup INT TERM EXIT

# === Config ===
# Parse arguments
USE_LARGE=false
SKIP_WARMUP=false
RESET_DB=false
HOST=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --large)
            USE_LARGE=true
            shift
            ;;
        --skip-warmup)
            SKIP_WARMUP=true
            shift
            ;;
        --reset-db)
            RESET_DB=true
            shift
            ;;
        *)
            HOST="$1"
            shift
            ;;
    esac
done

# Set default host if not provided
HOST="${HOST:-ec2-54-221-42-237.compute-1.amazonaws.com}"

# Select custom command file based on --large flag
if [ "$USE_LARGE" = true ]; then
    CUSTOM_CMD_FILE="hset_benchmark_large.py"
    CONFIG_DESC="Large Mixed (80×100MB + 1×1GB)"
else
    CUSTOM_CMD_FILE="hset_benchmark.py"
    CONFIG_DESC="Standard (90×100MB)"
fi

QPS=5000
NREQ=8500000000
THREADS=4
CONCURRENCY=6
OUTPUT="results.csv"
CMD="python3 valkey-benchmark.py"

# === Setup ===
rm -f "$OUTPUT"
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"

echo "=========================================="
echo "🚀 Valkey HSET Benchmark"
echo "=========================================="
echo "Configuration: $CONFIG_DESC"
echo "Custom Command File: $CUSTOM_CMD_FILE"
echo "Host: $HOST"
echo "Concurrency: $CONCURRENCY processes"
echo "Threads per process: $THREADS"
echo "QPS per process: $QPS"
echo "Total requests per process: $NREQ"
echo "Reset DB: $RESET_DB"
echo "Skip Warmup: $SKIP_WARMUP"
echo "=========================================="
echo ""

# === Phase 0: Database Reset ===
if [ "$RESET_DB" = true ]; then
    echo "🗑️  Phase 0: Database Reset - Flushing all data"
    echo "   Running: redis-cli -h $HOST flushall"
    echo ""
    
    if redis-cli -h "$HOST" flushall; then
        echo "✅ Database reset completed successfully!"
    else
        echo "❌ Warning: Database reset may have failed. Continuing anyway..."
    fi
    echo ""
fi

# === Phase 1: Warmup ===
if [ "$SKIP_WARMUP" = false ]; then
    echo "🔥 Phase 1: Warmup - Populating hash tables"
    echo "   Using concurrent tasks to populate hash tables in parallel..."
    echo ""

    WARMUP_LOG="$LOG_DIR/warmup.log"
    export HSET_WARMUP_MODE=1

    # Use single thread/client, but each call spawns 8 concurrent tasks internally
    # 10 calls × 8 hash tables per call = 80 hash tables
    # Each hash table: 100,000 fields in batches of 20 = 5,000 HSET ops × 8 concurrent = 40,000 HSET ops per call
    # Use 50-second timeout to allow concurrent operations to complete
    $CMD -c 1 --threads 1 -t custom \
         --custom-command-file "$CUSTOM_CMD_FILE" \
         -H "$HOST" \
         -n 10 \
         --timeout 50000 \
         > "$WARMUP_LOG" 2>&1

    unset HSET_WARMUP_MODE

    echo "✅ Warmup completed successfully!"
    echo ""
else
    echo "⏭️  Phase 1: Warmup - SKIPPED"
    echo ""
fi

# === Phase 2: Benchmark ===
echo "⚡ Phase 2: Benchmark - Launching $CONCURRENCY concurrent processes"
echo "   Each process will perform random HSET operations on the 80 hash tables"
echo "   Results will be saved to $OUTPUT and logs under $LOG_DIR"
echo ""

# Launch background workers
for i in $(seq 1 $((CONCURRENCY - 1))); do
  LOG_FILE="$LOG_DIR/run_$i.log"
  echo "▶️  Launching background worker $i (logging to $LOG_FILE)"
  $CMD -c $THREADS --threads $THREADS -t custom \
       --custom-command-file "$CUSTOM_CMD_FILE" \
       -H "$HOST" \
       --qps $QPS -n $NREQ --timeout 50\
       >"$LOG_FILE" 2>&1 &
done

# Launch the final worker with CSV output
LOG_FILE="$LOG_DIR/run_final.log"
echo "▶️  Launching final worker with CSV output ($OUTPUT)"
$CMD -c $THREADS --threads $THREADS -t custom \
     --custom-command-file "$CUSTOM_CMD_FILE" \
     -H "$HOST" \
     --qps $QPS -n $NREQ --timeout 50\
     --output-csv "$OUTPUT" >"$LOG_FILE" 2>&1 &

echo ""
echo "⏳ Waiting for all benchmark processes to complete..."
echo ""

# Wait for all workers
wait

echo ""
echo "=========================================="
echo "✅ All benchmark runs completed!"
echo "=========================================="
echo "📊 Results available in $OUTPUT"
echo "📋 Logs available in $LOG_DIR/"
echo ""

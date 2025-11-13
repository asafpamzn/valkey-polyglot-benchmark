#!/bin/bash
set -euo pipefail

# === Config ===
HOST="ec2-54-157-11-21.compute-1.amazonaws.com"
QPS=5000
NREQ=850000000
DATA_SIZE=1000
RANGE=8500000
THREADS=8
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
echo "Host: $HOST"
echo "Concurrency: $CONCURRENCY processes"
echo "Threads per process: $THREADS"
echo "QPS per process: $QPS"
echo "Total requests per process: $NREQ"
echo "Data size: $DATA_SIZE bytes"
echo "=========================================="
echo ""

# === Phase 1: Warmup ===
echo "🔥 Phase 1: Warmup - Creating 80 hash tables with 100,000 items each"
echo "   This will perform 8,000,000 HSET operations..."
echo ""

WARMUP_LOG="$LOG_DIR/warmup.log"
export HSET_WARMUP_MODE=1

$CMD -c $THREADS --threads $THREADS -t custom \
     --custom-command-file hset_benchmark.py \
     -H "$HOST" \
     -n 8000000 \
     > "$WARMUP_LOG" 2>&1

unset HSET_WARMUP_MODE

echo "✅ Warmup completed successfully!"
echo ""

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
       --custom-command-file hset_benchmark.py \
       -H "$HOST" \
       --qps $QPS -n $NREQ \
       >"$LOG_FILE" 2>&1 &
done

# Launch the final worker with CSV output
LOG_FILE="$LOG_DIR/run_final.log"
echo "▶️  Launching final worker with CSV output ($OUTPUT)"
$CMD -c $THREADS --threads $THREADS -t custom \
     --custom-command-file hset_benchmark.py \
     -H "$HOST" \
     --qps $QPS -n $NREQ \
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

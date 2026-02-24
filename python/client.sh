#!/bin/bash
set -euo pipefail

# Cleanup function to kill all benchmark processes
cleanup() {
    echo ""
    echo "🛑 Cleaning up: Killing all benchmark processes..."
    pkill -9 python3 || true
    pkill -9 valkey-benchmark || true
    exit 0
}

# Set trap to call cleanup on INT (Ctrl+C), TERM, or EXIT
trap cleanup INT TERM EXIT

# === Config ===
# Parse arguments
USE_LARGE=false
USE_SET=false
SKIP_WARMUP=false
RESET_DB=false
HOST=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --large)
            USE_LARGE=true
            shift
            ;;
        --set)
            USE_SET=true
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
HOST="${HOST:-ec2-54-80-89-59.compute-1.amazonaws.com}"

# Select custom command file based on flags
if [ "$USE_SET" = true ]; then
    CUSTOM_CMD_FILE="set_benchmark.py"
    CONFIG_DESC="SET Benchmark (90M keys × 50 bytes, valkey-benchmark format)"
    WARMUP_MODE_VAR="SET_WARMUP_MODE"
    WARMUP_PROCESSES=8  # Number of parallel warmup processes
    WARMUP_INVOCATIONS=125  # Each process needs 125 invocations (125M keys / 1M per call)
elif [ "$USE_LARGE" = true ]; then
    CUSTOM_CMD_FILE="hset_benchmark_large.py"
    CONFIG_DESC="Large Mixed (80×100MB + 1×1GB)"
    WARMUP_MODE_VAR="HSET_WARMUP_MODE"
    WARMUP_PROCESSES=1
    WARMUP_INVOCATIONS=10
else
    # Default: HSET + HGET combined benchmark
    CUSTOM_CMD_FILE="hset_benchmark.py"
    CONFIG_DESC="Standard (100 hash tables × 900K fields) - HSET + HGET"
    WARMUP_MODE_VAR="HSET_WARMUP_MODE"
    WARMUP_PROCESSES=10  # Parallel warmup: each process handles 10 hash tables
    WARMUP_INVOCATIONS=1  # 10 hashes / 20 concurrent per invocation = 1 invocation needed
fi

# HSET QPS and concurrency
QPS=2000
CONCURRENCY=10

# HGET-specific settings (used in default scenario alongside HSET)
HGET_QPS=3000
HGET_CONCURRENCY=10

# Native valkey-benchmark settings (used in SET scenario)
VB_CMD="valkey-benchmark"
VB_CONCURRENCY=10          # Number of native valkey-benchmark worker processes
VB_CLIENTS=4               # -c clients per native worker
VB_THREADS=4               # --threads per native worker
VB_KEYSPACE=90000000       # -r keyspacelen (must match set_benchmark.py total_keys)
VB_DATA_SIZE=50            # -d data size in bytes (must match set_benchmark.py value_size)

NREQ=8500000000
THREADS=4
OUTPUT="results.csv"
CMD="python3 valkey-benchmark.py"

# === Setup ===
rm -f "$OUTPUT"
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"

echo "=========================================="
echo "🚀 Valkey Benchmark"
echo "=========================================="
echo "Configuration: $CONFIG_DESC"
echo "Custom Command File: $CUSTOM_CMD_FILE"
echo "Host: $HOST"
echo "Python Concurrency: $CONCURRENCY processes"
if [ "$USE_SET" = false ] && [ "$USE_LARGE" = false ]; then
    echo "HGET Concurrency: $HGET_CONCURRENCY processes"
fi
if [ "$USE_SET" = true ]; then
    echo "Native valkey-benchmark Concurrency: $VB_CONCURRENCY processes"
    echo "Native valkey-benchmark clients/process: $VB_CLIENTS"
fi
echo "Threads per process: $THREADS"
echo "QPS per Python process: $QPS"
if [ "$USE_SET" = false ] && [ "$USE_LARGE" = false ]; then
    echo "HGET QPS per process: $HGET_QPS"
fi
echo "Total requests per process: $NREQ"
echo "Reset DB: $RESET_DB"
echo "Skip Warmup: $SKIP_WARMUP"
if [ "$USE_SET" = true ]; then
    echo "Warmup Processes: $WARMUP_PROCESSES"
fi
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
    if [ "$USE_SET" = true ]; then
        # SET scenario: use native valkey-benchmark with --sequential for fast warmup
        echo "🔥 Phase 1: Warmup - Populating $VB_KEYSPACE keys using native valkey-benchmark"
        echo "   Command: SET key:__rand_int__ __data__ (sequential, pipelined)"
        echo "   Data size: $VB_DATA_SIZE bytes"
        echo ""

        WARMUP_LOG="$LOG_DIR/warmup_vb.log"
        echo "▶️  Launching valkey-benchmark warmup (logging to $WARMUP_LOG)"
        $VB_CMD -h "$HOST" \
                -c 50 --threads 4 \
                -r $VB_KEYSPACE -d $VB_DATA_SIZE \
                -n $VB_KEYSPACE \
                -P 16 \
                --sequential \
                -- SET "key:__rand_int__" __data__ \
                > "$WARMUP_LOG" 2>&1

        echo "✅ Warmup completed successfully!"
        echo ""
    else
        # HSET/LARGE scenario: use Python warmup
        echo "🔥 Phase 1: Warmup - Populating data with $WARMUP_PROCESSES parallel process(es)"
        echo ""

        export ${WARMUP_MODE_VAR}=1

        # Launch warmup processes in parallel
        for i in $(seq 0 $((WARMUP_PROCESSES - 1))); do
            WARMUP_LOG="$LOG_DIR/warmup_$i.log"
            
            echo "▶️  Launching warmup process $i (logging to $WARMUP_LOG)"
            
            # Pass process partition info to all benchmarks (HSET ignores these, SET uses them)
            WARMUP_PROCESS_ID=$i WARMUP_TOTAL_PROCESSES=$WARMUP_PROCESSES \
            $CMD -c 1 --threads 1 -t custom \
                 --custom-command-file "$CUSTOM_CMD_FILE" \
                 -H "$HOST" \
                 -n $WARMUP_INVOCATIONS \
                 --timeout 50000 \
                 > "$WARMUP_LOG" 2>&1 &
        done

        echo ""
        echo "⏳ Waiting for all warmup processes to complete..."
        wait

        unset ${WARMUP_MODE_VAR}

        echo "✅ Warmup completed successfully!"
        echo ""
    fi
else
    echo "⏭️  Phase 1: Warmup - SKIPPED"
    echo ""
fi

# === Phase 2: Benchmark ===
if [ "$USE_SET" = false ] && [ "$USE_LARGE" = false ]; then
    # Default scenario: launch both HSET and HGET workers
    TOTAL_WORKERS=$((CONCURRENCY + HGET_CONCURRENCY))
    echo "⚡ Phase 2: Benchmark - Launching $TOTAL_WORKERS concurrent processes"
    echo "   HSET: $CONCURRENCY processes @ $QPS QPS each (using hset_benchmark.py)"
    echo "   HGET: $HGET_CONCURRENCY processes @ $HGET_QPS QPS each (using hget_benchmark.py)"
    echo "   Results will be saved to $OUTPUT and logs under $LOG_DIR"
    echo ""

    # Launch HSET background workers
    echo "--- Launching HSET workers ---"
    for i in $(seq 1 $((CONCURRENCY - 1))); do
      LOG_FILE="$LOG_DIR/hset_$i.log"
      echo "▶️  Launching HSET worker $i (logging to $LOG_FILE)"
      $CMD -c $THREADS --threads $THREADS -t custom \
           --custom-command-file "hset_benchmark.py" \
           -H "$HOST" \
           --qps $QPS -n $NREQ --timeout 50\
           >"$LOG_FILE" 2>&1 &
    done

    # Launch the final HSET worker with CSV output
    LOG_FILE="$LOG_DIR/hset_final.log"
    echo "▶️  Launching HSET final worker with CSV output ($OUTPUT)"
    $CMD -c $THREADS --threads $THREADS -t custom \
         --custom-command-file "hset_benchmark.py" \
         -H "$HOST" \
         --qps $QPS -n $NREQ --timeout 50\
         --output-csv "$OUTPUT" >"$LOG_FILE" 2>&1 &

    # Launch HGET background workers
    echo ""
    echo "--- Launching HGET workers ---"
    for i in $(seq 1 $HGET_CONCURRENCY); do
      LOG_FILE="$LOG_DIR/hget_$i.log"
      echo "▶️  Launching HGET worker $i (logging to $LOG_FILE)"
      $CMD -c $THREADS --threads $THREADS -t custom \
           --custom-command-file "hget_benchmark.py" \
           -H "$HOST" \
           --qps $HGET_QPS -n $NREQ --timeout 50\
           >"$LOG_FILE" 2>&1 &
    done
elif [ "$USE_SET" = true ]; then
    # SET scenario: Python GET workers + native valkey-benchmark GET workers
    TOTAL_WORKERS=$((CONCURRENCY + VB_CONCURRENCY))
    echo "⚡ Phase 2: Benchmark - Launching $TOTAL_WORKERS concurrent processes"
    echo "   Python GET: $CONCURRENCY processes @ $QPS QPS each (using set_benchmark.py)"
    echo "   Native valkey-benchmark GET: $VB_CONCURRENCY processes (using valkey-benchmark)"
    echo "   Key format: key:XXXXXXXXXXXX (12-digit zero-padded, range 0-$((VB_KEYSPACE - 1)))"
    echo "   Results will be saved to $OUTPUT and logs under $LOG_DIR"
    echo ""

    # Launch Python background workers
    echo "--- Launching Python GET workers ---"
    for i in $(seq 1 $((CONCURRENCY - 1))); do
      LOG_FILE="$LOG_DIR/run_$i.log"
      echo "▶️  Launching Python GET worker $i (logging to $LOG_FILE)"
      $CMD -c $THREADS --threads $THREADS -t custom \
           --custom-command-file "$CUSTOM_CMD_FILE" \
           -H "$HOST" \
           --qps $QPS -n $NREQ --timeout 50\
           >"$LOG_FILE" 2>&1 &
    done

    # Launch the final Python worker with CSV output
    LOG_FILE="$LOG_DIR/run_final.log"
    echo "▶️  Launching Python GET final worker with CSV output ($OUTPUT)"
    $CMD -c $THREADS --threads $THREADS -t custom \
         --custom-command-file "$CUSTOM_CMD_FILE" \
         -H "$HOST" \
         --qps $QPS -n $NREQ --timeout 50\
         --output-csv "$OUTPUT" >"$LOG_FILE" 2>&1 &

    # Launch native valkey-benchmark GET workers
    echo ""
    echo "--- Launching native valkey-benchmark GET workers ---"
    for i in $(seq 1 $VB_CONCURRENCY); do
      LOG_FILE="$LOG_DIR/vb_get_$i.log"
      echo "▶️  Launching valkey-benchmark GET worker $i (logging to $LOG_FILE)"
      $VB_CMD -h "$HOST" \
              -c $VB_CLIENTS --threads $VB_THREADS \
              -r $VB_KEYSPACE -d $VB_DATA_SIZE \
              -n $NREQ --rps $QPS \
              -- GET "key:__rand_int__" \
              >"$LOG_FILE" 2>&1 &
    done
else
    # LARGE scenario: single command type (HSET)
    echo "⚡ Phase 2: Benchmark - Launching $CONCURRENCY concurrent processes"
    echo "   Each process will perform random HSET operations"
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
fi

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

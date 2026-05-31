#!/bin/bash
# Large Machine - Scenario 3 with Data Integrity Validation
# Phase 1: Warmup (populate 450M keys with integrity-checked values)
# Phase 2: Traffic for 10 minutes (100K SET + 400K GET)
# Phase 3: Validate all data (CRC + primary/replica comparison)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PYTHON_DIR"

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
SKIP_TRAFFIC=false
HOST="ec2-98-80-5-25.compute-1.amazonaws.com"
REPLICA_HOST=""
USE_TLS=true
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-warmup)
            SKIP_WARMUP=true
            shift
            ;;
        --skip-traffic)
            SKIP_TRAFFIC=true
            shift
            ;;
        --host)
            HOST="$2"
            shift 2
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

if [ -z "$HOST" ]; then
    echo "ERROR: --host is required"
    echo "Usage: $0 --host <primary-host> --replica <replica-host> [--skip-warmup] [--skip-traffic]"
    exit 1
fi

if [ -z "$REPLICA_HOST" ]; then
    echo "ERROR: --replica is required"
    echo "Usage: $0 --host <primary-host> --replica <replica-host> [--skip-warmup] [--skip-traffic]"
    exit 1
fi

# TLS configuration for native valkey-benchmark
TLS_CERT="${VB_TLS_CERT:-/home/ubuntu/valkey-polyglot-benchmark/python/tls/client.crt}"
TLS_KEY="${VB_TLS_KEY:-/home/ubuntu/valkey-polyglot-benchmark/python/tls/client.key}"
TLS_CACERT="${VB_TLS_CACERT:-/home/ubuntu/valkey-polyglot-benchmark/python/tls/ca.crt}"
TLS_ARGS=""
PYTHON_TLS_ARGS=""
if [ "$USE_TLS" = true ]; then
    TLS_ARGS="--tls --cert $TLS_CERT --key $TLS_KEY --cacert $TLS_CACERT"
else
    PYTHON_TLS_ARGS="--no-tls"
fi

# Configuration
VB_DATA_SIZE=512
VB_KEYSPACE=450000000
TRAFFIC_DURATION=600  # 10 minutes

# Traffic: 400K GET = 20 processes x 20K RPS
VB_GET_CONCURRENCY=20
VB_GET_RPS=20000

# Traffic: 100K SET = 5 processes x 20K RPS
VB_SET_CONCURRENCY=5
VB_SET_RPS=20000

VB_CLIENTS=50
VB_THREADS=4
VB_CMD="valkey-benchmark"

# Warmup: multi-process Python
WARMUP_PROCESSES=16

# Validation
VALIDATION_PROCESSES=64

LOG_DIR="$PYTHON_DIR/logs"
mkdir -p "$LOG_DIR"

check_server_alive() {
    local host="$1"
    local label="${2:-server}"
    local response
    if [ "$USE_TLS" = true ]; then
        response=$(valkey-cli -h "$host" -p 6379 --tls --cert "$TLS_CERT" --key "$TLS_KEY" --cacert "$TLS_CACERT" PING 2>/dev/null)
    else
        response=$(valkey-cli -h "$host" -p 6379 PING 2>/dev/null)
    fi
    if [ "$response" != "PONG" ]; then
        echo "ERROR: $label at $host is NOT responding (expected PONG, got: '$response')"
        return 1
    fi
    echo "$label at $host is alive (PONG)"
    return 0
}

check_key_count() {
    local host="$1"
    local label="${2:-server}"
    local expected="$VB_KEYSPACE"
    local count
    if [ "$USE_TLS" = true ]; then
        count=$(valkey-cli -h "$host" -p 6379 --tls --cert "$TLS_CERT" --key "$TLS_KEY" --cacert "$TLS_CACERT" --raw DBSIZE 2>/dev/null)
    else
        count=$(valkey-cli -h "$host" -p 6379 --raw DBSIZE 2>/dev/null)
    fi
    if [ -z "$count" ]; then
        echo "ERROR: $label at $host returned empty DBSIZE (server may be down)"
        return 1
    fi
    if [ "$count" -ne "$expected" ]; then
        echo "ERROR: $label at $host has $count keys (expected $expected)"
        return 1
    fi
    echo "$label at $host has $count keys (expected $expected) - OK"
    return 0
}

echo "=========================================================="
echo "Large Machine - Scenario 3: Integrity Validation"
echo "=========================================================="
echo "Host:            $HOST"
echo "Replica:         $REPLICA_HOST"
echo "Data Size:       $VB_DATA_SIZE bytes"
echo "Keyspace:        $VB_KEYSPACE"
echo "Traffic:         ${VB_GET_CONCURRENCY}x${VB_GET_RPS} GET + ${VB_SET_CONCURRENCY}x${VB_SET_RPS} SET"
echo "Traffic Duration:$TRAFFIC_DURATION seconds"
echo "Skip Warmup:     $SKIP_WARMUP"
echo "Skip Traffic:    $SKIP_TRAFFIC"
echo "TLS:             $USE_TLS"
echo "=========================================================="
echo ""

# ============================================================
# PHASE 1: WARMUP
# ============================================================
if [ "$SKIP_WARMUP" = false ]; then
    echo "============================================"
    echo "PHASE 1: WARMUP - Populating $VB_KEYSPACE keys with integrity values"
    echo "         Using $WARMUP_PROCESSES parallel Python processes"
    echo "============================================"
    echo ""

    WARMUP_PIDS=()
    for i in $(seq 0 $((WARMUP_PROCESSES - 1))); do
        LOG_FILE="$LOG_DIR/warmup_process_$i.log"
        echo "Launching warmup process $i/$WARMUP_PROCESSES"
        SET_WARMUP_MODE=1 WARMUP_PROCESS_ID=$i WARMUP_TOTAL_PROCESSES=$WARMUP_PROCESSES \
            python3 valkey-benchmark.py -c 4 --threads 4 -t custom \
            --custom-command-file "scenarios/set_benchmark_integrity_large.py" \
            -H "$HOST" $PYTHON_TLS_ARGS \
            -n 1000000000 --timeout 5000 \
            >"$LOG_FILE" 2>&1 &
        WARMUP_PIDS+=($!)
    done

    echo ""
    echo "Waiting for all warmup processes to complete..."
    WARMUP_FAILED=false
    for pid in "${WARMUP_PIDS[@]}"; do
        if ! wait "$pid"; then
            echo "WARNING: Warmup process $pid failed"
            WARMUP_FAILED=true
        fi
    done

    if [ "$WARMUP_FAILED" = true ]; then
        echo "ERROR: Some warmup processes failed. Check logs in $LOG_DIR"
        exit 1
    fi

    echo "Warmup completed!"
    echo ""

    # Verify servers are still alive after warmup
    check_server_alive "$HOST" "Primary" || exit 1
    check_server_alive "$REPLICA_HOST" "Replica" || exit 1
    check_key_count "$HOST" "Primary" || exit 1
else
    echo "Skipping warmup phase"
    echo ""
fi

# ============================================================
# PHASE 2: TRAFFIC (10 minutes)
# ============================================================
if [ "$SKIP_TRAFFIC" = false ]; then
    echo "============================================"
    echo "PHASE 2: TRAFFIC - Running for $TRAFFIC_DURATION seconds"
    echo "         $((VB_GET_CONCURRENCY * VB_GET_RPS)) GET/s + $((VB_SET_CONCURRENCY * VB_SET_RPS)) SET/s"
    echo "============================================"
    echo ""

    # Calculate total requests to sustain for the duration
    # Use a very high number since we rely on timeout via the monitor process
    VB_GET_NREQ=$((VB_GET_RPS * TRAFFIC_DURATION * 2))
    VB_SET_NREQ=$((VB_SET_RPS * TRAFFIC_DURATION * 2))

    TRAFFIC_PIDS=()

    # Launch GET workers using valkey-benchmark
    echo "--- Launching GET workers ---"
    for i in $(seq 1 $VB_GET_CONCURRENCY); do
        LOG_FILE="$LOG_DIR/traffic_get_$i.log"
        echo "  GET worker $i @ $VB_GET_RPS RPS"
        $VB_CMD -h "$HOST" \
                -c $VB_CLIENTS --threads $VB_THREADS \
                -r $VB_KEYSPACE -d $VB_DATA_SIZE \
                -n $VB_GET_NREQ --rps $VB_GET_RPS \
                -- GET "key:__rand_int__" \
                >"$LOG_FILE" 2>&1 &
        TRAFFIC_PIDS+=($!)
    done

    # Launch SET workers using Python (integrity-aware values)
    echo ""
    echo "--- Launching SET workers (integrity-aware) ---"
    PYTHON_SET_CONCURRENCY=$VB_SET_CONCURRENCY
    PYTHON_SET_RPS=$((VB_SET_RPS))
    PYTHON_SET_NREQ=$((PYTHON_SET_RPS * TRAFFIC_DURATION * 2))

    for i in $(seq 1 $PYTHON_SET_CONCURRENCY); do
        LOG_FILE="$LOG_DIR/traffic_set_$i.log"
        echo "  SET worker $i @ $PYTHON_SET_RPS RPS"
        python3 valkey-benchmark.py -c 8 --threads 8 -t custom \
            --custom-command-file "scenarios/set_benchmark_integrity_large.py" \
            -H "$HOST" $PYTHON_TLS_ARGS \
            --qps $PYTHON_SET_RPS -n $PYTHON_SET_NREQ --timeout 50 \
            >"$LOG_FILE" 2>&1 &
        TRAFFIC_PIDS+=($!)
    done

    echo ""
    echo "Traffic running. Waiting $TRAFFIC_DURATION seconds..."
    sleep $TRAFFIC_DURATION

    echo ""
    echo "Traffic duration complete. Stopping all traffic processes..."
    for pid in "${TRAFFIC_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    sleep 2
    for pid in "${TRAFFIC_PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done

    echo "Traffic phase complete!"
    echo ""

    # Wait for replication to catch up
    echo "Waiting 10 seconds for replication to settle..."
    sleep 10
else
    echo "Skipping traffic phase"
    echo ""
fi

# ============================================================
# PHASE 3: VALIDATION
# ============================================================
echo "============================================"
echo "PHASE 3: VALIDATION - Checking all $VB_KEYSPACE keys"
echo "         $VALIDATION_PROCESSES parallel processes"
echo "         Verifying CRC + primary/replica consistency"
echo "============================================"
echo ""

# Verify servers are still alive before validation
check_server_alive "$HOST" "Primary" || exit 1
check_server_alive "$REPLICA_HOST" "Replica" || exit 1
check_key_count "$HOST" "Primary" || exit 1
check_key_count "$REPLICA_HOST" "Replica" || exit 1

# Remove trap so validation can run cleanly
trap - INT TERM EXIT

python3 scenarios/validate_integrity.py \
    --primary-host "$HOST" \
    --replica-host "$REPLICA_HOST" \
    --port 6379 \
    --processes $VALIDATION_PROCESSES \
    --total-keys $VB_KEYSPACE \
    --batch-size 100 \
    $PYTHON_TLS_ARGS

VALIDATION_EXIT=$?

echo ""
if [ $VALIDATION_EXIT -eq 0 ]; then
    echo "ALL PHASES COMPLETE - DATA INTEGRITY VERIFIED"
else
    echo "VALIDATION FAILED - SEE ERRORS ABOVE"
fi

exit $VALIDATION_EXIT

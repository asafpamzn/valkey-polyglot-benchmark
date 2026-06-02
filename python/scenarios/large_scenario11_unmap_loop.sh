#!/bin/bash
# Large Machine - Scenario 11: Migration with Key Deletion (Unmap Simulation)
#
# Simulates service unmap by deleting half the keys during CRIU data transfer.
#
# Warmup (once), then loop:
#   1. Start traffic (100K SET + 400K GET)
#   2. Kill replica, wait MIGRATE_DELAY
#   3. Start migration in background
#   4. Immediately start deleting keys (simulating unmap)
#   5. Wait for migration to complete
#   6. Stop traffic
#   7. Validate remaining keys (CRC + primary/replica)
#   8. Re-populate deleted keys
#   9. Kill replica, sleep for recovery
#   → repeat

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PYTHON_DIR"

LOG_DIR="$PYTHON_DIR/logs"
mkdir -p "$LOG_DIR"
RUN_LOG="$LOG_DIR/scenario11_run_$(date +%Y%m%d_%H%M%S).log"

exec > >(tee -a "$RUN_LOG") 2>&1
echo "Logging to: $RUN_LOG"

cleanup() {
    echo ""
    echo "Cleaning up: Killing all benchmark processes..."
    pkill -9 -P $$ 2>/dev/null || true
    exit 0
}
trap cleanup INT TERM

# Parse arguments
SKIP_WARMUP=false
HOST="ec2-54-173-41-193.compute-1.amazonaws.com"
REPLICA_HOST="ec2-54-160-129-85.compute-1.amazonaws.com"
PRIMARY_SSH="ssh -i ~/.ssh/mac.pem ubuntu@ec2-54-173-41-193.compute-1.amazonaws.com"
REPLICA_SSH="ssh -i ~/.ssh/mac.pem ubuntu@ec2-54-160-129-85.compute-1.amazonaws.com"
MIGRATE_SCRIPT="~/work/criu/scripts/migrate_new.sh"
MIGRATE_DELAY=30
ITERATIONS=3
USE_TLS=true
RECOVERY_SLEEP=60
DELETE_PERCENT=50  # Percentage of keys to delete during migration

while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-warmup)
            SKIP_WARMUP=true
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
        --primary-ssh)
            PRIMARY_SSH="$2"
            shift 2
            ;;
        --replica-ssh)
            REPLICA_SSH="$2"
            shift 2
            ;;
        --migrate-script)
            MIGRATE_SCRIPT="$2"
            shift 2
            ;;
        --migrate-delay)
            MIGRATE_DELAY="$2"
            shift 2
            ;;
        --iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        --recovery-sleep)
            RECOVERY_SLEEP="$2"
            shift 2
            ;;
        --delete-percent)
            DELETE_PERCENT="$2"
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

if [ -z "$HOST" ] || [ -z "$REPLICA_HOST" ]; then
    echo "Usage: $0 --host <primary> --replica <replica> [--primary-ssh <ssh-cmd>] [--replica-ssh <ssh-cmd>]"
    echo "       [--migrate-script <path>] [--migrate-delay <sec>] [--iterations <N>]"
    echo "       [--recovery-sleep <sec>] [--delete-percent <0-100>] [--skip-warmup] [--no-tls]"
    exit 1
fi

# TLS configuration
PYTHON_TLS_ARGS=""
TLS_ARGS=""
TLS_CERT="${VB_TLS_CERT:-/home/ubuntu/valkey-polyglot-benchmark/python/tls/client.crt}"
TLS_KEY="${VB_TLS_KEY:-/home/ubuntu/valkey-polyglot-benchmark/python/tls/client.key}"
TLS_CACERT="${VB_TLS_CACERT:-/home/ubuntu/valkey-polyglot-benchmark/python/tls/ca.crt}"
if [ "$USE_TLS" = true ]; then
    TLS_ARGS="--tls --cert $TLS_CERT --key $TLS_KEY --cacert $TLS_CACERT"
else
    PYTHON_TLS_ARGS="--no-tls"
fi

# Configuration
VB_DATA_SIZE=512
VB_KEYSPACE=450000000

# Calculate keys to delete (first N keys)
KEYS_TO_DELETE=$((VB_KEYSPACE * DELETE_PERCENT / 100))
KEYS_REMAINING=$((VB_KEYSPACE - KEYS_TO_DELETE))

VB_GET_CONCURRENCY=20
VB_GET_RPS=20000

VB_SET_CONCURRENCY=5
VB_SET_RPS=20000

VB_CLIENTS=50
VB_THREADS=4
VB_CMD="valkey-benchmark"

WARMUP_PROCESSES=16
VALIDATION_PROCESSES=64
DELETE_PROCESSES=16

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
    local expected="$2"
    local label="${3:-server}"
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

get_key_count() {
    local host="$1"
    if [ "$USE_TLS" = true ]; then
        valkey-cli -h "$host" -p 6379 --tls --cert "$TLS_CERT" --key "$TLS_KEY" --cacert "$TLS_CACERT" --raw DBSIZE 2>/dev/null
    else
        valkey-cli -h "$host" -p 6379 --raw DBSIZE 2>/dev/null
    fi
}

echo "=========================================================="
echo "Scenario 11: Migration with Key Deletion (Unmap Simulation)"
echo "=========================================================="
echo "Host:            $HOST"
echo "Replica:         $REPLICA_HOST"
echo "Iterations:      $ITERATIONS"
echo "Migrate Delay:   ${MIGRATE_DELAY}s"
echo "Recovery Sleep:  ${RECOVERY_SLEEP}s"
echo "Delete Percent:  ${DELETE_PERCENT}%"
echo "Keys to Delete:  $KEYS_TO_DELETE (keys 0 to $((KEYS_TO_DELETE - 1)))"
echo "Keys Remaining:  $KEYS_REMAINING"
echo "Skip Warmup:     $SKIP_WARMUP"
echo "TLS:             $USE_TLS"
echo "=========================================================="
echo ""

# ============================================================
# WARMUP (once)
# ============================================================
if [ "$SKIP_WARMUP" = false ]; then
    echo "============================================"
    echo "WARMUP - Populating $VB_KEYSPACE keys"
    echo "============================================"
    echo ""

    WARMUP_PIDS=()
    for i in $(seq 0 $((WARMUP_PROCESSES - 1))); do
        LOG_FILE="$LOG_DIR/warmup_process_$i.log"
        echo "Launching warmup process $i/$WARMUP_PROCESSES"
        SET_WARMUP_MODE=1 WARMUP_PROCESS_ID=$i WARMUP_TOTAL_PROCESSES=$WARMUP_PROCESSES \
            python3 valkey-benchmark.py -c 1 --threads 1 -t custom \
            --custom-command-file "scenarios/set_benchmark_integrity_large.py" \
            -H "$HOST" $PYTHON_TLS_ARGS \
            -n 1000000000 --timeout 5000 \
            >"$LOG_FILE" 2>&1 &
        WARMUP_PIDS+=($!)
    done

    echo ""
    echo "Waiting for warmup to complete..."
    WARMUP_FAILED=false
    for pid in "${WARMUP_PIDS[@]}"; do
        if ! wait "$pid"; then
            echo "WARNING: Warmup process $pid failed"
            WARMUP_FAILED=true
        fi
    done

    if [ "$WARMUP_FAILED" = true ]; then
        echo "ERROR: Warmup failed. Check logs in $LOG_DIR"
        exit 1
    fi
    echo "Warmup completed!"
    echo ""
else
    echo "Skipping warmup"
    echo ""
fi

# ============================================================
# MAIN LOOP
# ============================================================
PASS_COUNT=0
FAIL_COUNT=0

for ITER in $(seq 1 $ITERATIONS); do
    echo ""
    echo "##########################################################"
    echo "# ITERATION $ITER / $ITERATIONS"
    echo "##########################################################"
    echo ""

    # --- Pre-iteration cleanup ---
    echo "[Iter $ITER] Cleaning up local processes..."
    pkill -9 -f "valkey-benchmark" 2>/dev/null || true
    pkill -9 -f "set_benchmark_integrity\|validate_integrity\|delete_keys" 2>/dev/null || true
    sleep 2

    if pgrep -f "valkey-benchmark|set_benchmark_integrity|validate_integrity|delete_keys" >/dev/null 2>&1; then
        echo "[Iter $ITER] WARNING: Some processes still running, force killing..."
        pkill -9 -f "valkey-benchmark|set_benchmark_integrity|validate_integrity|delete_keys" 2>/dev/null || true
        sleep 2
    fi
    echo "[Iter $ITER] Local processes clean"

    # --- Verify primary is alive and has expected keys ---
    if ! check_server_alive "$HOST" "Primary"; then
        echo "[Iter $ITER] FATAL: Primary server is down, aborting"
        exit 1
    fi
    if ! check_key_count "$HOST" "$VB_KEYSPACE" "Primary"; then
        echo "[Iter $ITER] FATAL: Primary key count mismatch, aborting"
        exit 1
    fi

    # --- Step 1: Start traffic ---
    echo "[Iter $ITER] Starting traffic..."

    TRAFFIC_PIDS=()

    VB_GET_NREQ=$((VB_GET_RPS * 3600))
    VB_SET_NREQ=$((VB_SET_RPS * 3600))

    for i in $(seq 1 $VB_GET_CONCURRENCY); do
        LOG_FILE="$LOG_DIR/iter${ITER}_get_$i.log"
        $VB_CMD -h "$HOST" $TLS_ARGS \
                -c $VB_CLIENTS --threads $VB_THREADS \
                -r $VB_KEYSPACE -d $VB_DATA_SIZE \
                -n $VB_GET_NREQ --rps $VB_GET_RPS \
                -- GET "key:__rand_int__" \
                >"$LOG_FILE" 2>&1 &
        TRAFFIC_PIDS+=($!)
    done

    for i in $(seq 1 $VB_SET_CONCURRENCY); do
        LOG_FILE="$LOG_DIR/iter${ITER}_set_$i.log"
        python3 valkey-benchmark.py -c 8 --threads 8 -t custom \
            --custom-command-file "scenarios/set_benchmark_integrity_large.py" \
            -H "$HOST" $PYTHON_TLS_ARGS \
            --qps $VB_SET_RPS -n $VB_SET_NREQ --timeout 50 \
            >"$LOG_FILE" 2>&1 &
        TRAFFIC_PIDS+=($!)
    done

    echo "[Iter $ITER] Traffic running (${VB_GET_CONCURRENCY}x${VB_GET_RPS} GET + ${VB_SET_CONCURRENCY}x${VB_SET_RPS} SET)"

    # --- Step 2: Kill replica, wait ---
    echo "[Iter $ITER] Killing replica before migration..."
    $REPLICA_SSH "sudo pkill -9 valkey-server" 2>/dev/null || true
    sleep 2
    if $REPLICA_SSH "pgrep valkey-server" >/dev/null 2>&1; then
        echo "[Iter $ITER] WARNING: Replica still running, retrying..."
        $REPLICA_SSH "sudo kill -9 \$(pgrep valkey-server)" 2>/dev/null || true
    fi
    echo "[Iter $ITER] Replica killed"

    echo "[Iter $ITER] Waiting ${MIGRATE_DELAY}s before migration..."
    sleep $MIGRATE_DELAY

    # --- Step 3: Start migration in background ---
    echo "[Iter $ITER] Triggering migration (background)..."
    MIGRATE_LOG="$LOG_DIR/iter${ITER}_migration.log"
    MIGRATION_START=$(date +%s)

    MIGRATE_TLS_FLAG=""
    if [ "$USE_TLS" = false ]; then
        MIGRATE_TLS_FLAG="--no-tls"
    fi

    # Run migration in background
    $PRIMARY_SSH "$MIGRATE_SCRIPT $MIGRATE_TLS_FLAG" >"$MIGRATE_LOG" 2>&1 &
    MIGRATE_PID=$!

    # --- Step 4: Start deleting keys (simulating unmap) ---
    echo "[Iter $ITER] Starting key deletion (unmap simulation)..."
    echo "[Iter $ITER] Deleting keys 0 to $((KEYS_TO_DELETE - 1)) using $DELETE_PROCESSES processes"

    DELETE_PIDS=()
    KEYS_PER_PROCESS=$((KEYS_TO_DELETE / DELETE_PROCESSES))

    for i in $(seq 0 $((DELETE_PROCESSES - 1))); do
        START_KEY=$((i * KEYS_PER_PROCESS))
        if [ $i -eq $((DELETE_PROCESSES - 1)) ]; then
            END_KEY=$KEYS_TO_DELETE
        else
            END_KEY=$(((i + 1) * KEYS_PER_PROCESS))
        fi

        DELETE_LOG="$LOG_DIR/iter${ITER}_delete_$i.log"
        python3 scenarios/delete_keys.py \
            --host "$HOST" \
            --start-key $START_KEY \
            --end-key $END_KEY \
            --batch-size 1000 \
            $PYTHON_TLS_ARGS \
            >"$DELETE_LOG" 2>&1 &
        DELETE_PIDS+=($!)
    done

    # --- Step 5: Wait for migration to complete ---
    echo "[Iter $ITER] Waiting for migration to complete..."
    wait $MIGRATE_PID
    MIGRATE_EXIT=$?
    MIGRATION_END=$(date +%s)
    MIGRATION_ELAPSED=$((MIGRATION_END - MIGRATION_START))

    if [ $MIGRATE_EXIT -eq 0 ]; then
        echo "[Iter $ITER] Migration completed in ${MIGRATION_ELAPSED}s"
    else
        echo "[Iter $ITER] WARNING: Migration FAILED (exit $MIGRATE_EXIT) after ${MIGRATION_ELAPSED}s"
        echo "         Check: $MIGRATE_LOG"
    fi

    # Wait for deletion to complete
    echo "[Iter $ITER] Waiting for key deletion to complete..."
    DELETE_FAILED=false
    for pid in "${DELETE_PIDS[@]}"; do
        if ! wait "$pid"; then
            echo "[Iter $ITER] WARNING: Delete process $pid failed"
            DELETE_FAILED=true
        fi
    done

    if [ "$DELETE_FAILED" = true ]; then
        echo "[Iter $ITER] WARNING: Some delete processes failed"
    else
        echo "[Iter $ITER] Key deletion completed"
    fi

    # --- Step 6: Stop traffic ---
    echo "[Iter $ITER] Stopping traffic..."
    for pid in "${TRAFFIC_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    sleep 2
    for pid in "${TRAFFIC_PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done

    # Wait for replication to settle
    echo "[Iter $ITER] Waiting 10s for replication to settle..."
    sleep 10

    # --- Verify servers and check key counts ---
    if ! check_server_alive "$HOST" "Primary"; then
        echo "[Iter $ITER] FATAL: Primary server is down after migration, aborting"
        exit 1
    fi
    if ! check_server_alive "$REPLICA_HOST" "Replica"; then
        echo "[Iter $ITER] FATAL: Replica server is down after migration, aborting"
        exit 1
    fi

    PRIMARY_COUNT=$(get_key_count "$HOST")
    REPLICA_COUNT=$(get_key_count "$REPLICA_HOST")
    echo "[Iter $ITER] Primary has $PRIMARY_COUNT keys, Replica has $REPLICA_COUNT keys"
    echo "[Iter $ITER] Expected approximately $KEYS_REMAINING keys after deletion"

    if [ "$PRIMARY_COUNT" != "$REPLICA_COUNT" ]; then
        echo "[Iter $ITER] ERROR: Primary and Replica key counts do not match!"
        echo "[Iter $ITER] FAIL - Key count mismatch: Primary=$PRIMARY_COUNT, Replica=$REPLICA_COUNT"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi

    if [ "$PRIMARY_COUNT" != "$KEYS_REMAINING" ]; then
        echo "[Iter $ITER] WARNING: Key count ($PRIMARY_COUNT) differs from expected ($KEYS_REMAINING)"
    fi

    # --- Step 7: Validate ALL keys ---
    # Expect: keys 0 to KEYS_TO_DELETE-1 missing on both, keys KEYS_TO_DELETE to VB_KEYSPACE-1 intact
    echo "[Iter $ITER] Validating data integrity for ALL keys..."
    echo "[Iter $ITER] Expecting $KEYS_TO_DELETE keys missing (0 to $((KEYS_TO_DELETE - 1)))"
    echo "[Iter $ITER] Expecting $KEYS_REMAINING keys intact ($KEYS_TO_DELETE to $((VB_KEYSPACE - 1)))"

    python3 scenarios/validate_integrity.py \
        --primary-host "$HOST" \
        --replica-host "$REPLICA_HOST" \
        --port 6379 \
        --processes $VALIDATION_PROCESSES \
        --total-keys $VB_KEYSPACE \
        --batch-size 100 \
        --expect-missing-start 0 \
        --expect-missing-end $KEYS_TO_DELETE \
        $PYTHON_TLS_ARGS

    VALIDATION_EXIT=$?

    if [ $VALIDATION_EXIT -eq 0 ]; then
        echo "[Iter $ITER] PASS - Data integrity verified (deleted keys missing on both, remaining keys intact)"
        PASS_COUNT=$((PASS_COUNT + 1))
    else
        echo "[Iter $ITER] FAIL - Data integrity check failed"
        FAIL_COUNT=$((FAIL_COUNT + 1))
    fi

    # --- Step 8: Re-populate deleted keys ---
    echo "[Iter $ITER] Re-populating deleted keys (0 to $((KEYS_TO_DELETE - 1)))..."

    REPOP_PIDS=()
    for i in $(seq 0 $((WARMUP_PROCESSES - 1))); do
        LOG_FILE="$LOG_DIR/iter${ITER}_repopulate_$i.log"
        SET_WARMUP_MODE=1 WARMUP_PROCESS_ID=$i WARMUP_TOTAL_PROCESSES=$WARMUP_PROCESSES \
        WARMUP_MAX_KEY=$KEYS_TO_DELETE \
            python3 valkey-benchmark.py -c 1 --threads 1 -t custom \
            --custom-command-file "scenarios/set_benchmark_integrity_large.py" \
            -H "$HOST" $PYTHON_TLS_ARGS \
            -n 1000000000 --timeout 5000 \
            >"$LOG_FILE" 2>&1 &
        REPOP_PIDS+=($!)
    done

    echo "[Iter $ITER] Waiting for re-population to complete..."
    REPOP_FAILED=false
    for pid in "${REPOP_PIDS[@]}"; do
        if ! wait "$pid"; then
            echo "[Iter $ITER] WARNING: Re-populate process $pid failed"
            REPOP_FAILED=true
        fi
    done

    if [ "$REPOP_FAILED" = true ]; then
        echo "[Iter $ITER] WARNING: Some re-populate processes failed"
    else
        echo "[Iter $ITER] Re-population completed"
    fi

    # Verify full key count restored
    echo "[Iter $ITER] Waiting 10s for replication to settle..."
    sleep 10

    if ! check_key_count "$HOST" "$VB_KEYSPACE" "Primary"; then
        echo "[Iter $ITER] WARNING: Primary key count not fully restored"
    fi

    # --- Step 9: Kill replica for next iteration ---
    if [ $ITER -lt $ITERATIONS ]; then
        echo "[Iter $ITER] Killing replica..."
        $REPLICA_SSH "sudo pkill -9 valkey-server" 2>/dev/null || true
        sleep 2
        if $REPLICA_SSH "pgrep valkey-server" >/dev/null 2>&1; then
            echo "[Iter $ITER] WARNING: Replica still running, retrying..."
            $REPLICA_SSH "sudo kill -9 \$(pgrep valkey-server)" 2>/dev/null || true
        fi
        echo "[Iter $ITER] Replica killed"

        echo "[Iter $ITER] Sleeping ${RECOVERY_SLEEP}s for kernel memory reclaim..."
        sleep $RECOVERY_SLEEP
    fi
done

# ============================================================
# SUMMARY
# ============================================================
trap - INT TERM

echo ""
echo "=========================================================="
echo "FINAL SUMMARY"
echo "=========================================================="
echo "Iterations:      $ITERATIONS"
echo "Passed:          $PASS_COUNT"
echo "Failed:          $FAIL_COUNT"
echo "Delete Percent:  ${DELETE_PERCENT}%"
echo "Keys Deleted:    $KEYS_TO_DELETE per iteration"
echo "=========================================================="

if [ $FAIL_COUNT -gt 0 ]; then
    exit 1
fi
exit 0

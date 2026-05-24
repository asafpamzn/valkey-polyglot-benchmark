"""
Data Integrity Validation Script
=================================

Multi-process validation of all keys:
1. Verify CRC integrity on primary
2. Verify CRC integrity on replica
3. Verify primary == replica for every key

Uses 64 processes by default, each handling a chunk of the keyspace.
Each process uses MGET with batches of 100 keys.
"""

import os
import sys
import time
import asyncio
import argparse
from multiprocessing import Process, Queue, Value
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from set_benchmark_integrity_large import CustomCommands


@dataclass
class ChunkResult:
    process_id: int
    keys_checked: int
    crc_failures_primary: int
    crc_failures_replica: int
    mismatches: int
    missing_primary: int
    missing_replica: int
    errors: list
    elapsed_seconds: float


def parse_args():
    parser = argparse.ArgumentParser(description='Validate data integrity between primary and replica')
    parser.add_argument('--primary-host', required=True, help='Primary host')
    parser.add_argument('--replica-host', required=True, help='Replica host')
    parser.add_argument('--port', type=int, default=6379, help='Port')
    parser.add_argument('--processes', type=int, default=64, help='Number of parallel processes')
    parser.add_argument('--total-keys', type=int, default=450000000, help='Total keys to validate')
    parser.add_argument('--batch-size', type=int, default=100, help='Keys per MGET batch')
    parser.add_argument('--cluster', action='store_true', help='Use cluster client')
    parser.add_argument('--tls', action='store_true', default=True, help='Use TLS (default: enabled)')
    parser.add_argument('--no-tls', action='store_true', help='Disable TLS')
    parser.add_argument('--max-errors', type=int, default=100, help='Max error details to report per process')
    return parser.parse_args()


async def validate_chunk(process_id: int, start_key: int, end_key: int,
                         primary_host: str, replica_host: str, port: int,
                         batch_size: int, is_cluster: bool, use_tls: bool,
                         max_errors: int, result_queue: Queue):
    from glide import (
        GlideClient,
        GlideClientConfiguration,
        GlideClusterClient,
        GlideClusterClientConfiguration,
        NodeAddress,
        ReadFrom
    )

    start_time = time.time()
    keys_checked = 0
    crc_failures_primary = 0
    crc_failures_replica = 0
    mismatches = 0
    missing_primary = 0
    missing_replica = 0
    errors = []

    try:
        primary_addresses = [NodeAddress(host=primary_host, port=port)]
        replica_addresses = [NodeAddress(host=replica_host, port=port)]

        if is_cluster:
            primary_config = GlideClusterClientConfiguration(
                addresses=primary_addresses,
                request_timeout=5000,
                use_tls=use_tls,
                read_from=ReadFrom.PRIMARY
            )
            replica_config = GlideClusterClientConfiguration(
                addresses=replica_addresses,
                request_timeout=5000,
                use_tls=use_tls,
                read_from=ReadFrom.PREFER_REPLICA
            )
            primary_client = await GlideClusterClient.create(primary_config)
            replica_client = await GlideClusterClient.create(replica_config)
        else:
            primary_config = GlideClientConfiguration(
                addresses=primary_addresses,
                request_timeout=5000,
                use_tls=use_tls,
                read_from=ReadFrom.PRIMARY
            )
            replica_config = GlideClientConfiguration(
                addresses=replica_addresses,
                request_timeout=5000,
                use_tls=use_tls,
                read_from=ReadFrom.PREFER_REPLICA
            )
            primary_client = await GlideClient.create(primary_config)
            replica_client = await GlideClient.create(replica_config)

        total_keys_in_chunk = end_key - start_key
        last_progress = time.time()

        for batch_start in range(start_key, end_key, batch_size):
            batch_end = min(batch_start + batch_size, end_key)
            key_names = [f"key:{i:012d}" for i in range(batch_start, batch_end)]

            primary_values = await primary_client.mget(key_names)
            replica_values = await replica_client.mget(key_names)

            for idx, key_name in enumerate(key_names):
                keys_checked += 1
                pval = primary_values[idx]
                rval = replica_values[idx]

                if pval is None:
                    missing_primary += 1
                    if len(errors) < max_errors:
                        errors.append(f"{key_name}: missing on primary")
                    continue

                if rval is None:
                    missing_replica += 1
                    if len(errors) < max_errors:
                        errors.append(f"{key_name}: missing on replica")
                    continue

                # Ensure bytes
                if isinstance(pval, str):
                    pval = pval.encode('utf-8')
                if isinstance(rval, str):
                    rval = rval.encode('utf-8')

                ok, err = CustomCommands.verify_value(key_name, pval)
                if not ok:
                    crc_failures_primary += 1
                    if len(errors) < max_errors:
                        errors.append(f"{key_name}: primary CRC fail: {err}")

                ok, err = CustomCommands.verify_value(key_name, rval)
                if not ok:
                    crc_failures_replica += 1
                    if len(errors) < max_errors:
                        errors.append(f"{key_name}: replica CRC fail: {err}")

                if pval != rval:
                    mismatches += 1
                    if len(errors) < max_errors:
                        errors.append(f"{key_name}: primary/replica mismatch")

            now = time.time()
            if now - last_progress >= 10:
                pct = (keys_checked / total_keys_in_chunk) * 100
                print(f"  [Process {process_id:2d}] {keys_checked:,}/{total_keys_in_chunk:,} ({pct:.1f}%)", flush=True)
                last_progress = now

        await primary_client.close()
        await replica_client.close()

    except Exception as e:
        errors.append(f"FATAL: {str(e)}")

    elapsed = time.time() - start_time
    result = ChunkResult(
        process_id=process_id,
        keys_checked=keys_checked,
        crc_failures_primary=crc_failures_primary,
        crc_failures_replica=crc_failures_replica,
        mismatches=mismatches,
        missing_primary=missing_primary,
        missing_replica=missing_replica,
        errors=errors,
        elapsed_seconds=elapsed
    )
    result_queue.put(result)


def run_worker(process_id, start_key, end_key, primary_host, replica_host,
               port, batch_size, is_cluster, use_tls, max_errors, result_queue):
    asyncio.run(validate_chunk(
        process_id, start_key, end_key,
        primary_host, replica_host, port,
        batch_size, is_cluster, use_tls,
        max_errors, result_queue
    ))


def main():
    args = parse_args()

    total_keys = args.total_keys
    num_processes = args.processes
    keys_per_process = total_keys // num_processes

    use_tls = not args.no_tls

    print("=" * 60)
    print("DATA INTEGRITY VALIDATION")
    print("=" * 60)
    print(f"Primary:    {args.primary_host}:{args.port}")
    print(f"Replica:    {args.replica_host}:{args.port}")
    print(f"Total keys: {total_keys:,}")
    print(f"Processes:  {num_processes}")
    print(f"Batch size: {args.batch_size}")
    print(f"Cluster:    {args.cluster}")
    print(f"TLS:        {use_tls}")
    print("=" * 60)
    print()

    result_queue = Queue()
    processes = []
    start_time = time.time()

    print(f"Launching {num_processes} validation processes...")
    for i in range(num_processes):
        start_key = i * keys_per_process
        if i == num_processes - 1:
            end_key = total_keys
        else:
            end_key = start_key + keys_per_process

        p = Process(
            target=run_worker,
            args=(i, start_key, end_key, args.primary_host, args.replica_host,
                  args.port, args.batch_size, args.cluster, use_tls,
                  args.max_errors, result_queue)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    total_elapsed = time.time() - start_time

    # Collect results
    total_checked = 0
    total_crc_primary = 0
    total_crc_replica = 0
    total_mismatches = 0
    total_missing_primary = 0
    total_missing_replica = 0
    all_errors = []

    while not result_queue.empty():
        r = result_queue.get()
        total_checked += r.keys_checked
        total_crc_primary += r.crc_failures_primary
        total_crc_replica += r.crc_failures_replica
        total_mismatches += r.mismatches
        total_missing_primary += r.missing_primary
        total_missing_replica += r.missing_replica
        all_errors.extend(r.errors)

    # Print results
    print()
    print("=" * 60)
    print("VALIDATION RESULTS")
    print("=" * 60)
    print(f"Total keys checked:      {total_checked:,}")
    print(f"Total time:              {total_elapsed:.1f}s")
    print(f"Throughput:              {total_checked / total_elapsed:,.0f} keys/s")
    print()
    print(f"CRC failures (primary):  {total_crc_primary:,}")
    print(f"CRC failures (replica):  {total_crc_replica:,}")
    print(f"Primary/replica mismatch:{total_mismatches:,}")
    print(f"Missing on primary:      {total_missing_primary:,}")
    print(f"Missing on replica:      {total_missing_replica:,}")
    print()

    total_failures = total_crc_primary + total_crc_replica + total_mismatches + total_missing_primary + total_missing_replica

    if total_failures == 0:
        print("RESULT: PASS - All keys validated successfully")
    else:
        print(f"RESULT: FAIL - {total_failures:,} total issues found")
        if all_errors:
            print()
            print(f"First {min(len(all_errors), 50)} errors:")
            for err in all_errors[:50]:
                print(f"  {err}")

    print("=" * 60)
    sys.exit(0 if total_failures == 0 else 1)


if __name__ == '__main__':
    main()

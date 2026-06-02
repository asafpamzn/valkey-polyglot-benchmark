"""
Data Integrity Validation Script
=================================

Multi-process validation of all keys:
1. Verify CRC integrity on primary
2. Verify CRC integrity on replica
3. Verify primary == replica for every key

Uses 64 processes by default, each handling a chunk of the keyspace.
Each process uses MGET with batches of 100 keys via raw RESP protocol.
"""

import os
import sys
import time
import asyncio
import argparse
import ssl
from multiprocessing import Process, Queue
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
    missing_both_expected: int  # Keys missing on both as expected
    missing_both_unexpected: int  # Keys missing on both but shouldn't be
    present_but_expected_missing: int  # Keys present but should be missing
    errors: list
    elapsed_seconds: float


class RESPClient:
    """Minimal async RESP client for MGET operations."""

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, host, port, use_tls=False, timeout=10):
        if use_tls:
            ssl_ctx = ssl.create_default_context()
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        else:
            ssl_ctx = None

        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ssl_ctx),
            timeout=timeout
        )
        client = cls(reader, writer)
        client.read_timeout = 60  # 60 second read timeout
        return client

    async def mget(self, keys):
        cmd = f"*{len(keys) + 1}\r\n$4\r\nMGET\r\n"
        for key in keys:
            key_bytes = key.encode('utf-8') if isinstance(key, str) else key
            cmd += f"${len(key_bytes)}\r\n{key}\r\n"
        self.writer.write(cmd.encode('utf-8'))
        await self.writer.drain()

        # Read array response with timeout
        line = await asyncio.wait_for(self.reader.readline(), timeout=self.read_timeout)
        if not line.startswith(b'*'):
            raise Exception(f"Expected array, got: {line!r}")
        count = int(line[1:].strip())

        results = []
        for _ in range(count):
            line = await asyncio.wait_for(self.reader.readline(), timeout=self.read_timeout)
            if line.startswith(b'$-1'):
                results.append(None)
            elif line.startswith(b'$'):
                length = int(line[1:].strip())
                data = await asyncio.wait_for(self.reader.readexactly(length + 2), timeout=self.read_timeout)
                results.append(data[:-2])  # strip \r\n
            else:
                raise Exception(f"Unexpected response: {line!r}")
        return results

    async def close(self):
        self.writer.close()
        await self.writer.wait_closed()


def parse_args():
    parser = argparse.ArgumentParser(description='Validate data integrity between primary and replica')
    parser.add_argument('--primary-host', required=True, help='Primary host')
    parser.add_argument('--replica-host', required=True, help='Replica host')
    parser.add_argument('--port', type=int, default=6379, help='Port')
    parser.add_argument('--processes', type=int, default=64, help='Number of parallel processes')
    parser.add_argument('--total-keys', type=int, default=450000000, help='Total keys in keyspace (end key, exclusive)')
    parser.add_argument('--start-key', type=int, default=0, help='Start key index (default: 0)')
    parser.add_argument('--batch-size', type=int, default=100, help='Keys per MGET batch')
    parser.add_argument('--cluster', action='store_true', help='Use cluster client')
    parser.add_argument('--tls', action='store_true', default=True, help='Use TLS (default: enabled)')
    parser.add_argument('--no-tls', action='store_true', help='Disable TLS')
    parser.add_argument('--max-errors', type=int, default=100, help='Max error details to report per process')
    parser.add_argument('--expect-missing-start', type=int, default=-1, help='Start of key range expected to be missing (inclusive)')
    parser.add_argument('--expect-missing-end', type=int, default=-1, help='End of key range expected to be missing (exclusive)')
    return parser.parse_args()


async def validate_chunk(process_id: int, start_key: int, end_key: int,
                         primary_host: str, replica_host: str, port: int,
                         batch_size: int, is_cluster: bool, use_tls: bool,
                         max_errors: int, expect_missing_start: int, expect_missing_end: int,
                         result_queue: Queue):
    start_time = time.time()
    keys_checked = 0
    crc_failures_primary = 0
    crc_failures_replica = 0
    mismatches = 0
    missing_primary = 0
    missing_replica = 0
    missing_both_expected = 0
    missing_both_unexpected = 0
    present_but_expected_missing = 0
    errors = []

    def is_expected_missing(key_idx):
        return expect_missing_start <= key_idx < expect_missing_end

    try:
        print(f"  [Process {process_id:2d}] Connecting to primary {primary_host}:{port}...", flush=True)
        primary_client = await RESPClient.connect(primary_host, port, use_tls)
        print(f"  [Process {process_id:2d}] Connecting to replica {replica_host}:{port}...", flush=True)
        replica_client = await RESPClient.connect(replica_host, port, use_tls)
        print(f"  [Process {process_id:2d}] Connected, validating keys {start_key:,} to {end_key-1:,}", flush=True)

        total_keys_in_chunk = end_key - start_key
        last_progress = time.time()

        for batch_start in range(start_key, end_key, batch_size):
            batch_end = min(batch_start + batch_size, end_key)
            key_indices = list(range(batch_start, batch_end))
            key_names = [f"key:{i:012d}" for i in key_indices]

            try:
                primary_values = await primary_client.mget(key_names)
            except asyncio.TimeoutError:
                errors.append(f"Timeout reading from primary at batch {batch_start}")
                print(f"  [Process {process_id:2d}] TIMEOUT on primary at key {batch_start:,}", flush=True)
                break
            try:
                replica_values = await replica_client.mget(key_names)
            except asyncio.TimeoutError:
                errors.append(f"Timeout reading from replica at batch {batch_start}")
                print(f"  [Process {process_id:2d}] TIMEOUT on replica at key {batch_start:,}", flush=True)
                break

            for idx, key_name in enumerate(key_names):
                keys_checked += 1
                key_idx = key_indices[idx]
                pval = primary_values[idx]
                rval = replica_values[idx]
                expected_missing = is_expected_missing(key_idx)

                # Both missing
                if pval is None and rval is None:
                    if expected_missing:
                        missing_both_expected += 1
                    else:
                        missing_both_unexpected += 1
                        if len(errors) < max_errors:
                            errors.append(f"{key_name}: missing on both (unexpected)")
                    continue

                # Key exists but should be missing
                if expected_missing:
                    if pval is not None or rval is not None:
                        present_but_expected_missing += 1
                        if len(errors) < max_errors:
                            where = []
                            if pval is not None:
                                where.append("primary")
                            if rval is not None:
                                where.append("replica")
                            errors.append(f"{key_name}: present on {'/'.join(where)} but expected missing")
                    continue

                # Normal validation for keys that should exist
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
        print(f"  [Process {process_id:2d}] DONE - {keys_checked:,} keys validated in {time.time()-start_time:.1f}s", flush=True)

    except asyncio.TimeoutError as e:
        errors.append(f"FATAL: Timeout - {str(e)}")
        print(f"  [Process {process_id:2d}] FATAL TIMEOUT after {keys_checked:,} keys", flush=True)
    except Exception as e:
        errors.append(f"FATAL: {str(e)}")
        print(f"  [Process {process_id:2d}] FATAL ERROR: {str(e)}", flush=True)

    elapsed = time.time() - start_time
    result = ChunkResult(
        process_id=process_id,
        keys_checked=keys_checked,
        crc_failures_primary=crc_failures_primary,
        crc_failures_replica=crc_failures_replica,
        mismatches=mismatches,
        missing_primary=missing_primary,
        missing_replica=missing_replica,
        missing_both_expected=missing_both_expected,
        missing_both_unexpected=missing_both_unexpected,
        present_but_expected_missing=present_but_expected_missing,
        errors=errors,
        elapsed_seconds=elapsed
    )
    result_queue.put(result)


def run_worker(process_id, start_key, end_key, primary_host, replica_host,
               port, batch_size, is_cluster, use_tls, max_errors,
               expect_missing_start, expect_missing_end, result_queue):
    asyncio.run(validate_chunk(
        process_id, start_key, end_key,
        primary_host, replica_host, port,
        batch_size, is_cluster, use_tls,
        max_errors, expect_missing_start, expect_missing_end, result_queue
    ))


def main():
    args = parse_args()

    # Validate expect-missing arguments
    if (args.expect_missing_start >= 0) != (args.expect_missing_end >= 0):
        print("ERROR: --expect-missing-start and --expect-missing-end must be used together", file=sys.stderr)
        sys.exit(1)
    if args.expect_missing_start >= 0 and args.expect_missing_start >= args.expect_missing_end:
        print(f"ERROR: --expect-missing-start ({args.expect_missing_start}) must be less than --expect-missing-end ({args.expect_missing_end})", file=sys.stderr)
        sys.exit(1)

    start_key_base = args.start_key
    end_key_total = args.total_keys
    keys_to_validate = end_key_total - start_key_base
    num_processes = args.processes
    keys_per_process = keys_to_validate // num_processes

    use_tls = not args.no_tls

    expect_missing_start = args.expect_missing_start
    expect_missing_end = args.expect_missing_end
    expect_missing_count = max(0, expect_missing_end - expect_missing_start) if expect_missing_start >= 0 else 0

    print("=" * 60)
    print("DATA INTEGRITY VALIDATION")
    print("=" * 60)
    print(f"Primary:    {args.primary_host}:{args.port}")
    print(f"Replica:    {args.replica_host}:{args.port}")
    print(f"Key range:  {start_key_base:,} to {end_key_total - 1:,}")
    print(f"Total keys: {keys_to_validate:,}")
    if expect_missing_count > 0:
        print(f"Expect missing: {expect_missing_start:,} to {expect_missing_end - 1:,} ({expect_missing_count:,} keys)")
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
        chunk_start = start_key_base + (i * keys_per_process)
        if i == num_processes - 1:
            chunk_end = end_key_total
        else:
            chunk_end = chunk_start + keys_per_process

        p = Process(
            target=run_worker,
            args=(i, chunk_start, chunk_end, args.primary_host, args.replica_host,
                  args.port, args.batch_size, args.cluster, use_tls,
                  args.max_errors, expect_missing_start, expect_missing_end, result_queue)
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
    total_missing_both_expected = 0
    total_missing_both_unexpected = 0
    total_present_but_expected_missing = 0
    all_errors = []

    while not result_queue.empty():
        r = result_queue.get()
        total_checked += r.keys_checked
        total_crc_primary += r.crc_failures_primary
        total_crc_replica += r.crc_failures_replica
        total_mismatches += r.mismatches
        total_missing_primary += r.missing_primary
        total_missing_replica += r.missing_replica
        total_missing_both_expected += r.missing_both_expected
        total_missing_both_unexpected += r.missing_both_unexpected
        total_present_but_expected_missing += r.present_but_expected_missing
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
    print(f"Missing on primary only: {total_missing_primary:,}")
    print(f"Missing on replica only: {total_missing_replica:,}")
    if expect_missing_count > 0:
        print()
        print(f"Expected missing (both): {total_missing_both_expected:,} (expected {expect_missing_count:,})")
        print(f"Unexpected missing both: {total_missing_both_unexpected:,}")
        print(f"Present but should miss: {total_present_but_expected_missing:,}")
    print()

    # Calculate failures - keys unexpectedly missing or present when shouldn't be
    total_failures = (total_crc_primary + total_crc_replica + total_mismatches +
                      total_missing_primary + total_missing_replica +
                      total_missing_both_unexpected + total_present_but_expected_missing)

    if all_errors:
        print(f"Errors ({len(all_errors)}):")
        for err in all_errors[:50]:
            print(f"  {err}")
        print()

    if total_checked == 0:
        print("RESULT: FAIL - No keys were checked (all processes failed to connect?)")
        sys.exit(1)
    elif total_failures == 0:
        if expect_missing_count > 0:
            if total_missing_both_expected == expect_missing_count:
                print(f"RESULT: PASS - All {expect_missing_count:,} expected keys missing on both, remaining keys intact")
            else:
                print(f"RESULT: FAIL - Expected {expect_missing_count:,} missing, found {total_missing_both_expected:,}")
                sys.exit(1)
        else:
            print("RESULT: PASS - All keys validated successfully")
        sys.exit(0)
    else:
        print(f"RESULT: FAIL - {total_failures:,} total issues found")
        sys.exit(1)

    print("=" * 60)


if __name__ == '__main__':
    main()

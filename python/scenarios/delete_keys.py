#!/usr/bin/env python3
"""
Delete keys in a range using UNLINK (async delete).
Supports parallel deletion via process ID partitioning.
"""

import argparse
import sys
import os

try:
    import valkey as redis
except ImportError:
    import redis


def main():
    parser = argparse.ArgumentParser(description='Delete keys in a range')
    parser.add_argument('--host', required=True, help='Valkey host')
    parser.add_argument('--port', type=int, default=6379, help='Valkey port')
    parser.add_argument('--start-key', type=int, required=True, help='Start key index')
    parser.add_argument('--end-key', type=int, required=True, help='End key index (exclusive)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Keys per UNLINK batch')
    parser.add_argument('--no-tls', action='store_true', help='Disable TLS')
    parser.add_argument('--tls-cert', default=os.environ.get('VB_TLS_CERT', '/home/ubuntu/valkey-polyglot-benchmark/python/tls/client.crt'))
    parser.add_argument('--tls-key', default=os.environ.get('VB_TLS_KEY', '/home/ubuntu/valkey-polyglot-benchmark/python/tls/client.key'))
    parser.add_argument('--tls-ca', default=os.environ.get('VB_TLS_CACERT', '/home/ubuntu/valkey-polyglot-benchmark/python/tls/ca.crt'))
    args = parser.parse_args()

    use_tls = not args.no_tls
    total_keys = args.end_key - args.start_key
    errors_count = 0

    try:
        if use_tls:
            client = redis.Redis(
                host=args.host,
                port=args.port,
                ssl=True,
                ssl_certfile=args.tls_cert,
                ssl_keyfile=args.tls_key,
                ssl_ca_certs=args.tls_ca,
                decode_responses=True,
                socket_timeout=30,
                socket_connect_timeout=10
            )
        else:
            client = redis.Redis(
                host=args.host,
                port=args.port,
                decode_responses=True,
                socket_timeout=30,
                socket_connect_timeout=10
            )

        # Test connection
        client.ping()
    except Exception as e:
        print(f"ERROR: Failed to connect to {args.host}:{args.port}: {e}", file=sys.stderr)
        sys.exit(1)

    deleted = 0
    progress_interval = max(1, total_keys // 10)  # Report ~10 times

    print(f"Deleting keys {args.start_key} to {args.end_key - 1} ({total_keys:,} keys)")

    try:
        for batch_start in range(args.start_key, args.end_key, args.batch_size):
            batch_end = min(batch_start + args.batch_size, args.end_key)
            keys = [f"key:{i:012d}" for i in range(batch_start, batch_end)]

            try:
                pipe = client.pipeline(transaction=False)
                pipe.unlink(*keys)
                pipe.execute()
                deleted += len(keys)

                if deleted % progress_interval < args.batch_size:
                    print(f"  Deleted {deleted:,} / {total_keys:,} keys ({100*deleted/total_keys:.1f}%)")
            except Exception as e:
                errors_count += 1
                if errors_count <= 10:
                    print(f"Error deleting batch at key {batch_start}: {e}", file=sys.stderr)

    finally:
        client.close()

    print(f"Deletion complete: {deleted:,} keys deleted")
    if errors_count > 0:
        print(f"WARNING: {errors_count} batch errors occurred", file=sys.stderr)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()

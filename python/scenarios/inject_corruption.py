"""
Inject corruption into keys to verify that validation catches it.
Can target primary, replica, or both.
Run this, then run validation - it should report failures.
"""

import asyncio
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from set_benchmark_integrity_large import CustomCommands


async def inject(host, port, use_tls, target_name):
    from validate_integrity import RESPClient

    client = await RESPClient.connect(host, port, use_tls)

    # Test 1: Corrupt CRC (flip a byte in the payload, CRC won't match)
    key1 = "key:000000000042"
    good_value = CustomCommands.build_value(key1)
    corrupted = bytearray(good_value)
    corrupted[-1] ^= 0xFF  # flip last byte of payload
    await send_set(client, key1, bytes(corrupted))
    print(f"[{target_name}] Injected CRC corruption: {key1}")

    # Test 2: Wrong key prefix (value belongs to a different key)
    key2 = "key:000000000099"
    wrong_value = CustomCommands.build_value("key:000000000100")  # wrong key's value
    await send_set(client, key2, wrong_value)
    print(f"[{target_name}] Injected key prefix mismatch: {key2}")

    # Test 3: Wrong length
    key3 = "key:000000000150"
    await send_set(client, key3, b"too_short")
    print(f"[{target_name}] Injected wrong length: {key3}")

    # Test 4: Delete a key (will show as missing)
    key4 = "key:000000000200"
    await send_del(client, key4)
    print(f"[{target_name}] Deleted key: {key4}")

    await client.close()
    print(f"\n[{target_name}] Done - 4 corruptions injected.")


async def send_set(client, key, value):
    key_bytes = key.encode('utf-8')
    cmd = f"*3\r\n$3\r\nSET\r\n${len(key_bytes)}\r\n".encode() + key_bytes + f"\r\n${len(value)}\r\n".encode() + value + b"\r\n"
    client.writer.write(cmd)
    await client.writer.drain()
    await client.reader.readline()  # +OK


async def send_del(client, key):
    key_bytes = key.encode('utf-8')
    cmd = f"*2\r\n$3\r\nDEL\r\n${len(key_bytes)}\r\n{key}\r\n".encode()
    client.writer.write(cmd)
    await client.writer.drain()
    await client.reader.readline()  # :1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', help='Primary host (for --target primary or both)')
    parser.add_argument('--replica-host', help='Replica host (for --target replica or both)')
    parser.add_argument('--port', type=int, default=6379)
    parser.add_argument('--no-tls', action='store_true')
    parser.add_argument('--target', choices=['primary', 'replica', 'both'], default='replica',
                        help='Where to inject corruption (default: replica)')
    args = parser.parse_args()

    use_tls = not args.no_tls

    if args.target in ('primary', 'both'):
        if not args.host:
            print("ERROR: --host required for primary target")
            sys.exit(1)
        asyncio.run(inject(args.host, args.port, use_tls, "primary"))

    if args.target in ('replica', 'both'):
        if not args.replica_host:
            print("ERROR: --replica-host required for replica target")
            sys.exit(1)
        asyncio.run(inject(args.replica_host, args.port, use_tls, "replica"))


if __name__ == '__main__':
    main()

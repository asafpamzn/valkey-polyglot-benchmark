#!/usr/bin/env python3
"""
Delete keys in a range using UNLINK (async delete).
Supports parallel deletion via process ID partitioning.
Uses raw RESP protocol - no external dependencies.
"""

import argparse
import socket
import ssl
import sys
import os


class RESPClient:
    """Minimal RESP client for UNLINK operations."""

    def __init__(self, sock):
        self.sock = sock
        self.buffer = b""

    @classmethod
    def connect(cls, host, port, use_tls=False, timeout=30):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))

        if use_tls:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            sock = ctx.wrap_socket(sock, server_hostname=host)

        return cls(sock)

    def _read_line(self):
        while b"\r\n" not in self.buffer:
            data = self.sock.recv(4096)
            if not data:
                raise ConnectionError("Connection closed")
            self.buffer += data
        line, self.buffer = self.buffer.split(b"\r\n", 1)
        return line

    def _read_response(self):
        line = self._read_line()
        prefix = chr(line[0])

        if prefix == "+":
            return line[1:].decode()
        elif prefix == "-":
            raise Exception(line[1:].decode())
        elif prefix == ":":
            return int(line[1:])
        elif prefix == "$":
            length = int(line[1:])
            if length == -1:
                return None
            while len(self.buffer) < length + 2:
                data = self.sock.recv(4096)
                if not data:
                    raise ConnectionError("Connection closed")
                self.buffer += data
            result = self.buffer[:length]
            self.buffer = self.buffer[length + 2:]
            return result
        elif prefix == "*":
            count = int(line[1:])
            if count == -1:
                return None
            return [self._read_response() for _ in range(count)]
        else:
            raise Exception(f"Unknown RESP prefix: {prefix}")

    def _send_command(self, *args):
        cmd = f"*{len(args)}\r\n"
        for arg in args:
            if isinstance(arg, str):
                arg = arg.encode()
            cmd += f"${len(arg)}\r\n"
            if isinstance(cmd, str):
                cmd = cmd.encode()
            self.sock.sendall(cmd + arg + b"\r\n")
            cmd = ""

    def ping(self):
        self._send_command("PING")
        return self._read_response()

    def unlink(self, *keys):
        self._send_command("UNLINK", *keys)
        return self._read_response()

    def close(self):
        self.sock.close()


def main():
    parser = argparse.ArgumentParser(description='Delete keys in a range')
    parser.add_argument('--host', required=True, help='Valkey host')
    parser.add_argument('--port', type=int, default=6379, help='Valkey port')
    parser.add_argument('--start-key', type=int, required=True, help='Start key index')
    parser.add_argument('--end-key', type=int, required=True, help='End key index (exclusive)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Keys per UNLINK batch')
    parser.add_argument('--no-tls', action='store_true', help='Disable TLS')
    args = parser.parse_args()

    use_tls = not args.no_tls
    total_keys = args.end_key - args.start_key
    errors_count = 0

    try:
        client = RESPClient.connect(args.host, args.port, use_tls, timeout=30)
        client.ping()
    except Exception as e:
        print(f"ERROR: Failed to connect to {args.host}:{args.port}: {e}", file=sys.stderr)
        sys.exit(1)

    deleted = 0
    progress_interval = max(1, total_keys // 10)

    print(f"Deleting keys {args.start_key} to {args.end_key - 1} ({total_keys:,} keys)", flush=True)

    try:
        for batch_start in range(args.start_key, args.end_key, args.batch_size):
            batch_end = min(batch_start + args.batch_size, args.end_key)
            keys = [f"key:{i:012d}" for i in range(batch_start, batch_end)]

            try:
                client.unlink(*keys)
                deleted += len(keys)

                if deleted % progress_interval < args.batch_size:
                    print(f"  Deleted {deleted:,} / {total_keys:,} keys ({100*deleted/total_keys:.1f}%)", flush=True)
            except Exception as e:
                errors_count += 1
                if errors_count <= 10:
                    print(f"Error deleting batch at key {batch_start}: {e}", file=sys.stderr)

    finally:
        client.close()

    print(f"Deletion complete: {deleted:,} keys deleted", flush=True)
    if errors_count > 0:
        print(f"WARNING: {errors_count} batch errors occurred", file=sys.stderr)
        sys.exit(1)

    sys.exit(0)


if __name__ == '__main__':
    main()

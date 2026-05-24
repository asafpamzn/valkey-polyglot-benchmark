"""
Custom SET Benchmark Commands - INTEGRITY VALIDATION (LARGE MACHINE)
====================================================================

Config for large machine with data integrity:
- total_keys = 450,000,000
- value_size = 512 bytes
- Value format: [key (16 bytes)] + [CRC32 hex (8 bytes)] + [random payload (488 bytes)]

The key prefix and CRC allow post-test validation that data is correct
and consistent between primary and replica.
"""

import random
import os
import asyncio
import zlib
import struct


class CustomCommands:
    KEY_PREFIX_LEN = 16
    CRC_LEN = 8
    TOTAL_VALUE_SIZE = 512
    PAYLOAD_SIZE = TOTAL_VALUE_SIZE - KEY_PREFIX_LEN - CRC_LEN

    def __init__(self):
        self.total_keys = 450000000
        self.value_size = self.TOTAL_VALUE_SIZE

        self.warmup_mode = os.environ.get('SET_WARMUP_MODE', '0') == '1'

        self.process_id = int(os.environ.get('WARMUP_PROCESS_ID', '0'))
        self.total_processes = int(os.environ.get('WARMUP_TOTAL_PROCESSES', '1'))

        keys_per_process = self.total_keys // self.total_processes
        self.process_start_key = self.process_id * keys_per_process

        if self.process_id == self.total_processes - 1:
            self.process_end_key = self.total_keys
        else:
            self.process_end_key = self.process_start_key + keys_per_process

        self.process_total_keys = self.process_end_key - self.process_start_key

        self.keys_per_warmup_call = 20000
        self.warmup_current_key = self.process_start_key
        self.warmup_completed = False

    @staticmethod
    def build_value(key_name: str) -> bytes:
        """Build an integrity-checked value for a given key."""
        key_prefix = key_name.encode('utf-8')[:CustomCommands.KEY_PREFIX_LEN].ljust(CustomCommands.KEY_PREFIX_LEN, b'\x00')
        payload = os.urandom(CustomCommands.PAYLOAD_SIZE)
        crc = zlib.crc32(payload) & 0xFFFFFFFF
        crc_hex = f"{crc:08x}".encode('utf-8')
        return key_prefix + crc_hex + payload

    @staticmethod
    def verify_value(key_name: str, value: bytes) -> tuple:
        """Verify an integrity-checked value. Returns (ok, error_message)."""
        if value is None:
            return False, "value is None"
        if len(value) != CustomCommands.TOTAL_VALUE_SIZE:
            return False, f"wrong length: {len(value)} != {CustomCommands.TOTAL_VALUE_SIZE}"

        key_prefix = value[:CustomCommands.KEY_PREFIX_LEN]
        expected_prefix = key_name.encode('utf-8')[:CustomCommands.KEY_PREFIX_LEN].ljust(CustomCommands.KEY_PREFIX_LEN, b'\x00')
        if key_prefix != expected_prefix:
            return False, f"key mismatch: got {key_prefix!r}, expected {expected_prefix!r}"

        crc_hex = value[CustomCommands.KEY_PREFIX_LEN:CustomCommands.KEY_PREFIX_LEN + CustomCommands.CRC_LEN]
        payload = value[CustomCommands.KEY_PREFIX_LEN + CustomCommands.CRC_LEN:]

        try:
            stored_crc = int(crc_hex, 16)
        except ValueError:
            return False, f"invalid CRC hex: {crc_hex!r}"

        computed_crc = zlib.crc32(payload) & 0xFFFFFFFF
        if stored_crc != computed_crc:
            return False, f"CRC mismatch: stored={stored_crc:08x}, computed={computed_crc:08x}"

        return True, None

    async def execute(self, client):
        try:
            if self.warmup_mode:
                return await self._execute_warmup(client)
            else:
                return await self._execute_benchmark(client)
        except Exception:
            raise

    async def _warmup_key_chunk(self, client, start_key: int, num_keys: int):
        batch_size = 100

        for batch_start in range(start_key, start_key + num_keys, batch_size):
            key_value_dict = {}

            for key_offset in range(batch_size):
                key_id = batch_start + key_offset
                if key_id >= start_key + num_keys or key_id >= self.total_keys:
                    break

                key_name = f"key:{key_id:012d}"
                value = self.build_value(key_name)
                key_value_dict[key_name] = value

            if key_value_dict:
                await client.mset(key_value_dict)

    async def _execute_warmup(self, client):
        if self.warmup_completed:
            raise SystemExit(0)

        num_concurrent_chunks = 2
        keys_per_chunk = 10000

        tasks = []

        for i in range(num_concurrent_chunks):
            start_key = self.warmup_current_key + (i * keys_per_chunk)

            if start_key >= self.process_end_key:
                break

            remaining_keys = self.process_end_key - start_key
            chunk_size = min(keys_per_chunk, remaining_keys)

            task = self._warmup_key_chunk(client, start_key, chunk_size)
            tasks.append(task)

        await asyncio.gather(*tasks)

        self.warmup_current_key += self.keys_per_warmup_call

        keys_done = self.warmup_current_key - self.process_start_key
        pct = min(100.0, (keys_done / self.process_total_keys) * 100)
        print(f"  [Process {self.process_id:2d}] {keys_done:,}/{self.process_total_keys:,} keys ({pct:.1f}%)", flush=True)

        if self.warmup_current_key >= self.process_end_key:
            self.warmup_completed = True
            print(f"  [Process {self.process_id:2d}] DONE", flush=True)

        return True

    async def _execute_benchmark(self, client):
        key_id = random.randint(0, self.total_keys - 1)
        key_name = f"key:{key_id:012d}"
        value = self.build_value(key_name)
        await client.set(key_name, value)
        return True

"""
Custom SET Benchmark Commands - LARGE MACHINE CONFIG
=====================================================

Config for large machine:
- total_keys = 450,000,000
- value_size = 512 bytes
"""

import random
import os
import asyncio

class CustomCommands:
    def __init__(self):
        """Initialize the custom commands handler."""
        self.total_keys = 450000000  # 450 million keys for large machine
        self.value_size = 512  # 512 bytes per value

        # Determine if we're in warmup mode from environment
        self.warmup_mode = os.environ.get('SET_WARMUP_MODE', '0') == '1'

        # Multi-process warmup support
        self.process_id = int(os.environ.get('WARMUP_PROCESS_ID', '0'))
        self.total_processes = int(os.environ.get('WARMUP_TOTAL_PROCESSES', '1'))

        # Calculate this process's key range
        keys_per_process = self.total_keys // self.total_processes
        self.process_start_key = self.process_id * keys_per_process

        # Last process handles any remainder keys
        if self.process_id == self.total_processes - 1:
            self.process_end_key = self.total_keys
        else:
            self.process_end_key = self.process_start_key + keys_per_process

        self.process_total_keys = self.process_end_key - self.process_start_key

        # Warmup state tracking
        self.keys_per_warmup_call = 1000000
        self.warmup_current_key = self.process_start_key
        self.warmup_completed = False

    def generate_random_data(self, size: int) -> bytes:
        """Generate random non-compressible data."""
        return os.urandom(size)

    async def execute(self, client):
        """Execute SET command in either warmup or benchmark mode."""
        try:
            if self.warmup_mode:
                return await self._execute_warmup(client)
            else:
                return await self._execute_benchmark(client)
        except Exception as e:
            raise

    async def _warmup_key_chunk(self, client, start_key: int, num_keys: int):
        """Populate a chunk of keys using MSET for efficiency."""
        batch_size = 100

        for batch_start in range(start_key, start_key + num_keys, batch_size):
            key_value_dict = {}

            for key_offset in range(batch_size):
                key_id = batch_start + key_offset
                if key_id >= start_key + num_keys or key_id >= self.total_keys:
                    break

                key_name = f"key:{key_id:012d}"
                value = self.generate_random_data(self.value_size)
                key_value_dict[key_name] = value

            if key_value_dict:
                await client.mset(key_value_dict)

    async def _execute_warmup(self, client):
        """Execute warmup mode: populate keys with concurrent chunks."""
        if self.warmup_completed:
            return True

        num_concurrent_chunks = 10
        keys_per_chunk = 100000

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

        if self.warmup_current_key >= self.process_end_key:
            self.warmup_completed = True

        return True

    async def _execute_benchmark(self, client):
        """Execute benchmark mode: random SET operations."""
        key_id = random.randint(0, self.total_keys - 1)
        key_name = f"key:{key_id:012d}"

        value = self.generate_random_data(self.value_size)

        await client.set(key_name, value)

        return True

"""
Custom HSET Benchmark Commands - Scenario 5: Large Hash Tables (300GB)
======================================================================

Implements warmup and benchmark modes for HSET operations on large hash tables.

Configuration:
- 3000 hash tables (hash:0 to hash:2999)
- Each hash table: 100,000 fields × 1000 bytes = ~100MB
- Total data: 3000 × 100MB = 300GB
- Field size: 1000 bytes (70% compressible data)

Warmup Mode:
- Parallel partitioning: each process handles a subset of hash tables
- Uses WARMUP_PROCESS_ID and WARMUP_TOTAL_PROCESSES env vars

Benchmark Mode:
- Randomly selects one of 3000 hash tables
- Randomly selects one of 100,000 fields
- Updates field with fresh data
"""

import random
import os
import asyncio


class CustomCommands:
    def __init__(self):
        self.num_hash_tables = 3000
        self.fields_per_hash = 100000
        self.value_size = 1000

        self.warmup_mode = os.environ.get('HSET_WARMUP_MODE', '0') == '1'

        self.warmup_process_id = int(os.environ.get('WARMUP_PROCESS_ID', '0'))
        self.warmup_total_processes = int(os.environ.get('WARMUP_TOTAL_PROCESSES', '1'))

        hashes_per_process = self.num_hash_tables // self.warmup_total_processes
        self.warmup_start_hash = self.warmup_process_id * hashes_per_process
        self.warmup_end_hash = (self.warmup_process_id + 1) * hashes_per_process
        if self.warmup_process_id == self.warmup_total_processes - 1:
            self.warmup_end_hash = self.num_hash_tables

        self.warmup_current_hash = self.warmup_start_hash
        self.warmup_completed = False

    def generate_random_data(self, size: int) -> bytes:
        compressible_size = int(size * 0.7)
        random_size = size - compressible_size
        compressible_data = b'\x00' * compressible_size
        random_data = os.urandom(random_size)
        return compressible_data + random_data

    async def execute(self, client):
        try:
            if self.warmup_mode:
                return await self._execute_warmup(client)
            else:
                return await self._execute_benchmark(client)
        except Exception as e:
            raise

    async def _warmup_single_hash(self, client, hash_id: int):
        hash_name = f"hash:{hash_id}"
        batch_size = 50

        for start_field in range(0, self.fields_per_hash, batch_size):
            fields_dict = {}

            for field_offset in range(batch_size):
                field_id = start_field + field_offset
                if field_id >= self.fields_per_hash:
                    break

                field_name = f"field:{field_id}"
                value = self.generate_random_data(self.value_size)
                fields_dict[field_name] = value

            await client.hset(hash_name, fields_dict)

    async def _execute_warmup(self, client):
        if self.warmup_completed:
            return True

        num_concurrent = 10
        tasks = []

        for i in range(num_concurrent):
            hash_id = self.warmup_current_hash + i
            if hash_id >= self.warmup_end_hash:
                break

            task = self._warmup_single_hash(client, hash_id)
            tasks.append(task)

        await asyncio.gather(*tasks)

        self.warmup_current_hash += num_concurrent
        if self.warmup_current_hash >= self.warmup_end_hash:
            self.warmup_completed = True

        return True

    async def _execute_benchmark(self, client):
        hash_id = random.randint(0, self.num_hash_tables - 1)
        hash_name = f"hash:{hash_id}"

        field_id = random.randint(0, self.fields_per_hash - 1)
        field_name = f"field:{field_id}"

        value = self.generate_random_data(self.value_size)

        await client.hset(hash_name, {field_name: value})

        return True

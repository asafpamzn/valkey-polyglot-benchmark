"""
Custom HGET Benchmark Commands - Scenario 5: Large Hash Tables (300GB)
======================================================================

Simple HGET benchmark for reading from scenario 5 hash tables.

Configuration:
- 3000 hash tables (hash:0 to hash:2999)
- Each hash table: 100,000 fields

Benchmark Mode:
- Randomly selects one of 3000 hash tables
- Randomly selects one of 100,000 fields
- Reads the field value using HGET

Note: Assumes data has been pre-populated using hset_benchmark_scenario5 warmup.
"""

import random


class CustomCommands:
    def __init__(self):
        self.num_hash_tables = 3000
        self.fields_per_hash = 100000

    async def execute(self, client):
        hash_id = random.randint(0, self.num_hash_tables - 1)
        hash_name = f"hash:{hash_id}"

        field_id = random.randint(0, self.fields_per_hash - 1)
        field_name = f"field:{field_id}"

        await client.hget(hash_name, field_name)

        return True

"""
Custom HGET Benchmark Commands
==============================

Simple HGET benchmark for reading from hash tables.

Benchmark Mode:
- Randomly selects one of 100 hash tables
- Randomly selects one of 900,000 fields
- Reads the field value using HGET

Note: Assumes data has been pre-populated (e.g., using hset_benchmark warmup).
"""

import random


class CustomCommands:
    def __init__(self):
        """Initialize the custom commands handler."""
        self.num_hash_tables = 100
        self.fields_per_hash = 900000

    async def execute(self, client):
        """
        Execute random HGET operation.

        Args:
            client: Valkey/Redis client instance

        Returns:
            bool: True if operation succeeded
        """
        # Randomly select hash table (uniform distribution)
        hash_id = random.randint(0, self.num_hash_tables - 1)
        hash_name = f"hash:{hash_id}"

        # Randomly select field (uniform distribution)
        field_id = random.randint(0, self.fields_per_hash - 1)
        field_name = f"field:{field_id}"

        # Execute HGET
        await client.hget(hash_name, field_name)

        return True

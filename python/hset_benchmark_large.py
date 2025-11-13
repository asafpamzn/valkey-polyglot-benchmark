"""
Custom HSET Benchmark Commands - Large Mixed Configuration
===========================================================

Implements warmup and benchmark modes for HSET operations on hash tables
with mixed sizes.

Configuration:
- 80 hash tables (hash:0 to hash:79) - 100MB each
  * Each contains 100,000 fields
- 1 large hash table (hash:80) - 1GB
  * Contains 1,000,000 fields
- Field size: 1000 bytes (non-compressible random data)

Warmup Mode:
- Populates all 81 hash tables with their respective field counts
- Uses concurrent processing for efficiency

Benchmark Mode:
- Randomly selects one of 81 hash tables
- Randomly selects a field within the selected hash table
- Updates field with fresh random data
"""

import random
import os
import asyncio

class CustomCommands:
    def __init__(self):
        """Initialize the custom commands handler."""
        # 80 small hash tables (100MB each)
        self.num_small_hash_tables = 80
        self.fields_per_small_hash = 100000  # 100,000 fields * 1000 bytes = 100MB
        
        # 1 large hash table (1GB)
        self.large_hash_id = 80
        self.fields_per_large_hash = 1000000  # 1,000,000 fields * 1000 bytes = 1GB
        
        # Total hash tables
        self.total_hash_tables = self.num_small_hash_tables + 1  # 81 total
        
        self.value_size = 1000
        
        # Determine if we're in warmup mode from environment
        self.warmup_mode = os.environ.get('HSET_WARMUP_MODE', '0') == '1'
        
        # Warmup state tracking
        self.warmup_current_hash = 0
        self.warmup_completed = False
    
    def generate_random_data(self, size: int) -> bytes:
        """
        Generate random non-compressible data.
        
        Args:
            size (int): Size of data to generate in bytes
            
        Returns:
            bytes: Random bytes
        """
        return os.urandom(size)
    
    async def execute(self, client):
        """
        Execute HSET command in either warmup or benchmark mode.
        
        Args:
            client: Valkey/Redis client instance
            
        Returns:
            bool: True if operation succeeded, False otherwise
        """
        try:
            if self.warmup_mode:
                return await self._execute_warmup(client)
            else:
                return await self._execute_benchmark(client)
        except Exception as e:
            raise
    
    async def _warmup_single_hash(self, client, hash_id: int):
        """
        Populate a single hash table with all its fields.
        
        Args:
            client: Valkey/Redis client instance
            hash_id: The hash table ID to populate
        """
        hash_name = f"hash:{hash_id}"
        batch_size = 50
        
        # Determine the number of fields for this hash table
        if hash_id < self.num_small_hash_tables:
            num_fields = self.fields_per_small_hash
        else:
            num_fields = self.fields_per_large_hash
        
        # Populate all fields in batches
        for start_field in range(0, num_fields, batch_size):
            fields_dict = {}
            
            for field_offset in range(batch_size):
                field_id = start_field + field_offset
                if field_id >= num_fields:
                    break
                
                field_name = f"field:{field_id}"
                value = self.generate_random_data(self.value_size)
                fields_dict[field_name] = value
            
            # Execute HSET with batched fields
            await client.hset(hash_name, fields_dict)
    
    async def _execute_warmup(self, client):
        """
        Execute warmup mode: populate hash tables with concurrent tasks.
        
        For small hash tables (0-79): processes 10 concurrently
        For large hash table (80): processes individually
        
        Args:
            client: Valkey/Redis client instance
            
        Returns:
            bool: True if operation succeeded
        """
        if self.warmup_completed:
            return True
        
        # Process small hash tables concurrently (10 at a time)
        if self.warmup_current_hash < self.num_small_hash_tables:
            num_concurrent = 10
            tasks = []
            
            for i in range(num_concurrent):
                hash_id = self.warmup_current_hash + i
                if hash_id >= self.num_small_hash_tables:
                    break
                
                task = self._warmup_single_hash(client, hash_id)
                tasks.append(task)
            
            # Execute all tasks concurrently
            await asyncio.gather(*tasks)
            
            # Update state
            self.warmup_current_hash += num_concurrent
        
        # Process large hash table individually
        elif self.warmup_current_hash == self.num_small_hash_tables:
            await self._warmup_single_hash(client, self.large_hash_id)
            self.warmup_current_hash += 1
            self.warmup_completed = True
        
        return True
    
    async def _execute_benchmark(self, client):
        """
        Execute benchmark mode: random HSET operations.
        
        Randomly selects from all 81 hash tables with appropriate field ranges.
        
        Args:
            client: Valkey/Redis client instance
            
        Returns:
            bool: True if operation succeeded
        """
        # Randomly select hash table (0-80, uniform distribution)
        hash_id = random.randint(0, self.total_hash_tables - 1)
        hash_name = f"hash:{hash_id}"
        
        # Determine field range based on hash table type
        if hash_id < self.num_small_hash_tables:
            # Small hash table: 100,000 fields
            field_id = random.randint(0, self.fields_per_small_hash - 1)
        else:
            # Large hash table: 1,000,000 fields
            field_id = random.randint(0, self.fields_per_large_hash - 1)
        
        field_name = f"field:{field_id}"
        
        # Generate fresh random non-compressible data
        value = self.generate_random_data(self.value_size)
        
        # Execute HSET
        await client.hset(hash_name, {field_name: value})
        
        return True

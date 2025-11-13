"""
Custom HSET Benchmark Commands
==============================

Implements warmup and benchmark modes for HSET operations on hash tables.

Warmup Mode:
- Creates 80 hash tables (hash:0 to hash:79)
- Populates each with 100,000 fields (field:0 to field:99999)
- Each value is 1000 bytes of random, non-compressible data

Benchmark Mode:
- Randomly selects one of 80 hash tables
- Randomly selects one of 100,000 fields
- Updates field with fresh random data
"""

import random
import os

class CustomCommands:
    def __init__(self):
        """Initialize the custom commands handler."""
        self.num_hash_tables = 80
        self.fields_per_hash = 100000
        self.value_size = 1000
        
        # Determine if we're in warmup mode from environment
        self.warmup_mode = os.environ.get('HSET_WARMUP_MODE', '0') == '1'
        
        # Warmup state tracking
        self.warmup_current_hash = 0
        self.warmup_current_field = 0
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
            print(f'HSET operation error: {str(e)}')
            return False
    
    async def _execute_warmup(self, client):
        """
        Execute warmup mode: populate hash tables sequentially.
        Batches 20 fields per HSET call for better performance.
        
        Args:
            client: Valkey/Redis client instance
            
        Returns:
            bool: True if operation succeeded
        """
        if self.warmup_completed:
            return True
        
        hash_name = f"hash:{self.warmup_current_hash}"
        
        # Build a dictionary with up to 20 field-value pairs
        fields_dict = {}
        batch_size = 20
        
        for _ in range(batch_size):
            if self.warmup_current_field >= self.fields_per_hash:
                break
            
            field_name = f"field:{self.warmup_current_field}"
            value = self.generate_random_data(self.value_size)
            fields_dict[field_name] = value
            self.warmup_current_field += 1
        
        # Execute HSET with batched fields
        await client.hset(hash_name, fields_dict)
        
        # Move to next hash if current one is complete
        if self.warmup_current_field >= self.fields_per_hash:
            self.warmup_current_field = 0
            self.warmup_current_hash += 1
            
            if self.warmup_current_hash >= self.num_hash_tables:
                self.warmup_completed = True
        
        return True
    
    async def _execute_benchmark(self, client):
        """
        Execute benchmark mode: random HSET operations.
        
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
        
        # Generate fresh random non-compressible data
        value = self.generate_random_data(self.value_size)
        
        # Execute HSET
        await client.hset(hash_name, {field_name: value})
        
        return True

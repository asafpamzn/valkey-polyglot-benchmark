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
import asyncio

class CustomCommands:
    def __init__(self):
        """Initialize the custom commands handler."""
        self.num_hash_tables = 100
        self.fields_per_hash = 940000
        self.value_size = 50
        
        # Determine if we're in warmup mode from environment
        self.warmup_mode = os.environ.get('HSET_WARMUP_MODE', '0') == '1'
        
        # Parallel warmup partitioning support
        self.warmup_process_id = int(os.environ.get('WARMUP_PROCESS_ID', '0'))
        self.warmup_total_processes = int(os.environ.get('WARMUP_TOTAL_PROCESSES', '1'))
        
        # Calculate which hash tables this process handles
        hashes_per_process = self.num_hash_tables // self.warmup_total_processes
        self.warmup_start_hash = self.warmup_process_id * hashes_per_process
        self.warmup_end_hash = (self.warmup_process_id + 1) * hashes_per_process
        if self.warmup_process_id == self.warmup_total_processes - 1:
            self.warmup_end_hash = self.num_hash_tables  # Last process takes remainder
        
        # Warmup state tracking
        self.warmup_current_hash = self.warmup_start_hash
        self.warmup_current_field = 0
        self.warmup_completed = False
    
    def generate_random_data(self, size: int) -> bytes:
        """
        Generate data that is approximately 70% compressible.
        
        Args:
            size (int): Size of data to generate in bytes
            
        Returns:
            bytes: Mixed compressible and random bytes
        """
        compressible_size = int(size * 0.7)
        random_size = size - compressible_size
        
        # Compressible portion: repeated pattern (compresses very well)
        compressible_data = b'\x00' * compressible_size
        
        # Non-compressible portion: random bytes
        random_data = os.urandom(random_size)
        
        return compressible_data + random_data
    
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
        
        # Populate all 100,000 fields in batches of 50
        for start_field in range(0, self.fields_per_hash, batch_size):
            fields_dict = {}
            
            for field_offset in range(batch_size):
                field_id = start_field + field_offset
                if field_id >= self.fields_per_hash:
                    break
                
                field_name = f"field:{field_id}"
                value = self.generate_random_data(self.value_size)
                fields_dict[field_name] = value
            
            # Execute HSET with batched fields
            await client.hset(hash_name, fields_dict)
    
    async def _execute_warmup(self, client):
        """
        Execute warmup mode: populate hash tables with concurrent tasks.
        Each process handles its assigned partition of hash tables.
        
        Args:
            client: Valkey/Redis client instance
            
        Returns:
            bool: True if operation succeeded
        """
        if self.warmup_completed:
            return True
        
        # Process hash tables concurrently within this process's partition
        num_concurrent = 20
        tasks = []
        
        for i in range(num_concurrent):
            hash_id = self.warmup_current_hash + i
            if hash_id >= self.warmup_end_hash:
                break
            
            task = self._warmup_single_hash(client, hash_id)
            tasks.append(task)
        
        # Execute all tasks concurrently
        await asyncio.gather(*tasks)
        
        # Update state
        self.warmup_current_hash += num_concurrent
        if self.warmup_current_hash >= self.warmup_end_hash:
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

"""
Custom SET Benchmark Commands
==============================

Implements warmup and benchmark modes for SET operations on simple key-value pairs.

Warmup Mode:
- Creates 1,000,000,000 keys (key:0 to key:999999999)
- Each value is 400 bytes of random, non-compressible data
- Uses MSET for batching efficiency (100 keys per batch)
- Processes keys in concurrent chunks for parallel population
- Supports multi-process warmup for parallel execution via environment variables:
  * WARMUP_PROCESS_ID: This process's ID (0-based)
  * WARMUP_TOTAL_PROCESSES: Total number of warmup processes
  * Each process handles an equal partition of the key space

Benchmark Mode:
- Randomly selects one of 1 billion keys
- Updates key with fresh 400-byte random data
- Uses SET operation for single key updates
"""

import random
import os
import asyncio

class CustomCommands:
    def __init__(self):
        """Initialize the custom commands handler."""
        self.total_keys = 4000000000  # 1 billion keys
        self.value_size = 50  # 400 bytes per value
        
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
        # Each warmup call processes 1M keys (10 concurrent chunks of 100K keys)
        self.keys_per_warmup_call = 1000000
        self.warmup_current_key = self.process_start_key
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
        Execute SET command in either warmup or benchmark mode.
        
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
    
    async def _warmup_key_chunk(self, client, start_key: int, num_keys: int):
        """
        Populate a chunk of keys using MSET for efficiency.
        
        Args:
            client: Valkey/Redis client instance
            start_key: Starting key ID
            num_keys: Number of keys to populate in this chunk
        """
        batch_size = 100  # MSET 100 keys at a time
        
        for batch_start in range(start_key, start_key + num_keys, batch_size):
            # Build dictionary for MSET
            key_value_dict = {}
            
            for key_offset in range(batch_size):
                key_id = batch_start + key_offset
                if key_id >= start_key + num_keys or key_id >= self.total_keys:
                    break
                
                key_name = f"key:{key_id}"
                value = self.generate_random_data(self.value_size)
                key_value_dict[key_name] = value
            
            # Execute MSET with batched keys
            if key_value_dict:
                await client.mset(key_value_dict)
    
    async def _execute_warmup(self, client):
        """
        Execute warmup mode: populate keys with concurrent chunks.
        
        Each call processes 1M keys divided into 10 concurrent chunks of 100K keys.
        Progress is tracked so subsequent calls continue from where the previous left off.
        This process only handles its assigned partition of the key space.
        
        Args:
            client: Valkey/Redis client instance
            
        Returns:
            bool: True if operation succeeded
        """
        if self.warmup_completed:
            return True
        
        # Process keys in concurrent chunks
        num_concurrent_chunks = 10
        keys_per_chunk = 100000  # 100K keys per chunk
        
        tasks = []
        
        for i in range(num_concurrent_chunks):
            start_key = self.warmup_current_key + (i * keys_per_chunk)
            
            # Don't exceed this process's key range
            if start_key >= self.process_end_key:
                break
            
            # Calculate actual number of keys for this chunk
            remaining_keys = self.process_end_key - start_key
            chunk_size = min(keys_per_chunk, remaining_keys)
            
            task = self._warmup_key_chunk(client, start_key, chunk_size)
            tasks.append(task)
        
        # Execute all chunks concurrently
        await asyncio.gather(*tasks)
        
        # Update state
        self.warmup_current_key += self.keys_per_warmup_call
        
        if self.warmup_current_key >= self.process_end_key:
            self.warmup_completed = True
        
        return True
    
    async def _execute_benchmark(self, client):
        """
        Execute benchmark mode: random SET operations.
        
        Randomly selects one of 1 billion keys and updates it with fresh random data.
        
        Args:
            client: Valkey/Redis client instance
            
        Returns:
            bool: True if operation succeeded
        """
        # Randomly select key (uniform distribution across 1 billion keys)
        key_id = random.randint(0, self.total_keys - 1)
        key_name = f"key:{key_id}"
        
        # Generate fresh random non-compressible data
        value = self.generate_random_data(self.value_size)
        
        # Execute SET
        await client.get(key_name)
        
        return True

"""
Sample Custom Commands with Arguments
======================================

This file demonstrates how to create custom commands that accept
command-line arguments via the --custom-command-args flag.

The CustomCommands class must have an __init__ method that accepts
an optional 'args' parameter (as a single string). You can then parse
this string however you like in your implementation.

Example usage:
    python valkey-benchmark.py -t custom \
        --custom-command-file sample_custom_commands.py \
        --custom-command-args "operation=mset,batch_size=5"
"""

from typing import Any


class CustomCommands:
    """Sample custom commands class with argument parsing."""
    
    def __init__(self, args=None):
        """Initialize with optional command-line arguments.
        
        Args:
            args (str, optional): Command-line arguments as a single string.
                                 Format: "key1=value1,key2=value2"
        """
        # Default configuration
        self.operation = 'set'  # 'set', 'mset', 'hset'
        self.batch_size = 1
        self.key_prefix = 'sample'
        self.counter = 0
        
        # Parse arguments if provided
        if args:
            self._parse_args(args)
        
        print(f'CustomCommands initialized:')
        print(f'  operation: {self.operation}')
        print(f'  batch_size: {self.batch_size}')
        print(f'  key_prefix: {self.key_prefix}')
    
    def _parse_args(self, args_string):
        """Parse command-line arguments.
        
        Expected format: "key1=value1,key2=value2"
        
        Supported arguments:
            - operation: Type of operation (set, mset, hset)
            - batch_size: Number of keys to set at once (for mset)
            - key_prefix: Prefix for generated keys
        """
        try:
            pairs = args_string.split(',')
            for pair in pairs:
                if '=' in pair:
                    key, value = pair.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    
                    if key == 'operation':
                        self.operation = value
                    elif key == 'batch_size':
                        self.batch_size = int(value)
                    elif key == 'key_prefix':
                        self.key_prefix = value
        except Exception as e:
            print(f'Warning: Error parsing arguments: {e}')
    
    async def execute(self, client: Any) -> bool:
        """Execute the custom command.
        
        Args:
            client: Valkey client instance
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if self.operation == 'set':
                return await self._execute_set(client)
            elif self.operation == 'mset':
                return await self._execute_mset(client)
            elif self.operation == 'hset':
                return await self._execute_hset(client)
            else:
                print(f'Unknown operation: {self.operation}')
                return False
        except Exception as e:
            print(f'Custom command error: {str(e)}')
            return False
    
    async def _execute_set(self, client):
        """Execute a single SET command."""
        key = f'{self.key_prefix}:key:{self.counter}'
        value = f'value:{self.counter}'
        self.counter += 1
        await client.set(key, value)
        return True
    
    async def _execute_mset(self, client):
        """Execute an MSET command with batch_size keys."""
        kv_pairs = {}
        for i in range(self.batch_size):
            key = f'{self.key_prefix}:key:{self.counter}'
            value = f'value:{self.counter}'
            kv_pairs[key] = value
            self.counter += 1
        await client.mset(kv_pairs)
        return True
    
    async def _execute_hset(self, client):
        """Execute an HSET command."""
        hash_key = f'{self.key_prefix}:hash'
        field = f'field:{self.counter}'
        value = f'value:{self.counter}'
        self.counter += 1
        await client.hset(hash_key, {field: value})
        return True

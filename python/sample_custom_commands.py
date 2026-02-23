"""
Sample Custom Commands with Config Initialization
==================================================

LIFECYCLE:
    1. __init__(args) - Called first (args available here too, but prefer init)
    2. init(config)   - Called with full config including args - DO ALL SETUP HERE
    3. execute(client) - Called for each benchmark request

CONFIG PROPERTIES AVAILABLE IN init():
    config['custom_command_args']    - The --custom-command-args string (note: underscore not camelCase)
    config['data_size']              - Size of data in bytes (--datasize)
    config['keyspace_offset']        - Keyspace offset (--keyspace-offset)
    config['random_keyspace']        - Random keyspace size (--random)
    config['use_sequential']         - Whether sequential mode is enabled
    config['sequential_keyspacelen'] - Sequential keyspace length

EXAMPLE:
    python valkey-benchmark.py -t custom \
        --custom-command-file sample_custom_commands.py \
        --custom-command-args "operation=lpush_keyspace" \
        --sequential 1000000 --keyspace-offset 5000000
"""

import random
import string
from typing import Any, Dict, Optional


class CustomCommands:
    def __init__(self, args: Optional[str] = None):
        # Minimal constructor - all real setup happens in init()
        pass

    def init(self, config: Dict[str, Any]) -> None:
        """Initialize everything here - both custom args and benchmark config."""
        # Parse custom command args
        self.operation = 'set'
        self.batch_size = 1
        self.key_prefix = 'sample'
        self.counter = 0

        args = config.get('custom_command_args') or ''
        for pair in args.split(','):
            if '=' not in pair:
                continue
            key, value = pair.split('=', 1)
            key, value = key.strip(), value.strip()
            if key == 'operation':
                self.operation = value
            elif key == 'batch_size':
                self.batch_size = int(value) if value.isdigit() else 1
            elif key == 'key_prefix':
                self.key_prefix = value

        # Store benchmark config
        self.keyspace_offset = config.get('keyspace_offset', 0)
        self.random_keyspace = config.get('random_keyspace', 0)
        self.use_sequential = config.get('use_sequential', False)
        self.sequential_keyspacelen = config.get('sequential_keyspacelen', 0)
        self.data_size = config.get('data_size', 100)

        ks_type = 'sequential' if self.use_sequential else 'random' if self.random_keyspace > 0 else 'none'
        print(f'CustomCommands init: operation={self.operation}, keyspace={ks_type}, offset={self.keyspace_offset}')

    async def execute(self, client: Any) -> bool:
        ops = {
            'set': self._execute_set,
            'mset': self._execute_mset,
            'hset': self._execute_hset,
            'lpush': self._execute_lpush,
            'lpush_keyspace': self._execute_lpush_keyspace,
        }
        handler = ops.get(self.operation)
        return await handler(client) if handler else False

    def _get_next_key(self) -> str:
        if self.use_sequential:
            return f'key:{self.keyspace_offset + (self.counter % self.sequential_keyspacelen)}'
        elif self.random_keyspace > 0:
            return f'key:{self.keyspace_offset + random.randint(0, self.random_keyspace - 1)}'
        return f'key:{self.counter}'

    def _generate_data(self) -> str:
        return ''.join(random.choices(string.ascii_letters + string.digits, k=self.data_size))

    async def _execute_set(self, client: Any) -> bool:
        await client.set(f'{self.key_prefix}:key:{self.counter}', f'value:{self.counter}')
        self.counter += 1
        return True

    async def _execute_mset(self, client: Any) -> bool:
        kv_pairs = {}
        for _ in range(self.batch_size):
            kv_pairs[f'{self.key_prefix}:key:{self.counter}'] = f'value:{self.counter}'
            self.counter += 1
        await client.mset(kv_pairs)
        return True

    async def _execute_hset(self, client: Any) -> bool:
        await client.hset(f'{self.key_prefix}:hash', {f'field:{self.counter}': f'value:{self.counter}'})
        self.counter += 1
        return True

    async def _execute_lpush(self, client: Any) -> bool:
        await client.lpush(f'{self.key_prefix}:list', [f'value:{self.counter}'])
        self.counter += 1
        return True

    async def _execute_lpush_keyspace(self, client: Any) -> bool:
        key = self._get_next_key()
        data = self._generate_data()
        self.counter += 1
        await client.lpush(key, [data])
        return True

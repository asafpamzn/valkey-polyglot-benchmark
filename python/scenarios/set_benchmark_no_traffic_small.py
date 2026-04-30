"""
Custom SET Benchmark Commands - NO TRAFFIC (SMALL MACHINE)
==========================================================

Config for no-traffic scenario:
- Always writes to the same key (key:000000000000)
- value_size = 50 bytes
"""

import os

class CustomCommands:
    def __init__(self):
        """Initialize the custom commands handler."""
        self.value_size = 50  # 50 bytes per value
        self.fixed_key = "key:000000000000"

    def generate_random_data(self, size: int) -> bytes:
        """Generate random non-compressible data."""
        return os.urandom(size)

    async def execute(self, client):
        """Execute SET command to the same fixed key."""
        value = self.generate_random_data(self.value_size)
        await client.set(self.fixed_key, value)
        return True

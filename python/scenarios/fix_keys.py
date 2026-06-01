"""Fix corrupted keys back to valid integrity values."""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from set_benchmark_integrity_large import CustomCommands
from validate_integrity import RESPClient


async def fix():
    client = await RESPClient.connect('ec2-98-80-5-25.compute-1.amazonaws.com', 6379, False)
    for key_id in [42, 99, 150, 200]:
        key_name = f'key:{key_id:012d}'
        value = CustomCommands.build_value(key_name)
        key_bytes = key_name.encode()
        header = f"*3\r\n$3\r\nSET\r\n${len(key_bytes)}\r\n".encode()
        cmd = header + key_bytes + f"\r\n${len(value)}\r\n".encode() + value + b"\r\n"
        client.writer.write(cmd)
        await client.writer.drain()
        await client.reader.readline()
        print(f'Fixed {key_name}')
    await client.close()


asyncio.run(fix())

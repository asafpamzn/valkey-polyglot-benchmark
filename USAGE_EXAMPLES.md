# Request Timeout Configuration Examples

The valkey-benchmark tool now supports configuring the `request_timeout` parameter for Glide clients. This parameter allows you to set a timeout in milliseconds for individual requests.

## Overview

The `--request-timeout` parameter is available in all language implementations (Python, Node.js, Go, and Java) and works with both standalone and cluster modes.

- **Default behavior**: When not specified, no timeout is set (or uses Glide client defaults)
- **Units**: Milliseconds
- **Applies to**: Both standalone and cluster client configurations

## Usage Examples

### Python

```bash
# Set a 5-second timeout
python valkey-benchmark.py -H localhost -p 6379 --request-timeout 5000

# With cluster mode
python valkey-benchmark.py -H localhost -p 6379 --cluster --request-timeout 3000

# Combined with other options
python valkey-benchmark.py -H localhost -p 6379 -c 50 -n 100000 --request-timeout 2000
```

### Node.js

```bash
# Set a 5-second timeout
node valkey-benchmark.js -h localhost -p 6379 --request-timeout 5000

# With cluster mode
node valkey-benchmark.js -h localhost -p 6379 --cluster --request-timeout 3000

# Combined with other options
node valkey-benchmark.js -h localhost -p 6379 -c 50 -n 100000 --request-timeout 2000
```

### Go

```bash
# Set a 5-second timeout
./valkey-benchmark -H localhost -p 6379 -request-timeout 5000

# With cluster mode
./valkey-benchmark -H localhost -p 6379 -cluster -request-timeout 3000

# Combined with other options
./valkey-benchmark -H localhost -p 6379 -c 50 -n 100000 -request-timeout 2000
```

### Java

```bash
# Set a 5-second timeout
./gradlew :runBenchmark --args="-h localhost -p 6379 --request-timeout 5000"

# With cluster mode
./gradlew :runBenchmark --args="-h localhost -p 6379 --cluster --request-timeout 3000"

# Combined with other options
./gradlew :runBenchmark --args="-h localhost -p 6379 -c 50 -n 100000 --request-timeout 2000"
```

## Use Cases

### Testing Timeout Behavior

```bash
# Test with a very short timeout to simulate timeout conditions
python valkey-benchmark.py -H localhost -p 6379 --request-timeout 1

# Test with a generous timeout for slow networks
python valkey-benchmark.py -H localhost -p 6379 --request-timeout 10000
```

### Production-like Testing

```bash
# Set a reasonable timeout matching production settings
python valkey-benchmark.py -H localhost -p 6379 --request-timeout 3000 -c 100 -n 1000000
```

### Cluster Mode with Timeout

```bash
# Cluster benchmarking with custom timeout
python valkey-benchmark.py -H localhost -p 6379 --cluster --request-timeout 5000 --read-from-replica
```

## Notes

- The timeout value is in milliseconds
- A value of 0 (or omitting the parameter) means no timeout or uses the client's default
- Timeouts apply to individual requests, not the entire benchmark duration
- For production use, consider setting a timeout that matches your application's requirements
- Different operations may require different timeout values - test accordingly

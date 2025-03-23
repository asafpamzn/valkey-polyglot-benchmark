# Valkey Polyglot Benchmark

Valkey Polyglot Benchmark is a multilanguage benchmark tool for Valkey. This tool allows developers to conduct performance testing in their preferred programming language while maintaining consistent testing methodologies across different implementations. It supports various testing scenarios, including throughput testing, latency measurements, and custom command benchmarking.

Key Advantages:

1. **Multi-Language Support**: Built with native Valkey clients in multiple programming languages (Java, Node.js, Python, Go), providing authentic end-to-end performance measurements in your production environment's language of choice.

2. **Advanced Cluster Testing**: Leverages the [valkey-GLIDE](https://github.com/valkey-io/valkey-glide) client to support:
   - Dynamic cluster topology updates
   - Read replica operations
   - Automatic failover scenarios
   - Connection pooling optimization

3. **Extensible Command Framework**: 
   - Supports custom commands and complex operations
   - Test Lua scripts and multi-key transactions
   - Benchmark your specific use cases and data patterns
   - Measure performance of custom commands across different language implementations

4. **Consistent Methodology**: 
   - Unified benchmarking approach across all language implementations
   - Comparable results between different client libraries
   - Standardized metrics collection and reporting


## Java Benchmark Tool

The Java benchmark tool is located in the `java` directory. It provides a comprehensive set of features for benchmarking Valkey using the GLIDE client library.

For detailed instructions on how to use the Java benchmark tool, please refer to the [Java README](java/README.md).

## Node.js Benchmark Tool

The Node.js benchmark tool is located in the `node` directory. It offers similar benchmarking capabilities as the Java tool, but is implemented using Node.js.

For detailed instructions on how to use the Node.js benchmark tool, please refer to the [Node.js README](node/README.md).

## Python Benchmark Tool

The Python benchmark tool is located in the `python` directory. It offers similar benchmarking capabilities as the Java tool, but is implemented using Python.

For detailed instructions on how to use the Python benchmark tool, please refer to the [Python README](python/README.md).

## Go Benchmark Tool

The Go benchmark tool is located in the `go` directory. It offers similar benchmarking capabilities as the Java tool, but is implemented using Python.

For detailed instructions on how to use the Go benchmark tool, please refer to the [Go README](go/README.md).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
# Run SET benchmark
go run . -host localhost -port 6379 -connections 50 -duration 30 -pipeline 10 -op set -keysize 20 -valuesize 100 -concurrency 4

# Run GET benchmark
go run . -host localhost -port 6379 -connections 50 -duration 30 -pipeline 10 -op get -concurrency 4
// results.go
package main

import (
    "fmt"
    "time"
)

func printResults(results *BenchmarkResults) {
    results.RequestsPerSec = float64(results.TotalRequests) / results.TotalDuration.Seconds()
    avgLatency := results.TotalDuration / time.Duration(results.TotalRequests)
    
    fmt.Printf("\nBenchmark Results:\n")
    fmt.Printf("Total Requests: %d\n", results.TotalRequests)
    fmt.Printf("Total Errors: %d\n", results.TotalErrors)
    fmt.Printf("Requests per second: %.2f\n", results.RequestsPerSec)
    fmt.Printf("Average Latency: %v\n", avgLatency)
    fmt.Printf("Min Latency: %v\n", results.MinLatency)
    fmt.Printf("Max Latency: %v\n", results.MaxLatency)
    fmt.Printf("Total Duration: %v\n", results.TotalDuration)
}

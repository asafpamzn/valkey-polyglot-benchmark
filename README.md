# Valkey Benchmark Tool

This repository contains benchmark tools for Valkey using the [valkey-GLIDE](https://github.com/valkey-io/valkey-glide) client library. These tools support various testing scenarios, including throughput testing, latency measurements, and custom command benchmarking. Unlike the standard valkey-benchmark, this tool is implemented in multiple programming languages, allowing you to test Valkey's performance with the [valkey-GLIDE](https://github.com/valkey-io/valkey-glide) client using any usage pattern.

The commands are pluggable, enabling the use of any custom command. For example, you can benchmark complex Lua scripts, custom transactions, and more, all in your preferred programming language. This flexibility allows for comprehensive performance testing tailored to your specific use cases.

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

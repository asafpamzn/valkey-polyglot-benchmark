# Valkey GLIDE Benchmark Tool

This repository contains benchmark tools for Valkey using the (#[valkey-GLIDE](https://github.com/valkey-io/valkey-glide)) client library. These tools support various testing scenarios, including throughput testing, latency measurements, and custom command benchmarking. Unlike the standard valkey-benchmark, this tool is implemented in multiple programming languages, allowing you to test Valkey's performance with the (#[valkey-GLIDE](https://github.com/valkey-io/valkey-glide)) client using any usage pattern.

The commands are pluggable, enabling the use of any custom command. For example, you can benchmark complex Lua scripts, custom transactions, and more, all in your preferred programming language. This flexibility allows for comprehensive performance testing tailored to your specific use cases.




## Java Benchmark Tool

The Java benchmark tool is located in the `java` directory. It provides a comprehensive set of features for benchmarking Valkey using the GLIDE client library.

For detailed instructions on how to use the Java benchmark tool, please refer to the [Java README](java/README.md).

## Node.js Benchmark Tool

The Node.js benchmark tool is located in the `node` directory. It offers similar benchmarking capabilities as the Java tool, but is implemented using Node.js.

For detailed instructions on how to use the Node.js benchmark tool, please refer to the [Node.js README](node/README.md).

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
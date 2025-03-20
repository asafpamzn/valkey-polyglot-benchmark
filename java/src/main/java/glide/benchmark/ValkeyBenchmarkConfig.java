package glide.benchmark;

/**
 * Configuration class for the Valkey Benchmark tool.
 * This class handles all configuration parameters for the benchmark execution,
 * including connection settings, performance parameters, and test execution options.
 * It provides parameter parsing from command line arguments and validation of the
 * configuration settings.
 */
public class ValkeyBenchmarkConfig {
    /** Default host address for the server */
    private String host = "127.0.0.1";
    
    /** Default port number for the server */
    private int port = 6379;
    
    /** Number of worker threads to use for the benchmark */
    private int numThreads = 1;
    
    /** Total number of requests to execute during the benchmark */
    private int totalRequests = 100000;
    
    /** Size of data in bytes for SET operations */
    private int dataSize = 3;
    
    /** Command to benchmark (e.g., "set", "get", "custom") */
    private String command = "set";
    
    /** Flag to show help message */
    private boolean showHelp = false;
    
    /** Size of random key space for key generation */
    private int randomKeyspace = 0;
    
    /** Flag to use sequential keys instead of random */
    private boolean useSequential = false;
    
    /** Length of sequential key space */
    private int sequentialKeyspacelen = 0;
    
    /** Size of the client connection pool */
    private int poolSize = 1;
    
    /** Fixed QPS (Queries Per Second) limit */
    private int qps = 0;
    
    /** Starting QPS for dynamic QPS adjustment */
    private int startQps = 0;
    
    /** Target ending QPS for dynamic QPS adjustment */
    private int endQps = 0;
    
    /** Interval in seconds between QPS adjustments */
    private int qpsChangeInterval = 0;
    
    /** Amount to change QPS by during each adjustment */
    private int qpsChange = 0;
    
    /** Duration of the test in seconds (0 for request-count based) */
    private int testDuration = 0;
    
    /** Flag to enable TLS for connections */
    private boolean useTls = false;
    
    /** Flag to use cluster mode */
    private boolean isCluster = false;
    
    /** Flag to enable reading from replica nodes */
    private boolean readFromReplica = false;

    // Getters
    /**
     * Gets the server host address.
     * @return The configured host address
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the server port number.
     * @return The configured port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the number of worker threads.
     * @return The number of threads to use
     */
    public int getNumThreads() {
        return numThreads;
    }

    /**
     * Gets the total number of requests to execute.
     * @return The total request count
     */
    public int getTotalRequests() {
        return totalRequests;
    }

    /**
     * Gets the size of data for SET operations.
     * @return The data size in bytes
     */
    public int getDataSize() {
        return dataSize;
    }

    /**
     * Gets the command to benchmark.
     * @return The command string
     */
    public String getCommand() {
        return command;
    }

    /**
     * Checks if help should be displayed.
     * @return true if help should be shown
     */
    public boolean isShowHelp() {
        return showHelp;
    }

    /**
     * Gets the size of random key space.
     * @return The random key space size
     */
    public int getRandomKeyspace() {
        return randomKeyspace;
    }

    /**
     * Checks if sequential keys should be used.
     * @return true if using sequential keys
     */
    public boolean isUseSequential() {
        return useSequential;
    }

    /**
     * Gets the length of sequential key space.
     * @return The sequential key space length
     */
    public int getSequentialKeyspacelen() {
        return sequentialKeyspacelen;
    }

    /**
     * Gets the size of the client connection pool.
     * @return The pool size
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * Gets the fixed QPS limit.
     * @return The QPS limit
     */
    public int getQps() {
        return qps;
    }

    /**
     * Gets the starting QPS for dynamic adjustment.
     * @return The starting QPS value
     */
    public int getStartQps() {
        return startQps;
    }

    /**
     * Gets the target ending QPS for dynamic adjustment.
     * @return The ending QPS value
     */
    public int getEndQps() {
        return endQps;
    }

    /**
     * Gets the interval for QPS changes.
     * @return The interval in seconds
     */
    public int getQpsChangeInterval() {
        return qpsChangeInterval;
    }

    /**
     * Gets the amount of QPS change per interval.
     * @return The QPS change amount
     */
    public int getQpsChange() {
        return qpsChange;
    }

    /**
     * Gets the test duration in seconds.
     * @return The test duration
     */
    public int getTestDuration() {
        return testDuration;
    }

    /**
     * Checks if TLS should be used.
     * @return true if TLS should be enabled
     */
    public boolean isUseTls() {
        return useTls;
    }

    /**
     * Checks if cluster mode should be used.
     * @return true if using cluster mode
     */
    public boolean isCluster() {
        return isCluster;
    }

    /**
     * Checks if reading from replicas is enabled.
     * @return true if replica reads are enabled
     */
    public boolean isReadFromReplica() {
        return readFromReplica;
    }

    /**
     * Parses command line arguments and configures the benchmark parameters.
     * 
     * @param args Command line arguments to parse
     * @throws IllegalArgumentException if invalid arguments are provided
     */
    public void parse(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "--help":
                    showHelp = true;
                    break;
                case "-h":
                    if (i + 1 < args.length) {
                        host = args[++i];
                    }
                    break;
                case "-p":
                    if (i + 1 < args.length) {
                        port = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--threads":
                    if (i + 1 < args.length) {
                        numThreads = Integer.parseInt(args[++i]);
                    }
                    break;
                case "-n":
                    if (i + 1 < args.length) {
                        totalRequests = Integer.parseInt(args[++i]);
                    }
                    break;
                case "-d":
                    if (i + 1 < args.length) {
                        dataSize = Integer.parseInt(args[++i]);
                    }
                    break;
                case "-t":
                    if (i + 1 < args.length) {
                        command = args[++i];
                    }
                    break;
                case "-r":
                    if (i + 1 < args.length) {
                        randomKeyspace = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--sequential":
                    if (i + 1 < args.length) {
                        useSequential = true;
                        sequentialKeyspacelen = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--pool-size":
                    if (i + 1 < args.length) {
                        poolSize = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--qps":
                    if (i + 1 < args.length) {
                        qps = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--start-qps":
                    if (i + 1 < args.length) {
                        startQps = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--end-qps":
                    if (i + 1 < args.length) {
                        endQps = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--qps-change-interval":
                    if (i + 1 < args.length) {
                        qpsChangeInterval = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--qps-change":
                    if (i + 1 < args.length) {
                        qpsChange = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--test-duration":
                    if (i + 1 < args.length) {
                        testDuration = Integer.parseInt(args[++i]);
                    }
                    break;
                case "--tls":
                    useTls = true;
                    break;
                case "--cluster":
                    isCluster = true;
                    break;
                case "--read-from-replica":
                    readFromReplica = true;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }

        validate();
    }

    /**
     * Validates the configuration parameters to ensure they are consistent
     * and within acceptable ranges.
     * 
     * @throws IllegalArgumentException if validation fails
     */
    private void validate() {
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Invalid port number");
        }

        if (numThreads <= 0) {
            throw new IllegalArgumentException("Number of threads must be positive");
        }

        if (totalRequests <= 0 && testDuration <= 0) {
            throw new IllegalArgumentException("Either total requests or test duration must be positive");
        }

        if (dataSize <= 0) {
            throw new IllegalArgumentException("Data size must be positive");
        }

        if (randomKeyspace < 0) {
            throw new IllegalArgumentException("Random keyspace must be non-negative");
        }

        if (useSequential && totalRequests != 100000) {
            throw new IllegalArgumentException("--sequential is mutually exclusive with -n");
        }

        if (poolSize <= 0) {
            throw new IllegalArgumentException("Pool size must be positive");
        }

        if (qps < 0) {
            throw new IllegalArgumentException("QPS must be non-negative");
        }

        boolean hasStartQps = startQps > 0;
        boolean hasEndQps = endQps > 0;
        boolean hasQpsInterval = qpsChangeInterval > 0;
        boolean hasQpsChange = qpsChange != 0;

        if (hasStartQps || hasEndQps || hasQpsInterval || hasQpsChange) {
            if (!(hasStartQps && hasEndQps && hasQpsInterval && hasQpsChange)) {
                throw new IllegalArgumentException(
                    "Dynamic QPS requires all of: --start-qps, --end-qps, --qps-change-interval, --qps-change"
                );
            }

            if ((endQps - startQps > 0 && qpsChange <= 0) ||
                (endQps - startQps < 0 && qpsChange >= 0)) {
                throw new IllegalArgumentException(
                    "QPS change direction must match the difference between end-qps and start-qps"
                );
            }
        }

        if (qps > 0 && hasStartQps) {
            throw new IllegalArgumentException("--qps is mutually exclusive with dynamic QPS options");
        }
    }
}

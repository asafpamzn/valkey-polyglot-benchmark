package polyglot.benchmark;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;
import static glide.api.logging.Logger.Level.ERROR;
import static glide.api.logging.Logger.log;

import glide.api.GlideClient;
import glide.api.GlideClusterClient;
import glide.api.models.configuration.NodeAddress;
import polyglot.benchmark.ValkeyBenchmarkClients.BenchmarkClient;
import polyglot.benchmark.ValkeyBenchmarkClients.ClusterBenchmarkClient;
import polyglot.benchmark.ValkeyBenchmarkClients.StandaloneBenchmarkClient;

/**
 * ValkeyBenchmark is a performance testing utility for Valkey operations.
 * It supports both standalone and cluster modes, and provides comprehensive
 * performance metrics including throughput, latency statistics, and QPS control.
 * 
 * Features:
 * - Supports both standalone and cluster modes
 * - Configurable QPS (Queries Per Second) with dynamic adjustment
 * - Multiple worker threads for parallel testing
 * - Detailed latency reporting (min, max, avg, percentiles)
 * - Real-time throughput monitoring
 * - Support for various Redis commands (SET, GET, custom)
 * - TLS support
 * - Replica read support
 */
public class ValkeyBenchmark {
    /** Pool of benchmark clients for connection reuse */
    static List<BenchmarkClient> clientPool = new ArrayList<>();
    
    /** Queue of available client indices for thread-safe client allocation */
    static BlockingQueue<Integer> freeClients = new LinkedBlockingQueue<>();

    /** Global configuration instance */
    static ValkeyBenchmarkConfig gConfig = new ValkeyBenchmarkConfig();

    /** Counter for completed requests */
    static AtomicInteger gRequestsFinished = new AtomicInteger(0);
    
    /** Flag indicating if the test is still running */
    static AtomicBoolean gTestRunning = new AtomicBoolean(true);
    
    /** Sum of all operation latencies in microseconds */
    static AtomicLong gLatencySumUs = new AtomicLong(0);
    
    /** Count of latency measurements */
    static AtomicInteger gLatencyCount = new AtomicInteger(0);
    
    /** CSV interval tracking - start time of current interval */
    static volatile long gIntervalStartTime = System.currentTimeMillis();
    
    /** CSV interval tracking - latencies for current interval */
    static List<Long> gIntervalLatencies = Collections.synchronizedList(new ArrayList<>());
    
    /** CSV interval tracking - errors for current interval */
    static AtomicInteger gIntervalErrors = new AtomicInteger(0);
    
    /** CSV interval tracking - MOVED responses for current interval */
    static AtomicInteger gIntervalMoved = new AtomicInteger(0);
    
    /** CSV interval tracking - CLUSTERDOWN responses for current interval */
    static AtomicInteger gIntervalClusterdown = new AtomicInteger(0);
    
    /** CSV interval tracking - client disconnects for current interval */
    static AtomicInteger gIntervalDisconnects = new AtomicInteger(0);
    
    /** CSV interval tracking - requests completed in current interval */
    static AtomicInteger gIntervalRequests = new AtomicInteger(0);
    
    /** Flag to track if CSV header has been printed */
    static boolean gCsvHeaderPrinted = false;

    /**
     * Statistics collector for individual thread measurements.
     * Stores latency measurements for each operation performed by a thread.
     */
    static class ThreadStats {
        /** List of operation latencies in microseconds */
        List<Long> latencies = new ArrayList<>();
    }

    /**
     * Prints the command-line usage information including all available options
     * and their descriptions.
     */
    static void printUsage() {
        System.out.println("Valkey-Java Benchmark\n" +
                "Usage: java ValkeyBenchmark [OPTIONS]\n\n" +
                "Options:\n" +
                "  -h <hostname>      Server hostname (default 127.0.0.1)\n" +
                "  -p <port>          Server port (default 6379)\n" +
                "  -c <clients>       Number of parallel connections (default 50)\n" +
                "  -n <requests>      Total number of requests (default 100000)\n" +
                "  -d <size>          Data size of value in bytes for SET (default 3)\n" +
                "  -t <command>       Command to benchmark (get, set or custom)\n" +
                "  -r <keyspacelen>   Number of random keys to use (default 0: single key)\n" +
                "  --threads <threads>       Number of worker threads (default 1)\n" +
                "  --test-duration <seconds>   Test duration in seconds.\n" +
                "  --sequential <keyspacelen>\n" +
                "                    Use sequential keys from 0 to keyspacelen-1 for SET/GET/INCR,\n" +
                "                    sequential values for SADD, sequential members and scores for ZADD.\n" +
                "                    Using --sequential option will generate <keyspacelen> requests.\n" +
                "                    This flag is mutually exclusive with --test-duration and -n flags.\n" +
                "  --qps <limit>      Limit the maximum number of queries per second.\n" +
                "                    Must be a positive integer.\n" +
                "  --start-qps <val>  Starting QPS limit, must be > 0.\n" +
                "                    Requires --end-qps, --qps-change-interval, and --qps-change (for linear mode).\n" +
                "                    Mutually exclusive with --qps.\n" +
                "  --end-qps <val>    Ending QPS limit, must be > 0.\n" +
                "                    Requires --start-qps, --qps-change-interval, and --qps-change (for linear mode).\n" +
                "  --qps-change-interval <seconds>\n" +
                "                    Time interval (in seconds) to adjust QPS.\n" +
                "                    Requires --start-qps, --end-qps, and --qps-change (for linear mode).\n" +
                "  --qps-change <val> QPS adjustment applied every interval (linear mode only).\n" +
                "                    Must be non-zero and have the same sign as (end-qps - start-qps).\n" +
                "                    Not required for exponential mode.\n" +
                "  --qps-ramp-mode <mode>\n" +
                "                    QPS ramp mode: 'linear' or 'exponential' (default: linear).\n" +
                "                    In exponential mode, QPS grows/decays by a multiplier each interval.\n" +
                "  --tls              Use TLS for connection\n" +
                "  --cluster          Use cluster client\n" +
                "  --read-from-replica  Read from replica nodes\n" +
                "  --interval-metrics-interval-duration-sec <seconds>\n" +
                "                    Emit CSV metrics every N seconds (enables CSV output mode)\n" +
                "  --help             Show this help message and exit\n");
    }
    
    /**
     * Prints the CSV header line (once at start).
     */
    static synchronized void printCsvHeader() {
        if (!gCsvHeaderPrinted) {
            System.out.println("timestamp,request_sec,p50_usec,p90_usec,p95_usec,p99_usec,p99_9_usec,p99_99_usec,p99_999_usec,p100_usec,avg_usec,requests_total_failed,requests_moved,requests_clusterdown,client_disconnects");
            System.out.flush();
            gCsvHeaderPrinted = true;
        }
    }
    
    /**
     * Calculates percentile value from sorted latencies in microseconds (truncated).
     * @param sorted Sorted list of latency values in microseconds
     * @param percentile Percentile to calculate (0-100)
     * @return The calculated percentile value (truncated to integer)
     */
    static long calculatePercentileUsec(List<Long> sorted, double percentile) {
        if (sorted.isEmpty()) {
            return 0;
        }
        int idx = (int) Math.floor((percentile / 100.0) * sorted.size());
        if (idx >= sorted.size()) {
            idx = sorted.size() - 1;
        }
        return sorted.get(idx);
    }
    
    /**
     * Emits a CSV data line for the current interval.
     */
    static synchronized void emitCsvLine() {
        long now = System.currentTimeMillis();
        double intervalDuration = (now - gIntervalStartTime) / 1000.0; // in seconds
        
        // Calculate timestamp (Unix epoch seconds)
        long timestamp = now / 1000;
        
        // Calculate request_sec for this interval
        double requestSec = intervalDuration > 0 ? gIntervalRequests.get() / intervalDuration : 0.0;
        
        // Calculate percentiles from interval latencies
        long p50, p90, p95, p99, p99_9, p99_99, p99_999, p100, avg;
        List<Long> intervalLats = new ArrayList<>(gIntervalLatencies);
        
        if (!intervalLats.isEmpty()) {
            Collections.sort(intervalLats);
            p50 = calculatePercentileUsec(intervalLats, 50.0);
            p90 = calculatePercentileUsec(intervalLats, 90.0);
            p95 = calculatePercentileUsec(intervalLats, 95.0);
            p99 = calculatePercentileUsec(intervalLats, 99.0);
            p99_9 = calculatePercentileUsec(intervalLats, 99.9);
            p99_99 = calculatePercentileUsec(intervalLats, 99.99);
            p99_999 = calculatePercentileUsec(intervalLats, 99.999);
            p100 = intervalLats.get(intervalLats.size() - 1); // max
            avg = (long) intervalLats.stream().mapToLong(Long::longValue).average().orElse(0.0);
        } else {
            p50 = p90 = p95 = p99 = p99_9 = p99_99 = p99_999 = p100 = avg = 0;
        }
        
        // Output CSV line with exactly 15 fields
        System.out.printf("%d,%.6f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d%n",
            timestamp, requestSec, p50, p90, p95, p99, p99_9, p99_99, p99_999, p100, avg,
            gIntervalErrors.get(), gIntervalMoved.get(), gIntervalClusterdown.get(), gIntervalDisconnects.get());
        System.out.flush();
        
        // Reset interval counters
        gIntervalStartTime = now;
        gIntervalLatencies.clear();
        gIntervalErrors.set(0);
        gIntervalMoved.set(0);
        gIntervalClusterdown.set(0);
        gIntervalDisconnects.set(0);
        gIntervalRequests.set(0);
    }
    
    /**
     * Checks if it's time to emit a CSV line.
     */
    static void checkCsvInterval() {
        if (gConfig.getCsvIntervalSec() > 0) {
            long now = System.currentTimeMillis();
            if ((now - gIntervalStartTime) / 1000.0 >= gConfig.getCsvIntervalSec()) {
                emitCsvLine();
            }
        }
    }

    /** Lock object for QPS synchronization */
    static final Object throttleLock = new Object();
    
    /** Counter for operations in current second */
    static int opsThisSecond = 0;
    
    /** Start time of current second for QPS calculation */
    static long secondStart = System.nanoTime();
    
    /** Last time QPS was updated for dynamic QPS */
    static long lastQpsUpdate = System.nanoTime();
    
    /** Current QPS limit */
    static int currentQps = 0;
    
    /** Exponential multiplier for QPS ramp (computed at initialization) */
    static double exponentialMultiplier = 1.0;
    
    /** Flag indicating if QPS throttling has been initialized */
    static boolean throttleInitialized = false;

    /**
     * Controls the rate of operations to maintain the configured QPS limit.
     * Supports both static and dynamic QPS adjustment based on configuration.
     * Supports both linear and exponential ramp modes.
     */
    static void throttleQPS() {
        synchronized (throttleLock) {
            long now = System.nanoTime();
            if (!throttleInitialized) {
                currentQps = (gConfig.getQps() > 0) ? gConfig.getQps() : gConfig.getStartQps();
                
                // For exponential mode, compute the multiplier
                if ("exponential".equals(gConfig.getQpsRampMode()) && 
                    gConfig.getStartQps() > 0 && gConfig.getEndQps() > 0 && 
                    gConfig.getQpsChangeInterval() > 0 && gConfig.getTestDuration() > 0) {
                    // Calculate number of intervals
                    int numIntervals = gConfig.getTestDuration() / gConfig.getQpsChangeInterval();
                    if (numIntervals > 0) {
                        // multiplier = (endQps / startQps) ^ (1 / numIntervals)
                        exponentialMultiplier = Math.pow((double) gConfig.getEndQps() / gConfig.getStartQps(), 
                                                         1.0 / numIntervals);
                    } else {
                        System.err.println("Warning: test-duration is less than qps-change-interval, exponential mode will not ramp QPS");
                    }
                }
                throttleInitialized = true;
            }
            
            boolean isExponentialMode = "exponential".equals(gConfig.getQpsRampMode());
            boolean hasDynamicQps = (gConfig.getStartQps() > 0 && gConfig.getEndQps() > 0 &&
                    gConfig.getQpsChangeInterval() > 0);
            
            // For linear mode, also require qpsChange
            if (!isExponentialMode) {
                hasDynamicQps = hasDynamicQps && gConfig.getQpsChange() != 0;
            }
            
            if (hasDynamicQps) {
                long elapsedSec = TimeUnit.NANOSECONDS.toSeconds(now - lastQpsUpdate);
                if (elapsedSec >= gConfig.getQpsChangeInterval()) {
                    if (isExponentialMode) {
                        // Exponential mode: multiply by the computed multiplier
                        int newQps = (int) Math.round(currentQps * exponentialMultiplier);
                        
                        // Clamp to endQps
                        if (gConfig.getEndQps() > gConfig.getStartQps()) {
                            // Increasing QPS
                            if (newQps > gConfig.getEndQps()) {
                                newQps = gConfig.getEndQps();
                            }
                        } else {
                            // Decreasing QPS
                            if (newQps < gConfig.getEndQps()) {
                                newQps = gConfig.getEndQps();
                            }
                        }
                        currentQps = newQps;
                    } else {
                        // Linear mode: add qpsChange
                        int diff = gConfig.getEndQps() - currentQps;
                        if ((diff > 0 && gConfig.getQpsChange() > 0) ||
                            (diff < 0 && gConfig.getQpsChange() < 0)) {
                            currentQps += gConfig.getQpsChange();
                            if ((gConfig.getQpsChange() > 0 && currentQps > gConfig.getEndQps()) ||
                                (gConfig.getQpsChange() < 0 && currentQps < gConfig.getEndQps())) {
                                currentQps = gConfig.getEndQps();
                            }
                        }
                    }
                    lastQpsUpdate = System.nanoTime();
                }
            }
            if (currentQps > 0) {
                long secElapsed = TimeUnit.NANOSECONDS.toSeconds(now - secondStart);
                if (secElapsed >= 1) {
                    opsThisSecond = 0;
                    secondStart = now;
                }
                if (opsThisSecond >= currentQps) {
                    long nextSecond = secondStart + TimeUnit.SECONDS.toNanos(1);
                    long sleepTime = nextSecond - System.nanoTime();
                    if (sleepTime > 0) {
                        try {
                            TimeUnit.NANOSECONDS.sleep(sleepTime);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    opsThisSecond = 0;
                    secondStart = System.nanoTime();
                }
                opsThisSecond++;
            }
        }
    }

    /** State for random data generation */
    static int state = 1234;

    /**
     * Generates random string data of specified size.
     * @param size The size of the random string to generate
     * @return A random string of the specified size
     */
    static String generateRandomData(int size) {
        char[] data = new char[size];
        for (int i = 0; i < size; i++) {
            state = state * 1103515245 + 12345;
            data[i] = (char)('A' + ((state >>> 16) % 26));
        }
        return new String(data);
    }

    /**
     * Generates a random key within the configured keyspace.
     * @return A random key string
     */
    static String getRandomKey() {
        int r = new Random().nextInt(gConfig.getRandomKeyspace());
        return "key:" + r;
    }

    /**
     * Worker thread function that executes the benchmark operations.
     * Handles different commands (SET, GET, custom) and collects statistics.
     *
     * @param thread_id The ID of the worker thread
     * @param stats ThreadStats object to collect performance metrics
     */
    static void workerThreadFunc(int thread_id, ThreadStats stats) throws ExecutionException, InterruptedException {
        boolean timeBased = (gConfig.getTestDuration() > 0);
        long startTime = System.nanoTime();
        int requests_per_thread = 0;
        int remainder = 0;
        if (!timeBased) {
            requests_per_thread = gConfig.getTotalRequests() / gConfig.getNumThreads();
            remainder = gConfig.getTotalRequests() % gConfig.getNumThreads();
            if (thread_id < remainder) {
                requests_per_thread += 1;
            }
        }
        String data = "";
        if ("set".equals(gConfig.getCommand()))
            data = generateRandomData(gConfig.getDataSize());
        int completed = 0;
        while (true) {
            if (timeBased) {
                long elapsedSec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
                if (elapsedSec >= gConfig.getTestDuration())
                    break;
            } else {
                if (completed >= requests_per_thread)
                    break;
            }
            int clientIndex = -1;
            try {
                clientIndex = freeClients.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            BenchmarkClient client = clientPool.get(clientIndex);

            throttleQPS();
            long opStart = System.nanoTime();
            boolean success = true;
            String errorType = null;
            
            if ("set".equals(gConfig.getCommand())) {
                String key;
                if (gConfig.isUseSequential()) {
                    key = "key:" + (completed % gConfig.getSequentialKeyspacelen());
                } else if (gConfig.getRandomKeyspace() > 0) {
                    key = getRandomKey();
                } else {
                    key = "key:" + thread_id + ":" + completed;
                }
                try {
                    String result = client.set(key, data);
                    success = "OK".equalsIgnoreCase(result);
                } catch (Exception e) {
                    success = false;
                    errorType = e.getMessage() != null ? e.getMessage().toUpperCase() : "";
                }
            } else if ("get".equals(gConfig.getCommand())) {
                String key;
                if (gConfig.getRandomKeyspace() > 0) {
                    key = getRandomKey();
                } else {
                    key = "key:" + thread_id + ":" + completed;
                }
                try {
                    String val = client.get(key);
                    success = true;
                } catch (Exception e) {
                    success = false;
                    errorType = e.getMessage() != null ? e.getMessage().toUpperCase() : "";
                }
            } else if ("custom".equals(gConfig.getCommand())) {
                try {
                    if (gConfig.isCluster()) {
                        success = CustomCommandCluster.execute(((ClusterBenchmarkClient)client).getClusterClient());
                    } else {
                        success = CustomCommandStandalone.execute(((StandaloneBenchmarkClient)client).getClient());
                    }
                } catch (Exception e) {
                    success = false;
                    errorType = e.getMessage() != null ? e.getMessage().toUpperCase() : "";
                }
            } else {
                if (gConfig.getCsvIntervalSec() == 0) {
                    System.err.println("[Thread " + thread_id + "] Unknown command: " + gConfig.getCommand());
                }
                success = false;
            }
            long opEnd = System.nanoTime();
            long latencyUs = TimeUnit.NANOSECONDS.toMicros(opEnd - opStart);
            
            if (!success) {
                if (gConfig.getCsvIntervalSec() > 0) {
                    gIntervalErrors.incrementAndGet();
                    if (errorType != null) {
                        if (errorType.contains("MOVED")) {
                            gIntervalMoved.incrementAndGet();
                        } else if (errorType.contains("CLUSTERDOWN")) {
                            gIntervalClusterdown.incrementAndGet();
                        }
                    }
                } else {
                    System.err.println("[Thread " + thread_id + "] Command failed.");
                }
            }

            stats.latencies.add(latencyUs);
            gLatencySumUs.addAndGet(latencyUs);
            gLatencyCount.incrementAndGet();
            gRequestsFinished.incrementAndGet();
            
            // Track CSV interval metrics
            if (gConfig.getCsvIntervalSec() > 0) {
                gIntervalLatencies.add(latencyUs);
                gIntervalRequests.incrementAndGet();
                checkCsvInterval();
            }

            freeClients.add(clientIndex);
            completed++;
        }
    }

    /**
     * Monitors and reports throughput statistics in real-time.
     * @param startTimeNanos The start time of the benchmark in nanoseconds
     */
    static void throughputThreadFunc(long startTimeNanos) {
        // In CSV mode, don't print progress
        if (gConfig.getCsvIntervalSec() > 0) {
            return;
        }
        
        int previous_count = 0;
        long previous_lat_sum = 0;
        int previous_lat_count = 0;
        long previous_time = System.nanoTime();

        while (gTestRunning.get()) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            long now = System.nanoTime();
            double interval_sec = (now - previous_time) / 1e9;
            double overall_sec = (now - startTimeNanos) / 1e9;

            int total_count = gRequestsFinished.get();
            long total_lat_sum = gLatencySumUs.get();
            int total_lat_count = gLatencyCount.get();

            int interval_count = total_count - previous_count;
            long interval_lat_sum = total_lat_sum - previous_lat_sum;
            int interval_lat_count = total_lat_count - previous_lat_count;

            double current_rps = (interval_sec > 0) ? (interval_count / interval_sec) : 0.0;
            double overall_rps = (overall_sec > 0) ? (total_count / overall_sec) : 0.0;
            double interval_avg_latency_us = (interval_lat_count > 0) ? (interval_lat_sum / (double) interval_lat_count) : 0.0;

            System.out.print("[+] Throughput (1s interval): " + Math.round(current_rps) + " req/s, " +
                    "overall=" + Math.round(overall_rps) + " req/s, " +
                    "interval_avg_latency=" + Math.round(interval_avg_latency_us) + " us\r");

            previous_count = total_count;
            previous_lat_sum = total_lat_sum;
            previous_lat_count = total_lat_count;
            previous_time = now;
        }
        System.out.println();
    }

    /**
     * Calculates percentile values from sorted latency measurements.
     * @param sorted Sorted list of latency values
     * @param p Percentile to calculate (0-100)
     * @return The calculated percentile value
     */
    static long calculatePercentile(List<Long> sorted, double p) {
        if (p < 0.0) p = 0.0;
        if (p > 100.0) p = 100.0;
        int idx = (int) Math.floor((p / 100.0) * (sorted.size() - 1));
        return sorted.get(idx);
    }
    
    /**
     * Prints a detailed latency report including min, max, average, and percentiles.
     * @param all_latencies List of all latency measurements
     */
    static void printLatencyReport(List<Long> all_latencies) {
        if (all_latencies.isEmpty()) {
            System.out.println("[!] No latencies recorded.");
            return;
        }
        List<Long> sorted = new ArrayList<>(all_latencies);
        Collections.sort(sorted);
        long min_latency = sorted.get(0);
        long max_latency = sorted.get(sorted.size() - 1);
        double avg = sorted.stream().mapToLong(Long::longValue).sum() / (double) sorted.size();
    
        long p50 = calculatePercentile(sorted, 50.0);
        long p95 = calculatePercentile(sorted, 95.0);
        long p99 = calculatePercentile(sorted, 99.0);
    
        System.out.println("\n--- Latency Report (microseconds) ---");
        System.out.println("  Min: " + min_latency + " us");
        System.out.println("  P50: " + p50 + " us");
        System.out.println("  P95: " + p95 + " us");
        System.out.println("  P99: " + p99 + " us");
        System.out.println("  Max: " + max_latency + " us");
        System.out.println("  Avg: " + avg + " us");
    }

    /**
     * Main entry point for the benchmark application.
     * Handles configuration, initialization, execution, and result reporting.
     */
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new Random().setSeed(System.currentTimeMillis());

        try {
            gConfig.parse(args);
            
            if (gConfig.isShowHelp()) {
                printUsage();
                return;
            }

            // Only print banner if not in CSV mode
            if (gConfig.getCsvIntervalSec() == 0) {
                System.out.println("Valkey-Java Benchmark");
                System.out.println("Host: " + gConfig.getHost());
                System.out.println("Port: " + gConfig.getPort());
                System.out.println("Threads: " + gConfig.getNumThreads());
                System.out.println("Total Requests: " + gConfig.getTotalRequests());
                System.out.println("Data Size: " + gConfig.getDataSize());
                System.out.println("Command: " + gConfig.getCommand());
                System.out.println("Random Keyspace: " + gConfig.getRandomKeyspace());
                System.out.println("Test Duration: " + gConfig.getTestDuration());
                System.out.println("Is Cluster: " + gConfig.isCluster());
                System.out.println("Read from replica: " + gConfig.isReadFromReplica());
                System.out.println("Pool Size: " + gConfig.getPoolSize());
                System.out.println("QPS: " + gConfig.getQps());
                System.out.println("Start QPS: " + gConfig.getStartQps());
                System.out.println("End QPS: " + gConfig.getEndQps());
                System.out.println("QPS Change Interval: " + gConfig.getQpsChangeInterval());
                System.out.println("QPS Change: " + gConfig.getQpsChange());
                System.out.println("QPS Ramp Mode: " + gConfig.getQpsRampMode());
                System.out.println("Use TLS: " + gConfig.isUseTls());
                System.out.println();
            } else {
                // In CSV mode, print header to stdout
                printCsvHeader();
            }

            long startTime = System.nanoTime();

            for (int i = 0; i < gConfig.getPoolSize(); i++) {
                List<NodeAddress> nodeList = Collections.singletonList(
                    NodeAddress.builder()
                        .host(gConfig.getHost())
                        .port(gConfig.getPort())
                        .build()
                );
                
                if (gConfig.isCluster()) {
                    BenchmarkClient client = ValkeyBenchmarkClients.createClusterClient(nodeList, gConfig);
                    clientPool.add(client);
                } else {
                    BenchmarkClient client = ValkeyBenchmarkClients.createStandaloneClient(nodeList, gConfig);
                    clientPool.add(client);
                }

                freeClients.add(i);
            }

            Thread monitor = new Thread(() -> throughputThreadFunc(startTime));
            monitor.start();

            List<Thread> workers = new ArrayList<>();
            List<ThreadStats> threadStats = new ArrayList<>();
            for (int i = 0; i < gConfig.getNumThreads(); i++) {
                ThreadStats stats = new ThreadStats();
                threadStats.add(stats);
                final int tid = i;
                Thread worker = new Thread(() -> {
                    try {
                        workerThreadFunc(tid, stats);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                workers.add(worker);
                worker.start();
            }

            for (Thread t : workers) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            gTestRunning.set(false);
            try {
                monitor.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            
            // Emit final CSV line if in CSV mode and there's any data or errors
            if (gConfig.getCsvIntervalSec() > 0 && 
                (!gIntervalLatencies.isEmpty() || gIntervalErrors.get() > 0 ||
                 gIntervalMoved.get() > 0 || gIntervalClusterdown.get() > 0)) {
                emitCsvLine();
            }

            // Only print final stats if not in CSV mode
            if (gConfig.getCsvIntervalSec() == 0) {
                List<Long> all_latencies = threadStats.stream()
                        .flatMap(ts -> ts.latencies.stream())
                        .collect(Collectors.toList());

                double total_sec = (System.nanoTime() - startTime) / 1e9;
                int finished = gRequestsFinished.get();
                double req_per_sec = (total_sec > 0) ? finished / total_sec : 0.0;
                System.out.println("\n[+] Total test time: " + total_sec + " seconds");
                System.out.println("[+] Total requests completed: " + finished);
                System.out.println("[+] Overall throughput: " + req_per_sec + " req/s");

                printLatencyReport(all_latencies);
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    /**
     * Implementation of custom commands for standalone mode.
     */
    static class CustomCommandStandalone {
        static boolean execute(GlideClient client) {
            boolean success = true;
            int totalOperationsPerBatch = 500;
            int keySize = 16;
            int hashKeySize = 10;
            int fieldKeySize = 8;
    
            List<String> hashKeys = new ArrayList<>(totalOperationsPerBatch);
            List<Set<String>> fieldSets = new ArrayList<>(totalOperationsPerBatch);
            try {
                for (int i = 0; i < totalOperationsPerBatch; i++) {
                    hashKeys.add(generateKey("h", hashKeySize, i));
                    fieldSets.add(Set.of(generateKey("f", fieldKeySize, i)));
                }
    
                Map<String, Set<String>> hashFieldsMap = new HashMap<>();
                for (int i = 0; i < totalOperationsPerBatch; i++) {
                    hashFieldsMap.put(hashKeys.get(i), fieldSets.get(i));
                }
    
                Map<String, CompletableFuture<String[]>> futures = new HashMap<>(hashKeys.size());
            
                for (var entry : hashFieldsMap.entrySet()) {
                    try {
                        String key = entry.getKey();
                        List<String> fields = new ArrayList<>(entry.getValue());
                        CompletableFuture<String[]> getHashFuture = client.hmget(key, fields.toArray(new String[0]));
                        futures.put(key, getHashFuture);
                    } catch (Exception e) { 
                        return false;
                    }
                }
                      
                CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).get();
            
            } catch (Exception e) {
                log(ERROR, "glide", "Performance test failed: " + e.getMessage());
                return false;
            }
            return true;
        }
    }

    /**
     * Implementation of custom commands for cluster mode.
     */
    static class CustomCommandCluster {
        static boolean execute(GlideClusterClient client) {
            boolean success = true;
            int totalOperationsPerBatch = 500;
            int keySize = 16;
            int hashKeySize = 10;
            int fieldKeySize = 8;
    
            List<String> hashKeys = new ArrayList<>(totalOperationsPerBatch);
            List<Set<String>> fieldSets = new ArrayList<>(totalOperationsPerBatch);
            try {
                for (int i = 0; i < totalOperationsPerBatch; i++) {
                    hashKeys.add(generateKey("h", hashKeySize, i));
                    fieldSets.add(Set.of(generateKey("f", fieldKeySize, i)));
                }
    
                Map<String, Set<String>> hashFieldsMap = new HashMap<>();
                for (int i = 0; i < totalOperationsPerBatch; i++) {
                    hashFieldsMap.put(hashKeys.get(i), fieldSets.get(i));
                }
    
                Map<String, CompletableFuture<String[]>> futures = new HashMap<>(hashKeys.size());
            
                for (var entry : hashFieldsMap.entrySet()) {
                    try {
                        String key = entry.getKey();
                        List<String> fields = new ArrayList<>(entry.getValue());
                        CompletableFuture<String[]> getHashFuture = client.hmget(key, fields.toArray(new String[0]));
                        futures.put(key, getHashFuture);
                    } catch (Exception e) { 
                        return false;
                    }
                }
                      
                CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).get();
            
            } catch (Exception e) {
                log(ERROR, "glide", "Performance test failed: " + e.getMessage());
                return false;
            }
            return true;
        }
    }

    /**
     * Generates a key with a prefix and index, truncated to specified size.
     * @param prefix Prefix for the key
     * @param size Maximum size of the key
     * @param index Index to append to the key
     * @return Generated key string
     */
    private static String generateKey(String prefix, int size, int index) {
        String key = prefix + ":" + index;
        return key.length() > size ? key.substring(0, size) : key;
    }
}

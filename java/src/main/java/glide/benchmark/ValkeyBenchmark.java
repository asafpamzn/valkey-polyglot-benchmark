/**
 * ValkeyBenchmark - Performance Testing Utility for Valkey/Redis Operations
 * 
 * This benchmark tool provides comprehensive performance testing capabilities for
 * Valkey/Redis operations using the Valkey GLIDE client. It supports various
 * testing scenarios and configurations including:
 * 
 */
package glide.benchmark;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.stream.*;
import static glide.api.logging.Logger.Level.ERROR;
import static glide.api.logging.Logger.log;

import glide.api.GlideClient;
import glide.api.GlideClusterClient;
import glide.api.models.configuration.NodeAddress;
import glide.benchmark.BenchmarkClients.BenchmarkClient;
import glide.benchmark.BenchmarkClients.ClusterBenchmarkClient;
import glide.benchmark.BenchmarkClients.StandaloneBenchmarkClient;


public class ValkeyBenchmark {

 
    // Global Client Pool
    static List<BenchmarkClient> clientPool = new ArrayList<>();
    static BlockingQueue<Integer> freeClients = new LinkedBlockingQueue<>();

    // Global Configuration
    static class BenchmarkConfig {
        String host = "127.0.0.1";
        int port = 6379;
        int num_threads = 1;
        int total_requests = 100000;
        int data_size = 3;
        String command = "set";
        boolean show_help = false;
        int random_keyspace = 0;
        boolean use_sequential = false;
        int sequential_keyspacelen = 0;
        int pool_size = 1;
        int qps = 0;
        int start_qps = 0;
        int end_qps = 0;
        int qps_change_interval = 0;
        int qps_change = 0;
        int test_duration = 0;
        boolean use_tls = false;
        boolean is_cluster = false; // additional flag for cluster
        boolean read_from_replica = false; // new flag for reading from replicas
    }

    static BenchmarkConfig gConfig = new BenchmarkConfig();

    // Global Counters / Statistics
    static AtomicInteger gRequestsFinished = new AtomicInteger(0);
    static AtomicBoolean gTestRunning = new AtomicBoolean(true);
    static AtomicLong gLatencySumUs = new AtomicLong(0);
    static AtomicInteger gLatencyCount = new AtomicInteger(0);

    static class ThreadStats {
        List<Long> latencies = new ArrayList<>();
    }

    // Usage and Parsing
    static void printUsage() {
        System.out.println("Valkey-GLIDE-Java Benchmark\n" +
                "Usage: java ValkeyBenchmark [OPTIONS]\n\n" +
                "Options:\n" +
                "  -h <hostname>      Server hostname (default 127.0.0.1)\n" +
                "  -p <port>          Server port (default 6379)\n" +
                "  -c <clients>       Number of parallel connections (default 50)\n" +
                "  -n <requests>      Total number of requests (default 100000)\n" +
                "  -d <size>          Data size of value in bytes for SET (default 3)\n" +
                "  -t <command>       Command to benchmark (e.g. get, set, custom, etc.)\n" +
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
                "                    Requires --end-qps, --qps-change-interval, and --qps-change.\n" +
                "                    Mutually exclusive with --qps.\n" +
                "  --end-qps <val>    Ending QPS limit, must be > 0.\n" +
                "                    Requires --start-qps, --qps-change-interval, and --qps-change.\n" +
                "  --qps-change-interval <seconds>\n" +
                "                    Time interval (in seconds) to adjust QPS by --qps-change.\n" +
                "                    Requires --start-qps, --end-qps, and --qps-change.\n" +
                "  --qps-change <val> QPS adjustment applied every interval.\n" +
                "                    Must be non-zero and have the same sign as (end-qps - start-qps).\n" +
                "                    Requires --start-qps, --end-qps, and --qps-change-interval.\n" +
                "  --tls              Use TLS for connection\n" +
                "  --cluster          Use cluster client\n" +
                "  --read-from-replica  Read from replica nodes\n" +
                "  --help             Show this help message and exit\n");
    }

    static void parseOptions(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if ("--help".equals(arg)) {
                gConfig.show_help = true;
                break;
            } else if ("-h".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.host = args[++i];
                } else {
                    System.err.println("Missing argument for -h");
                    System.exit(1);
                }
            } else if ("-p".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.port = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for -p");
                    System.exit(1);
                }
            } else if ("-c".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.pool_size = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for -c (pool size)");
                    System.exit(1);
                }
            } else if ("--threads".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.num_threads = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for --threads");
                    System.exit(1);
                }
            } else if ("--test-duration".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.test_duration = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for --test-duration");
                    System.exit(1);
                }
            } else if ("-n".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.total_requests = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for -n");
                    System.exit(1);
                }
            } else if ("-d".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.data_size = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for -d");
                    System.exit(1);
                }
            } else if ("-t".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.command = args[++i];
                } else {
                    System.err.println("Missing argument for -t");
                    System.exit(1);
                }
            } else if ("-r".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.random_keyspace = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for -r");
                    System.exit(1);
                }
            } else if ("--sequential".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.use_sequential = true;
                    gConfig.sequential_keyspacelen = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for --sequential");
                    System.exit(1);
                }
            } else if ("--qps".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.qps = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for --qps");
                    System.exit(1);
                }
            } else if ("--start-qps".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.start_qps = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for --start-qps");
                    System.exit(1);
                }
            } else if ("--end-qps".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.end_qps = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for --end-qps");
                    System.exit(1);
                }
            } else if ("--qps-change-interval".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.qps_change_interval = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for --qps-change-interval");
                    System.exit(1);
                }
            } else if ("--qps-change".equals(arg)) {
                if (i + 1 < args.length) {
                    gConfig.qps_change = Integer.parseInt(args[++i]);
                } else {
                    System.err.println("Missing argument for --qps-change");
                    System.exit(1);
                }
            } else if ("--tls".equals(arg)) {
                gConfig.use_tls = true;
            } else if ("--cluster".equals(arg)) {
                gConfig.is_cluster = true;
            } else if ("--read-from-replica".equals(arg)) {
                gConfig.read_from_replica = true;
            } else {
                System.err.println("Unknown option: " + arg);
                System.exit(1);
            }
        }

        if (gConfig.use_sequential && gConfig.total_requests != 100000) {
            System.err.println("Error: --sequential is mutually exclusive with -n.");
            System.exit(1);
        }

        if (gConfig.use_sequential) {
            gConfig.total_requests = gConfig.sequential_keyspacelen;
        }

        if (gConfig.test_duration > 0) {
            if (gConfig.test_duration <= 0) {
                System.err.println("Error: --test-duration must be a positive integer.");
                System.exit(1);
            }
            if (gConfig.total_requests != 100000) {
                System.err.println("Error: --test-duration is mutually exclusive with -n.");
                System.exit(1);
            }
            if (gConfig.use_sequential) {
                System.err.println("Error: --test-duration is mutually exclusive with --sequential.");
                System.exit(1);
            }
        }

        boolean hasSimpleQps = (gConfig.qps > 0);
        boolean hasDynamicQps = (gConfig.start_qps > 0 || gConfig.end_qps > 0 ||
                gConfig.qps_change_interval > 0 || gConfig.qps_change != 0);

        if (hasSimpleQps && hasDynamicQps) {
            System.err.println("Error: --qps is mutually exclusive with --start-qps/--end-qps/--qps-change-interval/--qps-change.");
            System.exit(1);
        }

        if (hasSimpleQps && gConfig.qps <= 0) {
            System.err.println("Error: --qps must be a positive integer.");
            System.exit(1);
        }

        if (hasDynamicQps) {
            if (gConfig.start_qps <= 0 || gConfig.end_qps <= 0 ||
                    gConfig.qps_change_interval <= 0 || gConfig.qps_change == 0) {
                System.err.println("Error: --start-qps, --end-qps, --qps-change-interval, and --qps-change must be set and valid.");
                System.exit(1);
            }
            if (gConfig.start_qps == gConfig.end_qps) {
                System.err.println("Error: --start-qps and --end-qps must be different.");
                System.exit(1);
            }
            int diff = gConfig.end_qps - gConfig.start_qps;
            if ((diff > 0 && gConfig.qps_change <= 0) ||
                (diff < 0 && gConfig.qps_change >= 0)) {
                System.err.println("Error: --qps-change sign must match (end-qps - start-qps).");
                System.exit(1);
            }
        }
    }

    // Throttle QPS
    static final Object throttleLock = new Object();
    static int opsThisSecond = 0;
    static long secondStart = System.nanoTime();
    // Variables for dynamic QPS
    static long lastQpsUpdate = System.nanoTime();
    static int currentQps = 0;
    static boolean throttleInitialized = false;

    static void throttleQPS() {
        synchronized (throttleLock) {
            long now = System.nanoTime();
            if (!throttleInitialized) {
                currentQps = (gConfig.qps > 0) ? gConfig.qps : gConfig.start_qps;
                throttleInitialized = true;
            }
            // If dynamic QPS is enabled
            boolean hasDynamicQps = (gConfig.start_qps > 0 && gConfig.end_qps > 0 &&
                    gConfig.qps_change_interval > 0 && gConfig.qps_change != 0);
            if (hasDynamicQps) {
                long elapsedSec = TimeUnit.NANOSECONDS.toSeconds(now - lastQpsUpdate);
                if (elapsedSec >= gConfig.qps_change_interval) {
                    int diff = gConfig.end_qps - currentQps;
                    if ((diff > 0 && gConfig.qps_change > 0) ||
                        (diff < 0 && gConfig.qps_change < 0)) {
                        currentQps += gConfig.qps_change;
                        if ((gConfig.qps_change > 0 && currentQps > gConfig.end_qps) ||
                            (gConfig.qps_change < 0 && currentQps < gConfig.end_qps)) {
                            currentQps = gConfig.end_qps;
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

    // Random Data / Random Keys
    static int state = 1234;
    static String generateRandomData(int size) {
        char[] data = new char[size];
        for (int i = 0; i < size; i++) {
            state = state * 1103515245 + 12345;
            data[i] = (char)('A' + ((state >>> 16) % 26));
        }
        return new String(data);
    }

    static String getRandomKey() {
        int r = new Random().nextInt(gConfig.random_keyspace);
        return "key:" + r;
    }

    // Worker Thread Function
    static void workerThreadFunc(int thread_id, ThreadStats stats) throws ExecutionException, InterruptedException {
        boolean timeBased = (gConfig.test_duration > 0);
        long startTime = System.nanoTime();
        int requests_per_thread = 0;
        int remainder = 0;
        if (!timeBased) {
            requests_per_thread = gConfig.total_requests / gConfig.num_threads;
            remainder = gConfig.total_requests % gConfig.num_threads;
            if (thread_id < remainder) {
                requests_per_thread += 1;
            }
        }
        String data = "";
        if ("set".equals(gConfig.command))
            data = generateRandomData(gConfig.data_size);
        int completed = 0;
        while (true) {
            if (timeBased) {
                long elapsedSec = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
                if (elapsedSec >= gConfig.test_duration)
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
            if ("set".equals(gConfig.command)) {
                String key;
                if (gConfig.use_sequential) {
                    key = "key:" + (completed % gConfig.sequential_keyspacelen);
                } else if (gConfig.random_keyspace > 0) {
                    key = getRandomKey();
                } else {
                    key = "key:" + thread_id + ":" + completed;
                }
                try {
                    String result = client.set(key, data);
                    success = "OK".equalsIgnoreCase(result);
                } catch (Exception e) {
                    success = false;
                }
            } else if ("get".equals(gConfig.command)) {
                String key;
                if (gConfig.random_keyspace > 0) {
                    key = getRandomKey();
                } else {
                    key = "somekey";
                }
                String val = client.get(key);
                success = true;
            } else if ("custom".equals(gConfig.command)) {
                if (gConfig.is_cluster) {
                    success = CustomCommandCluster.execute(((ClusterBenchmarkClient)client).getClusterClient());
                } else {
                    success = CustomCommandStandalone.execute(((StandaloneBenchmarkClient)client).getClient());
                }
            } else {
                System.err.println("[Thread " + thread_id + "] Unknown command: " + gConfig.command);
                success = false;
            }
            long opEnd = System.nanoTime();
            long latencyUs = TimeUnit.NANOSECONDS.toMicros(opEnd - opStart);
            if (!success)
                System.err.println("[Thread " + thread_id + "] Command failed.");

            stats.latencies.add(latencyUs);
            gLatencySumUs.addAndGet(latencyUs);
            gLatencyCount.incrementAndGet();
            gRequestsFinished.incrementAndGet();

            freeClients.add(clientIndex);
            completed++;
        }
    }

    /////////////////////////////////////////////////////////////////////////////
    // Throughput + Partial Latency Printing Thread
    /////////////////////////////////////////////////////////////////////////////
    static void throughputThreadFunc(long startTimeNanos) {
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

            System.out.print("[+] Throughput (1s interval): " +  Math.round(current_rps) + " req/s, " +
                    "overall=" +  Math.round(overall_rps) + " req/s, " +
                    "interval_avg_latency=" +  Math.round(interval_avg_latency_us) + " us\r");

            previous_count = total_count;
            previous_lat_sum = total_lat_sum;
            previous_lat_count = total_lat_count;
            previous_time = now;
        }
        System.out.println();
    }

    /////////////////////////////////////////////////////////////////////////////
    // Final Latency Report
    /////////////////////////////////////////////////////////////////////////////
    /// 


    static long calculatePercentile(List<Long> sorted, double p) {
        if (p < 0.0) p = 0.0;
        if (p > 100.0) p = 100.0;
        int idx = (int) Math.floor((p / 100.0) * (sorted.size() - 1));
        return sorted.get(idx);
    }
    
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

    /////////////////////////////////////////////////////////////////////////////
    // Main
    /////////////////////////////////////////////////////////////////////////////
    public static void main(String[] args) throws ExecutionException, InterruptedException{
        // Seed random
        new Random().setSeed(System.currentTimeMillis());

        // Parse options
        parseOptions(args);
        if (gConfig.show_help) {
            printUsage();
            return;
        }

        System.out.println("Valkey-GLIDE-Java Benchmark");
        System.out.println("Host: " + gConfig.host);
        System.out.println("Port: " + gConfig.port);
        System.out.println("Threads: " + gConfig.num_threads);
        System.out.println("Total Requests: " + gConfig.total_requests);
        System.out.println("Data Size: " + gConfig.data_size);
        System.out.println("Command: " + gConfig.command);
        System.out.println("Random Keyspace: " + gConfig.random_keyspace);
        System.out.println("Test Duration: " + gConfig.test_duration);
        System.out.println("Is Cluster: " + gConfig.is_cluster);
        System.out.println("Read from replica: " + gConfig.read_from_replica);
        System.out.println("Pool Size: " + gConfig.pool_size);
        System.out.println("QPS: " + gConfig.qps);
        System.out.println("Start QPS: " + gConfig.start_qps);
        System.out.println("End QPS: " + gConfig.end_qps);
        System.out.println("QPS Change Interval: " + gConfig.qps_change_interval);
        System.out.println("QPS Change: " + gConfig.qps_change);
        System.out.println("Use TLS: " + gConfig.use_tls);
        System.out.println();

        long startTime = System.nanoTime();

        
        for (int i = 0; i < gConfig.pool_size; i++) {
            List<NodeAddress> nodeList = Collections.singletonList(NodeAddress.builder().host(gConfig.host).port(gConfig.port).build());
            if (gConfig.is_cluster) {
                BenchmarkClient client = BenchmarkClients.createClusterClient(nodeList, gConfig);
                clientPool.add(client);
            } else {
                BenchmarkClient client = BenchmarkClients.createStandaloneClient(nodeList, gConfig);
                clientPool.add(client);
            }

            freeClients.add(i);
        }

        

        // Launch throughput thread
        Thread monitor = new Thread(() -> throughputThreadFunc(startTime));
        monitor.start();

        // Launch worker threads
        List<Thread> workers = new ArrayList<>();
        List<ThreadStats> threadStats = new ArrayList<>();
        for (int i = 0; i < gConfig.num_threads; i++) {
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

        // Wait for worker threads to finish
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

        // Merge latencies
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

    /////////////////////////////////////////////////////////////////////////////
    // Placeholder classes for glide.Client, glide.Config, and CustomCommand.
    // In a real implementation these would be replaced by actual library classes.
    /////////////////////////////////////////////////////////////////////////////
    static class Config {
        String host;
        int port;
        Config(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }

    private static String generateKey(String prefix, int size, int index) {
        String key = prefix + ":" + index;
        return key.length() > size ? key.substring(0, size) : key;
    }

    static class CustomCommandStandalone {
        static boolean execute(GlideClient client) {
            // Simulate executing a custom command
            boolean success = true;
            int totalOperationsPerBatch = 500;
            int keySize = 16;
            int hashKeySize = 10;
            int fieldKeySize = 8;
    
            // Generate keys and values
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
    
                   // Execute HMGET operations
                   Map<String, CompletableFuture<String[]>> futures = new HashMap<>(hashKeys.size());
            
          ///  int clientIndex = 0;
            for (var entry : hashFieldsMap.entrySet()) {
                try {
                    String key = entry.getKey();
                    List<String> fields = new ArrayList<>(entry.getValue());
                    
               //     long operationStartTime = System.nanoTime();
               CompletableFuture<String[]> getHashFuture = client.hmget(key, fields.toArray(new String[0]));
    
                    futures.put(key, getHashFuture);
                } catch (Exception e) { 
                    return false;
                }
            }
                    // Wait for all operations in the batch to complete
                  
            CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).get();
        
    } catch (Exception e) {
        log(ERROR, "glide", "Performance test failed: " + e.getMessage());
        return false;
    }

    
            
            return true;
        }
    }

    static class CustomCommandCluster {
        static boolean execute(GlideClusterClient client) {
            // Simulate executing a custom command
            boolean success = true;
            int totalOperationsPerBatch = 500;
            int keySize = 16;
            int hashKeySize = 10;
            int fieldKeySize = 8;
    
            // Generate keys and values
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
    
                   // Execute HMGET operations
                   Map<String, CompletableFuture<String[]>> futures = new HashMap<>(hashKeys.size());
            
          ///  int clientIndex = 0;
            for (var entry : hashFieldsMap.entrySet()) {
                try {
                    String key = entry.getKey();
                    List<String> fields = new ArrayList<>(entry.getValue());
                    
               //     long operationStartTime = System.nanoTime();
               CompletableFuture<String[]> getHashFuture = client.hmget(key, fields.toArray(new String[0]));
    
                    futures.put(key, getHashFuture);
                } catch (Exception e) { 
                    return false;
                }
            }
                    // Wait for all operations in the batch to complete
                  
            CompletableFuture.allOf(futures.values().toArray(new CompletableFuture[0])).get();
        
    } catch (Exception e) {
        log(ERROR, "glide", "Performance test failed: " + e.getMessage());
        return false;
    }

    
            
            return true;
        }
    }
}

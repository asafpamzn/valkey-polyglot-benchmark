#include "glide/client.h"
#include "glide/config.h"

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <vector>
#include <ctime>

#include "custom_command.h"


///////////////////////////////////////////////////////////////////////////////
// Global Client Pool
///////////////////////////////////////////////////////////////////////////////
#include <queue>
#include <condition_variable>

static std::mutex gClientPoolMutex;
static std::condition_variable gClientPoolCV;
static std::vector<std::unique_ptr<glide::Client>> gClientPool;
static std::queue<int> gFreeClients;

///////////////////////////////////////////////////////////////////////////////
// Global Configuration
///////////////////////////////////////////////////////////////////////////////
struct BenchmarkConfig
{
    std::string host = "127.0.0.1";
    int port = 6379;
    int num_threads = 1;
    int total_requests = 100000;
    int data_size = 3;
    std::string command = "set";
    bool show_help = false;
    int random_keyspace = 0;

    bool use_sequential = false;
    int sequential_keyspacelen = 0;

    int pool_size = 1;

    int qps = 0;                 // --qps
    int start_qps = 0;           // --start-qps
    int end_qps = 0;             // --end-qps
    int qps_change_interval = 0; // --qps-change-interval
    int qps_change = 0;          // --qps-change
    std::string qps_ramp_mode = "linear"; // --qps-ramp-mode: "linear" or "exponential"
    double qps_ramp_factor = 0;  // --qps-ramp-factor: explicit multiplier for exponential mode

    int test_duration = 0;

    bool use_tls = false; // add this
};

static BenchmarkConfig gConfig;

///////////////////////////////////////////////////////////////////////////////
// Global Counters / Statistics
///////////////////////////////////////////////////////////////////////////////
static std::atomic<int> gRequestsFinished{0}; // how many requests have finished
static std::atomic<bool> gTestRunning{true};  // signals if the test is still running

static std::atomic<long long> gLatencySumUs{0}; // sum of latencies (us) for partial stats
static std::atomic<int> gLatencyCount{0};       // number of requests measured for partial stats

// Full latencies stored in each thread's vector for final distribution analysis
struct ThreadStats
{
    std::vector<long long> latencies;
};

///////////////////////////////////////////////////////////////////////////////
// Usage and Parsing
///////////////////////////////////////////////////////////////////////////////
void printUsage()
{
    std::cout << "Valkey-GLIDE-CPP Benchmark\n"
              << "Usage: valkey-benchmark [OPTIONS]\n\n"
              << "Options:\n"
              << "  -h <hostname>      Server hostname (default 127.0.0.1)\n"
              << "  -p <port>          Server port (default 6379)\n"
              << "  -c <clients>       Number of parallel connections (default 50)\n"
              << "  -n <requests>      Total number of requests (default 100000)\n"
              << "  -d <size>          Data size of value in bytes for SET (default 3)\n"
              << "  -t <command>       Command to benchmark (e.g. get, set, custom (will execute the comand in custom_command.h), etc.)\n"
              << "  -r <keyspacelen>   Number of random keys to use (default 0: single key)\n"
              << "  --threads <threads>       Number of worker threads (default 1)\n"
              << "  --test-duration <seconds>   Test duration in seconds.\n"
              << "  --sequential <keyspacelen>\n"
              << "                    Use sequential keys from 0 to keyspacelen-1 for SET/GET/INCR,\n"
              << "                    sequential values for SADD, sequential members and scores for ZADD.\n"
              << "                    Using --sequential option will generate <keyspacelen> requests.\n"
              << "                    This flag is mutually exclusive with --test-duration and -n flags.\n"
              << "  --qps <limit>      Limit the maximum number of queries per second.\n"
              << "                    Must be a positive integer.\n"
              << "  --start-qps <val>  Starting QPS limit, must be > 0.\n"
              << "                    Requires --end-qps, --qps-change-interval, and --qps-change (for linear mode).\n"
              << "                    Mutually exclusive with --qps.\n"
              << "  --end-qps <val>    Ending QPS limit, must be > 0.\n"
              << "                    Requires --start-qps, --qps-change-interval, and --qps-change (for linear mode).\n"
              << "  --qps-change-interval <seconds>\n"
              << "                    Time interval (in seconds) to adjust QPS.\n"
              << "                    Requires --start-qps, --end-qps, and --qps-change (for linear mode).\n"
              << "  --qps-change <val> QPS adjustment applied every interval (linear mode only).\n"
              << "                    Must be non-zero and have the same sign as (end-qps - start-qps).\n"
              << "                    Not required for exponential mode.\n"
              << "  --qps-ramp-mode <mode>\n"
              << "                    QPS ramp mode: 'linear' or 'exponential' (default: linear).\n"
              << "                    In exponential mode, QPS grows/decays by a multiplier each interval.\n"
              << "  --qps-ramp-factor <factor>\n"
              << "                    Multiplier for exponential QPS ramp (required for exponential mode).\n"
              << "                    E.g., 2.0 to double QPS each interval.\n"
              << "                    QPS caps at end-qps and stays there for remaining duration.\n\n"

              << "  --help             Show this help message and exit\n"
              << std::endl;
}

void parseOptions(int argc, char **argv)
{
    for (int i = 1; i < argc; i++)
    {
        if (!std::strcmp(argv[i], "--help"))
        {
            gConfig.show_help = true;
            break;
        }
        else if (!std::strcmp(argv[i], "-h"))
        {
            if (i + 1 < argc)
            {
                gConfig.host = argv[++i];
            }
            else
            {
                std::cerr << "Missing argument for -h\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "-p"))
        {
            if (i + 1 < argc)
            {
                gConfig.port = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for -p\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "-c"))
        {
            if (i + 1 < argc)
            {
                gConfig.pool_size = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for -m (pool size)\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--threads"))
        {
            if (i + 1 < argc)
            {
                gConfig.num_threads = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for -c\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--test-duration"))
        {
            if (i + 1 < argc)
            {
                gConfig.test_duration = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for --test-duration\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "-n"))
        {
            if (i + 1 < argc)
            {
                gConfig.total_requests = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for -n\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "-d"))
        {
            if (i + 1 < argc)
            {
                gConfig.data_size = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for -d\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "-t"))
        {
            if (i + 1 < argc)
            {
                gConfig.command = argv[++i];
            }
            else
            {
                std::cerr << "Missing argument for -t\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "-r"))
        {
            if (i + 1 < argc)
            {
                gConfig.random_keyspace = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for -r\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--sequential"))
        {
            if (i + 1 < argc)
            {
                gConfig.use_sequential = true;
                gConfig.sequential_keyspacelen = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for --sequential\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--qps"))
        {
            if (i + 1 < argc)
            {
                gConfig.qps = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for --qps\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--start-qps"))
        {
            if (i + 1 < argc)
            {
                gConfig.start_qps = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for --start-qps\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--end-qps"))
        {
            if (i + 1 < argc)
            {
                gConfig.end_qps = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for --end-qps\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--qps-change-interval"))
        {
            if (i + 1 < argc)
            {
                gConfig.qps_change_interval = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for --qps-change-interval\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--qps-change"))
        {
            if (i + 1 < argc)
            {
                gConfig.qps_change = std::atoi(argv[++i]);
            }
            else
            {
                std::cerr << "Missing argument for --qps-change\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--qps-ramp-mode"))
        {
            if (i + 1 < argc)
            {
                gConfig.qps_ramp_mode = argv[++i];
                if (gConfig.qps_ramp_mode != "linear" && gConfig.qps_ramp_mode != "exponential")
                {
                    std::cerr << "Error: --qps-ramp-mode must be 'linear' or 'exponential'\n";
                    exit(1);
                }
            }
            else
            {
                std::cerr << "Missing argument for --qps-ramp-mode\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--qps-ramp-factor"))
        {
            if (i + 1 < argc)
            {
                gConfig.qps_ramp_factor = std::atof(argv[++i]);
                if (gConfig.qps_ramp_factor <= 0)
                {
                    std::cerr << "Error: --qps-ramp-factor must be a positive number\n";
                    exit(1);
                }
            }
            else
            {
                std::cerr << "Missing argument for --qps-ramp-factor\n";
                exit(1);
            }
        }
        else if (!std::strcmp(argv[i], "--tls"))
        {
            gConfig.use_tls = true;
            continue;
        }        
        else
        {
            std::cerr << "Unknown option: " << argv[i] << "\n";
            exit(1);
        }
    }

    if (gConfig.use_sequential && gConfig.total_requests != 100000)
    {
        std::cerr << "Error: --sequential is mutually exclusive with -n.\n";
        exit(1);
    }

    if (gConfig.use_sequential)
    {
        gConfig.total_requests = gConfig.sequential_keyspacelen;
    }

    if (gConfig.test_duration > 0)
    {
        // Must be positive
        if (gConfig.test_duration <= 0)
        {
            std::cerr << "Error: --test-duration must be a positive integer.\n";
            exit(1);
        }
        // Cannot use with -n or --sequential
        if (gConfig.total_requests != 100000) // or any logic that indicates user set -n
        {
            std::cerr << "Error: --test-duration is mutually exclusive with -n.\n";
            exit(1);
        }
        if (gConfig.use_sequential)
        {
            std::cerr << "Error: --test-duration is mutually exclusive with --sequential.\n";
            exit(1);
        }
    }

    // If user selected --sequential, also check for conflict with test-duration
    if (gConfig.use_sequential && gConfig.test_duration > 0)
    {
        std::cerr << "Error: --sequential is mutually exclusive with --test-duration.\n";
        exit(1);
    }

    // Check mutual exclusivity: --qps vs. --start-qps + --end-qps + ...
    bool hasSimpleQps = (gConfig.qps > 0);
    bool hasDynamicQps = (gConfig.start_qps > 0 || gConfig.end_qps > 0 ||
                          gConfig.qps_change_interval > 0 || gConfig.qps_change != 0);

    if (hasSimpleQps && hasDynamicQps)
    {
        std::cerr << "Error: --qps is mutually exclusive with --start-qps/--end-qps/--qps-change-interval/--qps-change.\n";
        exit(1);
    }

    // Validate QPS
    if (hasSimpleQps && gConfig.qps <= 0)
    {
        std::cerr << "Error: --qps must be a positive integer.\n";
        exit(1);
    }

    // Validate dynamic QPS
    if (hasDynamicQps)
    {
        // Check all required fields
        if (gConfig.start_qps <= 0 || gConfig.end_qps <= 0 ||
            gConfig.qps_change_interval <= 0 || gConfig.qps_change == 0)
        {
            std::cerr << "Error: --start-qps, --end-qps, --qps-change-interval, and --qps-change must be set and valid.\n";
            exit(1);
        }

        if (gConfig.start_qps == gConfig.end_qps)
        {
            std::cerr << "Error: --start-qps and --end-qps must be different.\n";
            exit(1);
        }

        // Check sign of qps_change: must match (start_qps - end_qps)
        int diff = gConfig.end_qps - gConfig.start_qps;
        if ((diff > 0 && gConfig.qps_change <= 0) ||
            (diff < 0 && gConfig.qps_change >= 0))
        {
            std::cerr << "Error: --qps-change sign must match (end-qps - start-qps).\n";
            exit(1);
        }
    }
}

void throttleQPS()
{
    static std::mutex m;
    static int opsThisSecond = 0;
    static auto secondStart = std::chrono::steady_clock::now();

    // We'll store local static variables to keep track of dynamic QPS progression
    static auto lastQpsUpdate = std::chrono::steady_clock::now();
    static int currentQps = 0; // either gConfig.qps or start_qps if dynamic
    static bool initialized = false;
    static double exponentialMultiplier = 1.0;

    // Lock for thread safety
    std::unique_lock<std::mutex> lock(m);

    if (!initialized)
    {
        // Use local variable for effective start_qps instead of modifying global config
        int effectiveStartQps = gConfig.start_qps;
        
        // Determine initial QPS: use start_qps if set, otherwise fall back to qps or end_qps
        if (gConfig.start_qps > 0)
        {
            currentQps = gConfig.start_qps;
        }
        else if (gConfig.qps > 0)
        {
            currentQps = gConfig.qps;
        }
        else if (gConfig.end_qps > 0)
        {
            // For ramp-up modes without start_qps, use end_qps as initial value
            currentQps = gConfig.end_qps;
            effectiveStartQps = gConfig.end_qps;
            std::cerr << "Warning: start-qps not set for ramp mode, using end-qps as initial QPS\n";
        }
        
        // Validate start_qps if ramp mode is configured
        if (gConfig.qps_change_interval > 0 && gConfig.end_qps > 0)
        {
            if (gConfig.start_qps <= 0)
            {
                std::cerr << "Warning: start-qps must be positive for QPS ramping. Using end-qps as fallback.\n";
                effectiveStartQps = gConfig.end_qps;
            }
        }
        
        // For exponential mode, use the provided multiplier
        if (gConfig.qps_ramp_mode == "exponential" &&
            effectiveStartQps > 0 && gConfig.end_qps > 0 &&
            gConfig.qps_change_interval > 0)
        {
            // Exponential mode requires --qps-ramp-factor
            if (gConfig.qps_ramp_factor > 0)
            {
                exponentialMultiplier = gConfig.qps_ramp_factor;
                // Warn if factor < 1 (causes ramp-down instead of ramp-up)
                if (gConfig.qps_ramp_factor < 1)
                {
                    std::cerr << "Warning: qps-ramp-factor < 1 will cause QPS to decrease (ramp-down) each interval\n";
                }
            }
            else
            {
                std::cerr << "Error: exponential mode requires --qps-ramp-factor to be specified\n";
                exit(1);
            }
        }
        
        initialized = true;
    }

    auto now = std::chrono::steady_clock::now();
    
    bool isExponential = (gConfig.qps_ramp_mode == "exponential");

    // 1. If dynamic QPS, check if we need to update QPS
    bool hasDynamicQps = (gConfig.start_qps > 0 && gConfig.end_qps > 0 && gConfig.qps_change_interval > 0);
    
    // For linear mode, also require qps_change
    if (!isExponential)
    {
        hasDynamicQps = hasDynamicQps && (gConfig.qps_change != 0);
    }
    
    if (hasDynamicQps)
    {
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - lastQpsUpdate).count();
        if (elapsed >= gConfig.qps_change_interval)
        {
            if (isExponential)
            {
                // Exponential mode: multiply by the computed multiplier
                int newQps = static_cast<int>(std::round(currentQps * exponentialMultiplier));
                
                // Clamp to end_qps
                if (gConfig.end_qps > gConfig.start_qps)
                {
                    // Increasing QPS
                    if (newQps > gConfig.end_qps)
                    {
                        newQps = gConfig.end_qps;
                    }
                }
                else
                {
                    // Decreasing QPS
                    if (newQps < gConfig.end_qps)
                    {
                        newQps = gConfig.end_qps;
                    }
                }
                currentQps = newQps;
            }
            else
            {
                // Linear mode: add qps_change
                int diff = gConfig.end_qps - currentQps;
                if (((diff > 0) && gConfig.qps_change > 0) ||
                    ((diff < 0) && gConfig.qps_change < 0))
                {
                    // We are still moving towards the end_qps
                    currentQps += gConfig.qps_change;

                    // If we overshot, clamp to end_qps
                    if ((gConfig.qps_change > 0 && currentQps > gConfig.end_qps) ||
                        (gConfig.qps_change < 0 && currentQps < gConfig.end_qps))
                    {
                        currentQps = gConfig.end_qps;
                    }
                }
            }

            lastQpsUpdate = std::chrono::steady_clock::now();
        }
    }

    // 2. Throttle for the current second if QPS > 0
    if (currentQps > 0)
    {
        auto secElapsed = std::chrono::duration_cast<std::chrono::seconds>(now - secondStart).count();

        if (secElapsed >= 1)
        {
            // New second; reset counters
            opsThisSecond = 0;
            secondStart = now;
        }

        if (opsThisSecond >= currentQps)
        {
            // We exceeded the QPS in this second; sleep until next second
            auto nextSecond = secondStart + std::chrono::seconds(1);
            std::this_thread::sleep_until(nextSecond);

            // reset
            opsThisSecond = 0;
            secondStart = std::chrono::steady_clock::now();
        }

        // Count this operation
        opsThisSecond++;
    }
}

///////////////////////////////////////////////////////////////////////////////
// Random Data / Random Keys
///////////////////////////////////////////////////////////////////////////////
std::string generateRandomData(size_t size)
{
    static uint32_t state = 1234;
    std::string data(size, ' ');

    for (size_t i = 0; i < size; ++i)
    {
        state = (state * 1103515245 + 12345);
        // Use 'A'..'Z' as random characters
        data[i] = 'A' + ((state >> 16) % 26);
    }
    return data;
}

// Returns a random key from the range [0, random_keyspace-1]
static std::string getRandomKey()
{
    int r = std::rand() % gConfig.random_keyspace;
    return "key:" + std::to_string(r);
}

///////////////////////////////////////////////////////////////////////////////
// Worker Thread Function
///////////////////////////////////////////////////////////////////////////////
void workerThreadFunc(int thread_id, ThreadStats &stats)
{
    bool timeBased = (gConfig.test_duration > 0);
    std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();

    // If not time-based, compute how many requests this thread handles.
    int requests_per_thread = 0;
    int remainder = 0;
    if (!timeBased)
    {
        requests_per_thread = gConfig.total_requests / gConfig.num_threads;
        remainder = gConfig.total_requests % gConfig.num_threads;
        if (thread_id < remainder)
        {
            requests_per_thread += 1;
        }
    }

    // Pre-generate data if we're doing SET
    std::string data;
    if (gConfig.command == "set")
    {
        data = generateRandomData(gConfig.data_size);
    }

    stats.latencies.reserve(requests_per_thread);

    int completed = 0; // track how many requests this thread did

    while (true)
    {

        if (timeBased)
        {

            auto now = std::chrono::steady_clock::now();
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time).count();
            if (elapsed >= gConfig.test_duration)
            {
                break; // done, time is up
            }
        }
        else
        {
            // not time-based, so we have a limit of requests_per_thread
            if (completed >= requests_per_thread)
            {
                break;
            }
        }
        // -------------------------
        // 1. Acquire a free client
        // -------------------------
        int clientIndex = -1;
        {
            std::unique_lock<std::mutex> lock(gClientPoolMutex);
            // Wait until a client index is available
            gClientPoolCV.wait(lock, []
                               { return !gFreeClients.empty(); });

            clientIndex = gFreeClients.front();
            gFreeClients.pop();
            // Automatically unlock here when lock goes out of scope
        }

        // Access the acquired client
        glide::Client &client = *gClientPool[clientIndex];

        // -----------------------------
        // Throttle QPS if configured
        // -----------------------------
        throttleQPS();
        // Start timing

        bool success = true;
        auto start = std::chrono::high_resolution_clock::now();
        if (gConfig.command == "set")
        {
            std::string key;
            if (gConfig.use_sequential)
            {
                key = "key:" + std::to_string(completed % gConfig.sequential_keyspacelen);
            }
            else if (gConfig.random_keyspace > 0)
            {
                key = getRandomKey();
            }
            else
            {
                key = "key:" + std::to_string(thread_id) + ":" + std::to_string(completed);
            }
            success = client.set(key, data);
        }
        else if (gConfig.command == "get")
        {
            std::string key;
            if (gConfig.random_keyspace > 0)
            {
                key = getRandomKey();
            }
            else
            {
                key = "somekey";
            }
            std::string val = client.get(key);
            success = !val.empty();
        }
        else if (gConfig.command == "custom")
        {   
           
            success = CustomCommand::execute(client);   
            
        }
        else
        {
            std::cerr << "[Thread " << thread_id << "] Unknown command: " << gConfig.command << "\n";
            success = false;
        }

        // End timing
        auto end = std::chrono::high_resolution_clock::now();
        long long latency_us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

        if (!success)
        {
            std::cerr << "[Thread " << thread_id << "] Command failed.\n";
        }

        // Store latency for final distribution
        stats.latencies.push_back(latency_us);

        // Partial stats
        gLatencySumUs.fetch_add(latency_us, std::memory_order_relaxed);
        gLatencyCount.fetch_add(1, std::memory_order_relaxed);

        // Update global request count
        gRequestsFinished.fetch_add(1, std::memory_order_relaxed);

        // -------------------------
        // Return the client to pool
        // -------------------------
        {
            std::unique_lock<std::mutex> lock(gClientPoolMutex);
            gFreeClients.push(clientIndex);
        }
        gClientPoolCV.notify_one();
        completed++;
    }
}

///////////////////////////////////////////////////////////////////////////////
// Throughput + Partial Latency Printing Thread
///////////////////////////////////////////////////////////////////////////////
void throughputThreadFunc(std::chrono::steady_clock::time_point start_time)
{
    using namespace std::chrono_literals;

    // Track how many requests and latencies we had at the previous update
    int previous_count = 0;
    long long previous_lat_sum = 0;
    int previous_lat_count = 0;

    auto previous_time = std::chrono::steady_clock::now();

    while (gTestRunning)
    {
        std::this_thread::sleep_for(1s);

        auto now = std::chrono::steady_clock::now();
        double interval_sec = std::chrono::duration<double>(now - previous_time).count();
        double overall_sec = std::chrono::duration<double>(now - start_time).count();

        // Current total stats
        int total_count = gRequestsFinished.load(std::memory_order_relaxed);
        long long total_lat_sum = gLatencySumUs.load(std::memory_order_relaxed);
        int total_lat_count = gLatencyCount.load(std::memory_order_relaxed);

        // Compute deltas for the last interval
        int interval_count = total_count - previous_count;
        long long interval_lat_sum = total_lat_sum - previous_lat_sum;
        int interval_lat_count = total_lat_count - previous_lat_count;

        // Current (interval) RPS
        double current_rps = (interval_sec > 0.0) ? (interval_count / interval_sec) : 0.0;
        // Overall RPS since start
        double overall_rps = (overall_sec > 0.0) ? (total_count / overall_sec) : 0.0;

        // Interval average latency (us) for the last 1s window
        double interval_avg_latency_us = 0.0;
        if (interval_lat_count > 0)
        {
            interval_avg_latency_us = (double)interval_lat_sum / interval_lat_count;
        }

        // Update the console
        std::cout << "[+] Throughput (1s interval): " << current_rps << " req/s, "
                  << "overall=" << overall_rps << " req/s, "
                  << "interval_avg_latency=" << interval_avg_latency_us << " us\r"
                  << std::flush;

        // Update "previous" tracking
        previous_count = total_count;
        previous_lat_sum = total_lat_sum;
        previous_lat_count = total_lat_count;
        previous_time = now;
    }
    std::cout << "\n";
}

///////////////////////////////////////////////////////////////////////////////
// Final Latency Report
///////////////////////////////////////////////////////////////////////////////
void printLatencyReport(const std::vector<long long> &all_latencies)
{
    if (all_latencies.empty())
    {
        std::cout << "[!] No latencies recorded.\n";
        return;
    }

    std::vector<long long> sorted = all_latencies;
    std::sort(sorted.begin(), sorted.end());

    long long min_latency = sorted.front();
    long long max_latency = sorted.back();
    double avg = std::accumulate(sorted.begin(), sorted.end(), 0LL) / (double)sorted.size();

    auto percentile = [&](double p)
    {
        if (p < 0.0)
            p = 0.0;
        if (p > 100.0)
            p = 100.0;
        size_t idx = (size_t)std::floor((p / 100.0) * (sorted.size() - 1));
        return sorted[idx];
    };

    long long p50 = percentile(50.0);
    long long p95 = percentile(95.0);
    long long p99 = percentile(99.0);

    std::cout << "\n--- Latency Report (microseconds) ---\n"
              << "  Min: " << min_latency << " us\n"
              << "  P50: " << p50 << " us\n"
              << "  P95: " << p95 << " us\n"
              << "  P99: " << p99 << " us\n"
              << "  Max: " << max_latency << " us\n"
              << "  Avg: " << avg << " us\n";
}

///////////////////////////////////////////////////////////////////////////////
// Main
///////////////////////////////////////////////////////////////////////////////
int main(int argc, char **argv)
{

    // Seed random so each run differs
    std::srand((unsigned)std::time(nullptr));

    // Parse command-line options
    parseOptions(argc, argv);
    if (gConfig.show_help)
    {
        printUsage();
        return 0;
    }

    // Print configuration
    std::cout << "Valkey-GLIDE-CPP Benchmark\n"
              << "Host: " << gConfig.host << "\n"
              << "Port: " << gConfig.port << "\n"
              << "Threads: " << gConfig.num_threads << "\n"
              << "Total Requests: " << gConfig.total_requests << "\n"
              << "Data Size: " << gConfig.data_size << "\n"
              << "Command: " << gConfig.command << "\n"
              << "Random Keyspace: " << gConfig.random_keyspace << "\n"
              << "Test Duration: " << gConfig.test_duration << "\n\n";

    auto start_time = std::chrono::steady_clock::now();

    // Build and connect the client pool
    gClientPool.reserve(gConfig.pool_size);
    for (int i = 0; i < gConfig.pool_size; i++)
    {
        glide::Config cfg(gConfig.host, gConfig.port);
        auto clientPtr = std::make_unique<glide::Client>(cfg);
        if (!clientPtr->connect())
        {
            std::cerr << "Connection #" << i << " failed to connect.\n";
            // Handle error as you see fit (exit or continue)
            exit(1);
        }

        gClientPool.push_back(std::move(clientPtr));
        gFreeClients.push(i);
    }

    // Launch a thread to show throughput + partial avg latency
    std::thread th_monitor(throughputThreadFunc, start_time);

    // Launch worker threads
    std::vector<std::thread> workers;
    std::vector<ThreadStats> thread_stats(gConfig.num_threads);

    for (int i = 0; i < gConfig.num_threads; i++)
    {
        workers.emplace_back(workerThreadFunc, i, std::ref(thread_stats[i]));
    }

    // Wait for all worker threads to finish
    for (auto &t : workers)
    {
        t.join();
    }
    // Signal throughput thread to stop
    gTestRunning = false;
    th_monitor.join();

    // Merge latencies
    std::vector<long long> all_latencies;
    all_latencies.reserve(gConfig.total_requests);
    for (auto &ts : thread_stats)
    {
        all_latencies.insert(all_latencies.end(), ts.latencies.begin(), ts.latencies.end());
    }

    // Final throughput
    auto end_time = std::chrono::steady_clock::now();
    double total_sec = std::chrono::duration<double>(end_time - start_time).count();
    int finished = gRequestsFinished.load(std::memory_order_relaxed);
    double req_per_sec = (total_sec > 0) ? (finished / total_sec) : 0.0;

    std::cout << "\n[+] Total test time: " << total_sec << " seconds\n"
              << "[+] Total requests completed: " << finished << "\n"
              << "[+] Overall throughput: " << req_per_sec << " req/s\n";

    // Print final latency statistics
    printLatencyReport(all_latencies);

    return 0;
}

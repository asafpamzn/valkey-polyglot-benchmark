// Package main implements a benchmarking tool for the Valkey database system.
// It provides functionality to test performance of various database operations with
// configurable parameters like QPS, connection pooling, and data sizes.
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-glide/go/api"
)

// Configuration holds all benchmark settings
type Config struct {
	Host              string
	Port              int
	PoolSize          int
	TotalRequests     int64
	DataSize          int
	Command           string
	RandomKeyspace    int64
	NumThreads        int
	TestDuration      int
	UseSequential     bool
	SequentialKeyLen  int64
	QPS               int
	StartQPS          int
	EndQPS            int
	QPSChangeInterval int
	QPSChange         int
	QPSRampMode       string  // "linear" or "exponential"
	QPSRampFactor     float64 // Explicit multiplier for exponential mode (0 = auto-calculate)
	UseTLS            bool
	IsCluster         bool
	ReadFromReplica   bool
}

// BenchmarkStats tracks performance metrics
type BenchmarkStats struct {
	startTime         time.Time  // Test start timestamp
	requestsCompleted int64      // Counter for completed requests
	latencies         []float64  // All request latencies
	errors            int64      // Error counter
	lastPrint         time.Time  // Last progress print timestamp
	lastRequests      int64      // Request count at last print
	currentLatencies  []float64  // Recent request latencies
	mu                sync.Mutex // Protects shared data
}

// LatencyStats holds calculated statistics about request latencies
type LatencyStats struct {
	min float64 // Minimum latency
	max float64 // Maximum latency
	avg float64 // Average latency
	p50 float64 // 50th percentile (median)
	p95 float64 // 95th percentile
	p99 float64 // 99th percentile
}

// QPSController manages rate limiting to maintain target QPS
// Supports both linear and exponential ramp modes
type QPSController struct {
	config                *Config
	currentQPS            int
	lastUpdate            time.Time
	requestsInSecond      int
	secondStart           time.Time
	exponentialMultiplier float64
	mu                    sync.Mutex
}

func generateRandomData(size int) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	result := make([]byte, size)
	for i := 0; i < size; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	return string(result)
}

func getRandomKey(keyspace int64) string {
	return fmt.Sprintf("key:%d", rand.Int63n(keyspace))
}

// NewBenchmarkStats creates a new stats tracker
func NewBenchmarkStats() *BenchmarkStats {
	return &BenchmarkStats{
		startTime: time.Now(),
		lastPrint: time.Now(),
		latencies: make([]float64, 0, 1000000),
	}
}

// AddLatency records a request latency
func (s *BenchmarkStats) AddLatency(latency float64) {
	atomic.AddInt64(&s.requestsCompleted, 1)
	s.mu.Lock()
	s.latencies = append(s.latencies, latency)
	s.currentLatencies = append(s.currentLatencies, latency)
	s.mu.Unlock()
	s.PrintProgress()
}

// AddError increments the error counter
func (s *BenchmarkStats) AddError() {
	atomic.AddInt64(&s.errors, 1)
}

// PrintProgress displays real-time benchmark progress statistics
func (s *BenchmarkStats) PrintProgress() {
	now := time.Now()
	if now.Sub(s.lastPrint) >= time.Second {
		s.mu.Lock()
		defer s.mu.Unlock()

		completed := atomic.LoadInt64(&s.requestsCompleted)
		intervalRequests := completed - s.lastRequests
		currentRPS := float64(intervalRequests)
		overallRPS := float64(completed) / now.Sub(s.startTime).Seconds()

		// Calculate window statistics
		stats := calculateLatencyStats(s.currentLatencies)

		fmt.Printf("\r\x1b[K") // Clear line
		fmt.Printf("Progress: %d requests, Current RPS: %.2f, Overall RPS: %.2f, Errors: %d",
			completed, currentRPS, overallRPS, atomic.LoadInt64(&s.errors))
		if stats != nil {
			fmt.Printf(" | Latencies (ms) - Avg: %.2f, p50: %.2f, p99: %.2f",
				stats.avg, stats.p50, stats.p99)
		}

		s.currentLatencies = s.currentLatencies[:0]
		s.lastPrint = now
		s.lastRequests = completed
	}
}

// PrintFinalStats prints the final benchmark results
// PrintFinalStats outputs the final benchmark results and statistics
func (s *BenchmarkStats) PrintFinalStats() {
	totalTime := time.Since(s.startTime).Seconds()
	finalRPS := float64(s.requestsCompleted) / totalTime

	s.mu.Lock()
	finalStats := calculateLatencyStats(s.latencies)
	s.mu.Unlock()

	fmt.Printf("\n\nFinal Results:\n")
	fmt.Printf("=============\n")
	fmt.Printf("Total time: %.2f seconds\n", totalTime)
	fmt.Printf("Requests completed: %d\n", s.requestsCompleted)
	fmt.Printf("Requests per second: %.2f\n", finalRPS)
	fmt.Printf("Total errors: %d\n", s.errors)

	if finalStats != nil {
		fmt.Printf("\nLatency Statistics (ms):\n")
		fmt.Printf("=====================\n")
		fmt.Printf("Minimum: %.3f\n", finalStats.min)
		fmt.Printf("Average: %.3f\n", finalStats.avg)
		fmt.Printf("Maximum: %.3f\n", finalStats.max)
		fmt.Printf("Median (p50): %.3f\n", finalStats.p50)
		fmt.Printf("95th percentile: %.3f\n", finalStats.p95)
		fmt.Printf("99th percentile: %.3f\n", finalStats.p99)
	}
}

// calculateLatencyStats computes statistics from a slice of latency measurements
func calculateLatencyStats(latencies []float64) *LatencyStats {
	if len(latencies) == 0 {
		return nil
	}

	// Create a copy for sorting
	sorted := make([]float64, len(latencies))
	copy(sorted, latencies)
	sort.Float64s(sorted)

	return &LatencyStats{
		min: sorted[0],
		max: sorted[len(sorted)-1],
		avg: average(latencies),
		p50: sorted[len(sorted)*50/100],
		p95: sorted[len(sorted)*95/100],
		p99: sorted[len(sorted)*99/100],
	}
}

// average calculates the mean of a slice of float64 values
func average(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// Throttle implements rate limiting to maintain target QPS
// Supports both linear and exponential ramp modes
func (qps *QPSController) Throttle() {
	qps.mu.Lock()
	defer qps.mu.Unlock()

	if qps.currentQPS <= 0 {
		return
	}

	now := time.Now()

	isExponential := qps.config.QPSRampMode == "exponential"
	hasDynamicQps := qps.config.StartQPS > 0 && qps.config.EndQPS > 0 && qps.config.QPSChangeInterval > 0

	// For linear mode, also require QPSChange
	if !isExponential {
		hasDynamicQps = hasDynamicQps && qps.config.QPSChange != 0
	}

	// Handle dynamic QPS changes
	if hasDynamicQps {
		elapsedSeconds := int(now.Sub(qps.lastUpdate).Seconds())
		if elapsedSeconds >= qps.config.QPSChangeInterval {
			if isExponential {
				// Exponential mode: multiply by the computed multiplier
				newQPS := int(math.Round(float64(qps.currentQPS) * qps.exponentialMultiplier))

				// Clamp to EndQPS
				if qps.config.EndQPS > qps.config.StartQPS {
					// Increasing QPS
					if newQPS > qps.config.EndQPS {
						newQPS = qps.config.EndQPS
					}
				} else {
					// Decreasing QPS
					if newQPS < qps.config.EndQPS {
						newQPS = qps.config.EndQPS
					}
				}
				qps.currentQPS = newQPS
			} else {
				// Linear mode: add QPSChange
				if qps.config.StartQPS < qps.config.EndQPS {
					// Increasing QPS
					qps.currentQPS += qps.config.QPSChange
					if qps.currentQPS > qps.config.EndQPS {
						qps.currentQPS = qps.config.EndQPS
					}
				} else {
					// Decreasing QPS
					qps.currentQPS -= qps.config.QPSChange
					if qps.currentQPS < qps.config.EndQPS {
						qps.currentQPS = qps.config.EndQPS
					}
				}
			}
			qps.lastUpdate = now
			fmt.Printf("\nUpdated QPS target to: %d\n", qps.currentQPS)
		}
	}

	// Reset counter and update second start if we've moved to a new second
	if now.Sub(qps.secondStart) >= time.Second {
		qps.requestsInSecond = 0
		qps.secondStart = now.Truncate(time.Second)
	}

	// Calculate the target interval between requests
	interval := time.Second / time.Duration(qps.currentQPS)

	// Calculate the expected time for this request
	expectedTime := qps.secondStart.Add(time.Duration(qps.requestsInSecond) * interval)

	// If we're ahead of schedule, sleep until the expected time
	if now.Before(expectedTime) {
		time.Sleep(expectedTime.Sub(now))
	}

	// If we've hit the QPS limit for this second, wait for next second
	if qps.requestsInSecond >= qps.currentQPS {
		nextSecond := qps.secondStart.Add(time.Second)
		if now.Before(nextSecond) {
			time.Sleep(nextSecond.Sub(now))
		}
		qps.requestsInSecond = 0
		qps.secondStart = nextSecond
	}

	qps.requestsInSecond++
}

// Update the client configuration and usage
// ClientConfig holds client connection configuration
type ClientConfig struct {
	Addresses []struct {
		Host string
		Port int
	}
	UseTLS   bool
	ReadFrom string
}

// NewQPSController creates a new QPS controller
// Computes exponential multiplier if exponential mode is enabled
func NewQPSController(config *Config) *QPSController {
	now := time.Now()
	
	// Determine initial QPS: use StartQPS if set, otherwise fall back to QPS or EndQPS
	var currentQPS int
	effectiveStartQPS := config.StartQPS
	if config.StartQPS > 0 {
		currentQPS = config.StartQPS
	} else if config.QPS > 0 {
		currentQPS = config.QPS
	} else if config.EndQPS > 0 {
		// For ramp-up modes without StartQPS, use EndQPS as initial value
		currentQPS = config.EndQPS
		effectiveStartQPS = config.EndQPS
		fmt.Fprintln(os.Stderr, "Warning: start-qps not set for ramp mode, using end-qps as initial QPS")
	}
	
	// Validate StartQPS if ramp mode is configured
	if config.QPSChangeInterval > 0 && config.EndQPS > 0 {
		if config.StartQPS <= 0 {
			fmt.Fprintln(os.Stderr, "Warning: start-qps must be positive for QPS ramping. Using end-qps as fallback.")
			effectiveStartQPS = config.EndQPS
		}
	}

	exponentialMultiplier := 1.0
	// For exponential mode, use the provided multiplier
	if config.QPSRampMode == "exponential" &&
		effectiveStartQPS > 0 && config.EndQPS > 0 &&
		config.QPSChangeInterval > 0 {

		// Exponential mode requires --qps-ramp-factor
		if config.QPSRampFactor > 0 {
			exponentialMultiplier = config.QPSRampFactor
			// Warn if factor < 1 (causes ramp-down instead of ramp-up)
			if config.QPSRampFactor < 1 {
				fmt.Fprintln(os.Stderr, "Warning: qps-ramp-factor < 1 will cause QPS to decrease (ramp-down) each interval")
			}
		} else {
			fmt.Fprintln(os.Stderr, "Error: exponential mode requires --qps-ramp-factor to be specified")
			os.Exit(1)
		}
	}

	return &QPSController{
		config:                config,
		currentQPS:            currentQPS,
		lastUpdate:            now,
		secondStart:           now,
		requestsInSecond:      0,
		exponentialMultiplier: exponentialMultiplier,
	}
}

// RunBenchmark executes the benchmark with the given configuration
func RunBenchmark(ctx context.Context, config *Config) error {
	stats := NewBenchmarkStats()
	qpsController := NewQPSController(config)

	// Print benchmark configuration
	fmt.Println("Valkey Benchmark")
	fmt.Printf("Host: %s\n", config.Host)
	fmt.Printf("Port: %d\n", config.Port)
	fmt.Printf("Threads: %d\n", config.NumThreads)
	fmt.Printf("Total Requests: %d\n", config.TotalRequests)
	fmt.Printf("Data Size: %d\n", config.DataSize)
	fmt.Printf("Command: %s\n", config.Command)
	fmt.Printf("Is Cluster: %v\n", config.IsCluster)
	fmt.Printf("Read from Replica: %v\n", config.ReadFromReplica)
	fmt.Printf("Use TLS: %v\n", config.UseTLS)
	fmt.Println()
	// Create client pool
	clientPool := make([]interface{}, config.PoolSize)
	for i := 0; i < config.PoolSize; i++ {
		if config.IsCluster {
			clusterConfig := api.NewGlideClusterClientConfiguration().
				WithAddress(&api.NodeAddress{Host: config.Host, Port: config.Port}).
				WithRequestTimeout(500) // Default 500ms timeout

			if config.UseTLS {
				clusterConfig.WithUseTLS(true)
			}
			if config.ReadFromReplica {
				clusterConfig.WithReadFrom(api.PreferReplica)
			}

			client, err := api.NewGlideClusterClient(clusterConfig)
			if err != nil {
				return fmt.Errorf("failed to create cluster client: %v", err)
			}
			clientPool[i] = client
		} else {
			clientConfig := api.NewGlideClientConfiguration().
				WithAddress(&api.NodeAddress{Host: config.Host, Port: config.Port}).
				WithRequestTimeout(500) // Default 500ms timeout

			if config.UseTLS {
				clientConfig.WithUseTLS(true)
			}
			if config.ReadFromReplica {
				clientConfig.WithReadFrom(api.PreferReplica)
			}

			client, err := api.NewGlideClient(clientConfig)
			if err != nil {
				return fmt.Errorf("failed to create client: %v", err)
			}
			clientPool[i] = client
		}
	}

	// Update worker goroutine
	var wg sync.WaitGroup
	for i := 0; i < config.NumThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			data := ""
			if config.Command == "set" {
				data = generateRandomData(config.DataSize)
			}

			for {
				select {
				case <-ctx.Done():
					return
				default:
					if config.TestDuration == 0 &&
						atomic.LoadInt64(&stats.requestsCompleted) >= config.TotalRequests {
						return
					}

					clientIndex := int(atomic.LoadInt64(&stats.requestsCompleted)) % config.PoolSize
					client := clientPool[clientIndex]

					qpsController.Throttle()

					start := time.Now()
					var err error

					switch config.Command {
					case "set":
						key := fmt.Sprintf("key:%d:%d", threadID, stats.requestsCompleted)
						if config.UseSequential {
							key = fmt.Sprintf("key:%d",
								atomic.LoadInt64(&stats.requestsCompleted)%config.SequentialKeyLen)
						} else if config.RandomKeyspace > 0 {
							key = getRandomKey(config.RandomKeyspace)
						}
						if c, ok := client.(*api.GlideClient); ok {
							var result string
							result, err = c.Set(key, data)
							_ = result // Ignore the result value
						} else if c, ok := client.(*api.GlideClusterClient); ok {
							var result string
							result, err = c.Set(key, data)
							_ = result // Ignore the result value
						}

					case "get":
						key := "somekey"
						if config.RandomKeyspace > 0 {
							key = getRandomKey(config.RandomKeyspace)
						}
						if c, ok := client.(*api.GlideClient); ok {
							_, err = c.Get(key)
						} else if c, ok := client.(*api.GlideClusterClient); ok {
							_, err = c.Get(key)
						}

					case "custom":
						if config.IsCluster {
							clusterCmd := &CustomCommandCluster{}
							err = clusterCmd.execute(client.(*api.GlideClusterClient))

						} else {
							standaloneCmd := &CustomCommandStandalone{}
							err = standaloneCmd.execute(client.(*api.GlideClient))
						}
					}

					if err != nil {
						stats.AddError()
						fmt.Printf("Error in thread %d: %v\n", threadID, err)
					} else {
						stats.AddLatency(float64(time.Since(start).Microseconds()) / 1000.0)
					}
				}
			}
		}(i)
	}

	// Wait for completion or duration
	if config.TestDuration > 0 {
		time.Sleep(time.Duration(config.TestDuration) * time.Second)
	}
	wg.Wait()

	stats.PrintFinalStats()

	// Close all clients
	for _, client := range clientPool {
		if c, ok := client.(*api.GlideClient); ok {
			c.Close()
		} else if c, ok := client.(*api.GlideClusterClient); ok {
			c.Close()
		}
	}

	return nil
}

// Global configuration
var config Config

// CustomCommandStandalone implements custom commands for standalone mode
type CustomCommandStandalone struct{}

func (c *CustomCommandStandalone) execute(client *api.GlideClient) error {
	var err error
	_, err = client.Set("custom key", "custom value")

	return err
}

// CustomCommandCluster implements custom commands for cluster mode
type CustomCommandCluster struct{}

func (c *CustomCommandCluster) execute(client *api.GlideClusterClient) error {
	var err error
	_, err = client.Set("custom key", "custom value")

	return err
}

// main is the entry point for the benchmark tool
func main() {
	// Parse command line flags
	flag.StringVar(&config.Host, "H", "127.0.0.1", "Server hostname")
	flag.IntVar(&config.Port, "p", 6379, "Server port")
	flag.IntVar(&config.PoolSize, "c", 50, "Number of parallel connections")
	flag.Int64Var(&config.TotalRequests, "n", 100000, "Total number of requests")
	flag.IntVar(&config.DataSize, "d", 3, "Data size of value in bytes for SET")
	flag.StringVar(&config.Command, "t", "set", "Command to benchmark set, get or custom")
	flag.Int64Var(&config.RandomKeyspace, "r", 0, "Use random keys from 0 to keyspacelen-1")
	flag.IntVar(&config.NumThreads, "threads", 1, "Number of worker threads")
	flag.IntVar(&config.TestDuration, "test-duration", 0, "Test duration in seconds")
	flag.Int64Var(&config.SequentialKeyLen, "sequential", 0, "Use sequential keys")
	flag.IntVar(&config.QPS, "qps", 0, "Queries per second limit")
	flag.IntVar(&config.StartQPS, "start-qps", 0, "Starting QPS for dynamic rate")
	flag.IntVar(&config.EndQPS, "end-qps", 0, "Ending QPS for dynamic rate")
	flag.IntVar(&config.QPSChangeInterval, "qps-change-interval", 0, "Interval for QPS changes in seconds")
	flag.IntVar(&config.QPSChange, "qps-change", 0, "QPS change amount per interval (linear mode only)")
	flag.StringVar(&config.QPSRampMode, "qps-ramp-mode", "linear", "QPS ramp mode: linear or exponential")
	flag.Float64Var(&config.QPSRampFactor, "qps-ramp-factor", 0, "Explicit multiplier for exponential QPS ramp (e.g., 2.0 to double QPS each interval)")
	flag.BoolVar(&config.UseTLS, "tls", false, "Use TLS connection")
	flag.BoolVar(&config.IsCluster, "cluster", false, "Use cluster client")
	flag.BoolVar(&config.ReadFromReplica, "read-from-replica", false, "Read from replica nodes")
	flag.Parse()

	config.UseSequential = config.SequentialKeyLen > 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle Ctrl+C
	// Handle Ctrl+C gracefully
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		cancel()
	}()

	if err := RunBenchmark(ctx, &config); err != nil {
		fmt.Printf("Benchmark failed: %v\n", err)
		os.Exit(1)
	}
}

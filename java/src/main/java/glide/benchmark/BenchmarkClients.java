package glide.benchmark;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import static glide.api.logging.Logger.Level.ERROR;
import static glide.api.logging.Logger.log;

import glide.api.GlideClient;
import glide.api.GlideClusterClient;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.ReadFrom;

/**
 * Provides client implementations and factory methods for benchmark operations.
 * Supports both standalone and cluster mode operations.
 */
public class BenchmarkClients {

    /**
     * Interface defining the basic operations that can be performed by benchmark clients.
     * Provides abstraction for different client implementations (standalone vs cluster).
     */
    public interface BenchmarkClient {
        /**
         * Performs a SET operation.
         * @param key The key to set
         * @param value The value to set
         * @return Result of the SET operation
         * @throws ExecutionException If the operation fails
         * @throws InterruptedException If the operation is interrupted
         */
        String set(String key, String value) throws ExecutionException, InterruptedException;

        /**
         * Performs a GET operation.
         * @param key The key to retrieve
         * @return The value associated with the key
         * @throws ExecutionException If the operation fails
         * @throws InterruptedException If the operation is interrupted
         */
        String get(String key) throws ExecutionException, InterruptedException;

        /**
         * Performs an HMGET operation.
         * @param key The hash key
         * @param fields The fields to retrieve
         * @return Array of values corresponding to the requested fields
         * @throws ExecutionException If the operation fails
         * @throws InterruptedException If the operation is interrupted
         */
        String[] hmget(String key, String... fields) throws ExecutionException, InterruptedException;
    }

    /**
     * Implementation of BenchmarkClient for standalone mode operations.
     * Wraps a GlideClient instance to provide benchmark operations.
     */
    static class StandaloneBenchmarkClient implements BenchmarkClient {
        private final GlideClient client;

        public StandaloneBenchmarkClient(GlideClient client) {
            this.client = client;
        }

        @Override
        public String set(String key, String value) throws ExecutionException, InterruptedException {
            return client.set(key, value).get();
        }

        @Override
        public String get(String key) throws ExecutionException, InterruptedException {
            return client.get(key).get();
        }

        @Override
        public String[] hmget(String key, String... fields) throws ExecutionException, InterruptedException {
            return client.hmget(key, fields).get();
        }

        GlideClient getClient() {
            return this.client;
        }
    }

    /**
     * Implementation of BenchmarkClient for cluster mode operations.
     * Wraps a GlideClusterClient instance to provide benchmark operations.
     */
    static class ClusterBenchmarkClient implements BenchmarkClient {
        private final GlideClusterClient client;

        public ClusterBenchmarkClient(GlideClusterClient client) {
            this.client = client;
        }

        @Override
        public String set(String key, String value) throws ExecutionException, InterruptedException {
            return client.set(key, value).get();
        }

        @Override
        public String get(String key) throws ExecutionException, InterruptedException {
            return client.get(key).get();
        }

        @Override
        public String[] hmget(String key, String... fields) throws ExecutionException, InterruptedException {
            return client.hmget(key, fields).get();
        }

        GlideClusterClient getClusterClient() {
            return this.client;
        }
    }

    /**
     * Creates a standalone client with the specified configuration.
     * 
     * @param nodeList List of nodes to connect to
     * @param config Global benchmark configuration
     * @return Configured standalone benchmark client
     * @throws CancellationException If client creation is cancelled
     * @throws ExecutionException If client creation fails
     * @throws InterruptedException If the operation is interrupted
     */
    public static BenchmarkClient createStandaloneClient(List<NodeAddress> nodeList, ValkeyBenchmark.BenchmarkConfig config)
            throws CancellationException, ExecutionException, InterruptedException {
        GlideClientConfiguration clientConfig =
                GlideClientConfiguration.builder()
                        .addresses(nodeList)
                        .readFrom(config.read_from_replica ? ReadFrom.PREFER_REPLICA : ReadFrom.PRIMARY)
                        .clientAZ("AZ1")
                        .useTLS(config.use_tls)
                        .build();
        try {
            return new StandaloneBenchmarkClient(GlideClient.createClient(clientConfig).get());
        } catch (CancellationException | InterruptedException | ExecutionException e) {
            log(ERROR, "glide", "Client creation error: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Creates a cluster client with the specified configuration.
     * 
     * @param nodeList List of nodes to connect to
     * @param config Global benchmark configuration
     * @return Configured cluster benchmark client
     * @throws CancellationException If client creation is cancelled
     * @throws ExecutionException If client creation fails
     * @throws InterruptedException If the operation is interrupted
     */
    public static BenchmarkClient createClusterClient(List<NodeAddress> nodeList, ValkeyBenchmark.BenchmarkConfig config)
            throws CancellationException, ExecutionException, InterruptedException {
        GlideClusterClientConfiguration clientConfig =
            GlideClusterClientConfiguration.builder()
                        .addresses(nodeList)
                        .readFrom(config.read_from_replica ? ReadFrom.PREFER_REPLICA : ReadFrom.PRIMARY)
                        .clientAZ("AZ1")
                        .useTLS(config.use_tls)
                        .build();
        try {
            return new ClusterBenchmarkClient(GlideClusterClient.createClient(clientConfig).get());
        } catch (CancellationException | InterruptedException | ExecutionException e) {
            log(ERROR, "glide", "Client creation error: " + e.getMessage());
            throw e;
        }
    }
}

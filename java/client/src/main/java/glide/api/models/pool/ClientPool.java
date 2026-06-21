/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.pool;

import static connection_request.ConnectionRequestOuterClass.*;

import glide.api.BaseClient;
import glide.api.models.configuration.BackoffStrategy;
import glide.api.models.configuration.BaseClientConfiguration;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.ServerCredentials;
import glide.api.models.exceptions.ClosingException;
import glide.ffi.resolvers.GlidePoolResolver;
import glide.internal.GlideNativeBridge;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client-instance pool backed by a Rust-side pool managing CoreClient lifecycle.
 *
 * <p>Callers borrow a client via {@link #acquire()}, use it for commands, and return it
 * via {@code close()} on the borrowed client. The pool handles creation, reuse, health
 * checking, idle eviction, and graceful shutdown.
 *
 * <p>The Rust pool owns the actual state (LIFO idle list, bounded size, eviction task).
 * This Java class provides acquire-with-timeout semantics and the PooledClient proxy pattern.
 *
 * @param <T> the client type (GlideClient or GlideClusterClient)
 */
public class ClientPool<T extends BaseClient> implements AutoCloseable {

    /** Pool states */
    private static final int RUNNING = 0;
    private static final int CLOSING = 1;
    private static final int CLOSED = 2;

    /** Opaque handle to the Rust-side pool. */
    private final long poolId;

    /** Pool configuration (immutable after creation). */
    private final ClientPoolConfig config;

    /** Factory for wrapping client_id into Java proxy type. */
    private final ClientFactory<T> factory;

    /** Pool lifecycle state. */
    private final AtomicInteger state = new AtomicInteger(RUNNING);

    /**
     * Cache of Java client wrappers keyed by client_id.
     * The factory is called once per client_id (on first acquire of that id).
     * Subsequent borrows of the same client_id reuse the cached wrapper — no allocation.
     */
    private final java.util.concurrent.ConcurrentHashMap<Long, T> clientCache =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Private constructor — use {@link #create} factory method.
     */
    private ClientPool(long poolId, ClientPoolConfig config, ClientFactory<T> factory) {
        this.poolId = poolId;
        this.config = config;
        this.factory = factory;
    }

    /**
     * Creates a new client pool backed by the Rust core.
     *
     * <p>This method:
     * <ol>
     *   <li>Validates the config</li>
     *   <li>Serializes the ConnectionRequest to protobuf bytes</li>
     *   <li>Calls glide_pool_create via JNI to create the Rust pool</li>
     *   <li>Returns a ClientPool instance in RUNNING state</li>
     * </ol>
     *
     * @param config pool configuration
     * @param factory client proxy factory
     * @param <T> the client type
     * @return a new ClientPool
     * @throws IllegalArgumentException if config is invalid
     * @throws RuntimeException if Rust pool creation fails
     */
    public static <T extends BaseClient> ClientPool<T> create(
            ClientPoolConfig config, ClientFactory<T> factory) {
        // Validate config
        config.validate();

        // Serialize ConnectionRequest to protobuf bytes
        // TODO: Implement proper serialization using ConnectionManager's protobuf building logic
        // For now, use a placeholder — the actual serialization will be wired in integration
        byte[] connectionRequestBytes = serializeConnectionRequest(config.getClientConfig());

        // Create the Rust pool
        long poolId = GlidePoolResolver.glidePoolCreate(
                config.getMaxSize(),
                config.getMinIdle(),
                config.getIdleTimeout().toMillis(),
                config.getRequestTimeout().toMillis(),
                connectionRequestBytes);

        if (poolId == -1) {
            throw new IllegalArgumentException("Invalid pool configuration rejected by Rust core");
        }
        if (poolId < 0) {
            throw new RuntimeException(
                    "Failed to create pool in Rust core (error code: " + poolId + ")");
        }

        return new ClientPool<>(poolId, config, factory);
    }

    /**
     * Get the Rust pool handle.
     * @return the pool_id
     */
    public long getPoolId() {
        return poolId;
    }

    /**
     * Get the pool configuration.
     * @return the config
     */
    public ClientPoolConfig getConfig() {
        return config;
    }

    /**
     * Get the client factory.
     * @return the factory
     */
    public ClientFactory<T> getFactory() {
        return factory;
    }

    /**
     * Check if the pool is running (accepting acquire requests).
     * @return true if running
     */
    public boolean isRunning() {
        return state.get() == RUNNING;
    }

    /**
     * Check if the pool has been closed.
     * @return true if closed
     */
    public boolean isClosed() {
        return state.get() == CLOSED;
    }

    // ========== Pool metrics ==========

    /**
     * Get the number of idle clients in the pool.
     * @return idle count
     */
    public int getIdleCount() {
        int[] metrics = GlidePoolResolver.glidePoolMetrics(poolId);
        return metrics != null ? metrics[0] : 0;
    }

    /**
     * Get the number of actively borrowed clients.
     * @return active count
     */
    public int getActiveCount() {
        int[] metrics = GlidePoolResolver.glidePoolMetrics(poolId);
        return metrics != null ? metrics[1] : 0;
    }

    /**
     * Get the total number of clients (idle + active + cleaning + creating).
     * @return total count
     */
    public int getTotalCount() {
        int[] metrics = GlidePoolResolver.glidePoolMetrics(poolId);
        return metrics != null ? metrics[2] : 0;
    }

    /**
     * Get the configured max pool size.
     * @return max size
     */
    public int getMaxSize() {
        return config.getMaxSize();
    }

    // ========== Acquire / Release ==========

    /**
     * Acquire a client from the pool with the default timeout.
     *
     * @return a CompletableFuture that resolves to a borrowed client proxy
     */
    public CompletableFuture<T> acquire() {
        return acquire(config.getAcquireTimeout());
    }

    /**
     * Acquire a client from the pool with a custom timeout.
     *
     * <p>Internally retries {@code glidePoolTryAcquire} with exponential backoff
     * (1ms initial, 50ms cap) until a client_id is obtained or the timeout expires.
     *
     * @param timeout maximum time to wait
     * @return a CompletableFuture that resolves to a borrowed client proxy
     */
    public CompletableFuture<T> acquire(Duration timeout) {
        if (state.get() != RUNNING) {
            CompletableFuture<T> f = new CompletableFuture<>();
            f.completeExceptionally(new ClosingException("Pool is shutting down"));
            return f;
        }

        return CompletableFuture.supplyAsync(() -> {
            long deadlineNanos = System.nanoTime() + timeout.toNanos();
            long backoffMs = 1;

            while (System.nanoTime() < deadlineNanos) {
                // Check state on each iteration
                if (state.get() != RUNNING) {
                    throw new RuntimeException(new ClosingException("Pool is shutting down"));
                }

                long clientId = GlidePoolResolver.glidePoolTryAcquire(poolId);

                if (clientId >= 0) {
                    // Success — return cached wrapper or create on first use
                    T cached = clientCache.get(clientId);
                    if (cached != null) {
                        return cached;
                    }
                    return clientCache.computeIfAbsent(clientId,
                            id -> factory.wrapClientId(id, poolId));
                }

                if (clientId == -2) {
                    // Invalid pool_id — pool was destroyed
                    throw new RuntimeException(new ClosingException("Pool has been destroyed"));
                }

                // clientId == -1: pool exhausted or lock contended, backoff and retry
                long remainingMs = (deadlineNanos - System.nanoTime()) / 1_000_000;
                long sleepMs = Math.min(backoffMs, Math.max(remainingMs, 0));
                if (sleepMs <= 0) {
                    break;
                }
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Acquire interrupted", e);
                }
                backoffMs = Math.min(backoffMs * 2, 50);
            }

            throw new RuntimeException(new java.util.concurrent.TimeoutException(
                    "Pool exhausted: could not acquire client within " + timeout));
        });
    }

    /**
     * Release a borrowed client back to the pool. Called by PooledClient.close().
     *
     * @param clientId the Rust-assigned client_id to release
     */
    public void release(long clientId) {
        GlidePoolResolver.glidePoolRelease(poolId, clientId);
    }

    /**
     * Get or create a cached Java client wrapper for the given client_id.
     * The factory is called once per client_id; subsequent calls return the cached instance.
     * This is the fast path used by acquire() to avoid allocation on every borrow.
     *
     * @param clientId the Rust-assigned client_id
     * @return the cached client wrapper
     */
    public T getOrCreateClient(long clientId) {
        // Fast path: plain get() avoids bucket locking on cache hit
        T cached = clientCache.get(clientId);
        if (cached != null) {
            return cached;
        }
        // Slow path (first borrow of this client_id): allocate wrapper
        return clientCache.computeIfAbsent(clientId, id -> factory.wrapClientId(id, poolId));
    }

    // ========== Lifecycle ==========

    @Override
    public void close() {
        close(Duration.ofSeconds(30));
    }

    /**
     * Gracefully shut down the pool with a custom grace period.
     *
     * @param gracePeriod maximum time to wait for borrowed clients to be returned
     */
    public void close(Duration gracePeriod) {
        if (!state.compareAndSet(RUNNING, CLOSING)) {
            return; // Already closing or closed
        }
        // TODO: Wait for outstanding borrowed clients (task 6.3)
        GlidePoolResolver.glidePoolDestroy(poolId);
        state.set(CLOSED);
    }

    /**
     * Immediately destroy the pool without waiting for borrowed clients.
     */
    public void closeImmediately() {
        state.set(CLOSED);
        GlidePoolResolver.glidePoolDestroy(poolId);
    }

    // ========== Internal helpers ==========

    /**
     * Serialize a BaseClientConfiguration to protobuf ConnectionRequest bytes.
     *
     * <p>Builds a ConnectionRequest protobuf matching the logic in ConnectionManager.connectToValkey(),
     * covering the essential fields needed for the Rust side to create a real GlideClient.
     */
    private static byte[] serializeConnectionRequest(BaseClientConfiguration config) {
        ConnectionRequest.Builder requestBuilder = ConnectionRequest.newBuilder();

        // Add addresses
        for (glide.api.models.configuration.NodeAddress addr : config.getAddresses()) {
            NodeAddress nodeAddress =
                    NodeAddress.newBuilder()
                            .setHost(addr.getHost())
                            .setPort(addr.getPort())
                            .build();
            requestBuilder.addAddresses(nodeAddress);
        }

        // Set TLS mode
        if (config.isUseTLS()) {
            requestBuilder.setTlsMode(TlsMode.SecureTls);
        } else {
            requestBuilder.setTlsMode(TlsMode.NoTls);
        }

        // Set cluster mode
        boolean isCluster = config instanceof GlideClusterClientConfiguration;
        requestBuilder.setClusterModeEnabled(isCluster);

        // Set request timeout
        int requestTimeoutMs = config.getRequestTimeout() != null
                ? config.getRequestTimeout()
                : (int) GlideNativeBridge.getGlideCoreDefaultRequestTimeoutMs();
        requestBuilder.setRequestTimeout(requestTimeoutMs);

        // Set connection timeout (same as request timeout if not explicitly configured)
        requestBuilder.setConnectionTimeout(requestTimeoutMs);

        // Set inflight requests limit
        int inflightLimit = config.getInflightRequestsLimit() != null
                ? config.getInflightRequestsLimit()
                : GlideNativeBridge.getGlideCoreDefaultMaxInflightRequests();
        requestBuilder.setInflightRequestsLimit(inflightLimit);

        // Set authentication if credentials are present
        ServerCredentials credentials = config.getCredentials();
        if (credentials != null) {
            AuthenticationInfo.Builder authBuilder = AuthenticationInfo.newBuilder();
            if (credentials.getUsername() != null) {
                authBuilder.setUsername(credentials.getUsername());
            }
            if (credentials.getPassword() != null) {
                authBuilder.setPassword(credentials.getPassword());
            }
            requestBuilder.setAuthenticationInfo(authBuilder.build());
        }

        // Set read from strategy
        String readFromName = config.getReadFrom().name();
        if ("PRIMARY".equals(readFromName)) {
            requestBuilder.setReadFrom(ReadFrom.Primary);
        } else if ("PREFER_REPLICA".equals(readFromName)) {
            requestBuilder.setReadFrom(ReadFrom.PreferReplica);
        }

        // Set client name if provided
        if (config.getClientName() != null) {
            requestBuilder.setClientName(config.getClientName());
        }

        // Set database ID if provided
        if (config.getDatabaseId() != null) {
            requestBuilder.setDatabaseId(config.getDatabaseId());
        }

        // Set protocol version if specified
        if (config.getProtocol() != null) {
            if ("RESP2".equals(config.getProtocol().name())) {
                requestBuilder.setProtocol(ProtocolVersion.RESP2);
            } else if ("RESP3".equals(config.getProtocol().name())) {
                requestBuilder.setProtocol(ProtocolVersion.RESP3);
            }
        }

        // Set reconnect strategy if configured
        BackoffStrategy reconnectStrategy = config.getReconnectStrategy();
        if (reconnectStrategy != null) {
            ConnectionRetryStrategy.Builder retryBuilder = ConnectionRetryStrategy.newBuilder();
            if (reconnectStrategy.getNumOfRetries() != null) {
                retryBuilder.setNumberOfRetries(reconnectStrategy.getNumOfRetries());
            }
            if (reconnectStrategy.getFactor() != null) {
                retryBuilder.setFactor(reconnectStrategy.getFactor());
            }
            if (reconnectStrategy.getExponentBase() != null) {
                retryBuilder.setExponentBase(reconnectStrategy.getExponentBase());
            }
            requestBuilder.setConnectionRetryStrategy(retryBuilder.build());
        }

        return requestBuilder.build().toByteArray();
    }
}

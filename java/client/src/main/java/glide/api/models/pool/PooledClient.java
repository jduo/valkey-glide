/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.pool;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A proxy for a pooled client that intercepts close() to return the client to the pool.
 *
 * <p>After close() is called, the proxy is invalidated — subsequent command calls
 * should throw IllegalStateException.
 *
 * <p>This is a lightweight metadata holder. Actual command dispatch uses the client_id
 * to route through the existing JNI command path.
 */
public class PooledClient {

    /** The Rust-assigned client identifier used for command dispatch. */
    private final long clientId;

    /** The parent pool handle (for release on close). */
    private final long poolId;

    /** Reference to the parent pool for release. */
    private final ClientPool<?> pool;

    /** CAS guard ensuring close() releases exactly once. */
    private final AtomicBoolean released = new AtomicBoolean(false);

    /** Timestamp of when this client was borrowed (for diagnostics). */
    private final long borrowedAtNanos;

    /**
     * Creates a new PooledClient proxy.
     *
     * @param clientId the Rust-managed client identifier
     * @param poolId the parent pool handle
     * @param pool the parent pool instance
     */
    public PooledClient(long clientId, long poolId, ClientPool<?> pool) {
        this.clientId = clientId;
        this.poolId = poolId;
        this.pool = pool;
        this.borrowedAtNanos = System.nanoTime();
    }

    /**
     * Get the Rust-assigned client_id for command dispatch.
     *
     * @return the client_id
     * @throws IllegalStateException if this proxy has been closed
     */
    public long getClientId() {
        if (released.get()) {
            throw new IllegalStateException("PooledClient has been returned to the pool");
        }
        return clientId;
    }

    /**
     * Get the pool_id for the parent pool.
     *
     * @return the pool_id
     */
    public long getPoolId() {
        return poolId;
    }

    /**
     * Get the timestamp (in nanos) when this client was borrowed.
     *
     * @return the borrow time in System.nanoTime() units
     */
    public long getBorrowedAtNanos() {
        return borrowedAtNanos;
    }

    /**
     * Check if this proxy has been released.
     *
     * @return true if close() has been called
     */
    public boolean isReleased() {
        return released.get();
    }

    /**
     * Return this client to the pool. Idempotent — second call is a no-op.
     *
     * <p>After this call, the client_id is returned to the Rust pool's idle list
     * and this proxy should not be used for further commands.
     */
    public void close() {
        if (released.compareAndSet(false, true)) {
            pool.release(clientId);
        }
    }
}

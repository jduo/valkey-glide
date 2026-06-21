/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.ffi.resolvers;

/**
 * Native method declarations for client-instance pool operations.
 *
 * <p>These methods map to the JNI functions in java/src/jni_pool.rs.
 * All pool state management is delegated to the Rust core.
 */
public class GlidePoolResolver {

    // Load the native library (shared with all other JNI resolvers)
    static {
        NativeUtils.loadGlideLib();
    }

    /**
     * Create a new pool in the Rust core.
     *
     * @param maxSize maximum number of clients in the pool
     * @param minIdle minimum idle clients to pre-warm
     * @param idleTimeoutMs idle timeout in milliseconds
     * @param requestTimeoutMs request timeout in milliseconds (used for cleanup: 2×)
     * @param connectionRequestBytes protobuf-serialized ConnectionRequest
     * @return positive pool_id on success, -1 on invalid config, -2 on other errors
     */
    public static native long glidePoolCreate(
            int maxSize,
            int minIdle,
            long idleTimeoutMs,
            long requestTimeoutMs,
            byte[] connectionRequestBytes);

    /**
     * Non-blocking acquire. Attempts to borrow a client from the pool.
     *
     * @param poolId the pool handle
     * @return client_id >= 0 on success, -1 if pool exhausted/contended, -2 if invalid pool_id
     */
    public static native long glidePoolTryAcquire(long poolId);

    /**
     * Release a borrowed client back to the pool. Fire-and-forget.
     *
     * @param poolId the pool handle
     * @param clientId the client to release
     * @return 0 on success, -1 if pool not found
     */
    public static native int glidePoolRelease(long poolId, long clientId);

    /**
     * Destroy the pool and close all connections.
     *
     * @param poolId the pool handle
     * @return 0 on success, -1 if pool not found
     */
    public static native int glidePoolDestroy(long poolId);

    /**
     * Query pool metrics.
     *
     * @param poolId the pool handle
     * @return int array [idle_count, active_count, total_count], or null if invalid pool_id
     */
    public static native int[] glidePoolMetrics(long poolId);
}

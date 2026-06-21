/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.ffi.resolvers;

/** Native method declarations for isolated execution (Feature 2 scopes). */
public class GlideScopeResolver {

    static {
        NativeUtils.loadGlideLib();
    }

    /**
     * Acquire an isolated connection scope from a client's internal connection pool.
     *
     * @param clientId the native handle of the GlideClient
     * @param connectionRequestBytes protobuf ConnectionRequest for creating new connections
     * @return scope_id >= 0 on success, -1 if exhausted, -2 if invalid client_id
     */
    public static native long glideScopeTryAcquire(long clientId, byte[] connectionRequestBytes);

    /**
     * Release an isolated scope back to the client's connection pool.
     *
     * @param scopeId the scope to release
     * @param clientId the owning client
     * @return 0 on success, -1 if not found
     */
    public static native int glideScopeRelease(long scopeId, long clientId);

    /**
     * Execute a command on a scoped connection (bypasses multiplexer).
     *
     * @param scopeId identifies the scoped connection
     * @param commandBytes serialized command bytes
     * @param callbackId Java callback to complete on response
     * @return 0 if dispatched, -1 if invalid scope, -2 if deserialization failed
     */
    public static native int glideScopeExecute(long scopeId, byte[] commandBytes, long callbackId);
}

/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.scope;

import glide.ffi.resolvers.GlideScopeResolver;
import glide.internal.AsyncRegistry;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A borrowed dedicated connection for operations requiring per-connection server state.
 *
 * <p>Commands on this scope bypass the client's multiplexer and execute directly on a single TCP
 * connection. This enables WATCH/MULTI/EXEC, CLIENT TRACKING, blocking commands, and pub/sub
 * without interfering with other callers.
 *
 * <p><strong>Thread Safety:</strong> An individual IsolatedScope instance is NOT thread-safe.
 * It represents a serial execution context where command ordering matters (e.g., WATCH → read →
 * MULTI → write → EXEC). Sharing a single scope across threads will produce undefined behavior.
 * Instead, each thread should acquire its own scope via {@code client.scopedConnection()}.
 * The client's internal scope pool IS thread-safe — multiple threads can concurrently acquire
 * independent scopes from the same client.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (IsolatedScope scope = client.scopedConnection().get()) {
 *     scope.watch("counter").get();
 *     String val = scope.get("counter").get();
 *     scope.multi().get();
 *     scope.set("counter", String.valueOf(Integer.parseInt(val) + 1)).get();
 *     Object[] result = scope.exec().get();
 * }
 * }</pre>
 */
public class IsolatedScope implements AutoCloseable {

    private final long scopeId;
    private final long clientId;
    private final AtomicBoolean released = new AtomicBoolean(false);

    public IsolatedScope(long scopeId, long clientId) {
        this.scopeId = scopeId;
        this.clientId = clientId;
    }

    /** Get the native scope handle. */
    public long getScopeId() {
        if (released.get()) {
            throw new IllegalStateException("Scope has been released");
        }
        return scopeId;
    }

    /** Check if this scope has been released. */
    public boolean isReleased() {
        return released.get();
    }

    // ========== Core Commands ==========

    public CompletableFuture<String> watch(String... keys) {
        return executeCommand("WATCH", keys);
    }

    public CompletableFuture<String> unwatch() {
        return executeCommand("UNWATCH");
    }

    public CompletableFuture<String> multi() {
        return executeCommand("MULTI");
    }

    public CompletableFuture<Object[]> execArray() {
        return executeCommandRaw("EXEC").thenApply(result -> {
            if (result == null) return null; // WATCH failed
            // For prototype: return raw result
            return new Object[] {result};
        });
    }

    public CompletableFuture<String> exec() {
        return executeCommandRaw("EXEC").thenApply(result -> {
            if (result == null) return null;
            return result.toString();
        });
    }

    public CompletableFuture<String> discard() {
        return executeCommand("DISCARD");
    }

    public CompletableFuture<String> get(String key) {
        return executeCommand("GET", key);
    }

    public CompletableFuture<String> set(String key, String value) {
        return executeCommand("SET", key, value);
    }

    public CompletableFuture<String> incr(String key) {
        return executeCommand("INCR", key);
    }

    public CompletableFuture<String> decrBy(String key, long decrement) {
        return executeCommand("DECRBY", key, String.valueOf(decrement));
    }

    public CompletableFuture<String> select(int db) {
        return executeCommand("SELECT", String.valueOf(db));
    }

    public CompletableFuture<String> ping() {
        return executeCommand("PING");
    }

    public CompletableFuture<String> clientTracking(boolean on) {
        return executeCommand("CLIENT", "TRACKING", on ? "ON" : "OFF");
    }

    // ========== Command Execution ==========

    /**
     * Execute a command on the scoped connection and return the result as a String.
     *
     * @param command the command name
     * @param args command arguments
     * @return future completing with the string result
     */
    public CompletableFuture<String> executeCommand(String command, String... args) {
        return executeCommandRaw(command, args).thenApply(result -> {
            if (result == null) return null;
            return result.toString();
        });
    }

    /**
     * Execute a command on the scoped connection and return the raw result.
     * Uses SYNCHRONOUS JNI dispatch — the calling thread blocks until the command
     * completes on the dedicated connection. This eliminates all async thread hops
     * since scoped connections enforce serial execution anyway.
     *
     * @param command the command name
     * @param args command arguments
     * @return future completing with the raw result object
     */
    @SuppressWarnings("unchecked")
    public CompletableFuture<Object> executeCommandRaw(String command, String... args) {
        if (released.get()) {
            CompletableFuture<Object> f = new CompletableFuture<>();
            f.completeExceptionally(new IllegalStateException("Scope has been released"));
            return f;
        }

        // Register a callback (no inflight limit, no Java timeout for scope commands)
        CompletableFuture<Object> future = new CompletableFuture<>();
        long callbackId = AsyncRegistry.register(future, 0, clientId, 0);

        // Serialize command
        byte[] commandBytes = serializeCommand(command, args);

        // Dispatch via JNI — this blocks until the command completes on the
        // dedicated connection, then completes the callback before returning.
        // The future will be resolved by the time glideScopeExecute returns
        // (via the callback worker thread).
        int result = GlideScopeResolver.glideScopeExecute(scopeId, commandBytes, callbackId);

        if (result == -1) {
            future.completeExceptionally(new IllegalStateException("Invalid scope_id"));
        } else if (result == -2) {
            future.completeExceptionally(
                    new IllegalArgumentException("Failed to serialize command"));
        }

        return future;
    }

    // ========== Lifecycle ==========

    @Override
    public void close() {
        if (released.compareAndSet(false, true)) {
            GlideScopeResolver.glideScopeRelease(scopeId, clientId);
        }
    }

    // ========== Internal ==========

    /**
     * Serialize a command into the wire format expected by glideScopeExecute.
     * Format: [cmd_name_len(4 LE), cmd_name_bytes, num_args(4 LE), [arg_len(4 LE), arg_bytes]...]
     */
    static byte[] serializeCommand(String command, String... args) {
        byte[] cmdBytes = command.getBytes(StandardCharsets.UTF_8);

        // Calculate total size
        int totalSize = 4 + cmdBytes.length + 4; // cmd_len + cmd + num_args
        byte[][] argBytes = new byte[args.length][];
        for (int i = 0; i < args.length; i++) {
            argBytes[i] = args[i].getBytes(StandardCharsets.UTF_8);
            totalSize += 4 + argBytes[i].length; // arg_len + arg
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(cmdBytes.length);
        buffer.put(cmdBytes);
        buffer.putInt(args.length);
        for (byte[] arg : argBytes) {
            buffer.putInt(arg.length);
            buffer.put(arg);
        }

        return buffer.array();
    }
}

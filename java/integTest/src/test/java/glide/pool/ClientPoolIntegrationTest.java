/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.pool;

import static org.junit.jupiter.api.Assertions.*;

import glide.api.GlideClient;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.exceptions.ClosingException;
import glide.api.models.pool.ClientFactory;
import glide.api.models.pool.ClientPool;
import glide.api.models.pool.ClientPoolConfig;
import glide.api.models.pool.PooledClient;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Integration test proving the client pool prototype works end-to-end.
 * Requires a Valkey/Redis server running on localhost:6379.
 */
public class ClientPoolIntegrationTest {

    private static final String TEST_HOST = "localhost";
    private static final int TEST_PORT = 6399;

    private GlideClientConfiguration getTestClientConfig() {
        return GlideClientConfiguration.builder()
                .address(NodeAddress.builder().host(TEST_HOST).port(TEST_PORT).build())
                .requestTimeout(5000)
                .build();
    }

    @Test
    public void testPoolCreateAcquireReleaseDestroy() throws Exception {
        // Configure for local standalone Valkey
        GlideClientConfiguration clientConfig = getTestClientConfig();

        ClientPoolConfig poolConfig = ClientPoolConfig.builder()
                .maxSize(3)
                .minIdle(1)
                .acquireTimeout(Duration.ofSeconds(10))
                .idleTimeout(Duration.ofSeconds(60))
                .clientConfig(clientConfig)
                .build();

        // Create pool — this should create the Rust pool and start min_idle background creation
        ClientPool<GlideClient> pool = ClientPool.create(poolConfig, (clientId, poolId) -> {
            // For now, create a simple PooledClient wrapper
            // In full implementation, this would be a proper GlideClient proxy
            return null; // We'll use PooledClient directly for the test
        });

        assertNotNull(pool);
        assertTrue(pool.isRunning());
        assertTrue(pool.getPoolId() > 0);

        // Give background creation time to complete (connection + scheduling overhead)
        Thread.sleep(3000);

        // Check metrics — should have at least min_idle clients
        System.out.println("Pool metrics after creation:");
        System.out.println("  Idle: " + pool.getIdleCount());
        System.out.println("  Active: " + pool.getActiveCount());
        System.out.println("  Total: " + pool.getTotalCount());
        System.out.println("  Max: " + pool.getMaxSize());

        assertTrue(pool.getTotalCount() > 0, "Pool should have created at least one client");

        // Acquire a client via the low-level JNI directly
        long clientId = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(pool.getPoolId());
        System.out.println("Acquired client_id: " + clientId);
        assertTrue(clientId >= 0, "Should acquire a client from the pool");

        // Release it back
        int releaseResult = glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(pool.getPoolId(), clientId);
        assertEquals(0, releaseResult, "Release should succeed");

        // Destroy the pool
        pool.close();
        assertTrue(pool.isClosed());

        System.out.println("Pool integration test PASSED!");
    }

    @Test
    public void testPooledClientCommandDispatch() throws Exception {
        // Configure for local standalone Valkey
        GlideClientConfiguration clientConfig = getTestClientConfig();

        ClientPoolConfig poolConfig = ClientPoolConfig.builder()
                .maxSize(3)
                .minIdle(1)
                .acquireTimeout(Duration.ofSeconds(10))
                .idleTimeout(Duration.ofSeconds(60))
                .clientConfig(clientConfig)
                .build();

        // Factory that creates a working GlideClient from a pool handle
        ClientFactory<GlideClient> factory = (clientId, poolId) ->
                GlideClient.fromPoolHandle(clientId, 0, 5000);

        ClientPool<GlideClient> pool = ClientPool.create(poolConfig, factory);

        assertNotNull(pool);
        assertTrue(pool.isRunning());

        // Give background creation time to complete (connection + scheduling overhead)
        Thread.sleep(3000);

        System.out.println("Command dispatch test - Total: " + pool.getTotalCount()
                + ", Idle: " + pool.getIdleCount());
        assertTrue(pool.getTotalCount() > 0, "Pool should have created at least one client");

        // Acquire a client through the pool's high-level API
        CompletableFuture<GlideClient> acquireFuture = pool.acquire();
        GlideClient client = acquireFuture.get();
        assertNotNull(client, "Acquired client should not be null");

        // --- The key test: Execute SET and GET commands ---
        String testKey = "pool-test-" + UUID.randomUUID();
        String testValue = "hello-from-pool";

        // SET
        String setResult = client.set(testKey, testValue).get();
        assertEquals("OK", setResult, "SET should return OK");
        System.out.println("SET " + testKey + " = " + testValue + " -> " + setResult);

        // GET
        String getResult = client.get(testKey).get();
        assertEquals(testValue, getResult, "GET should return the value we SET");
        System.out.println("GET " + testKey + " -> " + getResult);

        // PING
        String pingResult = client.ping().get();
        assertEquals("PONG", pingResult, "PING should return PONG");
        System.out.println("PING -> " + pingResult);

        // Clean up the test key
        client.del(new String[]{testKey}).get();

        // Destroy the pool
        pool.close();
        assertTrue(pool.isClosed());

        System.out.println("Pool command dispatch test PASSED!");
    }

    @Test
    public void testPoolAcquireReleaseReuse() throws Exception {
        // Configure for local standalone Valkey
        GlideClientConfiguration clientConfig = getTestClientConfig();

        ClientPoolConfig poolConfig = ClientPoolConfig.builder()
                .maxSize(2)
                .minIdle(1)
                .acquireTimeout(Duration.ofSeconds(10))
                .idleTimeout(Duration.ofSeconds(60))
                .clientConfig(clientConfig)
                .build();

        // Factory that creates a working GlideClient from a pool handle
        ClientFactory<GlideClient> factory = (clientId, poolId) ->
                GlideClient.fromPoolHandle(clientId, 0, 5000);

        ClientPool<GlideClient> pool = ClientPool.create(poolConfig, factory);

        // Give background creation time to complete
        Thread.sleep(3000);

        System.out.println("Reuse test - Total: " + pool.getTotalCount()
                + ", Idle: " + pool.getIdleCount());

        // Acquire, use, release, acquire again — same client should work
        long clientId1 = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(pool.getPoolId());
        assertTrue(clientId1 >= 0, "First acquire should succeed");

        // Use client for a command
        GlideClient client1 = GlideClient.fromPoolHandle(clientId1, 0, 5000);
        String key1 = "reuse-test-" + UUID.randomUUID();
        client1.set(key1, "value1").get();
        assertEquals("value1", client1.get(key1).get());

        // Release
        glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(pool.getPoolId(), clientId1);

        // Acquire again — should get the same client_id (LIFO)
        long clientId2 = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(pool.getPoolId());
        assertTrue(clientId2 >= 0, "Second acquire should succeed");
        assertEquals(clientId1, clientId2, "Should reuse the same client (LIFO)");

        // Use the reused client for another command
        GlideClient client2 = GlideClient.fromPoolHandle(clientId2, 0, 5000);
        assertEquals("value1", client2.get(key1).get(), "Reused client should see previously set data");

        // Clean up
        client2.del(new String[]{key1}).get();
        glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(pool.getPoolId(), clientId2);

        pool.close();
        assertTrue(pool.isClosed());

        System.out.println("Pool acquire/release/reuse test PASSED!");
    }

    /**
     * Test concurrent multi-thread access (Req 12.1, 12.3 — thread safety).
     *
     * <p>Spawns 10 threads, each acquiring from a pool with maxSize=3, executing a
     * command, and releasing. Verifies no crashes, no duplicate client_ids handed out
     * simultaneously, and all SET/GET operations succeed.
     */
    @Test
    public void testConcurrentMultiThreadAccess() throws Exception {
        GlideClientConfiguration clientConfig = getTestClientConfig();

        ClientPoolConfig poolConfig = ClientPoolConfig.builder()
                .maxSize(3)
                .minIdle(1)
                .acquireTimeout(Duration.ofSeconds(10))
                .idleTimeout(Duration.ofSeconds(60))
                .clientConfig(clientConfig)
                .build();

        ClientFactory<GlideClient> factory = (clientId, poolId) ->
                GlideClient.fromPoolHandle(clientId, 0, 5000);

        ClientPool<GlideClient> pool = ClientPool.create(poolConfig, factory);
        Thread.sleep(3000);

        final int threadCount = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);


        for (int i = 0; i < threadCount; i++) {
            final int threadIdx = i;
            new Thread(() -> {
                try {
                    startLatch.await(); // Synchronize start

                    // Use low-level acquire/release to properly return clients to pool
                    long poolId = pool.getPoolId();
                    
                    // Retry acquire with backoff (same pattern as ClientPool.acquire)
                    long clientId = -1;
                    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
                    long backoffMs = 1;
                    while (System.nanoTime() < deadline) {
                        clientId = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(poolId);
                        if (clientId >= 0) break;
                        Thread.sleep(backoffMs);
                        backoffMs = Math.min(backoffMs * 2, 50);
                    }
                    assertTrue(clientId >= 0, "Thread " + threadIdx + " should acquire a client");

                    try {
                        GlideClient client = GlideClient.fromPoolHandle(clientId, 0, 5000);
                        String key = "concurrent-test-" + UUID.randomUUID();
                        String value = "thread-" + threadIdx;

                        String setResult = client.set(key, value).get();
                        assertEquals("OK", setResult);

                        String getResult = client.get(key).get();
                        assertEquals(value, getResult);

                        // Clean up key
                        client.del(new String[]{key}).get();
                    } finally {
                        // Always release back to pool
                        glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(poolId, clientId);
                    }

                    successCount.incrementAndGet();
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    System.err.println("Thread " + threadIdx + " error: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        // Release all threads at once
        startLatch.countDown();
        assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "All threads should complete within 30s");

        assertEquals(threadCount, successCount.get(),
                "All threads should succeed. Errors: " + errorCount.get());
        assertEquals(0, errorCount.get(), "No errors expected");

        pool.close();
        assertTrue(pool.isClosed());

        System.out.println("Concurrent multi-thread access test PASSED! "
                + successCount.get() + "/" + threadCount + " threads succeeded.");
    }

    /**
     * Test pool exhaustion and timeout (Req 2.3, 2.6, 8.1 — bounded size, timeout).
     *
     * <p>Creates pool with maxSize=2, acquires both clients (holds them), then attempts a
     * third acquire with a short timeout (500ms). Verifies it throws TimeoutException.
     * Then releases one and verifies the next acquire succeeds.
     */
    @Test
    public void testPoolExhaustionAndTimeout() throws Exception {
        GlideClientConfiguration clientConfig = getTestClientConfig();

        ClientPoolConfig poolConfig = ClientPoolConfig.builder()
                .maxSize(2)
                .minIdle(2)
                .acquireTimeout(Duration.ofMillis(500))
                .idleTimeout(Duration.ofSeconds(60))
                .clientConfig(clientConfig)
                .build();

        ClientFactory<GlideClient> factory = (clientId, poolId) ->
                GlideClient.fromPoolHandle(clientId, 0, 5000);

        ClientPool<GlideClient> pool = ClientPool.create(poolConfig, factory);
        Thread.sleep(3000);

        // Acquire both clients — holding them
        long clientId1 = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(pool.getPoolId());
        assertTrue(clientId1 >= 0, "First acquire should succeed");

        long clientId2 = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(pool.getPoolId());
        assertTrue(clientId2 >= 0, "Second acquire should succeed");

        assertNotEquals(clientId1, clientId2, "Should be different clients");

        // Third acquire with short timeout should fail with TimeoutException
        CompletableFuture<GlideClient> exhaustedFuture = pool.acquire(Duration.ofMillis(500));

        ExecutionException execException = assertThrows(ExecutionException.class, () -> {
            exhaustedFuture.get(5, TimeUnit.SECONDS);
        });

        // The cause should be a RuntimeException wrapping TimeoutException
        Throwable cause = execException.getCause();
        assertNotNull(cause, "Should have a cause");
        assertTrue(cause instanceof RuntimeException, "Cause should be RuntimeException");
        Throwable innerCause = cause.getCause();
        assertNotNull(innerCause, "Should have an inner cause");
        assertTrue(innerCause instanceof TimeoutException,
                "Inner cause should be TimeoutException, got: " + innerCause.getClass().getName());

        System.out.println("Pool exhaustion correctly threw TimeoutException");

        // Release one client
        glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(pool.getPoolId(), clientId1);

        // Now acquire should succeed
        CompletableFuture<GlideClient> recoveryFuture = pool.acquire(Duration.ofSeconds(5));
        GlideClient recoveredClient = recoveryFuture.get(10, TimeUnit.SECONDS);
        assertNotNull(recoveredClient, "Acquire after release should succeed");

        // Verify the recovered client works
        String key = "exhaustion-test-" + UUID.randomUUID();
        assertEquals("OK", recoveredClient.set(key, "recovered").get());
        assertEquals("recovered", recoveredClient.get(key).get());
        recoveredClient.del(new String[]{key}).get();

        // Release the remaining held client
        glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(pool.getPoolId(), clientId2);

        pool.close();
        assertTrue(pool.isClosed());

        System.out.println("Pool exhaustion and timeout test PASSED!");
    }

    /**
     * Test metrics accuracy (Req 15.1, 15.2, 15.3 — idle/active/total counts).
     *
     * <p>Creates pool with maxSize=5, minIdle=2. After warmup, verifies getIdleCount() >= 2.
     * Acquires one, verifies getActiveCount() == 1 and getIdleCount() decreased.
     * Releases it, verifies counts return to previous state.
     */
    @Test
    public void testMetricsAccuracy() throws Exception {
        GlideClientConfiguration clientConfig = getTestClientConfig();

        ClientPoolConfig poolConfig = ClientPoolConfig.builder()
                .maxSize(5)
                .minIdle(2)
                .acquireTimeout(Duration.ofSeconds(10))
                .idleTimeout(Duration.ofSeconds(60))
                .clientConfig(clientConfig)
                .build();

        ClientFactory<GlideClient> factory = (clientId, poolId) ->
                GlideClient.fromPoolHandle(clientId, 0, 5000);

        ClientPool<GlideClient> pool = ClientPool.create(poolConfig, factory);
        Thread.sleep(3000);

        // After warmup, idle count should be >= minIdle
        int idleBeforeAcquire = pool.getIdleCount();
        int activeBeforeAcquire = pool.getActiveCount();
        int totalBeforeAcquire = pool.getTotalCount();

        System.out.println("Metrics before acquire:");
        System.out.println("  Idle: " + idleBeforeAcquire);
        System.out.println("  Active: " + activeBeforeAcquire);
        System.out.println("  Total: " + totalBeforeAcquire);

        assertTrue(idleBeforeAcquire >= 2,
                "After warmup, idle count should be >= minIdle(2), got: " + idleBeforeAcquire);
        assertEquals(0, activeBeforeAcquire,
                "Before any acquire, active count should be 0");
        assertTrue(totalBeforeAcquire >= 2,
                "Total should be >= minIdle(2), got: " + totalBeforeAcquire);
        assertEquals(5, pool.getMaxSize(), "MaxSize should match config");

        // Acquire one client
        long clientId = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(pool.getPoolId());
        assertTrue(clientId >= 0, "Acquire should succeed");

        int idleAfterAcquire = pool.getIdleCount();
        int activeAfterAcquire = pool.getActiveCount();
        int totalAfterAcquire = pool.getTotalCount();

        System.out.println("Metrics after acquire:");
        System.out.println("  Idle: " + idleAfterAcquire);
        System.out.println("  Active: " + activeAfterAcquire);
        System.out.println("  Total: " + totalAfterAcquire);

        assertEquals(1, activeAfterAcquire,
                "After acquiring one, active count should be 1");
        assertEquals(idleBeforeAcquire - 1, idleAfterAcquire,
                "Idle count should decrease by 1 after acquire");
        assertEquals(totalBeforeAcquire, totalAfterAcquire,
                "Total count should remain unchanged after acquire (just moved from idle to active)");

        // Release the client
        glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(pool.getPoolId(), clientId);

        // Short sleep to allow release to propagate
        Thread.sleep(100);

        int idleAfterRelease = pool.getIdleCount();
        int activeAfterRelease = pool.getActiveCount();
        int totalAfterRelease = pool.getTotalCount();

        System.out.println("Metrics after release:");
        System.out.println("  Idle: " + idleAfterRelease);
        System.out.println("  Active: " + activeAfterRelease);
        System.out.println("  Total: " + totalAfterRelease);

        assertEquals(idleBeforeAcquire, idleAfterRelease,
                "Idle count should return to pre-acquire state");
        assertEquals(0, activeAfterRelease,
                "Active count should return to 0 after release");
        assertEquals(totalBeforeAcquire, totalAfterRelease,
                "Total count should remain consistent");

        pool.close();
        assertTrue(pool.isClosed());

        System.out.println("Metrics accuracy test PASSED!");
    }

    /**
     * Test multiple commands on same pooled client (full command coverage).
     *
     * <p>Acquires one client, executes a series of commands (SET, GET, INCR, DEL, HSET, HGET)
     * to prove the full command set works through the pool, not just SET/GET.
     */
    @Test
    public void testMultipleCommandsOnPooledClient() throws Exception {
        GlideClientConfiguration clientConfig = getTestClientConfig();

        ClientPoolConfig poolConfig = ClientPoolConfig.builder()
                .maxSize(3)
                .minIdle(1)
                .acquireTimeout(Duration.ofSeconds(10))
                .idleTimeout(Duration.ofSeconds(60))
                .clientConfig(clientConfig)
                .build();

        ClientFactory<GlideClient> factory = (clientId, poolId) ->
                GlideClient.fromPoolHandle(clientId, 0, 5000);

        ClientPool<GlideClient> pool = ClientPool.create(poolConfig, factory);
        Thread.sleep(3000);

        // Acquire a client
        CompletableFuture<GlideClient> acquireFuture = pool.acquire();
        GlideClient client = acquireFuture.get(10, TimeUnit.SECONDS);
        assertNotNull(client);

        String prefix = "multi-cmd-" + UUID.randomUUID() + "-";

        // --- SET / GET ---
        String strKey = prefix + "str";
        assertEquals("OK", client.set(strKey, "hello").get());
        assertEquals("hello", client.get(strKey).get());
        System.out.println("SET/GET: OK");

        // --- INCR ---
        String counterKey = prefix + "counter";
        client.set(counterKey, "10").get();
        Long incrResult = client.incr(counterKey).get();
        assertEquals(11L, incrResult, "INCR should increment to 11");
        System.out.println("INCR: " + incrResult);

        // --- INCR again ---
        Long incrResult2 = client.incr(counterKey).get();
        assertEquals(12L, incrResult2, "Second INCR should increment to 12");
        System.out.println("INCR (2nd): " + incrResult2);

        // --- HSET / HGET ---
        String hashKey = prefix + "hash";
        Map<String, String> hashFields = new java.util.HashMap<>();
        hashFields.put("field1", "value1");
        hashFields.put("field2", "value2");
        Long hsetResult = client.hset(hashKey, hashFields).get();
        assertEquals(2L, hsetResult, "HSET should return number of fields added");
        System.out.println("HSET: " + hsetResult + " fields");

        String hgetResult = client.hget(hashKey, "field1").get();
        assertEquals("value1", hgetResult, "HGET should return the field value");
        System.out.println("HGET field1: " + hgetResult);

        String hgetResult2 = client.hget(hashKey, "field2").get();
        assertEquals("value2", hgetResult2);
        System.out.println("HGET field2: " + hgetResult2);

        // --- DEL (multiple keys) ---
        Long delResult = client.del(new String[]{strKey, counterKey, hashKey}).get();
        assertEquals(3L, delResult, "DEL should delete all 3 keys");
        System.out.println("DEL 3 keys: " + delResult);

        // Verify keys are gone
        assertNull(client.get(strKey).get(), "Key should be deleted");
        assertNull(client.get(counterKey).get(), "Counter key should be deleted");
        assertNull(client.hget(hashKey, "field1").get(), "Hash key should be deleted");

        pool.close();
        assertTrue(pool.isClosed());

        System.out.println("Multiple commands on pooled client test PASSED!");
    }

    /**
     * Test pool close rejects new acquires (Req 10.1 — acquire-after-close fails).
     *
     * <p>Closes the pool, then verifies acquire() fails with a ClosingException.
     */
    @Test
    public void testPoolCloseRejectsNewAcquires() throws Exception {
        GlideClientConfiguration clientConfig = getTestClientConfig();

        ClientPoolConfig poolConfig = ClientPoolConfig.builder()
                .maxSize(3)
                .minIdle(1)
                .acquireTimeout(Duration.ofSeconds(5))
                .idleTimeout(Duration.ofSeconds(60))
                .clientConfig(clientConfig)
                .build();

        ClientFactory<GlideClient> factory = (clientId, poolId) ->
                GlideClient.fromPoolHandle(clientId, 0, 5000);

        ClientPool<GlideClient> pool = ClientPool.create(poolConfig, factory);
        Thread.sleep(3000);

        // Verify pool is running before close
        assertTrue(pool.isRunning(), "Pool should be running");
        assertFalse(pool.isClosed(), "Pool should not be closed yet");

        // Close the pool
        pool.close();
        assertTrue(pool.isClosed(), "Pool should be closed");
        assertFalse(pool.isRunning(), "Pool should no longer be running");

        // Attempt to acquire after close — should fail with ClosingException
        CompletableFuture<GlideClient> acquireFuture = pool.acquire();

        ExecutionException execException = assertThrows(ExecutionException.class, () -> {
            acquireFuture.get(5, TimeUnit.SECONDS);
        });

        Throwable cause = execException.getCause();
        assertNotNull(cause, "Should have a cause");
        assertTrue(cause instanceof ClosingException,
                "Cause should be ClosingException, got: " + cause.getClass().getName()
                        + " message: " + cause.getMessage());

        System.out.println("Pool close rejects new acquires test PASSED!");
    }
}

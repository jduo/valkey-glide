/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.pool;

import glide.api.GlideClient;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.pool.ClientFactory;
import glide.api.models.pool.ClientPool;
import glide.api.models.pool.ClientPoolConfig;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Benchmark comparing pooled vs non-pooled client usage patterns.
 * Measures creation cost amortization — the primary value proposition of pooling.
 */
public class ClientPoolBenchmark {

    private static final String HOST = "localhost";
    private static final int PORT = 6399;
    private static final int ITERATIONS = 50;
    private static final int MICRO_ITERATIONS = 10_000;

    @Test
    public void benchmarkPoolVsCreateDestroy() throws Exception {
        System.out.println("\n=== Pool vs Create/Destroy Benchmark ===");
        System.out.println("Iterations: " + ITERATIONS + " (each does SET + GET)");
        System.out.println();

        // Warm up JVM
        GlideClient warmup = GlideClient.createClient(
                GlideClientConfiguration.builder()
                        .address(NodeAddress.builder().host(HOST).port(PORT).build())
                        .requestTimeout(5000)
                        .build()).get();
        warmup.set("warmup", "warmup").get();
        warmup.close();

        // --- Scenario 1: Create/Destroy per operation ---
        System.out.println("--- Scenario 1: Create new client per operation ---");
        long createDestroyStart = System.nanoTime();

        for (int i = 0; i < ITERATIONS; i++) {
            GlideClient client = GlideClient.createClient(
                    GlideClientConfiguration.builder()
                            .address(NodeAddress.builder().host(HOST).port(PORT).build())
                            .requestTimeout(5000)
                            .build()).get();
            client.set("bench-key-" + i, "value-" + i).get();
            client.get("bench-key-" + i).get();
            client.close();
        }

        long createDestroyElapsed = System.nanoTime() - createDestroyStart;
        double createDestroyMs = createDestroyElapsed / 1_000_000.0;
        double createDestroyPerOp = createDestroyMs / ITERATIONS;

        System.out.printf("  Total: %.1f ms%n", createDestroyMs);
        System.out.printf("  Per operation (create + SET + GET + close): %.2f ms%n", createDestroyPerOp);
        System.out.println();

        // --- Scenario 2: Pool (acquire/release per operation) ---
        System.out.println("--- Scenario 2: Pool acquire/release per operation ---");

        ClientPoolConfig poolConfig = ClientPoolConfig.builder()
                .maxSize(3)
                .minIdle(1)
                .acquireTimeout(Duration.ofSeconds(10))
                .idleTimeout(Duration.ofSeconds(60))
                .clientConfig(GlideClientConfiguration.builder()
                        .address(NodeAddress.builder().host(HOST).port(PORT).build())
                        .requestTimeout(5000)
                        .build())
                .build();

        ClientPool<GlideClient> pool = ClientPool.create(poolConfig,
                (clientId, poolId) -> GlideClient.fromPoolHandle(clientId, 0, 5000));

        // Wait for min_idle warmup
        Thread.sleep(3000);
        System.out.println("  Pool warmed up. Idle: " + pool.getIdleCount() + ", Total: " + pool.getTotalCount());

        long poolStart = System.nanoTime();

        for (int i = 0; i < ITERATIONS; i++) {
            // Acquire from pool (uses client cache — factory called once per client_id)
            long clientId = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(pool.getPoolId());
            if (clientId < 0) throw new RuntimeException("Failed to acquire from pool");

            // Get cached client wrapper (no allocation after first call for this client_id)
            GlideClient client = pool.getOrCreateClient(clientId);
            client.set("bench-key-" + i, "value-" + i).get();
            client.get("bench-key-" + i).get();

            glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(pool.getPoolId(), clientId);
        }

        long poolElapsed = System.nanoTime() - poolStart;
        double poolMs = poolElapsed / 1_000_000.0;
        double poolPerOp = poolMs / ITERATIONS;

        System.out.printf("  Total: %.1f ms%n", poolMs);
        System.out.printf("  Per operation (acquire + SET + GET + release): %.2f ms%n", poolPerOp);
        System.out.println();

        // --- Scenario 3: Single shared client (no pool, no create/destroy) ---
        System.out.println("--- Scenario 3: Single shared client (baseline) ---");

        GlideClient sharedClient = GlideClient.createClient(
                GlideClientConfiguration.builder()
                        .address(NodeAddress.builder().host(HOST).port(PORT).build())
                        .requestTimeout(5000)
                        .build()).get();

        long sharedStart = System.nanoTime();

        for (int i = 0; i < ITERATIONS; i++) {
            sharedClient.set("bench-key-" + i, "value-" + i).get();
            sharedClient.get("bench-key-" + i).get();
        }

        long sharedElapsed = System.nanoTime() - sharedStart;
        double sharedMs = sharedElapsed / 1_000_000.0;
        double sharedPerOp = sharedMs / ITERATIONS;

        System.out.printf("  Total: %.1f ms%n", sharedMs);
        System.out.printf("  Per operation (SET + GET only): %.2f ms%n", sharedPerOp);
        System.out.println();

        // --- Summary ---
        System.out.println("=== Summary ===");
        System.out.printf("  Create/Destroy per op: %.2f ms/op%n", createDestroyPerOp);
        System.out.printf("  Pool acquire/release:  %.2f ms/op%n", poolPerOp);
        System.out.printf("  Shared client (no pool): %.2f ms/op%n", sharedPerOp);
        System.out.printf("  Pool speedup vs create/destroy: %.1fx%n", createDestroyPerOp / poolPerOp);
        System.out.printf("  Pool overhead vs shared client: +%.2f ms/op%n", poolPerOp - sharedPerOp);
        System.out.println();

        // Cleanup
        pool.close();
        sharedClient.close();
    }

    /**
     * Microbenchmark: isolates just the JNI acquire/release cost with NO network I/O.
     * This measures the true pool overhead: JNI transition + Mutex try_lock + VecDeque pop/push.
     */
    @Test
    public void microbenchmarkAcquireRelease() throws Exception {
        System.out.println("\n=== Microbenchmark: Acquire/Release Only (no commands) ===");
        System.out.println("Iterations: " + MICRO_ITERATIONS);
        System.out.println();

        ClientPoolConfig poolConfig = ClientPoolConfig.builder()
                .maxSize(3)
                .minIdle(1)
                .acquireTimeout(Duration.ofSeconds(10))
                .idleTimeout(Duration.ofSeconds(60))
                .clientConfig(GlideClientConfiguration.builder()
                        .address(NodeAddress.builder().host(HOST).port(PORT).build())
                        .requestTimeout(5000)
                        .build())
                .build();

        ClientPool<GlideClient> pool = ClientPool.create(poolConfig,
                (clientId, poolId) -> GlideClient.fromPoolHandle(clientId, 0, 5000));

        // Wait for warmup
        Thread.sleep(3000);
        long poolId = pool.getPoolId();

        // Warmup JNI path
        for (int i = 0; i < 100; i++) {
            long cid = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(poolId);
            if (cid >= 0) {
                glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(poolId, cid);
            }
        }

        // Measure: acquire + release (no commands)
        long start = System.nanoTime();
        int successCount = 0;

        for (int i = 0; i < MICRO_ITERATIONS; i++) {
            long clientId = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(poolId);
            if (clientId >= 0) {
                glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(poolId, clientId);
                successCount++;
            }
        }

        long elapsed = System.nanoTime() - start;
        double totalMs = elapsed / 1_000_000.0;
        double perOpNs = (double) elapsed / successCount;
        double perOpUs = perOpNs / 1_000.0;

        System.out.printf("  Total: %.2f ms for %d iterations%n", totalMs, successCount);
        System.out.printf("  Per acquire+release: %.2f µs (%.0f ns)%n", perOpUs, perOpNs);
        System.out.println();

        // Measure: acquire + getOrCreateClient + release (includes Java-side cache lookup)
        System.out.println("--- With client cache lookup (getOrCreateClient) ---");
        long start2 = System.nanoTime();
        int successCount2 = 0;

        for (int i = 0; i < MICRO_ITERATIONS; i++) {
            long clientId = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(poolId);
            if (clientId >= 0) {
                pool.getOrCreateClient(clientId); // cache hit
                glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(poolId, clientId);
                successCount2++;
            }
        }

        long elapsed2 = System.nanoTime() - start2;
        double totalMs2 = elapsed2 / 1_000_000.0;
        double perOpNs2 = (double) elapsed2 / successCount2;
        double perOpUs2 = perOpNs2 / 1_000.0;

        System.out.printf("  Total: %.2f ms for %d iterations%n", totalMs2, successCount2);
        System.out.printf("  Per acquire+cache+release: %.2f µs (%.0f ns)%n", perOpUs2, perOpNs2);
        System.out.println();

        System.out.printf("  Cache lookup overhead: %.2f µs%n", perOpUs2 - perOpUs);
        System.out.println();

        // Measure: just the ConcurrentHashMap.get() call (no JNI at all)
        System.out.println("--- Isolated cache.get() only (no JNI) ---");
        // Pre-populate: do one acquire/release to ensure client is cached
        long cidForCache = glide.ffi.resolvers.GlidePoolResolver.glidePoolTryAcquire(poolId);
        if (cidForCache >= 0) {
            pool.getOrCreateClient(cidForCache);
            glide.ffi.resolvers.GlidePoolResolver.glidePoolRelease(poolId, cidForCache);
        }

        long start3 = System.nanoTime();
        for (int i = 0; i < MICRO_ITERATIONS; i++) {
            pool.getOrCreateClient(cidForCache); // pure cache.get() hit
        }
        long elapsed3 = System.nanoTime() - start3;
        double perOpNs3 = (double) elapsed3 / MICRO_ITERATIONS;
        double perOpUs3 = perOpNs3 / 1_000.0;
        System.out.printf("  Per cache.get() hit: %.2f µs (%.0f ns)%n", perOpUs3, perOpNs3);
        System.out.println();

        pool.close();
    }
}

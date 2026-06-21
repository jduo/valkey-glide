/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
import glide.api.GlideClusterClient;
import glide.api.models.configuration.AdvancedGlideClusterClientConfiguration;
import glide.api.models.configuration.GlideClusterClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import glide.api.models.configuration.PeriodicChecksManualInterval;
import glide.api.models.configuration.ReadFrom;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * E2E test for synchronous inflight limit check under memory pressure.
 *
 * <p>Run in a Docker container with tight cgroup memory limit (1.4 GB). The test fires requests at
 * high rate using ForkJoinPool.managedBlock, then checks whether the thread pool explodes (FAIL) or
 * stays bounded (PASS).
 *
 * <p>Exit codes: 0 = PASS (thread pool stayed bounded, no stuck state) 1 = FAIL (thread pool
 * exploded or got stuck)
 */
public class InflightPressureTest {

    static final AtomicLong totalGets = new AtomicLong();
    static final AtomicLong totalErrs = new AtomicLong();
    static final AtomicInteger blocking = new AtomicInteger();
    static final AtomicLong latencySumNs = new AtomicLong();
    static final AtomicLong latencySamples = new AtomicLong();

    // Test parameters
    static final int TEST_DURATION_SEC = 60;
    static final int MAX_ACCEPTABLE_POOL_SIZE = 300;
    static final long NS_PER_REQUEST = 2_000_000L; // 500 req/s

    interface Client {
        String get(String key) throws Exception;

        void set(String key, String value) throws Exception;
    }

    public static void main(String[] args) throws Exception {
        String host = args[0];
        int port = Integer.parseInt(args[1]);

        System.out.printf("[INIT] host=%s port=%d%n", host, port);

        // Retry connection (cluster may still be initializing)
        GlideClusterClient glide = null;
        for (int attempt = 1; attempt <= 30 && glide == null; attempt++) {
            try {
                glide =
                        GlideClusterClient.createClient(
                                        GlideClusterClientConfiguration.builder()
                                                .address(NodeAddress.builder().host(host).port(port).build())
                                                .requestTimeout(30)
                                                .readFrom(ReadFrom.AZ_AFFINITY_REPLICAS_AND_PRIMARY)
                                                .advancedConfiguration(
                                                        AdvancedGlideClusterClientConfiguration.builder()
                                                                .connectionTimeout(1000)
                                                                .periodicChecks(
                                                                        PeriodicChecksManualInterval.builder()
                                                                                .durationInSec(60)
                                                                                .build())
                                                                .build())
                                                .build())
                                .get();
            } catch (Exception e) {
                System.out.println("[INIT] waiting for cluster... attempt " + attempt);
                Thread.sleep(2000);
            }
        }
        if (glide == null) {
            System.out.println("[FAIL] Cannot connect");
            System.exit(1);
        }

        final GlideClusterClient client = glide;
        Client c =
                new Client() {
                    public String get(String k) throws Exception {
                        return client.get(k).get();
                    }

                    public void set(String k, String v) throws Exception {
                        client.set(k, v).get();
                    }
                };

        // Pre-populate
        System.out.println("[INIT] pre-populating 1000 keys...");
        for (int i = 0; i < 1000; i++) c.set("k:" + i, "v");
        System.out.println("[INIT] done");

        AtomicInteger tid = new AtomicInteger();
        ForkJoinPool pool =
                new ForkJoinPool(
                        50,
                        p -> {
                            ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(p);
                            t.setName("worker-" + tid.incrementAndGet());
                            t.setDaemon(true);
                            return t;
                        },
                        null,
                        true);

        // Start request generator
        Thread generator = new Thread(() -> requestLoop(pool, c), "request-gen");
        generator.setDaemon(true);
        generator.start();

        // Monitor for TEST_DURATION_SEC
        int maxPoolSize = 0;
        boolean stuck = false;
        long startTime = System.currentTimeMillis();

        while (System.currentTimeMillis() - startTime < TEST_DURATION_SEC * 1000L) {
            Thread.sleep(5000);
            long g = totalGets.get();
            int poolSize = pool.getPoolSize();
            int blk = blocking.get();
            long threads = Thread.activeCount();

            if (poolSize > maxPoolSize) maxPoolSize = poolSize;

            long samples = latencySamples.getAndSet(0);
            long sumNs = latencySumNs.getAndSet(0);
            double avgMs = samples == 0 ? 0.0 : (sumNs / samples) / 1e6;

            String tag = blk > 300 && g == totalGets.get() ? "[STUCK]" : "[MONITOR]";
            if (tag.equals("[STUCK]")) stuck = true;

            System.out.printf(
                    "%s gets=%d errs=%d blocking=%d pool=%d threads=%d avg=%.1fms%n",
                    tag, g, totalErrs.get(), blk, poolSize, threads, avgMs);
        }

        // Verdict
        System.out.printf("%n[RESULT] maxPoolSize=%d stuck=%s%n", maxPoolSize, stuck);

        if (stuck || maxPoolSize > MAX_ACCEPTABLE_POOL_SIZE) {
            System.out.printf(
                    "[FAIL] Thread pool exploded to %d (limit %d) or got stuck%n",
                    maxPoolSize, MAX_ACCEPTABLE_POOL_SIZE);
            System.exit(1);
        } else {
            System.out.printf(
                    "[PASS] Thread pool stayed bounded at %d (limit %d)%n",
                    maxPoolSize, MAX_ACCEPTABLE_POOL_SIZE);
            System.exit(0);
        }
    }

    static void requestLoop(ForkJoinPool pool, Client c) {
        Random rng = new Random();
        long next = System.nanoTime();
        while (true) {
            next += NS_PER_REQUEST;
            int fanout = 1 + rng.nextInt(16);
            for (int i = 0; i < fanout; i++) {
                final int k = rng.nextInt(1000);
                pool.execute(
                        () -> {
                            try {
                                ForkJoinPool.managedBlock(
                                        new ForkJoinPool.ManagedBlocker() {
                                            boolean done = false;

                                            public boolean block() {
                                                blocking.incrementAndGet();
                                                long t0 = System.nanoTime();
                                                try {
                                                    c.get("k:" + k);
                                                    long dt = System.nanoTime() - t0;
                                                    latencySumNs.addAndGet(dt);
                                                    latencySamples.incrementAndGet();
                                                    totalGets.incrementAndGet();
                                                } catch (Exception e) {
                                                    totalErrs.incrementAndGet();
                                                } finally {
                                                    blocking.decrementAndGet();
                                                    done = true;
                                                }
                                                return true;
                                            }

                                            public boolean isReleasable() {
                                                return done;
                                            }
                                        });
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        });
            }
            long sleep = next - System.nanoTime();
            if (sleep > 0) LockSupport.parkNanos(sleep);
        }
    }
}

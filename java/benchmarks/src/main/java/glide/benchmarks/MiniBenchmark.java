/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.benchmarks;

import glide.api.GlideClient;
import glide.api.models.configuration.GlideClientConfiguration;
import glide.api.models.configuration.NodeAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Minimal benchmark for measuring GLIDE Java client throughput. Tests GET, SET, HGET, LRANGE with
 * varying concurrency levels and payload sizes.
 */
public class MiniBenchmark {

    static final int TEST_DURATION_MS = 5000;
    static final int CONCURRENCY = 50;
    static final int[] PAYLOAD_SIZES = {100, 1000, 10000};
    static final String HOST = System.getProperty("benchmark.host", "localhost");
    static final int PORT = Integer.getInteger("benchmark.port", 6379);

    public static void main(String[] args) throws Exception {
        System.out.printf(
                "Mini Benchmark - Host: %s:%d, Duration: %ds, Clients: %d%n%n",
                HOST, PORT, TEST_DURATION_MS / 1000, CONCURRENCY);

        System.out.printf("%-15s %8s %12s %12s%n", "COMMAND", "PAYLOAD", "OPS/SEC", "AVG_LATENCY");
        StringBuilder sep = new StringBuilder();
        for (int i = 0; i < 52; i++) sep.append('-');
        System.out.println(sep);

        for (int payloadSize : PAYLOAD_SIZES) {
            GlideClient setupClient = createClient();
            setupData(setupClient, payloadSize);
            setupClient.close();

            String[] commands = {"SET", "GET", "LRANGE_100", "LRANGE_600"};
            for (String command : commands) {
                runBenchmark(command, CONCURRENCY, payloadSize);
            }
            System.out.println();
        }
    }

    static GlideClient createClient() throws Exception {
        GlideClientConfiguration config =
                GlideClientConfiguration.builder()
                        .address(NodeAddress.builder().host(HOST).port(PORT).build())
                        .build();
        return GlideClient.createClient(config).get();
    }

    static String makePayload(int size) {
        char[] chars = new char[size];
        java.util.Arrays.fill(chars, 'x');
        return new String(chars);
    }

    static void setupData(GlideClient client, int payloadSize) throws Exception {
        String data = makePayload(payloadSize);
        client.del(new String[] {"mylist"}).get();
        for (int i = 0; i < 600; i++) {
            client.lpush("mylist", new String[] {data}).get();
        }
        // Pre-populate a key for GET
        client.set("bench_key", data).get();
    }

    static void runBenchmark(String command, int concurrency, int payloadSize) throws Exception {
        List<GlideClient> clients = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            clients.add(createClient());
        }

        String data = makePayload(payloadSize);
        AtomicLong totalOps = new AtomicLong();
        AtomicLong totalLatencyNs = new AtomicLong();
        AtomicLong totalErrors = new AtomicLong();

        long startTime = System.nanoTime();
        long endTime = startTime + (long) TEST_DURATION_MS * 1_000_000L;

        CompletableFuture<?>[] tasks = new CompletableFuture[concurrency];
        for (int i = 0; i < concurrency; i++) {
            final GlideClient client = clients.get(i);
            final String key = "key:" + i;
            tasks[i] =
                    CompletableFuture.runAsync(
                            () -> {
                                while (System.nanoTime() < endTime) {
                                    long opStart = System.nanoTime();
                                    try {
                                        executeCommand(client, command, key, data);
                                        long latency = System.nanoTime() - opStart;
                                        totalOps.incrementAndGet();
                                        totalLatencyNs.addAndGet(latency);
                                    } catch (Exception e) {
                                        totalErrors.incrementAndGet();
                                    }
                                }
                            });
        }

        CompletableFuture.allOf(tasks).join();
        long actualDurationNs = System.nanoTime() - startTime;

        long ops = totalOps.get();
        double opsPerSec = ops / (actualDurationNs / 1_000_000_000.0);
        long avgLatencyNs = ops > 0 ? totalLatencyNs.get() / ops : 0;

        String label = payloadSize >= 1000 ? (payloadSize / 1000) + "KB" : payloadSize + "B";
        System.out.printf(
                "%-15s %8s %12.0f %12s%n", command, label, opsPerSec, formatLatency(avgLatencyNs));

        if (totalErrors.get() > 0) {
            System.out.printf("             -> Errors: %d%n", totalErrors.get());
        }

        for (GlideClient client : clients) {
            client.close();
        }
    }

    static void executeCommand(GlideClient client, String command, String key, String data)
            throws Exception {
        switch (command) {
            case "SET":
                client.set(key, data).get();
                break;
            case "GET":
                client.get(key).get();
                break;
            case "LRANGE_100":
                client.lrange("mylist", 0, 99).get();
                break;
            case "LRANGE_600":
                client.lrange("mylist", 0, 599).get();
                break;
            default:
                throw new IllegalArgumentException("Unknown command: " + command);
        }
    }

    static String formatLatency(long ns) {
        if (ns < 1000) return ns + "ns";
        if (ns < 1_000_000) return String.format("%.1fus", ns / 1000.0);
        return String.format("%.2fms", ns / 1_000_000.0);
    }
}

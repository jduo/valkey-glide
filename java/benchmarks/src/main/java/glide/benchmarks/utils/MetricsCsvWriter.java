/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.benchmarks.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public class MetricsCsvWriter {

    private final String clientName;
    private final int dataSize;
    private final int targetTps;
    private final long startTimeNanos;
    private final long startTimeEpochMs;
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Interval tracking (reset each collection interval)
    private final int intervalSeconds;
    private final ConcurrentLinkedQueue<Long> intervalLatenciesNanos = new ConcurrentLinkedQueue<>();
    private final LongAdder intervalSuccessOps = new LongAdder();
    private final LongAdder intervalTimeoutErrors = new LongAdder();
    private final LongAdder intervalOtherErrors = new LongAdder();

    // Interval GET/SET/DEL tracking
    private final LongAdder intervalGetOps = new LongAdder();
    private final LongAdder intervalSetOps = new LongAdder();
    private final LongAdder intervalDelOps = new LongAdder(); // <-- ADD

    // Cumulative tracking
    private final AtomicLong totalSuccessOps = new AtomicLong(0);
    private final AtomicLong totalTimeoutErrors = new AtomicLong(0);
    private final AtomicLong totalOtherErrors = new AtomicLong(0);

    // Cumulative GET/SET/DEL tracking
    private final AtomicLong totalGetOps = new AtomicLong(0);
    private final AtomicLong totalSetOps = new AtomicLong(0);
    private final AtomicLong totalDelOps = new AtomicLong(0); // <-- ADD

    // CSV writer
    private PrintWriter csvWriter;
    private String csvFilename;

    // Scheduler
    private ScheduledExecutorService scheduler;

    public MetricsCsvWriter(
            String clientName,
            String outputDir,
            int dataSize,
            int targetTps,
            int intervalSeconds,
            boolean tcpNoDelay) {

        this.clientName = clientName;
        this.dataSize = dataSize;
        this.targetTps = targetTps;
        this.intervalSeconds = intervalSeconds;
        this.startTimeNanos = System.nanoTime();
        this.startTimeEpochMs = System.currentTimeMillis();

        // Create output directory
        new File(outputDir).mkdirs();

        // Initialize CSV file
        try {
            csvFilename =
                    String.format(
                            "%s/benchmark_%s_%d_%d_tcpnodelay-%s_%d.csv",
                            outputDir, clientName, dataSize, targetTps, tcpNoDelay, startTimeEpochMs);
            csvWriter = new PrintWriter(new FileWriter(csvFilename));

            csvWriter.println(
                    String.join(
                            ",",
                            "timestamp",
                            "elapsed_hours",
                            "interval_success_ops",
                            "interval_gets",
                            "interval_sets",
                            "interval_dels", // <-- ADD
                            "interval_timeout_errors",
                            "interval_other_errors",
                            "total_success_ops",
                            "total_gets",
                            "total_sets",
                            "total_dels", // <-- ADD
                            "total_timeout_errors",
                            "total_other_errors",
                            "timeout_rate_percent",
                            "interval_tps",
                            "avg_latency_ms",
                            "min_latency_ms",
                            "p50_latency_ms",
                            "p90_latency_ms",
                            "p99_latency_ms",
                            "p999_latency_ms",
                            "max_latency_ms"));
            csvWriter.flush();

            System.out.println("Metrics CSV file: " + csvFilename);
        } catch (IOException e) {
            System.err.println("Failed to create CSV file: " + e.getMessage());
        }
    }

    public void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        scheduler.scheduleAtFixedRate(
                this::collectAndWrite, intervalSeconds, intervalSeconds, TimeUnit.SECONDS);

        System.out.printf("Metrics collection started (every %d seconds)%n", intervalSeconds);
    }

    /** Record a successful operation with its latency and action type */
    public void recordSuccess(long latencyNanos, ChosenAction action) {
        intervalLatenciesNanos.add(latencyNanos);
        intervalSuccessOps.increment();

        // Track GET vs SET vs DEL
        if (action == ChosenAction.SET) {
            intervalSetOps.increment();
        } else if (action == ChosenAction.GET_EXISTING || action == ChosenAction.GET_NON_EXISTING) {
            intervalGetOps.increment();
        } else if (action == ChosenAction.DEL) { // <-- ADD
            intervalDelOps.increment();
        }
    }

    /** Record a timeout error */
    public void recordTimeoutError() {
        intervalTimeoutErrors.increment();
    }

    /** Record other (non-timeout) errors */
    public void recordOtherError() {
        intervalOtherErrors.increment();
    }

    private void collectAndWrite() {
        try {
            double elapsedHours = (System.nanoTime() - startTimeNanos) / 1e9 / 3600;

            // Get and reset interval counters
            long successOps = intervalSuccessOps.sumThenReset();
            long timeoutErrors = intervalTimeoutErrors.sumThenReset();
            long otherErrors = intervalOtherErrors.sumThenReset();

            // Get and reset GET/SET/DEL counters
            long getOps = intervalGetOps.sumThenReset();
            long setOps = intervalSetOps.sumThenReset();
            long delOps = intervalDelOps.sumThenReset(); // <-- ADD

            // Update totals
            long totalSuccessNow = totalSuccessOps.addAndGet(successOps);
            long totalTimeoutsNow = totalTimeoutErrors.addAndGet(timeoutErrors);
            long totalOtherNow = totalOtherErrors.addAndGet(otherErrors);

            // Update GET/SET/DEL totals
            long totalGetsNow = totalGetOps.addAndGet(getOps);
            long totalSetsNow = totalSetOps.addAndGet(setOps);
            long totalDelsNow = totalDelOps.addAndGet(delOps); // <-- ADD

            // Collect latencies for this interval
            List<Long> latencies = new ArrayList<>();
            Long lat;
            while ((lat = intervalLatenciesNanos.poll()) != null) {
                latencies.add(lat);
            }

            // Calculate latency stats
            double avgMs = 0, minMs = 0, p50Ms = 0, p90Ms = 0, p99Ms = 0, p999Ms = 0, maxMs = 0;
            double intervalTps = (double) successOps / intervalSeconds;

            if (!latencies.isEmpty()) {
                Collections.sort(latencies);
                int size = latencies.size();

                long sum = 0;
                for (Long l : latencies) {
                    sum += l;
                }
                avgMs = (sum / (double) size) / 1_000_000.0;
                minMs = latencies.get(0) / 1_000_000.0;
                maxMs = latencies.get(size - 1) / 1_000_000.0;
                p50Ms = latencies.get((int) (size * 0.50)) / 1_000_000.0;
                p90Ms = latencies.get((int) (size * 0.90)) / 1_000_000.0;
                p99Ms = latencies.get(Math.min((int) (size * 0.99), size - 1)) / 1_000_000.0;
                p999Ms = latencies.get(Math.min((int) (size * 0.999), size - 1)) / 1_000_000.0;
            }

            // Calculate timeout rate
            long totalOpsInterval = successOps + timeoutErrors + otherErrors;
            double timeoutRatePercent =
                    totalOpsInterval > 0 ? (timeoutErrors * 100.0 / totalOpsInterval) : 0;

            // Write to CSV
            if (csvWriter != null) {
                csvWriter.printf(
                        "%s,%.4f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%.4f,%.1f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f%n",
                        LocalDateTime.now().format(formatter),
                        elapsedHours,
                        successOps,
                        getOps,
                        setOps,
                        delOps, // <-- ADD
                        timeoutErrors,
                        otherErrors,
                        totalSuccessNow,
                        totalGetsNow,
                        totalSetsNow,
                        totalDelsNow, // <-- ADD
                        totalTimeoutsNow,
                        totalOtherNow,
                        timeoutRatePercent,
                        intervalTps,
                        avgMs,
                        minMs,
                        p50Ms,
                        p90Ms,
                        p99Ms,
                        p999Ms,
                        maxMs);
                csvWriter.flush();
            }

            // Console output
            System.out.printf(
                    "[%s] %.2fh | TPS: %.0f | Success: %,d (GET: %d, SET: %d, DEL: %d) "
                            + "| Timeouts: %d (total: %d) | Other Errors: %d (total: %d) " // <-- ADD
                            + "| P50: %.1fms | P99: %.1fms | Max: %.1fms%n",
                    LocalDateTime.now().format(formatter),
                    elapsedHours,
                    intervalTps,
                    totalSuccessNow,
                    getOps,
                    setOps,
                    delOps,
                    timeoutErrors,
                    totalTimeoutsNow,
                    otherErrors, // <-- ADD
                    totalOtherNow, // <-- ADD
                    p50Ms,
                    p99Ms,
                    maxMs);

        } catch (Exception e) {
            System.err.println("Error collecting metrics: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }

        // Final collection
        collectAndWrite();

        if (csvWriter != null) {
            csvWriter.close();
        }

        // Print summary
        printSummary();
    }

    private void printSummary() {
        double totalHours = (System.nanoTime() - startTimeNanos) / 1e9 / 3600;
        long totalOps = totalSuccessOps.get() + totalTimeoutErrors.get() + totalOtherErrors.get();
        double avgTps = totalSuccessOps.get() / ((System.nanoTime() - startTimeNanos) / 1e9);
        double timeoutRate = totalOps > 0 ? (totalTimeoutErrors.get() * 100.0 / totalOps) : 0;

        System.out.println("\n" + "=".repeat(60));
        System.out.println("BENCHMARK SUMMARY");
        System.out.println("=".repeat(60));
        System.out.printf("Client:              %s%n", clientName);
        System.out.printf("Duration:            %.2f hours%n", totalHours);
        System.out.printf("Data size:           %d bytes%n", dataSize);
        System.out.printf("Target TPS:          %d%n", targetTps);
        System.out.println("-".repeat(60));
        System.out.printf("Total operations:    %,d%n", totalOps);
        System.out.printf("Successful ops:      %,d%n", totalSuccessOps.get());
        System.out.printf("  - GET ops:         %,d%n", totalGetOps.get());
        System.out.printf("  - SET ops:         %,d%n", totalSetOps.get());
        System.out.printf("  - DEL ops:         %,d%n", totalDelOps.get()); // <-- ADD
        System.out.printf("Timeout errors:      %,d (%.4f%%)%n", totalTimeoutErrors.get(), timeoutRate);
        System.out.printf("Other errors:        %,d%n", totalOtherErrors.get());
        System.out.printf("Average TPS:         %.1f%n", avgTps);
        System.out.println("-".repeat(60));
        System.out.printf("CSV file:            %s%n", csvFilename);
        System.out.println("=".repeat(60) + "\n");
    }

    // Getters for final stats
    public long getTotalSuccessOps() {
        return totalSuccessOps.get();
    }

    public long getTotalTimeoutErrors() {
        return totalTimeoutErrors.get();
    }

    public long getTotalOtherErrors() {
        return totalOtherErrors.get();
    }

    public long getTotalGetOps() {
        return totalGetOps.get();
    }

    public long getTotalSetOps() {
        return totalSetOps.get();
    }

    public long getTotalDelOps() { // <-- ADD
        return totalDelOps.get();
    }
}

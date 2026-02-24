/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.internal;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class AsyncRegistry {

    private static final ConcurrentHashMap<Long, CompletableFuture<Object>> activeFutures =
            new ConcurrentHashMap<>(estimateInitialCapacity());

    private static final ConcurrentHashMap<Long, java.util.concurrent.atomic.AtomicInteger>
            clientInflightCounts = new ConcurrentHashMap<>();

    private static final AtomicLong nextId = new AtomicLong(1);

    // ==================== MONITORING COUNTERS ====================
    private static final AtomicLong totalRegistered = new AtomicLong(0);
    private static final AtomicLong totalCompleted = new AtomicLong(0);
    private static final AtomicLong totalErrors = new AtomicLong(0);
    private static final DateTimeFormatter ts = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // Track registration time per correlationId to detect stuck futures
    private static final ConcurrentHashMap<Long, Long> registrationTimes = new ConcurrentHashMap<>();

    private static final ScheduledExecutorService monitor;

    static {
        monitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "AsyncRegistry-Monitor");
            t.setDaemon(true);
            return t;
        });
        monitor.scheduleAtFixedRate(AsyncRegistry::printMonitorStats, 10, 10, TimeUnit.SECONDS);
    }

    private static void printMonitorStats() {
        int pending = activeFutures.size();
        long registered = totalRegistered.get();
        long completed = totalCompleted.get();
        long errors = totalErrors.get();

        // Always print if there are pending futures, otherwise print every 6th time (once per minute)
        if (pending > 0) {
            System.err.printf(
                    "[%s] ASYNC REGISTRY | pending=%d | registered=%d | completed=%d | errors=%d%n",
                    LocalDateTime.now().format(ts), pending, registered, completed, errors);

            // Check for stuck futures (registered more than 5 seconds ago)
            long now = System.currentTimeMillis();
            registrationTimes.forEach((id, regTime) -> {
                long age = now - regTime;
                if (age > 5000) {
                    System.err.printf(
                            "[%s] STUCK FUTURE | correlationId=%d | stuck for %dms%n",
                            LocalDateTime.now().format(ts), id, age);
                }
            });
        }
    }
    // ==================== END MONITORING ====================

    private static int estimateInitialCapacity() {
        String env = System.getenv("GLIDE_MAX_INFLIGHT_REQUESTS");
        if (env != null) {
            try {
                int v = Integer.parseInt(env.trim());
                if (v > 0) return Math.max(16, v * 2);
            } catch (NumberFormatException ignored) {
            }
        }

        String prop = System.getProperty("glide.maxInflightRequests");
        if (prop != null) {
            try {
                int v = Integer.parseInt(prop.trim());
                if (v > 0) return Math.max(16, v * 2);
            } catch (NumberFormatException ignored) {
            }
        }

        return 2000;
    }

    public static <T> long register(
            CompletableFuture<T> future, int maxInflightRequests, long clientHandle) {
        if (future == null) {
            throw new IllegalArgumentException("Future cannot be null");
        }

        if (maxInflightRequests > 0) {
            clientInflightCounts.compute(
                    clientHandle,
                    (key, counter) -> {
                        java.util.concurrent.atomic.AtomicInteger value =
                                counter != null ? counter : new java.util.concurrent.atomic.AtomicInteger(0);

                        int updated = value.incrementAndGet();
                        if (updated > maxInflightRequests) {
                            value.decrementAndGet();
                            throw new glide.api.models.exceptions.RequestException(
                                    "Client reached maximum inflight requests");
                        }

                        return value;
                    });
        }

        long correlationId = nextId.getAndIncrement();
        totalRegistered.incrementAndGet();                          // <-- ADD
        registrationTimes.put(correlationId, System.currentTimeMillis());  // <-- ADD

        @SuppressWarnings("unchecked")
        CompletableFuture<Object> originalFuture = (CompletableFuture<Object>) future;

        activeFutures.put(correlationId, originalFuture);

        originalFuture.whenComplete(
                (result, throwable) -> {
                    activeFutures.remove(correlationId);
                    registrationTimes.remove(correlationId);        // <-- ADD

                    if (throwable != null) {                        // <-- ADD
                        totalErrors.incrementAndGet();
                    } else {
                        totalCompleted.incrementAndGet();
                    }

                    if (maxInflightRequests > 0) {
                        clientInflightCounts.compute(
                                clientHandle,
                                (key, counter) -> {
                                    if (counter == null) {
                                        return null;
                                    }

                                    int remaining = counter.decrementAndGet();

                                    return remaining <= 0 ? null : counter;
                                });
                    }
                });

        return correlationId;
    }

    public static boolean completeCallback(long correlationId, Object result) {
        CompletableFuture<Object> future = activeFutures.get(correlationId);

        if (future == null) {
            return false;
        }

        boolean completed = future.complete(result);

        return completed;
    }

    public static boolean completeCallbackWithErrorCode(
            long correlationId, int errorTypeCode, String errorMessage) {
        CompletableFuture<Object> future = activeFutures.get(correlationId);
        if (future == null) {
            return false;
        }

        String msg =
                (errorMessage == null || errorMessage.isBlank())
                        ? "Unknown error from native code"
                        : errorMessage;

        RuntimeException ex;
        switch (errorTypeCode) {
            case 2: // TIMEOUT
                ex = new glide.api.models.exceptions.TimeoutException(msg);
                break;
            case 3: // DISCONNECT
                ex = new glide.api.models.exceptions.ClosingException(msg);
                break;
            case 1: // EXEC_ABORT
                ex = new glide.api.models.exceptions.ExecAbortException(msg);
                break;
            case 0: // UNSPECIFIED
            default:
                ex = new glide.api.models.exceptions.RequestException(msg);
                break;
        }

        return future.completeExceptionally(ex);
    }

    public static int getPendingCount() {
        return activeFutures.size();
    }

    public static void shutdown() {
        monitor.shutdownNow();                                      // <-- ADD
        activeFutures
                .values()
                .forEach(
                        future -> {
                            if (!future.isDone()) {
                                future.cancel(true);
                            }
                        });

        activeFutures.clear();
        clientInflightCounts.clear();
        registrationTimes.clear();                                  // <-- ADD
    }

    public static void cleanupClient(long clientHandle) {
        clientInflightCounts.remove(clientHandle);
    }

    public static void reset() {
        activeFutures.clear();
        clientInflightCounts.clear();
        registrationTimes.clear();                                  // <-- ADD
        nextId.set(1);
        totalRegistered.set(0);                                     // <-- ADD
        totalCompleted.set(0);                                      // <-- ADD
        totalErrors.set(0);                                         // <-- ADD
    }

    private static final Thread shutdownHook =
            new Thread(AsyncRegistry::shutdown, "AsyncRegistry-Shutdown");

    static {
        if (!"false".equalsIgnoreCase(System.getProperty("glide.autoShutdownHook", "true"))) {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }
    }

    public static void removeShutdownHook() {
        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
        }
    }
}
/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.pool;

import glide.api.models.configuration.BaseClientConfiguration;
import java.time.Duration;
import lombok.Builder;
import lombok.Getter;

/**
 * Configuration for the client-instance pool.
 *
 * <p>Controls pool sizing, timeouts, and behavior. All clients in a pool share
 * the same BaseClientConfiguration (same server, credentials, protocol).
 */
@Getter
@Builder
public class ClientPoolConfig {

    /** Maximum number of clients in the pool. Must be >= 1. Default: 10. */
    @Builder.Default private final int maxSize = 10;

    /** Minimum idle clients to maintain. Must be >= 0 and <= maxSize. Default: 1. */
    @Builder.Default private final int minIdle = 1;

    /** Evict idle clients after this duration. Must be > 0. Default: 300 seconds. */
    @Builder.Default private final Duration idleTimeout = Duration.ofSeconds(300);

    /**
     * Maximum time to wait for a client when the pool is exhausted. The acquire retry loop
     * will give up after this duration. Must be > 0. Default: 5 seconds.
     */
    @Builder.Default private final Duration acquireTimeout = Duration.ofSeconds(5);

    /**
     * Warn about clients borrowed longer than this threshold. Used for leak detection.
     * Default: 300 seconds.
     */
    @Builder.Default private final Duration leakDetectionThreshold = Duration.ofSeconds(300);

    /**
     * Request timeout used for cleanup timeout calculation (2×). Sent to Rust
     * for determining how long to wait for subscription cleanup confirmations.
     * Default: 5 seconds.
     */
    @Builder.Default private final Duration requestTimeout = Duration.ofSeconds(5);

    /** The client configuration used to create new clients in the pool. Required. */
    private final BaseClientConfiguration clientConfig;

    /** Shutdown mode for the pool. */
    public enum ShutdownMode {
        GRACEFUL,
        IMMEDIATE
    }

    /** How the pool shuts down. Default: GRACEFUL. */
    @Builder.Default private final ShutdownMode shutdownMode = ShutdownMode.GRACEFUL;

    /**
     * Validates this configuration.
     *
     * @throws IllegalArgumentException if any parameter is invalid
     */
    public void validate() {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be >= 1, got: " + maxSize);
        }
        if (minIdle < 0 || minIdle > maxSize) {
            throw new IllegalArgumentException(
                    "minIdle must be >= 0 and <= maxSize (" + maxSize + "), got: " + minIdle);
        }
        if (idleTimeout.isZero() || idleTimeout.isNegative()) {
            throw new IllegalArgumentException("idleTimeout must be > 0, got: " + idleTimeout);
        }
        if (acquireTimeout.isZero() || acquireTimeout.isNegative()) {
            throw new IllegalArgumentException("acquireTimeout must be > 0, got: " + acquireTimeout);
        }
        if (leakDetectionThreshold.isZero() || leakDetectionThreshold.isNegative()) {
            throw new IllegalArgumentException(
                    "leakDetectionThreshold must be > 0, got: " + leakDetectionThreshold);
        }
        if (requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be > 0, got: " + requestTimeout);
        }
        if (clientConfig == null) {
            throw new IllegalArgumentException("clientConfig must not be null");
        }
    }
}

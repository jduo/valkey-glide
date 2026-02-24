/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.benchmarks.utils;

/** Valkey-client settings */
public class ConnectionSettings {
    public final String host;
    public final int port;
    public final boolean useSsl;
    public final boolean clusterMode;
    public final boolean tcpNoDelay; // ADD THIS

    public ConnectionSettings(
            String host, int port, boolean useSsl, boolean clusterMode, boolean tcpNoDelay) {
        this.host = host;
        this.port = port;
        this.useSsl = useSsl;
        this.clusterMode = clusterMode;
        this.tcpNoDelay = tcpNoDelay;
    }
}

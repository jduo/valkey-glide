/** Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0 */
package glide.api.models.pool;

import glide.api.BaseClient;

/**
 * Factory interface for wrapping a Rust-managed client_id into a Java proxy type.
 *
 * <p>The Rust pool creates CoreClient instances directly. This factory provides
 * the type mapping so Java knows which proxy to instantiate for a given client_id.
 *
 * @param <T> the client type (GlideClient or GlideClusterClient)
 */
@FunctionalInterface
public interface ClientFactory<T extends BaseClient> {

    /**
     * Wraps a Rust-managed client_id into a Java proxy of type T.
     *
     * @param clientId the Rust-assigned client identifier
     * @param poolId the parent pool handle
     * @return a Java proxy wrapping the client_id
     */
    T wrapClientId(long clientId, long poolId);
}

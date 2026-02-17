use redis::{aio::MultiplexedConnection, Client as RedisClient, RedisResult, RedisError, ErrorKind};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Handle to a dedicated connection set
#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq)]
pub struct ConnectionHandle(u64);

impl ConnectionHandle {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

/// Connection state tracking
struct ManagedConnection {
    conn: MultiplexedConnection,
    node_id: String,
    is_healthy: bool,
}

impl ManagedConnection {
    fn new(conn: MultiplexedConnection, node_id: String) -> Self {
        Self {
            conn,
            node_id,
            is_healthy: true,
        }
    }
    
    fn mark_unhealthy(&mut self) {
        self.is_healthy = false;
    }
}

/// Pool of dedicated connections per node
struct NodeConnectionPool {
    available: Vec<ManagedConnection>,
    in_use: usize,
}

impl NodeConnectionPool {
    fn new() -> Self {
        Self {
            available: Vec::new(),
            in_use: 0,
        }
    }

    async fn acquire(&mut self, client: &RedisClient, node_id: &str) -> RedisResult<ManagedConnection> {
        // Try to reuse healthy connection
        while let Some(mut managed) = self.available.pop() {
            if managed.is_healthy {
                self.in_use += 1;
                return Ok(managed);
            }
            // Discard unhealthy connection
        }
        
        // Create new connection
        self.in_use += 1;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(ManagedConnection::new(conn, node_id.to_string()))
    }

    fn release(&mut self, conn: ManagedConnection) {
        self.in_use = self.in_use.saturating_sub(1);
        if conn.is_healthy {
            self.available.push(conn);
        }
        // Unhealthy connections are dropped
    }
}

/// Manages dedicated connection sets for WATCH/transaction isolation
pub struct ConnectionPool {
    // Map from handle to node connections
    dedicated_sets: Arc<RwLock<HashMap<ConnectionHandle, HashMap<String, ManagedConnection>>>>,
    // Pool of available connections per node
    pools: Arc<RwLock<HashMap<String, NodeConnectionPool>>>,
    // Redis clients per node for creating connections
    clients: Arc<RwLock<HashMap<String, RedisClient>>>,
}
    dedicated_sets: Arc<RwLock<HashMap<ConnectionHandle, HashMap<String, MultiplexedConnection>>>>,
    // Pool of available connections per node
    pools: Arc<RwLock<HashMap<String, NodeConnectionPool>>>,
}

impl ConnectionPool {
    pub fn new() -> Self {
        Self {
            dedicated_sets: Arc::new(RwLock::new(HashMap::new())),
            pools: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a Redis client for a node
    pub async fn register_node(&self, node_id: String, client: RedisClient) {
        let mut clients = self.clients.write().await;
        clients.insert(node_id, client);
    }

    /// Acquire a dedicated connection handle
    pub async fn acquire_dedicated(&self) -> ConnectionHandle {
        let handle = ConnectionHandle::new();
        let mut sets = self.dedicated_sets.write().await;
        sets.insert(handle, HashMap::new());
        handle
    }

    /// Get or create a dedicated connection for a specific node
    pub async fn get_connection(
        &self,
        handle: ConnectionHandle,
        node_id: &str,
    ) -> RedisResult<MultiplexedConnection> {
        let mut sets = self.dedicated_sets.write().await;
        let node_conns = sets.get_mut(&handle)
            .ok_or_else(|| RedisError::from((ErrorKind::ClientError, "Invalid handle")))?;

        // Check if we already have a connection to this node
        if let Some(managed) = node_conns.get(node_id) {
            if managed.is_healthy {
                return Ok(managed.conn.clone());
            }
            // Connection is unhealthy, will recreate below
        }

        // Get or create connection from pool
        let clients = self.clients.read().await;
        let client = clients.get(node_id)
            .ok_or_else(|| RedisError::from((ErrorKind::ClientError, "Node not found")))?;

        let mut pools = self.pools.write().await;
        let pool = pools.entry(node_id.to_string()).or_insert_with(NodeConnectionPool::new);
        let managed = pool.acquire(client, node_id).await?;
        
        let conn = managed.conn.clone();
        node_conns.insert(node_id.to_string(), managed);
        Ok(conn)
    }

    /// Release all connections associated with a handle
    pub async fn release_dedicated(&self, handle: ConnectionHandle) {
        let mut sets = self.dedicated_sets.write().await;
        if let Some(node_conns) = sets.remove(&handle) {
            let mut pools = self.pools.write().await;
            for (node_id, managed) in node_conns {
                if let Some(pool) = pools.get_mut(&node_id) {
                    pool.release(managed);
                }
            }
        }
    }

    /// Handle failover: mark old node connections as unhealthy and remap to new node
    pub async fn handle_failover(&self, old_node_id: &str, new_node_id: &str) {
        let mut sets = self.dedicated_sets.write().await;
        
        // Mark all connections to old node as unhealthy
        for (_handle, node_conns) in sets.iter_mut() {
            if let Some(managed) = node_conns.get_mut(old_node_id) {
                managed.mark_unhealthy();
            }
        }
        
        // Clean up pool for old node
        let mut pools = self.pools.write().await;
        if let Some(mut old_pool) = pools.remove(old_node_id) {
            // Mark all pooled connections as unhealthy so they get discarded
            for managed in &mut old_pool.available {
                managed.mark_unhealthy();
            }
        }
    }

    /// Handle reconnection: mark connection as unhealthy, will be recreated on next use
    pub async fn mark_connection_unhealthy(&self, handle: ConnectionHandle, node_id: &str) {
        let mut sets = self.dedicated_sets.write().await;
        if let Some(node_conns) = sets.get_mut(&handle) {
            if let Some(managed) = node_conns.get_mut(node_id) {
                managed.mark_unhealthy();
            }
        }
    }

    /// Handle topology change: remove connections to nodes no longer in cluster
    pub async fn handle_topology_change(&self, active_nodes: &[String]) {
        let active_set: std::collections::HashSet<_> = active_nodes.iter().collect();
        
        let mut sets = self.dedicated_sets.write().await;
        
        // Remove connections to nodes no longer in cluster
        for (_handle, node_conns) in sets.iter_mut() {
            node_conns.retain(|node_id, managed| {
                let should_keep = active_set.contains(&node_id);
                if !should_keep {
                    // Mark as unhealthy so it gets cleaned up
                    managed.mark_unhealthy();
                }
                should_keep
            });
        }
        
        // Clean up pools for removed nodes
        let mut pools = self.pools.write().await;
        pools.retain(|node_id, _| active_set.contains(&node_id));
    }
}

impl Default for ConnectionPool {
    fn default() -> Self {
        Self::new()
    }
}

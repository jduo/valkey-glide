// Integration with existing client failover logic

use crate::connection_pool::ConnectionPool;

impl Client {
    // Existing failover handler - add connection pool notification
    async fn handle_cluster_failover(&self, old_primary: &str, new_primary: &str) {
        // Existing failover logic...
        self.update_routing_table(old_primary, new_primary).await;
        
        // NEW: Notify connection pool about failover
        self.connection_pool.handle_failover(old_primary, new_primary).await;
        
        log_info("Failover", format!("Migrated connections from {} to {}", old_primary, new_primary));
    }
    
    // Existing reconnection handler - add connection pool notification
    async fn handle_connection_error(&self, node_id: &str, handle: Option<ConnectionHandle>) {
        // Existing reconnection logic...
        
        // NEW: Mark connection as unhealthy if it's a dedicated connection
        if let Some(h) = handle {
            self.connection_pool.mark_connection_unhealthy(h, node_id).await;
        }
        
        // Connection will be recreated on next command
    }
    
    // Existing topology update handler - add connection pool notification
    async fn handle_topology_update(&self, new_topology: &ClusterTopology) {
        // Existing topology update logic...
        self.routing_table = new_topology.clone();
        
        // NEW: Notify connection pool about topology changes
        let active_nodes: Vec<String> = new_topology.nodes.keys().cloned().collect();
        self.connection_pool.handle_topology_change(&active_nodes).await;
        
        log_info("Topology", format!("Updated to {} nodes", active_nodes.len()));
    }
}

// Command execution with failover support
impl Client {
    pub async fn send_command_dedicated(
        &self,
        cmd: &redis::Cmd,
        handle: ConnectionHandle,
    ) -> RedisResult<Value> {
        // Determine target node from routing
        let node_id = self.get_node_for_command(cmd)?;
        
        // Get connection (will create if needed, or reuse healthy one)
        let mut conn = self.connection_pool.get_connection(handle, &node_id).await?;
        
        // Execute command with retry on connection error
        match cmd.query_async(&mut conn).await {
            Ok(value) => Ok(value),
            Err(e) if is_connection_error(&e) => {
                // Mark connection as unhealthy
                self.connection_pool.mark_connection_unhealthy(handle, &node_id).await;
                
                // Retry once with new connection
                let mut new_conn = self.connection_pool.get_connection(handle, &node_id).await?;
                cmd.query_async(&mut new_conn).await
            }
            Err(e) => Err(e),
        }
    }
}

fn is_connection_error(err: &RedisError) -> bool {
    matches!(
        err.kind(),
        ErrorKind::IoError | ErrorKind::ConnectionRefused | ErrorKind::Timeout
    )
}

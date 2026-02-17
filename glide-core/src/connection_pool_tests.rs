use crate::connection_pool::{ConnectionPool, ConnectionHandle};
use redis::Client as RedisClient;

#[tokio::test]
async fn test_failover_handling() {
    let pool = ConnectionPool::new();
    
    // Setup: Register two nodes
    let primary_client = RedisClient::open("redis://primary:6379").unwrap();
    let replica_client = RedisClient::open("redis://replica:6379").unwrap();
    
    pool.register_node("primary".to_string(), primary_client).await;
    pool.register_node("replica".to_string(), replica_client).await;
    
    // Acquire dedicated handle and connect to primary
    let handle = pool.acquire_dedicated().await;
    let conn1 = pool.get_connection(handle, "primary").await.unwrap();
    
    // Verify connection is to primary
    assert!(conn1.is_connected());
    
    // Simulate failover: primary fails, replica becomes new primary
    pool.handle_failover("primary", "replica").await;
    
    // Next connection request should get connection to replica
    let conn2 = pool.get_connection(handle, "replica").await.unwrap();
    assert!(conn2.is_connected());
    
    // Old primary connection should be marked unhealthy
    // Attempting to use it would trigger reconnection
    
    pool.release_dedicated(handle).await;
}

#[tokio::test]
async fn test_connection_error_recovery() {
    let pool = ConnectionPool::new();
    
    let client = RedisClient::open("redis://localhost:6379").unwrap();
    pool.register_node("node1".to_string(), client).await;
    
    let handle = pool.acquire_dedicated().await;
    
    // Get initial connection
    let _conn1 = pool.get_connection(handle, "node1").await.unwrap();
    
    // Simulate connection error
    pool.mark_connection_unhealthy(handle, "node1").await;
    
    // Next get_connection should create new connection
    let conn2 = pool.get_connection(handle, "node1").await.unwrap();
    assert!(conn2.is_connected());
    
    pool.release_dedicated(handle).await;
}

#[tokio::test]
async fn test_topology_change() {
    let pool = ConnectionPool::new();
    
    // Setup: 3 nodes
    for i in 1..=3 {
        let client = RedisClient::open(format!("redis://node{}:6379", i)).unwrap();
        pool.register_node(format!("node{}", i), client).await;
    }
    
    let handle = pool.acquire_dedicated().await;
    
    // Connect to all 3 nodes
    pool.get_connection(handle, "node1").await.unwrap();
    pool.get_connection(handle, "node2").await.unwrap();
    pool.get_connection(handle, "node3").await.unwrap();
    
    // Topology change: node3 removed
    pool.handle_topology_change(&["node1".to_string(), "node2".to_string()]).await;
    
    // Connections to node1 and node2 should still work
    assert!(pool.get_connection(handle, "node1").await.is_ok());
    assert!(pool.get_connection(handle, "node2").await.is_ok());
    
    // Connection to node3 should fail (node removed)
    assert!(pool.get_connection(handle, "node3").await.is_err());
    
    pool.release_dedicated(handle).await;
}

#[tokio::test]
async fn test_multiple_handles_during_failover() {
    let pool = ConnectionPool::new();
    
    let primary_client = RedisClient::open("redis://primary:6379").unwrap();
    let replica_client = RedisClient::open("redis://replica:6379").unwrap();
    
    pool.register_node("primary".to_string(), primary_client).await;
    pool.register_node("replica".to_string(), replica_client).await;
    
    // Create multiple dedicated clients
    let handle1 = pool.acquire_dedicated().await;
    let handle2 = pool.acquire_dedicated().await;
    let handle3 = pool.acquire_dedicated().await;
    
    // All connect to primary
    pool.get_connection(handle1, "primary").await.unwrap();
    pool.get_connection(handle2, "primary").await.unwrap();
    pool.get_connection(handle3, "primary").await.unwrap();
    
    // Failover affects all handles
    pool.handle_failover("primary", "replica").await;
    
    // All handles should now connect to replica
    assert!(pool.get_connection(handle1, "replica").await.is_ok());
    assert!(pool.get_connection(handle2, "replica").await.is_ok());
    assert!(pool.get_connection(handle3, "replica").await.is_ok());
    
    pool.release_dedicated(handle1).await;
    pool.release_dedicated(handle2).await;
    pool.release_dedicated(handle3).await;
}

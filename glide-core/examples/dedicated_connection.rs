// Example: Using dedicated connections for WATCH/transaction isolation

use glide_core::client::Client;
use redis::cmd;

async fn cas_transaction_example(client: &Client, key: &str) -> Result<(), Box<dyn std::error::Error>> {
    // Acquire a dedicated connection handle
    let handle = client.acquire_dedicated().await;
    
    // All commands using this handle will use isolated connections
    // This ensures WATCH semantics work correctly even with concurrent operations
    
    // WATCH the key
    let mut watch_cmd = cmd("WATCH");
    watch_cmd.arg(key);
    // Note: Would need to extend send_command to accept handle parameter
    // client.send_command_dedicated(&watch_cmd, handle).await?;
    
    // Read current value
    let mut get_cmd = cmd("GET");
    get_cmd.arg(key);
    // let current_value = client.send_command_dedicated(&get_cmd, handle).await?;
    
    // Perform business logic...
    // let new_value = compute_new_value(current_value);
    
    // Execute transaction
    let mut multi_cmd = cmd("MULTI");
    // client.send_command_dedicated(&multi_cmd, handle).await?;
    
    let mut set_cmd = cmd("SET");
    set_cmd.arg(key).arg("new_value");
    // client.send_command_dedicated(&set_cmd, handle).await?;
    
    let mut exec_cmd = cmd("EXEC");
    // let result = client.send_command_dedicated(&exec_cmd, handle).await?;
    
    // Release the dedicated connection back to the pool
    client.release_dedicated(handle).await;
    
    // if result.is_null() {
    //     // Transaction failed due to WATCH - retry
    // }
    
    Ok(())
}

// Usage pattern for Go API:
// 
// dedicatedClient := client.UsingDedicated()
// defer dedicatedClient.Close()
// 
// dedicatedClient.Watch(ctx, []string{key})
// value := dedicatedClient.Get(ctx, key)
// // ... business logic ...
// result := dedicatedClient.Exec(ctx, transaction)

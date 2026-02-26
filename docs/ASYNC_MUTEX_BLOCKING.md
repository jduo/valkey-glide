# std::sync Mutex Blocking in Async Contexts

## Issue

Several `std::sync::Mutex` and `std::sync::RwLock` instances are used in async code paths,
causing Tokio runtime starvation under high concurrency.

## Affected Locations

### Critical (Hot Path)
1. `glide-core/redis-rs/redis/src/cluster_async/mod.rs:431`
   - `pending_requests: Mutex<Vec<PendingRequest<C>>>`
   - Used in both sync (`Sink::start_send`) and async (`poll_complete`) contexts
   - Affects EVERY command

### High Priority (Async Only)
2. `glide-core/redis-rs/redis/src/cluster_async/mod.rs:429-430`
   - `conn_lock: StdRwLock<ConnectionsContainer<C>>`
   - `cluster_params: StdRwLock<ClusterParams>`

3. `glide-core/src/client/reconnecting_connection.rs:58`
   - `state: Mutex<ConnectionState>`

### Medium Priority (Infrequent but Async)
4. `glide-core/src/cluster_scan_container.rs:17`
   - `CONTAINER: Lazy<Mutex<HashMap<...>>>`

5. `glide-core/src/scripts_container.rs:23`
   - `CONTAINER: Lazy<Mutex<HashMap<...>>>`

6. `glide-core/src/socket_listener.rs:942`
   - `INITIALIZED_SOCKETS: Lazy<RwLock<HashSet<...>>>`

## Recommended Fixes

### For pending_requests (sync/async shared)
Replace with lock-free `mpsc::UnboundedChannel`:
```rust
pending_requests_tx: mpsc::UnboundedSender<PendingRequest<C>>,
pending_requests_rx: Mutex<mpsc::UnboundedReceiver<PendingRequest<C>>>,
```

### For async-only mutexes
Replace `std::sync::{Mutex, RwLock}` with `tokio::sync::{Mutex, RwLock}`:
- Change `.lock().unwrap()` → `.lock().await`
- Change `.read().expect()` → `.read().await`
- Change `.write().expect()` → `.write().await`

## Detection

Run clippy to detect violations:
```bash
cargo clippy --all-targets -- -D clippy::await_holding_lock
```

## Impact

Under high concurrency (60+ concurrent operations), observed:
- 423ms Tokio runtime starvation (expected: <100ms)
- 60 operations stuck simultaneously for 3+ seconds
- Cascading failures

## References

- GitHub Issue: [To be created]
- Analysis: /home/ubuntu/STD_SYNC_AUDIT.md

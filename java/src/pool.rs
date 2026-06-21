// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Client-instance pool data structures and configuration.
//!
//! This module defines the core types for the Rust-side connection pool that manages
//! `CoreClient` instances with LIFO idle ordering, bounded size, and background eviction.

#![allow(dead_code)]

use glide_core::client::Client as GlideClient;

use dashmap::DashMap;
use protobuf::Message;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as TokioMutex;

/// Default leak detection threshold (300 seconds).
/// Clients borrowed for longer than this duration will trigger a warning log.
const LEAK_DETECTION_THRESHOLD: Duration = Duration::from_secs(300);

/// Global registry mapping pool_id to pool instances.
/// JNI calls use this to look up the correct pool by its opaque handle.
static POOL_REGISTRY: OnceLock<DashMap<u64, Arc<TokioMutex<Pool>>>> = OnceLock::new();

/// Counter for generating unique pool_id values.
static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

/// Get the pool registry, initializing it on first access.
pub fn get_pool_registry() -> &'static DashMap<u64, Arc<TokioMutex<Pool>>> {
    POOL_REGISTRY.get_or_init(DashMap::new)
}

/// Register a new pool in the registry and return its pool_id.
pub fn register_pool(pool: Pool) -> u64 {
    let pool_id = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
    let registry = get_pool_registry();
    registry.insert(pool_id, Arc::new(TokioMutex::new(pool)));
    pool_id
}

/// Remove a pool from the registry by pool_id.
/// Returns the pool if found, None otherwise.
pub fn unregister_pool(pool_id: u64) -> Option<Arc<TokioMutex<Pool>>> {
    let registry = get_pool_registry();
    registry.remove(&pool_id).map(|(_, pool)| pool)
}

/// Look up a pool by pool_id. Returns a clone of the Arc (cheap).
pub fn get_pool(pool_id: u64) -> Option<Arc<TokioMutex<Pool>>> {
    let registry = get_pool_registry();
    registry.get(&pool_id).map(|entry| entry.value().clone())
}

/// Errors that can occur during pool operations.
#[derive(Debug)]
pub enum PoolError {
    /// The pool configuration is invalid.
    InvalidConfig(String),
    /// The pool has been closed and cannot serve requests.
    PoolClosed,
    /// A client could not be created.
    ClientCreationFailed(String),
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PoolError::InvalidConfig(msg) => write!(f, "Invalid pool configuration: {}", msg),
            PoolError::PoolClosed => write!(f, "Pool is closed"),
            PoolError::ClientCreationFailed(msg) => {
                write!(f, "Client creation failed: {}", msg)
            }
        }
    }
}

impl std::error::Error for PoolError {}

// Pool state constants
pub const POOL_RUNNING: u8 = 0;
pub const POOL_CLOSING: u8 = 1;
pub const POOL_CLOSED: u8 = 2;

/// Configuration for the connection pool.
pub struct PoolConfig {
    /// Maximum number of clients in the pool.
    pub max_size: u32,
    /// Minimum number of idle clients to maintain.
    pub min_idle: u32,
    /// Evict idle clients after this duration.
    pub idle_timeout: Duration,
    /// Used for cleanup timeout calculation (2×).
    pub request_timeout: Duration,
    /// Serialized protobuf ConnectionRequest bytes.
    /// We store the raw bytes and parse when needed to create clients.
    pub connection_request: Vec<u8>,
}

/// Tracks the accumulated connection state for a borrowed client so the cleanup logic
/// knows exactly which reset commands are needed on return.
pub struct ConnectionState {
    pub watch_active: bool,
    pub multi_active: bool,
    pub tracking_enabled: bool,
    pub subscriptions: Vec<Subscription>,
    /// 0 = default database, non-zero = SELECT was used.
    pub db_selected: u8,
    pub client_name_changed: bool,
}

impl ConnectionState {
    /// Returns true if the connection has no state mutations that need cleanup.
    pub fn is_clean(&self) -> bool {
        !self.watch_active
            && !self.multi_active
            && !self.tracking_enabled
            && self.subscriptions.is_empty()
            && self.db_selected == 0
            && !self.client_name_changed
    }

    /// Returns true if the connection has active subscriptions.
    pub fn has_subscriptions(&self) -> bool {
        !self.subscriptions.is_empty()
    }
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            watch_active: false,
            multi_active: false,
            tracking_enabled: false,
            subscriptions: Vec::new(),
            db_selected: 0,
            client_name_changed: false,
        }
    }
}

/// Represents an active subscription on a connection.
pub enum Subscription {
    /// SUBSCRIBE channel
    Channel(Vec<u8>),
    /// PSUBSCRIBE pattern
    Pattern(Vec<u8>),
    /// SSUBSCRIBE sharded channel
    ShardedChannel(Vec<u8>),
}

/// The lifecycle state of a pooled client.
pub enum PooledClientState {
    Idle,
    InUse,
    Cleaning,
}

/// A client managed by the pool, wrapping the actual GlideClient connection
/// along with metadata for lifecycle management.
pub struct PooledClient {
    /// Unique identifier for this client within the pool.
    pub client_id: u64,
    /// The actual Valkey client connection.
    pub client: GlideClient,
    /// When this client was created.
    pub created_at: Instant,
    /// When this client was last returned to the idle list.
    pub last_returned_at: Instant,
    /// Set when acquired, used for leak detection.
    pub borrowed_at: Option<Instant>,
    /// Tracks accumulated state mutations during borrow.
    pub connection_state: ConnectionState,
    /// Current lifecycle state.
    pub state: PooledClientState,
}

/// The Rust-side connection pool managing a bounded collection of `GlideClient` instances.
pub struct Pool {
    /// Pool configuration.
    pub config: PoolConfig,
    /// LIFO idle stack — most recently returned client is at the back.
    pub idle: VecDeque<PooledClient>,
    /// Currently borrowed clients, keyed by client_id.
    pub in_use: DashMap<u64, PooledClient>,
    /// Clients undergoing subscription cleanup (CLEANING state).
    pub cleaning: DashMap<u64, PooledClient>,
    /// Counter for generating unique client_id values.
    pub next_client_id: AtomicU64,
    /// Current total (idle + in_use + cleaning + being_created).
    pub total_count: AtomicU32,
    /// Pool state: 0=RUNNING, 1=CLOSING, 2=CLOSED.
    pub state: AtomicU8,
    /// Handle to the eviction background task (for cancellation).
    pub eviction_handle: Option<tokio::task::JoinHandle<()>>,
}

impl Pool {
    /// Creates a new pool with the given configuration.
    ///
    /// Validates the config and returns the pool in RUNNING state.
    /// Does NOT start the eviction task or spawn min_idle clients — those are
    /// handled separately during pool registration (task 2.5 / 3.2).
    pub fn new(config: PoolConfig) -> Result<Self, PoolError> {
        // Validate config
        if config.max_size < 1 {
            return Err(PoolError::InvalidConfig("max_size must be >= 1".into()));
        }
        if config.min_idle > config.max_size {
            return Err(PoolError::InvalidConfig(
                "min_idle must be <= max_size".into(),
            ));
        }
        if config.idle_timeout.is_zero() {
            return Err(PoolError::InvalidConfig(
                "idle_timeout must be > 0".into(),
            ));
        }
        if config.request_timeout.is_zero() {
            return Err(PoolError::InvalidConfig(
                "request_timeout must be > 0".into(),
            ));
        }

        Ok(Self {
            config,
            idle: VecDeque::new(),
            in_use: DashMap::new(),
            cleaning: DashMap::new(),
            next_client_id: AtomicU64::new(1),
            total_count: AtomicU32::new(0),
            state: AtomicU8::new(POOL_RUNNING),
            eviction_handle: None,
        })
    }

    /// Non-blocking acquire. Returns client_id on success, or -1 if no client available.
    ///
    /// Algorithm:
    /// 1. Check pool state == RUNNING, return -1 if not
    /// 2. Pop from idle VecDeque back (LIFO)
    /// 3. If popped:
    ///    a. Check connection liveness (placeholder — always healthy for now)
    ///    b. If unhealthy: destroy, decrement total_count, return -1
    ///    c. If healthy: move to in_use DashMap, set state=InUse, set borrowed_at, return client_id
    /// 4. If idle empty and total < max_size: return -1 (background creation triggered by caller)
    /// 5. If idle empty and total == max_size: return -1
    pub fn try_acquire(&mut self) -> i64 {
        // Step 1: Check pool state
        if self.state.load(Ordering::Acquire) != POOL_RUNNING {
            return -1;
        }

        // Step 2: LIFO pop from idle list
        if let Some(mut entry) = self.idle.pop_back() {
            // Step 3a: Health check (placeholder — Client doesn't expose is_connected directly)
            // TODO: Replace with actual health check when Client exposes is_connected()
            let is_healthy = true;

            if !is_healthy {
                // Step 3b: Unhealthy — destroy and decrement
                self.total_count.fetch_sub(1, Ordering::AcqRel);
                drop(entry);
                return -1;
            }

            // Step 3c: Healthy — move to in_use
            let client_id = entry.client_id;
            entry.state = PooledClientState::InUse;
            entry.borrowed_at = Some(Instant::now());
            entry.connection_state = ConnectionState::default();
            self.in_use.insert(client_id, entry);
            return client_id as i64;
        }

        // Step 4 & 5: Idle list empty, return -1
        // The caller (JNI layer) will check should_create_background() and trigger creation if needed
        -1
    }

    /// Returns true if background client creation should be triggered.
    ///
    /// This is true when the current total_count is below max_size, meaning there
    /// is room to create additional clients. The JNI layer calls this after
    /// try_acquire returns -1 to decide whether to spawn background creation.
    pub fn should_create_background(&self) -> bool {
        self.total_count.load(Ordering::Acquire) < self.config.max_size
    }

    /// Release a borrowed client back to the pool. Fire-and-forget.
    ///
    /// - If the client has no subscriptions: push directly to idle (zero-cost path)
    /// - If the client has active subscriptions: move to CLEANING state (subscription
    ///   cleanup will be triggered by the caller/JNI layer via spawn_subscription_cleanup)
    /// - If pool is CLOSING/CLOSED: destroy the client
    /// - If client_id not found in in_use: no-op (idempotent)
    ///
    /// Returns true if the client was found and processed, false if not found (no-op).
    pub fn release(&mut self, client_id: u64, pool_arc: Arc<TokioMutex<Pool>>) -> bool {
        // Remove from in_use map
        let entry = self.in_use.remove(&client_id);
        let Some((_, mut entry)) = entry else {
            return false; // Not found — idempotent no-op
        };

        // If pool is shutting down, destroy the client
        if self.state.load(Ordering::Acquire) != POOL_RUNNING {
            self.total_count.fetch_sub(1, Ordering::AcqRel);
            drop(entry);
            return true;
        }

        // Update return timestamp
        entry.last_returned_at = Instant::now();
        entry.borrowed_at = None;

        // Check if subscriptions are active
        if entry.connection_state.has_subscriptions() {
            // Move to CLEANING state — not available for re-borrowing
            entry.state = PooledClientState::Cleaning;
            let cid = entry.client_id;
            self.cleaning.insert(cid, entry);
            // Spawn subscription cleanup async task
            spawn_subscription_cleanup(pool_arc, cid);
            return true;
        }

        // Common path: no subscriptions, direct push to idle (zero cost)
        entry.state = PooledClientState::Idle;
        entry.connection_state = ConnectionState::default();
        self.idle.push_back(entry);
        true
    }

    /// Destroys the pool: cancels eviction, drains idle/in-use/cleaning clients.
    ///
    /// Sets state to CLOSED. After this call, all client connections are dropped
    /// and no further operations are possible on this pool.
    /// Also removes all clients from `JNI_HANDLE_TABLE` so that stale handles
    /// cannot be used for command dispatch.
    pub fn destroy(&mut self) {
        // Set state to CLOSED to prevent any new operations
        self.state.store(POOL_CLOSED, Ordering::Release);

        // Cancel the eviction task if running
        if let Some(handle) = self.eviction_handle.take() {
            handle.abort();
        }

        let handle_table = crate::jni_client::get_handle_table();

        // Drain the idle list — drop closes connections
        while let Some(entry) = self.idle.pop_back() {
            handle_table.remove(&entry.client_id);
            self.total_count.fetch_sub(1, Ordering::AcqRel);
            drop(entry);
        }

        // Drain in-use clients — these are forcibly closed
        let in_use_keys: Vec<u64> = self.in_use.iter().map(|entry| *entry.key()).collect();
        for key in in_use_keys {
            if let Some((_, entry)) = self.in_use.remove(&key) {
                handle_table.remove(&entry.client_id);
                self.total_count.fetch_sub(1, Ordering::AcqRel);
                drop(entry);
            }
        }

        // Drain cleaning clients — these are also forcibly closed
        let cleaning_keys: Vec<u64> = self.cleaning.iter().map(|entry| *entry.key()).collect();
        for key in cleaning_keys {
            if let Some((_, entry)) = self.cleaning.remove(&key) {
                handle_table.remove(&entry.client_id);
                self.total_count.fetch_sub(1, Ordering::AcqRel);
                drop(entry);
            }
        }
    }
}

/// Creates a GlideClient from serialized ConnectionRequest protobuf bytes.
///
/// Parses the protobuf `ConnectionRequest` from the raw bytes stored in the pool config,
/// converts it to the glide-core domain type, and creates a new client connection.
async fn create_client_from_bytes(connection_request_bytes: &[u8]) -> Result<GlideClient, PoolError> {
    // Parse the protobuf ConnectionRequest
    let proto_request =
        glide_core::connection_request::ConnectionRequest::parse_from_bytes(connection_request_bytes)
            .map_err(|e| {
                PoolError::ClientCreationFailed(format!("Failed to parse ConnectionRequest: {}", e))
            })?;

    // Convert protobuf to glide-core domain type
    let connection_request = glide_core::client::ConnectionRequest::from(proto_request);

    // Create the client using glide-core's client creation (no push sender for pool clients)
    GlideClient::new(connection_request, None)
        .await
        .map_err(|e| PoolError::ClientCreationFailed(format!("{}", e)))
}

/// Spawns a subscription cleanup task for a client in CLEANING state.
///
/// Sends UNSUBSCRIBE/PUNSUBSCRIBE/SUNSUBSCRIBE commands with a timeout of 2 × request_timeout.
/// On success: clears subscriptions, moves client from cleaning to idle.
/// On timeout/error: destroys the client, decrements total_count.
pub fn spawn_subscription_cleanup(pool: Arc<TokioMutex<Pool>>, client_id: u64) {
    let runtime = crate::jni_client::get_runtime();
    runtime.spawn(async move {
        let timeout_duration = {
            let pool_guard = pool.lock().await;
            pool_guard.config.request_timeout * 2
        };

        // For the prototype, we implement a simplified cleanup:
        // Since the actual UNSUBSCRIBE command dispatch through the glide-core Client
        // is complex (requires building redis::Cmd and sending via the client's send_command),
        // we use a timeout-based approach: wait for the timeout duration, then check
        // if the client should be kept or destroyed.
        //
        // In a full implementation, we would:
        // 1. Build UNSUBSCRIBE/PUNSUBSCRIBE/SUNSUBSCRIBE commands
        // 2. Send them via client.send_command()
        // 3. Wait for confirmation push messages
        //
        // For the prototype, we simply move the client back to idle after clearing subscriptions,
        // simulating successful cleanup. This validates the state machine without requiring
        // full command dispatch integration.

        let cleanup_result = tokio::time::timeout(timeout_duration, async {
            // Simulate unsubscribe completion
            // In production: send actual UNSUBSCRIBE commands and await confirmations
            tokio::time::sleep(Duration::from_millis(1)).await;
            Ok::<(), PoolError>(())
        })
        .await;

        let mut pool_guard = pool.lock().await;

        // Remove from cleaning map
        let entry = pool_guard.cleaning.remove(&client_id);
        let Some((_, mut entry)) = entry else {
            // Already removed (e.g., pool was destroyed) — nothing to do
            return;
        };

        match cleanup_result {
            Ok(Ok(())) => {
                // Cleanup successful — clear subscriptions and return to idle
                if pool_guard.state.load(Ordering::Acquire) != POOL_RUNNING {
                    // Pool closed during cleanup — destroy
                    pool_guard.total_count.fetch_sub(1, Ordering::AcqRel);
                    drop(entry);
                    return;
                }
                entry.connection_state.subscriptions.clear();
                entry.connection_state = ConnectionState::default();
                entry.state = PooledClientState::Idle;
                entry.last_returned_at = Instant::now();
                pool_guard.idle.push_back(entry);
            }
            _ => {
                // Timeout or error — destroy the client
                log::warn!(
                    "Subscription cleanup timed out for client_id={}, destroying connection",
                    client_id
                );
                crate::jni_client::get_handle_table().remove(&client_id);
                pool_guard.total_count.fetch_sub(1, Ordering::AcqRel);
                drop(entry);

                // Trigger replenishment if below min_idle
                let idle_count = pool_guard.idle.len() as u32;
                let total = pool_guard.total_count.load(Ordering::Acquire);
                if idle_count < pool_guard.config.min_idle
                    && total < pool_guard.config.max_size
                {
                    pool_guard.total_count.fetch_add(1, Ordering::AcqRel);
                    let pool_clone = pool.clone();
                    drop(pool_guard); // Release lock before spawning
                    spawn_background_create(pool_clone);
                }
            }
        }
    });
}

/// Spawns a background client creation task on the Tokio runtime.
///
/// The caller must have already incremented `total_count` to reserve a slot.
/// On success, the new client is added to the idle list AND registered in
/// `JNI_HANDLE_TABLE` so that the existing Java command dispatch path
/// (`executeBinaryCommandAsync(handle, ...)`) can find the client.
/// On failure, `total_count` is decremented to release the reserved slot.
pub fn spawn_background_create(pool: Arc<TokioMutex<Pool>>) {
    let runtime = crate::jni_client::get_runtime();
    runtime.spawn(async move {
        // Extract connection request bytes while holding the lock briefly
        let connection_request_bytes = {
            let pool_guard = pool.lock().await;
            pool_guard.config.connection_request.clone()
        };

        // Try to create the client (does not hold the lock during I/O)
        match create_client_from_bytes(&connection_request_bytes).await {
            Ok(client) => {
                // Use the JNI handle table's ID generator so the client_id IS the
                // native handle that Java uses for command dispatch.
                let handle_id = crate::jni_client::generate_safe_handle();

                // Register in JNI_HANDLE_TABLE so executeBinaryCommandAsync(handle_id, ...) works
                crate::jni_client::get_handle_table().insert(handle_id, client.clone());

                let mut pool_guard = pool.lock().await;
                // Double-check pool is still running before adding client
                if pool_guard.state.load(Ordering::Acquire) != POOL_RUNNING {
                    pool_guard.total_count.fetch_sub(1, Ordering::AcqRel);
                    // Remove from handle table since we're not keeping it
                    crate::jni_client::get_handle_table().remove(&handle_id);
                    return;
                }
                let entry = PooledClient {
                    client_id: handle_id,
                    client,
                    created_at: Instant::now(),
                    last_returned_at: Instant::now(),
                    borrowed_at: None,
                    connection_state: ConnectionState::default(),
                    state: PooledClientState::Idle,
                };
                pool_guard.idle.push_back(entry);
                log::info!("Pool: background client creation succeeded, handle_id={}", handle_id);
            }
            Err(e) => {
                log::error!("Pool: background client creation FAILED: {}", e);
                let pool_guard = pool.lock().await;
                pool_guard.total_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
    });
}

/// Starts the eviction background task for the pool.
///
/// Runs at interval = idle_timeout / 4. Each tick:
/// 1. Evicts idle clients past idle_timeout (retaining at least min_idle)
/// 2. Replenishes to min_idle via background creation
/// 3. Logs warnings for leaked in-use clients (borrowed > LEAK_DETECTION_THRESHOLD)
///
/// The task exits when the pool state is no longer RUNNING.
///
/// Returns a JoinHandle for cancellation on pool destroy.
pub fn start_eviction_task(
    pool: Arc<TokioMutex<Pool>>,
    idle_timeout: Duration,
    min_idle: u32,
    max_size: u32,
) -> tokio::task::JoinHandle<()> {
    let interval_duration = idle_timeout / 4;
    let runtime = crate::jni_client::get_runtime();

    runtime.spawn(async move {
        let mut interval = tokio::time::interval(interval_duration);
        interval.tick().await; // first tick is immediate, skip it

        loop {
            interval.tick().await;

            let mut pool_guard = pool.lock().await;

            // Check if pool is still running
            if pool_guard.state.load(Ordering::Acquire) != POOL_RUNNING {
                return; // Pool shutting down — stop eviction
            }

            let now = Instant::now();

            // --- Eviction: remove expired idle clients ---
            let current_idle = pool_guard.idle.len() as u32;
            let can_evict = current_idle.saturating_sub(min_idle);
            let mut evicted = 0u32;
            let mut to_remove_indices = Vec::new();

            for (idx, entry) in pool_guard.idle.iter().enumerate() {
                if evicted >= can_evict {
                    break;
                }
                if now.duration_since(entry.last_returned_at) > idle_timeout {
                    to_remove_indices.push(idx);
                    evicted += 1;
                }
            }

            let handle_table = crate::jni_client::get_handle_table();

            // Remove from back to front to preserve indices
            for &idx in to_remove_indices.iter().rev() {
                if let Some(entry) = pool_guard.idle.remove(idx) {
                    handle_table.remove(&entry.client_id);
                    pool_guard.total_count.fetch_sub(1, Ordering::AcqRel);
                }
            }

            // --- Replenishment: create clients to reach min_idle ---
            let current_idle_after = pool_guard.idle.len() as u32;
            let total = pool_guard.total_count.load(Ordering::Acquire);
            let deficit = min_idle.saturating_sub(current_idle_after);
            let can_create = max_size.saturating_sub(total);
            let to_create = deficit.min(can_create);

            for _ in 0..to_create {
                pool_guard.total_count.fetch_add(1, Ordering::AcqRel);
                spawn_background_create(pool.clone());
            }

            // --- Leak detection: warn about long-held in-use clients ---
            for entry in pool_guard.in_use.iter() {
                if let Some(borrowed_at) = entry.value().borrowed_at {
                    if now.duration_since(borrowed_at) > LEAK_DETECTION_THRESHOLD {
                        log::warn!(
                            "Potential connection leak: client_id={} borrowed for {:?}",
                            entry.key(),
                            now.duration_since(borrowed_at)
                        );
                    }
                }
            }

            drop(pool_guard); // Release lock explicitly
        }
    })
}

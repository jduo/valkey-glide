// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Isolated execution connection pool (Feature 2).
//!
//! Manages dedicated TCP connections per-client for operations requiring
//! per-connection server state (WATCH, CLIENT TRACKING, BLPOP, pub/sub).
//! Reuses the same pool patterns as Feature 1: LIFO idle list, bounded size,
//! conditional cleanup, fire-and-forget release.

#![allow(dead_code)]

use crate::jni_client::get_runtime;
use dashmap::DashMap;
use redis::aio::MultiplexedConnection;
use redis::Cmd;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as TokioMutex;

// ============ Configuration ============

/// Configuration for the per-client connection pool.
pub struct ScopePoolConfig {
    /// Max idle connections per node. Default: 4.
    pub max_idle_per_node: u32,
    /// Hard cap on total connections across all nodes. Default: 64.
    pub max_total: u32,
    /// Idle timeout for eviction. Default: 30s.
    pub idle_timeout: Duration,
    /// Request timeout for commands on scoped connections.
    pub request_timeout: Duration,
    /// Connection timeout for creating new connections.
    pub connection_timeout: Duration,
}

impl Default for ScopePoolConfig {
    fn default() -> Self {
        Self {
            max_idle_per_node: 4,
            max_total: 64,
            idle_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(5),
            connection_timeout: Duration::from_secs(5),
        }
    }
}

// ============ Connection State ============

/// Tracks per-connection state mutations during a borrow.
/// Shared concept with Feature 1's ConnectionState — same cleanup semantics.
#[derive(Default)]
pub struct ScopeConnectionState {
    pub watch_active: bool,
    pub multi_active: bool,
    pub tracking_enabled: bool,
    pub db_selected: u8,
    pub client_name_changed: bool,
    pub subscriptions: Vec<ScopeSubscription>,
}

impl ScopeConnectionState {
    /// Returns true if no state-mutating commands were issued (zero-cost release).
    pub fn is_clean(&self) -> bool {
        !self.watch_active
            && !self.multi_active
            && !self.tracking_enabled
            && self.db_selected == 0
            && !self.client_name_changed
            && self.subscriptions.is_empty()
    }

    /// Returns true if subscriptions are active (requires async cleanup).
    pub fn has_subscriptions(&self) -> bool {
        !self.subscriptions.is_empty()
    }

    /// Count of dirty flags (for RESET threshold decision).
    pub fn dirty_count(&self) -> u32 {
        let mut count = 0u32;
        if self.watch_active { count += 1; }
        if self.multi_active { count += 1; }
        if self.tracking_enabled { count += 1; }
        if self.db_selected != 0 { count += 1; }
        if self.client_name_changed { count += 1; }
        if !self.subscriptions.is_empty() { count += 1; }
        count
    }
}

pub enum ScopeSubscription {
    Channel(Vec<u8>),
    Pattern(Vec<u8>),
    ShardedChannel(Vec<u8>),
}

// ============ Scoped Connection ============

/// A dedicated connection managed by the scope pool.
pub struct ScopedConnection {
    /// Unique scope identifier.
    pub scope_id: u64,
    /// The underlying async connection (same type as the multiplexer uses).
    pub connection: MultiplexedConnection,
    /// When this connection was created.
    pub created_at: Instant,
    /// When last returned to idle.
    pub last_returned_at: Instant,
    /// When borrowed (for leak detection).
    pub borrowed_at: Option<Instant>,
    /// Tracks state mutations during the current borrow.
    pub state: ScopeConnectionState,
}

// ============ Pool State Constants ============

pub const SCOPE_POOL_RUNNING: u8 = 0;
pub const SCOPE_POOL_CLOSING: u8 = 1;
pub const SCOPE_POOL_CLOSED: u8 = 2;

// ============ Connection Pool ============

/// Per-client connection pool for isolated execution.
/// For the prototype, we use a single flat idle list (standalone mode only).
pub struct ScopePool {
    /// Configuration.
    pub config: ScopePoolConfig,
    /// LIFO idle list (flat for standalone mode — single node).
    pub idle: VecDeque<ScopedConnection>,
    /// Currently borrowed scopes.
    pub in_use: DashMap<u64, ()>, // scope_id → placeholder (connection is in SCOPE_REGISTRY)
    /// Counter for scope_id generation.
    pub next_scope_id: AtomicU64,
    /// Total connections (idle + in_use + creating).
    pub total_count: AtomicU32,
    /// Pool state.
    pub state: AtomicU8,
    /// Serialized ConnectionRequest bytes (for creating new connections).
    pub connection_request_bytes: Vec<u8>,
}

impl ScopePool {
    /// Create a new scope pool with the given config and connection info.
    pub fn new(config: ScopePoolConfig, connection_request_bytes: Vec<u8>) -> Self {
        Self {
            config,
            idle: VecDeque::new(),
            in_use: DashMap::new(),
            next_scope_id: AtomicU64::new(1),
            total_count: AtomicU32::new(0),
            state: AtomicU8::new(SCOPE_POOL_RUNNING),
            connection_request_bytes,
        }
    }

    /// Non-blocking acquire. Returns scope_id on success, -1 if exhausted/creating.
    pub fn try_acquire(&mut self) -> i64 {
        if self.state.load(Ordering::Acquire) != SCOPE_POOL_RUNNING {
            return -1;
        }

        // LIFO pop from idle list
        if let Some(mut conn) = self.idle.pop_back() {
            let scope_id = conn.scope_id;
            conn.borrowed_at = Some(Instant::now());
            conn.state = ScopeConnectionState::default();

            // Register in scope registry for command routing
            get_scope_registry().insert(scope_id, ScopeEntry {
                connection: Arc::new(TokioMutex::new(conn)),
            });
            self.in_use.insert(scope_id, ());

            return scope_id as i64;
        }

        // No idle connection — check if we can create one
        if self.total_count.load(Ordering::Acquire) < self.config.max_total {
            // Reserve a slot and trigger background creation
            self.total_count.fetch_add(1, Ordering::AcqRel);
            return -1; // Caller will trigger background creation
        }

        -1 // At capacity
    }

    /// Returns true if background creation should be triggered.
    pub fn should_create_background(&self) -> bool {
        self.total_count.load(Ordering::Acquire) < self.config.max_total
    }

    /// Release a scope back to the pool.
    /// If clean state: push to idle (zero-cost).
    /// If dirty: pipeline cleanup commands first.
    pub fn release(&mut self, scope_id: u64) -> bool {
        // Remove from in_use
        if self.in_use.remove(&scope_id).is_none() {
            return false; // Not found — idempotent
        }

        // Get the connection from the scope registry
        let entry = get_scope_registry().remove(&scope_id);
        let Some((_, entry)) = entry else {
            return false;
        };

        // If pool is shutting down, destroy
        if self.state.load(Ordering::Acquire) != SCOPE_POOL_RUNNING {
            self.total_count.fetch_sub(1, Ordering::AcqRel);
            return true;
        }

        // Try to get the connection synchronously
        match entry.connection.try_lock() {
            Ok(conn) => {
                if conn.state.is_clean() {
                    // Zero-cost fast path: move connection back to idle
                    // We need to take ownership of the inner ScopedConnection from the Mutex.
                    // Since we own the Arc (removed from registry), we can reconstruct the entry.
                    let idle_conn = ScopedConnection {
                        scope_id: conn.scope_id,
                        connection: conn.connection.clone(), // MultiplexedConnection is Arc-backed, clone is cheap
                        created_at: conn.created_at,
                        last_returned_at: Instant::now(),
                        borrowed_at: None,
                        state: ScopeConnectionState::default(),
                    };
                    drop(conn); // Release mutex guard before push
                    self.idle.push_back(idle_conn);
                    true
                } else if conn.state.has_subscriptions() {
                    // Subscriptions active — spawn unsubscribe cleanup
                    let conn_arc = entry.connection.clone();
                    let sid = conn.scope_id;
                    drop(conn);
                    self.spawn_subscription_cleanup(conn_arc, sid);
                    true
                } else {
                    // Other dirty state — spawn conditional cleanup
                    let conn_arc = entry.connection.clone();
                    let sid = conn.scope_id;
                    drop(conn);
                    self.spawn_scope_cleanup(conn_arc, sid);
                    true
                }
            }
            Err(_) => {
                // Lock contended — for prototype: just decrement and discard
                self.total_count.fetch_sub(1, Ordering::AcqRel);
                true
            }
        }
    }

    /// Spawn async cleanup for a dirty connection (non-subscription state).
    fn spawn_scope_cleanup(&mut self, conn: Arc<TokioMutex<ScopedConnection>>, scope_id: u64) {
        let runtime = get_runtime();
        let request_timeout = self.config.request_timeout;

        // For the prototype: send cleanup commands and discard the connection
        // (simplified — production would return to idle after successful cleanup)
        self.total_count.fetch_sub(1, Ordering::AcqRel);

        runtime.spawn(async move {
            let mut guard = conn.lock().await;
            let timeout = request_timeout * 2;

            let cleanup_result = tokio::time::timeout(timeout, async {
                // Pipeline cleanup commands based on state
                if guard.state.multi_active {
                    // DISCARD clears both MULTI and WATCH
                    let _ = guard.connection.send_packed_command(&Cmd::new().arg("DISCARD")).await;
                } else if guard.state.watch_active {
                    let _ = guard.connection.send_packed_command(&Cmd::new().arg("UNWATCH")).await;
                }
                if guard.state.tracking_enabled {
                    let _ = guard.connection.send_packed_command(
                        &Cmd::new().arg("CLIENT").arg("TRACKING").arg("OFF")
                    ).await;
                }
                if guard.state.db_selected != 0 {
                    let _ = guard.connection.send_packed_command(
                        &Cmd::new().arg("SELECT").arg("0")
                    ).await;
                }
                Ok::<(), ()>(())
            }).await;

            if cleanup_result.is_err() {
                log::warn!("Scope cleanup timed out for scope_id={}", scope_id);
            }
            // Connection is dropped (destroyed) for prototype simplicity
        });
    }

    /// Spawn async cleanup for a connection with active subscriptions.
    /// Sends UNSUBSCRIBE/PUNSUBSCRIBE/SUNSUBSCRIBE for all tracked channels,
    /// then discards the connection.
    fn spawn_subscription_cleanup(&mut self, conn: Arc<TokioMutex<ScopedConnection>>, scope_id: u64) {
        let runtime = get_runtime();
        let request_timeout = self.config.request_timeout;

        self.total_count.fetch_sub(1, Ordering::AcqRel);

        runtime.spawn(async move {
            let mut guard = conn.lock().await;
            let timeout = request_timeout * 2;

            let cleanup_result = tokio::time::timeout(timeout, async {
                // Collect channels/patterns to unsubscribe from
                let mut channels: Vec<Vec<u8>> = Vec::new();
                let mut patterns: Vec<Vec<u8>> = Vec::new();
                let mut sharded: Vec<Vec<u8>> = Vec::new();

                for sub in &guard.state.subscriptions {
                    match sub {
                        ScopeSubscription::Channel(ch) => channels.push(ch.clone()),
                        ScopeSubscription::Pattern(p) => patterns.push(p.clone()),
                        ScopeSubscription::ShardedChannel(s) => sharded.push(s.clone()),
                    }
                }

                // Send UNSUBSCRIBE for each type
                if !channels.is_empty() {
                    let mut cmd = Cmd::new();
                    cmd.arg("UNSUBSCRIBE");
                    for ch in &channels {
                        cmd.arg(ch.as_slice());
                    }
                    let _ = guard.connection.send_packed_command(&cmd).await;
                }

                if !patterns.is_empty() {
                    let mut cmd = Cmd::new();
                    cmd.arg("PUNSUBSCRIBE");
                    for p in &patterns {
                        cmd.arg(p.as_slice());
                    }
                    let _ = guard.connection.send_packed_command(&cmd).await;
                }

                if !sharded.is_empty() {
                    let mut cmd = Cmd::new();
                    cmd.arg("SUNSUBSCRIBE");
                    for s in &sharded {
                        cmd.arg(s.as_slice());
                    }
                    let _ = guard.connection.send_packed_command(&cmd).await;
                }

                // Also clean other dirty state if present
                if guard.state.multi_active {
                    let _ = guard.connection.send_packed_command(&Cmd::new().arg("DISCARD")).await;
                } else if guard.state.watch_active {
                    let _ = guard.connection.send_packed_command(&Cmd::new().arg("UNWATCH")).await;
                }
                if guard.state.tracking_enabled {
                    let _ = guard.connection.send_packed_command(
                        &Cmd::new().arg("CLIENT").arg("TRACKING").arg("OFF")
                    ).await;
                }
                if guard.state.db_selected != 0 {
                    let _ = guard.connection.send_packed_command(
                        &Cmd::new().arg("SELECT").arg("0")
                    ).await;
                }

                Ok::<(), ()>(())
            }).await;

            if cleanup_result.is_err() {
                log::warn!(
                    "Scope subscription cleanup timed out for scope_id={}, discarding connection",
                    scope_id
                );
            }
            // Connection is dropped (destroyed) regardless — pub/sub connections
            // are not safe to reuse without confirming all unsubscribe responses
        });
    }

    /// Destroy the pool — close all connections.
    pub fn destroy(&mut self) {
        self.state.store(SCOPE_POOL_CLOSED, Ordering::Release);

        // Clear idle connections
        while let Some(_conn) = self.idle.pop_back() {
            self.total_count.fetch_sub(1, Ordering::AcqRel);
        }

        // Clear in_use registry entries
        let in_use_keys: Vec<u64> = self.in_use.iter().map(|e| *e.key()).collect();
        for key in in_use_keys {
            self.in_use.remove(&key);
            get_scope_registry().remove(&key);
            self.total_count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

// ============ Scope Registry ============

/// Global registry mapping scope_id → ScopeEntry.
/// Used by glide_scope_execute to route commands to the correct connection.
static SCOPE_REGISTRY: OnceLock<DashMap<u64, ScopeEntry>> = OnceLock::new();

pub struct ScopeEntry {
    pub connection: Arc<TokioMutex<ScopedConnection>>,
}

pub fn get_scope_registry() -> &'static DashMap<u64, ScopeEntry> {
    SCOPE_REGISTRY.get_or_init(DashMap::new)
}

// ============ Per-Client Scope Pool Registry ============

/// Maps client_id → ScopePool (lazily created on first scopedConnection() call).
static CLIENT_SCOPE_POOLS: OnceLock<DashMap<u64, Arc<TokioMutex<ScopePool>>>> = OnceLock::new();

pub fn get_client_scope_pools() -> &'static DashMap<u64, Arc<TokioMutex<ScopePool>>> {
    CLIENT_SCOPE_POOLS.get_or_init(DashMap::new)
}

/// Get or create the scope pool for a given client_id.
/// Uses DashMap::entry to avoid TOCTOU races when two threads call simultaneously.
pub fn get_or_create_scope_pool(
    client_id: u64,
    connection_request_bytes: Vec<u8>,
) -> Arc<TokioMutex<ScopePool>> {
    let pools = get_client_scope_pools();

    // Use entry API for atomic get-or-insert
    pools.entry(client_id).or_insert_with(|| {
        let config = ScopePoolConfig::default();
        let pool = ScopePool::new(config, connection_request_bytes);
        Arc::new(TokioMutex::new(pool))
    }).value().clone()
}

// ============ Background Connection Creation ============

/// Creates a new dedicated connection for isolated execution.
/// Uses the same connection setup as the main client but produces a separate
/// MultiplexedConnection (its own TCP socket).
pub fn spawn_background_scope_create(
    client_id: u64,
    scope_pool: Arc<TokioMutex<ScopePool>>,
) {
    let runtime = get_runtime();

    runtime.spawn(async move {
        let connection_request_bytes = {
            let pool = scope_pool.lock().await;
            pool.connection_request_bytes.clone()
        };

        // Parse the ConnectionRequest protobuf to get connection info
        let result = create_scope_connection(&connection_request_bytes).await;

        match result {
            Ok(connection) => {
                let mut pool = scope_pool.lock().await;
                if pool.state.load(Ordering::Acquire) != SCOPE_POOL_RUNNING {
                    pool.total_count.fetch_sub(1, Ordering::AcqRel);
                    return;
                }

                let scope_id = pool.next_scope_id.fetch_add(1, Ordering::Relaxed);
                let scoped_conn = ScopedConnection {
                    scope_id,
                    connection,
                    created_at: Instant::now(),
                    last_returned_at: Instant::now(),
                    borrowed_at: None,
                    state: ScopeConnectionState::default(),
                };
                pool.idle.push_back(scoped_conn);
                log::info!(
                    "ScopePool: background connection created, scope_id={}, client_id={}",
                    scope_id, client_id
                );
            }
            Err(e) => {
                log::error!("ScopePool: background connection creation FAILED: {}", e);
                let pool = scope_pool.lock().await;
                pool.total_count.fetch_sub(1, Ordering::AcqRel);
            }
        }
    });
}

/// Create a new MultiplexedConnection from serialized ConnectionRequest bytes.
async fn create_scope_connection(
    connection_request_bytes: &[u8],
) -> Result<MultiplexedConnection, String> {
    use protobuf::Message;

    // Parse protobuf to extract address info
    let proto_request =
        glide_core::connection_request::ConnectionRequest::parse_from_bytes(connection_request_bytes)
            .map_err(|e| format!("Failed to parse ConnectionRequest: {}", e))?;

    // Get connection details from protobuf directly
    let address = proto_request.addresses.first()
        .ok_or_else(|| "No addresses in ConnectionRequest".to_string())?;

    let host = &address.host;
    let port = if address.port == 0 { 6379 } else { address.port as u16 };

    let use_tls = proto_request.tls_mode.value() != 0; // 0 = NoTls
    let scheme = if use_tls { "rediss" } else { "redis" };
    let url = format!("{}://{}:{}", scheme, host, port);

    let client = redis::Client::open(url.as_str())
        .map_err(|e| format!("Failed to open redis client: {}", e))?;

    let connection_timeout = if proto_request.connection_timeout > 0 {
        Duration::from_millis(proto_request.connection_timeout as u64)
    } else {
        Duration::from_secs(5)
    };

    // Create connection options — minimal, no push handling needed for scoped connections
    let connection_options = redis::GlideConnectionOptions {
        push_sender: None,
        disconnect_notifier: None,
        discover_az: false,
        connection_timeout: Some(connection_timeout),
        connection_retry_strategy: None,
        tcp_nodelay: true,
        pubsub_synchronizer: None,
        iam_token_provider: None,
    };

    // Create a new MultiplexedConnection (its own TCP socket)
    let conn = tokio::time::timeout(
        connection_timeout,
        client.get_multiplexed_async_connection(connection_options),
    )
    .await
    .map_err(|_| "Connection timed out".to_string())?
    .map_err(|e| format!("Connection failed: {}", e))?;

    Ok(conn)
}

// ============ State Tracking ============

/// Update ConnectionState based on the command being executed.
/// Called before each command dispatch on a scoped connection.
pub fn update_state_for_command(state: &mut ScopeConnectionState, cmd_name: &str, args: &[&[u8]]) {
    match cmd_name.to_uppercase().as_str() {
        "WATCH" => state.watch_active = true,
        "UNWATCH" => state.watch_active = false,
        "MULTI" => state.multi_active = true,
        "EXEC" => {
            // EXEC clears both WATCH and MULTI regardless of success/failure
            state.watch_active = false;
            state.multi_active = false;
        }
        "DISCARD" => {
            // DISCARD clears both MULTI and WATCH
            state.watch_active = false;
            state.multi_active = false;
        }
        "SELECT" => {
            if let Some(db_bytes) = args.first() {
                if let Ok(db_str) = std::str::from_utf8(db_bytes) {
                    if let Ok(db) = db_str.parse::<u8>() {
                        state.db_selected = db;
                    }
                }
            }
        }
        "CLIENT" => {
            if args.len() >= 2 {
                let sub_cmd = std::str::from_utf8(args[0]).unwrap_or("").to_uppercase();
                if sub_cmd == "TRACKING" {
                    let on_off = std::str::from_utf8(args[1]).unwrap_or("").to_uppercase();
                    if on_off == "ON" {
                        state.tracking_enabled = true;
                    } else if on_off == "OFF" {
                        state.tracking_enabled = false;
                    }
                } else if sub_cmd == "SETNAME" {
                    state.client_name_changed = true;
                }
            }
        }
        "SUBSCRIBE" | "PSUBSCRIBE" | "SSUBSCRIBE" => {
            for arg in args {
                let sub = match cmd_name.to_uppercase().as_str() {
                    "SUBSCRIBE" => ScopeSubscription::Channel(arg.to_vec()),
                    "PSUBSCRIBE" => ScopeSubscription::Pattern(arg.to_vec()),
                    _ => ScopeSubscription::ShardedChannel(arg.to_vec()),
                };
                state.subscriptions.push(sub);
            }
        }
        _ => {} // No state change for most commands
    }
}

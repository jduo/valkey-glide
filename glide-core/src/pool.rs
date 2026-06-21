// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Shared connection pool infrastructure for Valkey GLIDE.
//!
//! Provides two pooling features used by all language bindings:
//!
//! - **Feature 1 (Client-Instance Pool)**: Pools `GlideClient` instances for reuse
//!   across callers. Eliminates per-request client creation cost.
//!
//! - **Feature 2 (Isolated Execution)**: Per-client pool of dedicated connections
//!   for operations requiring per-connection state (WATCH, CLIENT TRACKING, BLPOP).
//!
//! Both features share the same primitives: LIFO idle list, bounded size, background
//! creation, conditional cleanup, and fire-and-forget release.
//!
//! Language bindings (Java JNI, Python CFFI, Go CGO, Node N-API) call into this
//! module via their respective FFI layers. The pool logic is entirely in Rust.

#![allow(dead_code)]

use crate::client::Client as GlideClient;
use dashmap::DashMap;
use redis::aio::MultiplexedConnection;
use redis::Cmd;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Mutex as TokioMutex;

// ═══════════════════════════════════════════════════════════════════════════════
// SHARED TYPES
// ═══════════════════════════════════════════════════════════════════════════════

/// Pool lifecycle states (shared by Feature 1 and Feature 2).
pub const POOL_RUNNING: u8 = 0;
pub const POOL_CLOSING: u8 = 1;
pub const POOL_CLOSED: u8 = 2;

/// Tracks per-connection state mutations during a borrow.
/// Used by both Feature 1 (client pool) and Feature 2 (scope pool) for
/// conditional cleanup on release.
#[derive(Default)]
pub struct ConnectionState {
    pub watch_active: bool,
    pub multi_active: bool,
    pub tracking_enabled: bool,
    pub subscriptions: Vec<Subscription>,
    pub db_selected: u8,
    pub client_name_changed: bool,
}

impl ConnectionState {
    /// Returns true if no state mutations occurred (zero-cost release path).
    pub fn is_clean(&self) -> bool {
        !self.watch_active
            && !self.multi_active
            && !self.tracking_enabled
            && self.subscriptions.is_empty()
            && self.db_selected == 0
            && !self.client_name_changed
    }

    /// Returns true if subscriptions are active (needs async unsubscribe).
    pub fn has_subscriptions(&self) -> bool {
        !self.subscriptions.is_empty()
    }

    /// Count dirty flags for RESET-vs-conditional-cleanup threshold.
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

/// Represents an active subscription tracked for cleanup.
pub enum Subscription {
    Channel(Vec<u8>),
    Pattern(Vec<u8>),
    ShardedChannel(Vec<u8>),
}

/// Update ConnectionState based on the command being executed.
/// Called before command dispatch on scoped connections (Feature 2).
pub fn update_state_for_command(state: &mut ConnectionState, cmd_name: &str, args: &[&[u8]]) {
    match cmd_name.to_uppercase().as_str() {
        "WATCH" => state.watch_active = true,
        "UNWATCH" => state.watch_active = false,
        "MULTI" => state.multi_active = true,
        "EXEC" => {
            state.watch_active = false;
            state.multi_active = false;
        }
        "DISCARD" => {
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
                    "SUBSCRIBE" => Subscription::Channel(arg.to_vec()),
                    "PSUBSCRIBE" => Subscription::Pattern(arg.to_vec()),
                    _ => Subscription::ShardedChannel(arg.to_vec()),
                };
                state.subscriptions.push(sub);
            }
        }
        _ => {}
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// FEATURE 1: CLIENT-INSTANCE POOL
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for a client-instance pool.
pub struct ClientPoolConfig {
    pub max_size: u32,
    pub min_idle: u32,
    pub idle_timeout: Duration,
    pub request_timeout: Duration,
    /// Serialized protobuf ConnectionRequest bytes for client creation.
    pub connection_request: Vec<u8>,
}

/// A client managed by the pool.
pub struct PooledClient {
    pub client_id: u64,
    pub client: GlideClient,
    pub created_at: Instant,
    pub last_returned_at: Instant,
    pub borrowed_at: Option<Instant>,
    pub connection_state: ConnectionState,
    pub state: PooledClientState,
}

pub enum PooledClientState {
    Idle,
    InUse,
    Cleaning,
}

/// The client-instance pool. Manages a bounded set of GlideClient instances.
pub struct ClientPool {
    pub config: ClientPoolConfig,
    pub idle: VecDeque<PooledClient>,
    pub in_use: DashMap<u64, PooledClient>,
    pub cleaning: DashMap<u64, PooledClient>,
    pub next_client_id: AtomicU64,
    pub total_count: AtomicU32,
    pub state: AtomicU8,
    pub eviction_handle: Option<tokio::task::JoinHandle<()>>,
}

impl ClientPool {
    /// Create a new pool with validated config.
    pub fn new(config: ClientPoolConfig) -> Result<Self, PoolError> {
        if config.max_size < 1 {
            return Err(PoolError::InvalidConfig("max_size must be >= 1".into()));
        }
        if config.min_idle > config.max_size {
            return Err(PoolError::InvalidConfig("min_idle must be <= max_size".into()));
        }
        if config.idle_timeout.is_zero() {
            return Err(PoolError::InvalidConfig("idle_timeout must be > 0".into()));
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

    /// Non-blocking acquire. Returns client_id >= 0 on success, -1 if exhausted.
    pub fn try_acquire(&mut self) -> i64 {
        if self.state.load(Ordering::Acquire) != POOL_RUNNING {
            return -1;
        }

        if let Some(mut entry) = self.idle.pop_back() {
            let client_id = entry.client_id;
            entry.state = PooledClientState::InUse;
            entry.borrowed_at = Some(Instant::now());
            entry.connection_state = ConnectionState::default();
            self.in_use.insert(client_id, entry);
            return client_id as i64;
        }

        -1
    }

    /// Check if background creation should be triggered.
    pub fn should_create_background(&self) -> bool {
        self.total_count.load(Ordering::Acquire) < self.config.max_size
    }

    /// Release a client back to the pool (fire-and-forget).
    pub fn release(&mut self, client_id: u64) -> bool {
        let entry = self.in_use.remove(&client_id);
        let Some((_, mut entry)) = entry else { return false; };

        if self.state.load(Ordering::Acquire) != POOL_RUNNING {
            self.total_count.fetch_sub(1, Ordering::AcqRel);
            return true;
        }

        entry.last_returned_at = Instant::now();
        entry.borrowed_at = None;
        entry.state = PooledClientState::Idle;
        entry.connection_state = ConnectionState::default();
        self.idle.push_back(entry);
        true
    }

    /// Destroy the pool.
    pub fn destroy(&mut self) {
        self.state.store(POOL_CLOSED, Ordering::Release);
        if let Some(handle) = self.eviction_handle.take() {
            handle.abort();
        }
        while let Some(_) = self.idle.pop_back() {
            self.total_count.fetch_sub(1, Ordering::AcqRel);
        }
        let keys: Vec<u64> = self.in_use.iter().map(|e| *e.key()).collect();
        for key in keys {
            self.in_use.remove(&key);
            self.total_count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// FEATURE 2: ISOLATED EXECUTION (SCOPE POOL)
// ═══════════════════════════════════════════════════════════════════════════════

/// Configuration for a per-client scope pool.
pub struct ScopePoolConfig {
    pub max_total: u32,
    pub idle_timeout: Duration,
    pub request_timeout: Duration,
    pub connection_timeout: Duration,
}

impl Default for ScopePoolConfig {
    fn default() -> Self {
        Self {
            max_total: 64,
            idle_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(5),
            connection_timeout: Duration::from_secs(5),
        }
    }
}

/// A dedicated connection for isolated execution.
pub struct ScopedConnection {
    pub scope_id: u64,
    pub connection: MultiplexedConnection,
    pub created_at: Instant,
    pub last_returned_at: Instant,
    pub borrowed_at: Option<Instant>,
    pub state: ConnectionState,
}

/// Per-client connection pool for isolated execution.
pub struct ScopePool {
    pub config: ScopePoolConfig,
    pub idle: VecDeque<ScopedConnection>,
    pub in_use: DashMap<u64, ()>,
    pub next_scope_id: AtomicU64,
    pub total_count: AtomicU32,
    pub state: AtomicU8,
    pub connection_request_bytes: Vec<u8>,
}

impl ScopePool {
    pub fn new(config: ScopePoolConfig, connection_request_bytes: Vec<u8>) -> Self {
        Self {
            config,
            idle: VecDeque::new(),
            in_use: DashMap::new(),
            next_scope_id: AtomicU64::new(1),
            total_count: AtomicU32::new(0),
            state: AtomicU8::new(POOL_RUNNING),
            connection_request_bytes,
        }
    }

    /// Non-blocking acquire. Returns scope_id >= 0 on success, -1 if exhausted.
    pub fn try_acquire(&mut self, scope_registry: &DashMap<u64, ScopeEntry>) -> i64 {
        if self.state.load(Ordering::Acquire) != POOL_RUNNING {
            return -1;
        }

        if let Some(mut conn) = self.idle.pop_back() {
            let scope_id = conn.scope_id;
            conn.borrowed_at = Some(Instant::now());
            conn.state = ConnectionState::default();
            scope_registry.insert(scope_id, ScopeEntry {
                connection: Arc::new(TokioMutex::new(conn)),
            });
            self.in_use.insert(scope_id, ());
            return scope_id as i64;
        }

        if self.total_count.load(Ordering::Acquire) < self.config.max_total {
            self.total_count.fetch_add(1, Ordering::AcqRel);
        }
        -1
    }

    /// Release a scope back to the pool.
    pub fn release(&mut self, scope_id: u64, scope_registry: &DashMap<u64, ScopeEntry>) -> bool {
        if self.in_use.remove(&scope_id).is_none() {
            return false;
        }

        let entry = scope_registry.remove(&scope_id);
        let Some((_, entry)) = entry else { return false; };

        if self.state.load(Ordering::Acquire) != POOL_RUNNING {
            self.total_count.fetch_sub(1, Ordering::AcqRel);
            return true;
        }

        match entry.connection.try_lock() {
            Ok(conn) => {
                if conn.state.is_clean() {
                    let idle_conn = ScopedConnection {
                        scope_id: conn.scope_id,
                        connection: conn.connection.clone(),
                        created_at: conn.created_at,
                        last_returned_at: Instant::now(),
                        borrowed_at: None,
                        state: ConnectionState::default(),
                    };
                    drop(conn);
                    self.idle.push_back(idle_conn);
                } else {
                    // Dirty — discard for prototype simplicity
                    self.total_count.fetch_sub(1, Ordering::AcqRel);
                }
                true
            }
            Err(_) => {
                self.total_count.fetch_sub(1, Ordering::AcqRel);
                true
            }
        }
    }

    pub fn destroy(&mut self, scope_registry: &DashMap<u64, ScopeEntry>) {
        self.state.store(POOL_CLOSED, Ordering::Release);
        while let Some(_) = self.idle.pop_back() {
            self.total_count.fetch_sub(1, Ordering::AcqRel);
        }
        let keys: Vec<u64> = self.in_use.iter().map(|e| *e.key()).collect();
        for key in keys {
            self.in_use.remove(&key);
            scope_registry.remove(&key);
            self.total_count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Entry in the global scope registry for command routing.
pub struct ScopeEntry {
    pub connection: Arc<TokioMutex<ScopedConnection>>,
}

// ═══════════════════════════════════════════════════════════════════════════════
// GLOBAL REGISTRIES
// ═══════════════════════════════════════════════════════════════════════════════

/// Global client pool registry: pool_id → Pool
static POOL_REGISTRY: OnceLock<DashMap<u64, Arc<TokioMutex<ClientPool>>>> = OnceLock::new();
static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

/// Global scope registry: scope_id → ScopeEntry
static SCOPE_REGISTRY: OnceLock<DashMap<u64, ScopeEntry>> = OnceLock::new();

/// Per-client scope pools: client_id → ScopePool
static CLIENT_SCOPE_POOLS: OnceLock<DashMap<u64, Arc<TokioMutex<ScopePool>>>> = OnceLock::new();

pub fn get_pool_registry() -> &'static DashMap<u64, Arc<TokioMutex<ClientPool>>> {
    POOL_REGISTRY.get_or_init(DashMap::new)
}

pub fn get_scope_registry() -> &'static DashMap<u64, ScopeEntry> {
    SCOPE_REGISTRY.get_or_init(DashMap::new)
}

pub fn get_client_scope_pools() -> &'static DashMap<u64, Arc<TokioMutex<ScopePool>>> {
    CLIENT_SCOPE_POOLS.get_or_init(DashMap::new)
}

/// Register a new client pool, returns pool_id.
pub fn register_pool(pool: ClientPool) -> u64 {
    let pool_id = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
    get_pool_registry().insert(pool_id, Arc::new(TokioMutex::new(pool)));
    pool_id
}

/// Look up a pool by ID.
pub fn get_pool(pool_id: u64) -> Option<Arc<TokioMutex<ClientPool>>> {
    get_pool_registry().get(&pool_id).map(|e| e.value().clone())
}

/// Remove a pool from the registry.
pub fn unregister_pool(pool_id: u64) -> Option<Arc<TokioMutex<ClientPool>>> {
    get_pool_registry().remove(&pool_id).map(|(_, v)| v)
}

/// Get or create a scope pool for a client.
pub fn get_or_create_scope_pool(
    client_id: u64,
    connection_request_bytes: Vec<u8>,
) -> Arc<TokioMutex<ScopePool>> {
    get_client_scope_pools()
        .entry(client_id)
        .or_insert_with(|| {
            let config = ScopePoolConfig::default();
            let pool = ScopePool::new(config, connection_request_bytes);
            Arc::new(TokioMutex::new(pool))
        })
        .value()
        .clone()
}

// ═══════════════════════════════════════════════════════════════════════════════
// ERRORS
// ═══════════════════════════════════════════════════════════════════════════════

#[derive(Debug)]
pub enum PoolError {
    InvalidConfig(String),
    PoolClosed,
    ClientCreationFailed(String),
}

impl std::fmt::Display for PoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PoolError::InvalidConfig(msg) => write!(f, "Invalid pool config: {}", msg),
            PoolError::PoolClosed => write!(f, "Pool is closed"),
            PoolError::ClientCreationFailed(msg) => write!(f, "Client creation failed: {}", msg),
        }
    }
}

impl std::error::Error for PoolError {}

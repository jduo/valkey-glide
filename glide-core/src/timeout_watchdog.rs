//! Dedicated timeout watchdog thread that fires timeouts independently of the
//! Tokio runtime and provides structured diagnostics about timeout causes.
//!
//! Design: The hot path (`register()`) sends deadlines through a lock-free MPSC
//! channel. The watchdog thread owns the deadline queue exclusively — no shared
//! Mutex on the command path.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

// ─── Public Types ────────────────────────────────────────────────────────────

/// Handle shared between the caller and the watchdog. Allows the routing layer
/// to update the node address and mark the command as sent after connection
/// resolution, without any allocation on the hot path.
#[derive(Debug)]
pub struct WatchdogHandle {
    phase: AtomicU8,
    node: std::sync::OnceLock<Arc<str>>,
}

impl WatchdogHandle {
    fn new() -> Self {
        Self {
            phase: AtomicU8::new(PHASE_QUEUED),
            node: std::sync::OnceLock::new(),
        }
    }

    /// Mark the command as sent and record the resolved node address.
    /// Called from the routing layer after connection resolution.
    #[inline]
    pub fn mark_sent(&self, node_address: &str) {
        let _ = self.node.set(Arc::from(node_address));
        self.phase.store(PHASE_SENT, Ordering::Release);
    }
}

impl redis::DiagnosticHandle for WatchdogHandle {
    fn on_sent(&self, node_address: &str) {
        self.mark_sent(node_address);
    }
}

/// Resolve a command name from raw bytes to a &'static str without allocation.
/// Falls back to "UNKNOWN" for unrecognized commands.
pub fn cmd_name_from_bytes(bytes: &[u8]) -> &'static str {
    // Compare case-insensitively against common commands
    match bytes.len() {
        2 => match_upper(bytes, &[(b"EX", "EX")]),
        3 => match_upper(bytes, &[
            (b"GET", "GET"), (b"SET", "SET"), (b"DEL", "DEL"), (b"TTL", "TTL"),
            (b"ACL", "ACL"),
        ]),
        4 => match_upper(bytes, &[
            (b"PING", "PING"), (b"MGET", "MGET"), (b"MSET", "MSET"), (b"INCR", "INCR"),
            (b"DECR", "DECR"), (b"HGET", "HGET"), (b"HSET", "HSET"), (b"HDEL", "HDEL"),
            (b"HLEN", "HLEN"), (b"LLEN", "LLEN"), (b"LPOS", "LPOS"), (b"LPOP", "LPOP"),
            (b"RPOP", "RPOP"), (b"SADD", "SADD"), (b"SREM", "SREM"), (b"ZADD", "ZADD"),
            (b"ZREM", "ZREM"), (b"KEYS", "KEYS"), (b"SCAN", "SCAN"), (b"TYPE", "TYPE"),
            (b"INFO", "INFO"), (b"WAIT", "WAIT"), (b"DUMP", "DUMP"), (b"COPY", "COPY"),
            (b"SORT", "SORT"), (b"EVAL", "EVAL"), (b"ECHO", "ECHO"), (b"AUTH", "AUTH"),
            (b"XADD", "XADD"), (b"XLEN", "XLEN"), (b"PTTL", "PTTL"),
        ]),
        5 => match_upper(bytes, &[
            (b"LPUSH", "LPUSH"), (b"RPUSH", "RPUSH"), (b"HKEYS", "HKEYS"),
            (b"HVALS", "HVALS"), (b"SCARD", "SCARD"), (b"ZCARD", "ZCARD"),
            (b"WATCH", "WATCH"), (b"MULTI", "MULTI"), (b"XREAD", "XREAD"),
            (b"GETEX", "GETEX"), (b"SETEX", "SETEX"), (b"SETNX", "SETNX"),
            (b"HMGET", "HMGET"), (b"HMSET", "HMSET"),
        ]),
        6 => match_upper(bytes, &[
            (b"APPEND", "APPEND"), (b"EXISTS", "EXISTS"), (b"EXPIRE", "EXPIRE"),
            (b"INCRBY", "INCRBY"), (b"DECRBY", "DECRBY"), (b"LPUSHX", "LPUSHX"),
            (b"RPUSHX", "RPUSHX"), (b"LRANGE", "LRANGE"), (b"LINDEX", "LINDEX"),
            (b"HSETNX", "HSETNX"), (b"SMOVE", "SMOVE"), (b"SUBSTR", "SUBSTR"),
            (b"STRLEN", "STRLEN"), (b"SELECT", "SELECT"), (b"DBSIZE", "DBSIZE"),
            (b"OBJECT", "OBJECT"), (b"CONFIG", "CONFIG"), (b"SCRIPT", "SCRIPT"),
            (b"PUBSUB", "PUBSUB"), (b"XRANGE", "XRANGE"), (b"GETDEL", "GETDEL"),
            (b"GETSET", "GETSET"), (b"RENAME", "RENAME"), (b"UNLINK", "UNLINK"),
        ]),
        7 => match_upper(bytes, &[
            (b"HGETALL", "HGETALL"), (b"LINSERT", "LINSERT"), (b"PERSIST", "PERSIST"),
            (b"PUBLISH", "PUBLISH"), (b"CLUSTER", "CLUSTER"), (b"EVALSHA", "EVALSHA"),
            (b"RESTORE", "RESTORE"), (b"FLUSHDB", "FLUSHDB"), (b"PEXPIRE", "PEXPIRE"),
        ]),
        8 => match_upper(bytes, &[
            (b"EXPIREAT", "EXPIREAT"), (b"BITCOUNT", "BITCOUNT"),
            (b"BITFIELD", "BITFIELD"), (b"FLUSHALL", "FLUSHALL"),
            (b"GETRANGE", "GETRANGE"), (b"SETRANGE", "SETRANGE"),
        ]),
        9 => match_upper(bytes, &[
            (b"PEXPIREAT", "PEXPIREAT"), (b"SUBSCRIBE", "SUBSCRIBE"),
            (b"RANDOMKEY", "RANDOMKEY"),
        ]),
        10 => match_upper(bytes, &[
            (b"PSUBSCRIBE", "PSUBSCRIBE"), (b"SINTERCARD", "SINTERCARD"),
            (b"XREADGROUP", "XREADGROUP"),
        ]),
        11 => match_upper(bytes, &[
            (b"UNSUBSCRIBE", "UNSUBSCRIBE"), (b"INCRBYFLOAT", "INCRBYFLOAT"),
            (b"ZRANGESTORE", "ZRANGESTORE"),
        ]),
        12 => match_upper(bytes, &[
            (b"PUNSUBSCRIBE", "PUNSUBSCRIBE"),
        ]),
        _ => None,
    }
    .unwrap_or("UNKNOWN")
}

fn match_upper(input: &[u8], table: &[(&[u8], &'static str)]) -> Option<&'static str> {
    for &(pattern, name) in table {
        if input.eq_ignore_ascii_case(pattern) {
            return Some(name);
        }
    }
    None
}

/// The phase a command was in when the timeout fired.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandPhase {
    /// Command was queued but never sent to the server (client-side bottleneck).
    Queued,
    /// Command was sent to the server, awaiting response.
    Sent,
}

/// Classified root cause of the timeout.
#[derive(Debug, Clone, PartialEq)]
pub enum TimeoutCause {
    /// Command was sent but the server didn't respond in time.
    ServerUnresponsive {
        node: Arc<str>,
    },
    /// Command never left the client — Tokio or connection pool bottleneck.
    ClientBackpressure {
        queue_depth: usize,
        scheduling_delay: Duration,
    },
    /// Multiple commands to the same node are timing out concurrently.
    NodeDegraded {
        node: Arc<str>,
        concurrent_timeouts: usize,
    },
    /// Broad timeout storm across multiple nodes — likely local resource exhaustion.
    SystemOverload {
        pending_total: usize,
        rss_bytes: Option<u64>,
    },
}

/// Structured timeout event returned to the caller when a deadline fires.
#[derive(Debug, Clone)]
pub struct TimeoutEvent {
    /// Classified cause of the timeout.
    pub cause: TimeoutCause,
    /// The command that timed out (e.g. "GET", "SET").
    pub command: &'static str,
    /// Target node address.
    pub node: Arc<str>,
    /// What phase the command was in when the timeout fired.
    pub phase: CommandPhase,
    /// The timeout duration that was configured.
    pub configured_timeout: Duration,
    /// Actual wall-clock time elapsed since the command was submitted.
    pub actual_elapsed: Duration,
    /// Total commands pending across all nodes at fire time.
    pub pending_commands: usize,
    /// Commands pending to the same node at fire time.
    pub same_node_pending: usize,
    /// Recent p99 latency for the target node (if available).
    pub recent_p99_latency: Option<Duration>,
    /// Process RSS at fire time (Linux/macOS only).
    pub rss_bytes: Option<u64>,
    /// Suggested timeout based on recent latency observations.
    pub suggested_timeout: Option<Duration>,
    /// Number of inflight requests at the time of timeout.
    pub inflight_count: Option<usize>,
}

// ─── Latency Tracker ─────────────────────────────────────────────────────────

/// Per-node latency ring buffer. Lock-free writes via atomic index.
/// Shared between the command completion path and the watchdog fire path.
#[derive(Debug)]
pub struct LatencyTracker {
    /// Ring buffer of latency samples in microseconds.
    samples: Box<[AtomicU64]>,
    /// Write index (wraps around).
    write_idx: AtomicUsize,
    /// Number of samples written (saturates at capacity).
    count: AtomicUsize,
    capacity: usize,
}

/// Sentinel value indicating an unwritten slot.
const LATENCY_UNWRITTEN: u64 = u64::MAX;

impl LatencyTracker {
    pub fn new(capacity: usize) -> Self {
        let samples: Vec<AtomicU64> = (0..capacity)
            .map(|_| AtomicU64::new(LATENCY_UNWRITTEN))
            .collect();
        Self {
            samples: samples.into_boxed_slice(),
            write_idx: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            capacity,
        }
    }

    /// Record a completed command latency. Called on the success path.
    pub fn record(&self, latency: Duration) {
        let micros = latency.as_micros() as u64;
        let idx = self.write_idx.fetch_add(1, Ordering::Relaxed) % self.capacity;
        self.samples[idx].store(micros, Ordering::Release);
        let _ = self.count.fetch_update(Ordering::Release, Ordering::Relaxed, |c| {
            if c < self.capacity { Some(c + 1) } else { None }
        });
    }

    /// Compute p99 from the ring buffer. Called only at fire time (rare).
    pub fn p99(&self) -> Option<Duration> {
        let n = self.count.load(Ordering::Acquire).min(self.capacity);
        if n < 10 {
            return None; // Not enough data
        }
        let mut buf: Vec<u64> = (0..n)
            .map(|i| self.samples[i].load(Ordering::Acquire))
            .filter(|&v| v != LATENCY_UNWRITTEN)
            .collect();
        if buf.len() < 10 {
            return None;
        }
        buf.sort_unstable();
        let idx = (buf.len() as f64 * 0.99) as usize;
        Some(Duration::from_micros(buf[idx.min(buf.len() - 1)]))
    }
}

// ─── System Diagnostics ──────────────────────────────────────────────────────

fn get_rss() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("VmRSS:"))
                    .and_then(|l| {
                        l.split_whitespace()
                            .nth(1)
                            .and_then(|v| v.parse::<u64>().ok())
                    })
                    .map(|kb| kb * 1024) // convert to bytes
            })
    }
    #[cfg(target_os = "macos")]
    {
        use std::sync::Mutex;
        use std::time::Instant as StdInstant;

        static CACHED: Mutex<(Option<u64>, Option<StdInstant>)> = Mutex::new((None, None));
        const CACHE_TTL: Duration = Duration::from_secs(5);

        let mut cache = CACHED.lock().ok()?;
        if let (Some(val), Some(ts)) = *cache {
            if ts.elapsed() < CACHE_TTL {
                return Some(val);
            }
        }

        let output = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &std::process::id().to_string()])
            .output()
            .ok()?;
        let rss_kb = String::from_utf8_lossy(&output.stdout)
            .trim()
            .parse::<u64>()
            .ok()?;
        let rss_bytes = rss_kb * 1024;
        *cache = (Some(rss_bytes), Some(StdInstant::now()));
        Some(rss_bytes)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

// ─── Deadline Entry ──────────────────────────────────────────────────────────

/// Internal entry sent from callers to the watchdog thread.
struct DeadlineEntry {
    deadline: Instant,
    sender: oneshot::Sender<TimeoutEvent>,
    // Diagnostic metadata (cheap to attach at register time):
    command: &'static str,
    handle: Arc<WatchdogHandle>,
    submitted_at: Instant,
    latency_tracker: Option<Arc<LatencyTracker>>,
    inflight_count: Option<usize>,
}

// ─── Timeout Watchdog ────────────────────────────────────────────────────────

/// Handle to the watchdog thread. Register deadlines and receive diagnostic
/// timeout events.
#[derive(Clone)]
pub struct TimeoutWatchdog {
    tx: mpsc::Sender<DeadlineEntry>,
}

/// Global singleton watchdog instance.
static GLOBAL_WATCHDOG: std::sync::OnceLock<TimeoutWatchdog> = std::sync::OnceLock::new();

/// Global latency tracker shared across all commands.
static GLOBAL_LATENCY_TRACKER: std::sync::OnceLock<Arc<LatencyTracker>> =
    std::sync::OnceLock::new();

/// Get the global latency tracker (lazily initialized).
pub fn global_latency_tracker() -> &'static Arc<LatencyTracker> {
    GLOBAL_LATENCY_TRACKER.get_or_init(|| Arc::new(LatencyTracker::new(4096)))
}

/// Atomic phase value constants.
const PHASE_QUEUED: u8 = 0;
const PHASE_SENT: u8 = 1;

impl TimeoutWatchdog {
    /// Get or initialize the global shared watchdog instance.
    pub fn global() -> &'static Self {
        GLOBAL_WATCHDOG.get_or_init(Self::start)
    }

    /// Start a watchdog instance. Spawns a dedicated OS thread.
    pub fn start() -> Self {
        let (tx, rx) = mpsc::channel();
        std::thread::Builder::new()
            .name("glide-timeout-watchdog".into())
            .spawn(move || Self::run(rx))
            .expect("Failed to spawn timeout watchdog thread");
        Self { tx }
    }

    /// Register a timeout with diagnostic context. Returns:
    /// - A `oneshot::Receiver<TimeoutEvent>` that resolves with diagnostics if the deadline fires
    /// - A `WatchdogHandle` that the routing layer uses to mark the command as sent and record the node
    ///
    /// This is the hot path — only cheap copies here.
    #[inline]
    pub fn register(
        &self,
        timeout: Duration,
        command: &'static str,
        latency_tracker: Option<Arc<LatencyTracker>>,
        inflight_count: Option<usize>,
    ) -> (oneshot::Receiver<TimeoutEvent>, Arc<WatchdogHandle>) {
        let (sender, rx) = oneshot::channel();
        let submitted_at = Instant::now();
        let handle = Arc::new(WatchdogHandle::new());
        let handle_clone = handle.clone();
        let deadline = submitted_at + timeout;

        let _ = self.tx.send(DeadlineEntry {
            deadline,
            sender,
            command,
            handle,
            submitted_at,
            latency_tracker,
            inflight_count,
        });

        (rx, handle_clone)
    }

    /// Watchdog thread main loop.
    fn run(rx: mpsc::Receiver<DeadlineEntry>) {
        let mut deadlines: BTreeMap<Instant, Vec<DeadlineEntry>> = BTreeMap::new();

        loop {
            let now = Instant::now();

            // Fire all expired deadlines
            while let Some(entry) = deadlines.first_entry() {
                if *entry.key() > now {
                    break;
                }
                let (_, entries) = entry.remove_entry();
                for e in entries {
                    if e.sender.is_closed() {
                        continue; // Command completed before timeout
                    }
                    let event = Self::build_event(&e, &deadlines);
                    let _ = e.sender.send(event);
                }
            }

            // Drain new registrations
            while let Ok(entry) = rx.try_recv() {
                if !entry.sender.is_closed() {
                    deadlines.entry(entry.deadline).or_default().push(entry);
                }
            }

            // Wait for next event
            let wait_result = if let Some(next_deadline) = deadlines.keys().next() {
                let sleep_duration = next_deadline.saturating_duration_since(Instant::now());
                rx.recv_timeout(sleep_duration)
            } else {
                rx.recv().map_err(|_| mpsc::RecvTimeoutError::Disconnected)
            };

            match wait_result {
                Ok(entry) => {
                    if !entry.sender.is_closed() {
                        deadlines.entry(entry.deadline).or_default().push(entry);
                    }
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => return,
            }
        }
    }

    /// Build a TimeoutEvent with diagnostics. Only called at fire time (rare).
    fn build_event(
        entry: &DeadlineEntry,
        deadlines: &BTreeMap<Instant, Vec<DeadlineEntry>>,
    ) -> TimeoutEvent {
        let now = Instant::now();
        let actual_elapsed = now.duration_since(entry.submitted_at);
        let configured_timeout = entry.deadline.duration_since(entry.submitted_at);

        let phase_val = entry.handle.phase.load(Ordering::Acquire);
        let phase = if phase_val == PHASE_SENT {
            CommandPhase::Sent
        } else {
            CommandPhase::Queued
        };

        // Resolve node: use the address set by try_cmd_request, or "unknown"
        static UNKNOWN_NODE: std::sync::OnceLock<Arc<str>> = std::sync::OnceLock::new();
        let unknown = UNKNOWN_NODE.get_or_init(|| Arc::from("unknown"));
        let node = entry.handle.node.get().unwrap_or(unknown).clone();

        // Count pending commands (total and same-node)
        let mut pending_total = 0usize;
        let mut same_node_pending = 0usize;
        for entries in deadlines.values() {
            for e in entries {
                if !e.sender.is_closed() {
                    pending_total += 1;
                    let e_node = e.handle.node.get().unwrap_or(unknown);
                    if *e_node == node {
                        same_node_pending += 1;
                    }
                }
            }
        }

        let recent_p99 = entry.latency_tracker.as_ref().and_then(|t| t.p99());
        let rss_bytes = get_rss();

        // Classify the cause
        let cause = Self::classify(
            phase,
            &node,
            actual_elapsed,
            configured_timeout,
            same_node_pending,
            pending_total,
            rss_bytes,
        );

        // Suggest a timeout: 3x the observed p99, minimum the configured timeout
        let suggested_timeout = recent_p99.map(|p99| (p99 * 3).max(configured_timeout));

        TimeoutEvent {
            cause,
            command: entry.command,
            node,
            phase,
            configured_timeout,
            actual_elapsed,
            pending_commands: pending_total,
            same_node_pending,
            recent_p99_latency: recent_p99,
            rss_bytes,
            suggested_timeout,
            inflight_count: entry.inflight_count,
        }
    }

    fn classify(
        phase: CommandPhase,
        node: &Arc<str>,
        _actual_elapsed: Duration,
        _configured_timeout: Duration,
        same_node_pending: usize,
        pending_total: usize,
        rss_bytes: Option<u64>,
    ) -> TimeoutCause {
        // Client never sent the command — backpressure or Tokio starvation
        if phase == CommandPhase::Queued {
            return TimeoutCause::ClientBackpressure {
                queue_depth: pending_total,
                scheduling_delay: _actual_elapsed,
            };
        }

        // Majority of pending commands target the same node
        if same_node_pending > 5 && same_node_pending > pending_total / 2 {
            return TimeoutCause::NodeDegraded {
                node: node.clone(),
                concurrent_timeouts: same_node_pending,
            };
        }

        // High overall pending count — system-wide problem
        if pending_total > 100 {
            return TimeoutCause::SystemOverload {
                pending_total,
                rss_bytes,
            };
        }

        // Default: server didn't respond
        TimeoutCause::ServerUnresponsive { node: node.clone() }
    }
}

// ─── Unit Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Basic Firing Behavior ────────────────────────────────────────────

    #[tokio::test]
    async fn fires_with_diagnostic_event() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, handle) = watchdog.register(Duration::from_millis(50), "GET", None, None);
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        assert_eq!(event.command, "GET");
        assert_eq!(event.node.as_ref(), "127.0.0.1:6379");
        assert_eq!(event.phase, CommandPhase::Sent);
        assert!(event.actual_elapsed >= Duration::from_millis(50));
        assert!(event.actual_elapsed < Duration::from_millis(150));
        assert_eq!(event.configured_timeout, Duration::from_millis(50));
    }

    #[tokio::test]
    async fn does_not_fire_before_deadline() {
        let watchdog = TimeoutWatchdog::start();
        let (mut rx, _handle) = watchdog.register(Duration::from_millis(200), "GET", None, None);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn multiple_deadlines_fire_in_order() {
        let watchdog = TimeoutWatchdog::start();
        let (rx1, handle1) = watchdog.register(Duration::from_millis(30), "GET", None, None);
        let (rx2, handle2) = watchdog.register(Duration::from_millis(60), "SET", None, None);
        handle1.mark_sent("127.0.0.1:6379");
        handle2.mark_sent("127.0.0.1:6379");

        let event1 = rx1.await.unwrap();
        let mid = Instant::now();
        let event2 = rx2.await.unwrap();
        let end = Instant::now();

        assert_eq!(event1.command, "GET");
        assert_eq!(event2.command, "SET");
        assert!(end.duration_since(mid) >= Duration::from_millis(20));
    }

    #[tokio::test]
    async fn cancelled_before_deadline_does_not_fire() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, _handle) = watchdog.register(Duration::from_millis(200), "GET", None, None);
        // Drop the receiver — simulates command completing before timeout
        drop(rx);
        tokio::time::sleep(Duration::from_millis(250)).await;
        // No panic, no event — watchdog handles closed senders gracefully
    }

    // ── Phase Tracking ───────────────────────────────────────────────────

    #[tokio::test]
    async fn phase_defaults_to_queued() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, _handle) = watchdog.register(Duration::from_millis(30), "SET", None, None);
        // Don't call mark_sent

        let event = rx.await.unwrap();
        assert_eq!(event.phase, CommandPhase::Queued);
    }

    #[tokio::test]
    async fn phase_transitions_to_sent() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, handle) = watchdog.register(Duration::from_millis(30), "SET", None, None);
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        assert_eq!(event.phase, CommandPhase::Sent);
    }

    #[tokio::test]
    async fn late_phase_transition_captured() {
        // Phase transitions after registration but before fire should be captured
        let watchdog = TimeoutWatchdog::start();
        let (rx, handle) = watchdog.register(Duration::from_millis(80), "SET", None, None);

        // Transition after 30ms (before the 80ms deadline)
        tokio::time::sleep(Duration::from_millis(30)).await;
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        assert_eq!(event.phase, CommandPhase::Sent);
    }

    // ── Classification Logic ─────────────────────────────────────────────

    #[tokio::test]
    async fn classifies_client_backpressure_when_queued() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, _handle) = watchdog.register(Duration::from_millis(30), "SET", None, None);

        let event = rx.await.unwrap();
        assert!(matches!(event.cause, TimeoutCause::ClientBackpressure { .. }));
    }

    #[tokio::test]
    async fn classifies_server_unresponsive_single_command() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, handle) = watchdog.register(Duration::from_millis(30), "GET", None, None);
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        assert!(matches!(event.cause, TimeoutCause::ServerUnresponsive { .. }));
    }

    #[tokio::test]
    async fn classifies_node_degraded_many_same_node() {
        let watchdog = TimeoutWatchdog::start();

        let mut receivers = Vec::new();
        for _ in 0..10 {
            let (rx, handle) = watchdog.register(Duration::from_millis(50), "GET", None, None);
            handle.mark_sent("10.0.0.1:6379");
            receivers.push(rx);
        }

        let event = receivers.remove(0).await.unwrap();
        assert!(
            matches!(&event.cause, TimeoutCause::NodeDegraded { concurrent_timeouts, .. } if *concurrent_timeouts >= 5)
        );
        assert!(event.same_node_pending >= 5);
    }

    #[tokio::test]
    async fn classifies_system_overload_many_nodes() {
        let watchdog = TimeoutWatchdog::start();

        // Register >100 commands across different nodes
        let mut receivers = Vec::new();
        for i in 0..110 {
            let (rx, handle) = watchdog.register(Duration::from_millis(50), "GET", None, None);
            handle.mark_sent(&format!("10.0.0.{}:6379", i % 50));
            receivers.push(rx);
        }

        let event = receivers.remove(0).await.unwrap();
        assert!(matches!(event.cause, TimeoutCause::SystemOverload { .. }));
    }

    // ── Pending Command Counts ───────────────────────────────────────────

    #[tokio::test]
    async fn reports_pending_command_counts() {
        let watchdog = TimeoutWatchdog::start();

        // 3 commands to node_a, 2 to node_b, all with same deadline
        let (rx_target, handle) = watchdog.register(Duration::from_millis(50), "GET", None, None);
        handle.mark_sent("10.0.0.1:6379");
        let mut _holders = Vec::new();
        for _ in 0..2 {
            let (rx, h) = watchdog.register(Duration::from_millis(50), "GET", None, None);
            h.mark_sent("10.0.0.1:6379");
            _holders.push(rx);
        }
        for _ in 0..2 {
            let (rx, h) = watchdog.register(Duration::from_millis(50), "SET", None, None);
            h.mark_sent("10.0.0.2:6379");
            _holders.push(rx);
        }

        let event = rx_target.await.unwrap();
        // At fire time, the other 4 are still pending (this one already fired)
        assert!(event.pending_commands >= 4);
        assert!(event.same_node_pending >= 2);
    }

    // ── Latency Tracker ──────────────────────────────────────────────────

    #[tokio::test]
    async fn latency_tracker_reports_p99() {
        let tracker = Arc::new(LatencyTracker::new(100));
        for i in 1..=100 {
            tracker.record(Duration::from_millis(i));
        }
        let p99 = tracker.p99().unwrap();
        assert!(p99 >= Duration::from_millis(95));
        assert!(p99 <= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn latency_tracker_returns_none_with_few_samples() {
        let tracker = Arc::new(LatencyTracker::new(100));
        for i in 1..=5 {
            tracker.record(Duration::from_millis(i));
        }
        assert!(tracker.p99().is_none());
    }

    #[tokio::test]
    async fn latency_tracker_wraps_around() {
        let tracker = Arc::new(LatencyTracker::new(10));
        // Write 20 samples into a 10-slot buffer
        for i in 1..=20 {
            tracker.record(Duration::from_millis(i));
        }
        let p99 = tracker.p99().unwrap();
        // Buffer should contain samples 11..=20 (the last 10)
        assert!(p99 >= Duration::from_millis(19));
        assert!(p99 <= Duration::from_millis(20));
    }

    #[tokio::test]
    async fn timeout_event_includes_p99_from_tracker() {
        let watchdog = TimeoutWatchdog::start();
        let tracker = Arc::new(LatencyTracker::new(100));

        // Populate tracker with latency data
        for i in 1..=100 {
            tracker.record(Duration::from_millis(i));
        }

        let (rx, handle) = watchdog.register(Duration::from_millis(30), "GET", Some(tracker), None);
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        assert!(event.recent_p99_latency.is_some());
        let p99 = event.recent_p99_latency.unwrap();
        assert!(p99 >= Duration::from_millis(95));
    }

    // ── Suggested Timeout ────────────────────────────────────────────────

    #[tokio::test]
    async fn suggested_timeout_is_3x_p99() {
        let watchdog = TimeoutWatchdog::start();
        let tracker = Arc::new(LatencyTracker::new(100));

        // All latencies are 10ms → p99 ≈ 10ms → suggested ≈ 30ms
        for _ in 0..100 {
            tracker.record(Duration::from_millis(10));
        }

        let (rx, handle) = watchdog.register(Duration::from_millis(20), "GET", Some(tracker), None);
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        let suggested = event.suggested_timeout.unwrap();
        // 3x p99 = 30ms, but configured is 20ms, so max(30, 20) = 30ms
        assert!(suggested >= Duration::from_millis(28));
        assert!(suggested <= Duration::from_millis(35));
    }

    #[tokio::test]
    async fn suggested_timeout_at_least_configured() {
        let watchdog = TimeoutWatchdog::start();
        let tracker = Arc::new(LatencyTracker::new(100));

        // All latencies are 1ms → p99 ≈ 1ms → 3x = 3ms, but configured is 30ms
        for _ in 0..100 {
            tracker.record(Duration::from_millis(1));
        }

        let (rx, handle) = watchdog.register(Duration::from_millis(30), "GET", Some(tracker), None);
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        let suggested = event.suggested_timeout.unwrap();
        // max(3ms, 30ms) = 30ms
        assert!(suggested >= Duration::from_millis(30));
    }

    #[tokio::test]
    async fn no_suggested_timeout_without_tracker() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, handle) = watchdog.register(Duration::from_millis(30), "GET", None, None);
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        assert!(event.suggested_timeout.is_none());
    }

    // ── RSS Diagnostics ──────────────────────────────────────────────────

    #[tokio::test]
    async fn rss_is_populated_on_supported_platforms() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, handle) = watchdog.register(Duration::from_millis(30), "GET", None, None);
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(event.rss_bytes.is_some());
            assert!(event.rss_bytes.unwrap() > 0);
        } else {
            assert!(event.rss_bytes.is_none());
        }
    }

    // ── Concurrency & Throughput ─────────────────────────────────────────

    #[tokio::test]
    async fn high_throughput_register() {
        let watchdog = TimeoutWatchdog::start();
        let start = Instant::now();
        let mut receivers = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
            let (rx, _) = watchdog.register(Duration::from_secs(60), "GET", None, None);
            receivers.push(rx);
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "10K registrations took {:?} — possible contention",
            elapsed
        );
        drop(receivers);
    }

    #[tokio::test]
    async fn completed_commands_dont_accumulate() {
        let watchdog = TimeoutWatchdog::start();

        // Register 1000 timeouts and immediately drop receivers
        for _ in 0..1000 {
            let (_rx, _) = watchdog.register(Duration::from_secs(1), "GET", None, None);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Watchdog should still function after cleanup
        let (rx, handle) = watchdog.register(Duration::from_millis(30), "PING", None, None);
        handle.mark_sent("127.0.0.1:6379");
        let result = tokio::time::timeout(Duration::from_millis(200), rx).await;
        assert!(result.is_ok(), "Watchdog should still function after cleanup");
    }

    #[tokio::test]
    async fn concurrent_register_from_multiple_tasks() {
        let watchdog = TimeoutWatchdog::start();

        let mut handles = Vec::new();
        for _ in 0..10 {
            let w = watchdog.clone();
            handles.push(tokio::spawn(async move {
                let mut rxs = Vec::new();
                for _ in 0..100 {
                    let (rx, handle) = w.register(
                        Duration::from_millis(50),
                        "GET",
                        None,
                        None,
                    );
                    handle.mark_sent("127.0.0.1:6379");
                    rxs.push(rx);
                }
                // All should fire
                for rx in rxs {
                    rx.await.unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
    }

    // ── Tokio Starvation ─────────────────────────────────────────────────

    #[tokio::test(flavor = "current_thread")]
    async fn watchdog_fires_under_tokio_starvation() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, _handle) = watchdog.register(Duration::from_millis(100), "PING", None, None);

        let blocker = tokio::spawn(async {
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(2) {
                tokio::task::yield_now().await;
                std::thread::sleep(Duration::from_millis(50));
            }
        });

        let result = tokio::time::timeout(Duration::from_secs(1), rx).await;
        assert!(result.is_ok(), "Watchdog should fire despite Tokio starvation");
        blocker.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn starvation_produces_diagnostic_event() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, _handle) = watchdog.register(Duration::from_millis(80), "CLUSTER SLOTS", None, None);

        let blocker = tokio::spawn(async {
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(2) {
                tokio::task::yield_now().await;
                std::thread::sleep(Duration::from_millis(30));
            }
        });

        let result = tokio::time::timeout(Duration::from_secs(1), rx).await;
        assert!(result.is_ok());
        let event = result.unwrap().unwrap();
        // Command was never sent (Tokio couldn't schedule it)
        assert_eq!(event.phase, CommandPhase::Queued);
        assert!(matches!(event.cause, TimeoutCause::ClientBackpressure { .. }));
        assert_eq!(event.command, "CLUSTER SLOTS");
        blocker.abort();
    }

    // ── Display / Debug ──────────────────────────────────────────────────

    #[tokio::test]
    async fn timeout_event_is_debug_printable() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, handle) = watchdog.register(Duration::from_millis(30), "GET", None, None);
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("GET"));
        assert!(debug_str.contains("127.0.0.1:6379"));
        assert!(debug_str.contains("ServerUnresponsive"));
    }

    // ── Wiring Verification ──────────────────────────────────────────────

    #[tokio::test]
    async fn diagnostic_handle_on_cmd_sets_node_and_phase() {
        // Simulates the full flow: register → attach to Cmd → on_sent → fire
        let watchdog = TimeoutWatchdog::start();
        let (rx, handle) = watchdog.register(Duration::from_millis(50), "SET", None, None);

        // Simulate what try_cmd_request does: call on_sent via DiagnosticHandle trait
        use redis::DiagnosticHandle;
        handle.on_sent("10.0.0.5:6379");

        let event = rx.await.unwrap();
        assert_eq!(event.command, "SET");
        assert_eq!(event.node.as_ref(), "10.0.0.5:6379");
        assert_eq!(event.phase, CommandPhase::Sent);
    }

    #[tokio::test]
    async fn diagnostic_handle_attaches_to_cmd() {
        // Verify the handle can be stored on and retrieved from a Cmd
        let watchdog = TimeoutWatchdog::start();
        let (_rx, handle) = watchdog.register(Duration::from_secs(60), "GET", None, None);

        let mut cmd = redis::cmd("GET");
        cmd.arg("mykey");
        cmd.set_diagnostic_handle(handle.clone());

        // Retrieve and call on_sent (simulates try_cmd_request)
        let retrieved = cmd.diagnostic_handle().unwrap();
        retrieved.on_sent("192.168.1.1:6379");

        // Verify the original handle was updated
        assert_eq!(handle.node.get().unwrap().as_ref(), "192.168.1.1:6379");
        assert_eq!(handle.phase.load(Ordering::Acquire), PHASE_SENT);
    }

    #[tokio::test]
    async fn cmd_name_from_bytes_resolves_common_commands() {
        assert_eq!(cmd_name_from_bytes(b"GET"), "GET");
        assert_eq!(cmd_name_from_bytes(b"get"), "GET");
        assert_eq!(cmd_name_from_bytes(b"Set"), "SET");
        assert_eq!(cmd_name_from_bytes(b"HGETALL"), "HGETALL");
        assert_eq!(cmd_name_from_bytes(b"ping"), "PING");
        assert_eq!(cmd_name_from_bytes(b"ZADD"), "ZADD");
        assert_eq!(cmd_name_from_bytes(b"LPUSH"), "LPUSH");
        assert_eq!(cmd_name_from_bytes(b"EXPIRE"), "EXPIRE");
        assert_eq!(cmd_name_from_bytes(b"CLUSTER"), "CLUSTER");
        assert_eq!(cmd_name_from_bytes(b"SUBSCRIBE"), "SUBSCRIBE");
        // Previously broken: wrong length arms
        assert_eq!(cmd_name_from_bytes(b"PTTL"), "PTTL");
        assert_eq!(cmd_name_from_bytes(b"HMGET"), "HMGET");
        assert_eq!(cmd_name_from_bytes(b"HMSET"), "HMSET");
        assert_eq!(cmd_name_from_bytes(b"BITCOUNT"), "BITCOUNT");
        assert_eq!(cmd_name_from_bytes(b"EXPIREAT"), "EXPIREAT");
        assert_eq!(cmd_name_from_bytes(b"PEXPIREAT"), "PEXPIREAT");
        assert_eq!(cmd_name_from_bytes(b"INCRBYFLOAT"), "INCRBYFLOAT");
        assert_eq!(cmd_name_from_bytes(b"unknowncmd"), "UNKNOWN");
    }

    #[tokio::test]
    async fn timeout_without_mark_sent_reports_queued_and_unknown_node() {
        // If routing never completes (e.g., connection failure), phase stays Queued
        // and node is "unknown"
        let watchdog = TimeoutWatchdog::start();
        let (rx, _handle) = watchdog.register(Duration::from_millis(30), "GET", None, None);
        // Don't call mark_sent — simulates routing failure

        let event = rx.await.unwrap();
        assert_eq!(event.phase, CommandPhase::Queued);
        assert_eq!(event.node.as_ref(), "unknown");
        assert!(matches!(event.cause, TimeoutCause::ClientBackpressure { .. }));
    }

    #[tokio::test]
    async fn inflight_count_propagated_to_event() {
        let watchdog = TimeoutWatchdog::start();
        let (rx, handle) = watchdog.register(Duration::from_millis(30), "GET", None, Some(42));
        handle.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        assert_eq!(event.inflight_count, Some(42));
    }
}

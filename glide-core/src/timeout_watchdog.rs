//! Dedicated timeout watchdog thread that fires timeouts independently of the
//! Tokio runtime and provides structured diagnostics about timeout causes.
//!
//! Design: The hot path (`register()`) sends deadlines through a lock-free MPSC
//! channel. The watchdog thread owns the deadline queue exclusively — no shared
//! Mutex on the command path.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

// ─── Public Types ────────────────────────────────────────────────────────────

// WatchdogHandle has been removed. Diagnostic state (phase, node, retry_count)
// now lives inline on redis::Cmd. The watchdog holds a Weak<redis::Cmd> and
// reads from it at fire time.
/// Resolve a command name from raw bytes to a &'static str without allocation.
/// Exhaustive coverage of all commands in the RequestType enum.
/// Falls back to "UNKNOWN" for unrecognized commands.
pub fn cmd_name_from_bytes(bytes: &[u8]) -> &'static str {
    match bytes.len() {
        3 => match_upper(
            bytes,
            &[
                (b"ACL", "ACL"),
                (b"DEL", "DEL"),
                (b"GET", "GET"),
                (b"LCS", "LCS"),
                (b"SET", "SET"),
                (b"TTL", "TTL"),
            ],
        ),
        4 => match_upper(
            bytes,
            &[
                (b"AUTH", "AUTH"),
                (b"COPY", "COPY"),
                (b"DECR", "DECR"),
                (b"DUMP", "DUMP"),
                (b"ECHO", "ECHO"),
                (b"EVAL", "EVAL"),
                (b"EXEC", "EXEC"),
                (b"HDEL", "HDEL"),
                (b"HGET", "HGET"),
                (b"HLEN", "HLEN"),
                (b"HSET", "HSET"),
                (b"HTTL", "HTTL"),
                (b"INCR", "INCR"),
                (b"INFO", "INFO"),
                (b"KEYS", "KEYS"),
                (b"LLEN", "LLEN"),
                (b"LPOP", "LPOP"),
                (b"LPOS", "LPOS"),
                (b"LREM", "LREM"),
                (b"LSET", "LSET"),
                (b"MGET", "MGET"),
                (b"MOVE", "MOVE"),
                (b"MSET", "MSET"),
                (b"PING", "PING"),
                (b"PTTL", "PTTL"),
                (b"QUIT", "QUIT"),
                (b"ROLE", "ROLE"),
                (b"RPOP", "RPOP"),
                (b"SADD", "SADD"),
                (b"SAVE", "SAVE"),
                (b"SCAN", "SCAN"),
                (b"SORT", "SORT"),
                (b"SPOP", "SPOP"),
                (b"SREM", "SREM"),
                (b"SYNC", "SYNC"),
                (b"TIME", "TIME"),
                (b"TYPE", "TYPE"),
                (b"WAIT", "WAIT"),
                (b"XACK", "XACK"),
                (b"XADD", "XADD"),
                (b"XDEL", "XDEL"),
                (b"XLEN", "XLEN"),
                (b"ZADD", "ZADD"),
                (b"ZREM", "ZREM"),
            ],
        ),
        5 => match_upper(
            bytes,
            &[
                (b"BITOP", "BITOP"),
                (b"BLPOP", "BLPOP"),
                (b"BRPOP", "BRPOP"),
                (b"FCALL", "FCALL"),
                (b"GETEX", "GETEX"),
                (b"HELLO", "HELLO"),
                (b"HKEYS", "HKEYS"),
                (b"HMGET", "HMGET"),
                (b"HMSET", "HMSET"),
                (b"HPTTL", "HPTTL"),
                (b"HSCAN", "HSCAN"),
                (b"HVALS", "HVALS"),
                (b"LMOVE", "LMOVE"),
                (b"LMPOP", "LMPOP"),
                (b"LPUSH", "LPUSH"),
                (b"LTRIM", "LTRIM"),
                (b"MULTI", "MULTI"),
                (b"PFADD", "PFADD"),
                (b"PSYNC", "PSYNC"),
                (b"RESET", "RESET"),
                (b"RPUSH", "RPUSH"),
                (b"SCARD", "SCARD"),
                (b"SDIFF", "SDIFF"),
                (b"SMOVE", "SMOVE"),
                (b"SSCAN", "SSCAN"),
                (b"TOUCH", "TOUCH"),
                (b"WATCH", "WATCH"),
                (b"XINFO", "XINFO"),
                (b"XREAD", "XREAD"),
                (b"XTRIM", "XTRIM"),
                (b"ZCARD", "ZCARD"),
                (b"ZDIFF", "ZDIFF"),
                (b"ZMPOP", "ZMPOP"),
                (b"ZRANK", "ZRANK"),
                (b"ZSCAN", "ZSCAN"),
            ],
        ),
        6 => match_upper(
            bytes,
            &[
                (b"APPEND", "APPEND"),
                (b"ASKING", "ASKING"),
                (b"BGSAVE", "BGSAVE"),
                (b"BITPOS", "BITPOS"),
                (b"BLMOVE", "BLMOVE"),
                (b"BLMPOP", "BLMPOP"),
                (b"BZMPOP", "BZMPOP"),
                (b"CLIENT", "CLIENT"),
                (b"CONFIG", "CONFIG"),
                (b"DBSIZE", "DBSIZE"),
                (b"DECRBY", "DECRBY"),
                (b"EXISTS", "EXISTS"),
                (b"EXPIRE", "EXPIRE"),
                (b"GEOADD", "GEOADD"),
                (b"GEOPOS", "GEOPOS"),
                (b"GETBIT", "GETBIT"),
                (b"GETDEL", "GETDEL"),
                (b"GETSET", "GETSET"),
                (b"HGETEX", "HGETEX"),
                (b"HSETEX", "HSETEX"),
                (b"HSETNX", "HSETNX"),
                (b"INCRBY", "INCRBY"),
                (b"LINDEX", "LINDEX"),
                (b"LOLWUT", "LOLWUT"),
                (b"LPUSHX", "LPUSHX"),
                (b"LRANGE", "LRANGE"),
                (b"MEMORY", "MEMORY"),
                (b"MODULE", "MODULE"),
                (b"MSETNX", "MSETNX"),
                (b"OBJECT", "OBJECT"),
                (b"PSETEX", "PSETEX"),
                (b"PUBSUB", "PUBSUB"),
                (b"RENAME", "RENAME"),
                (b"RPUSHX", "RPUSHX"),
                (b"SCRIPT", "SCRIPT"),
                (b"SELECT", "SELECT"),
                (b"SETBIT", "SETBIT"),
                (b"SETNX", "SETNX"),
                (b"SINTER", "SINTER"),
                (b"STRLEN", "STRLEN"),
                (b"SUBSTR", "SUBSTR"),
                (b"SUNION", "SUNION"),
                (b"SWAPDB", "SWAPDB"),
                (b"UNLINK", "UNLINK"),
                (b"XCLAIM", "XCLAIM"),
                (b"XGROUP", "XGROUP"),
                (b"XRANGE", "XRANGE"),
                (b"ZCOUNT", "ZCOUNT"),
                (b"ZINTER", "ZINTER"),
                (b"ZRANGE", "ZRANGE"),
                (b"ZSCORE", "ZSCORE"),
                (b"ZUNION", "ZUNION"),
            ],
        ),
        7 => match_upper(
            bytes,
            &[
                (b"CLUSTER", "CLUSTER"),
                (b"COMMAND", "COMMAND"),
                (b"DISCARD", "DISCARD"),
                (b"EVALSHA", "EVALSHA"),
                (b"FLUSHDB", "FLUSHDB"),
                (b"GEODIST", "GEODIST"),
                (b"GEOHASH", "GEOHASH"),
                (b"HEXISTS", "HEXISTS"),
                (b"HEXPIRE", "HEXPIRE"),
                (b"HGETALL", "HGETALL"),
                (b"HINCRBY", "HINCRBY"),
                (b"HSTRLEN", "HSTRLEN"),
                (b"LATENCY", "LATENCY"),
                (b"LINSERT", "LINSERT"),
                (b"MIGRATE", "MIGRATE"),
                (b"MONITOR", "MONITOR"),
                (b"PERSIST", "PERSIST"),
                (b"PEXPIRE", "PEXPIRE"),
                (b"PFCOUNT", "PFCOUNT"),
                (b"PFMERGE", "PFMERGE"),
                (b"PUBLISH", "PUBLISH"),
                (b"RESTORE", "RESTORE"),
                (b"SLAVEOF", "SLAVEOF"),
                (b"SLOWLOG", "SLOWLOG"),
                (b"UNWATCH", "UNWATCH"),
                (b"WAITAOF", "WAITAOF"),
                (b"ZINCRBY", "ZINCRBY"),
                (b"ZMSCORE", "ZMSCORE"),
                (b"ZPOPMAX", "ZPOPMAX"),
                (b"ZPOPMIN", "ZPOPMIN"),
            ],
        ),
        8 => match_upper(
            bytes,
            &[
                (b"BITCOUNT", "BITCOUNT"),
                (b"BITFIELD", "BITFIELD"),
                (b"BZPOPMAX", "BZPOPMAX"),
                (b"BZPOPMIN", "BZPOPMIN"),
                (b"EXPIREAT", "EXPIREAT"),
                (b"FAILOVER", "FAILOVER"),
                (b"FLUSHALL", "FLUSHALL"),
                (b"FUNCTION", "FUNCTION"),
                (b"GETRANGE", "GETRANGE"),
                (b"HPERSIST", "HPERSIST"),
                (b"HPEXPIRE", "HPEXPIRE"),
                (b"LASTSAVE", "LASTSAVE"),
                (b"READONLY", "READONLY"),
                (b"RENAMENX", "RENAMENX"),
                (b"REPLCONF", "REPLCONF"),
                (b"SETRANGE", "SETRANGE"),
                (b"SHUTDOWN", "SHUTDOWN"),
                (b"SMEMBERS", "SMEMBERS"),
                (b"SPUBLISH", "SPUBLISH"),
                (b"XPENDING", "XPENDING"),
                (b"ZREVRANK", "ZREVRANK"),
            ],
        ),
        9 => match_upper(
            bytes,
            &[
                (b"GEOSEARCH", "GEOSEARCH"),
                (b"HEXPIREAT", "HEXPIREAT"),
                (b"PEXPIREAT", "PEXPIREAT"),
                (b"RANDOMKEY", "RANDOMKEY"),
                (b"READWRITE", "READWRITE"),
                (b"REPLICAOF", "REPLICAOF"),
                (b"SISMEMBER", "SISMEMBER"),
                (b"SUBSCRIBE", "SUBSCRIBE"),
                (b"XREVRANGE", "XREVRANGE"),
                (b"ZLEXCOUNT", "ZLEXCOUNT"),
                (b"ZREVRANGE", "ZREVRANGE"),
            ],
        ),
        10 => match_upper(
            bytes,
            &[
                (b"EXPIRETIME", "EXPIRETIME"),
                (b"HPEXPIREAT", "HPEXPIREAT"),
                (b"HRANDFIELD", "HRANDFIELD"),
                (b"PSUBSCRIBE", "PSUBSCRIBE"),
                (b"SDIFFSTORE", "SDIFFSTORE"),
                (b"SINTERCARD", "SINTERCARD"),
                (b"SMISMEMBER", "SMISMEMBER"),
                (b"SSUBSCRIBE", "SSUBSCRIBE"),
                (b"XAUTOCLAIM", "XAUTOCLAIM"),
                (b"XREADGROUP", "XREADGROUP"),
                (b"ZDIFFSTORE", "ZDIFFSTORE"),
                (b"ZINTERCARD", "ZINTERCARD"),
            ],
        ),
        11 => match_upper(
            bytes,
            &[
                (b"HEXPIRETIME", "HEXPIRETIME"),
                (b"INCRBYFLOAT", "INCRBYFLOAT"),
                (b"PEXPIRETIME", "PEXPIRETIME"),
                (b"SINTERSTORE", "SINTERSTORE"),
                (b"SRANDMEMBER", "SRANDMEMBER"),
                (b"SUNIONSTORE", "SUNIONSTORE"),
                (b"UNSUBSCRIBE", "UNSUBSCRIBE"),
                (b"ZINTERSTORE", "ZINTERSTORE"),
                (b"ZRANDMEMBER", "ZRANDMEMBER"),
                (b"ZRANGEBYLEX", "ZRANGEBYLEX"),
                (b"ZRANGESTORE", "ZRANGESTORE"),
                (b"ZUNIONSTORE", "ZUNIONSTORE"),
            ],
        ),
        12 => match_upper(
            bytes,
            &[
                (b"BGREWRITEAOF", "BGREWRITEAOF"),
                (b"HINCRBYFLOAT", "HINCRBYFLOAT"),
                (b"HPEXPIRETIME", "HPEXPIRETIME"),
                (b"PUNSUBSCRIBE", "PUNSUBSCRIBE"),
                (b"SUNSUBSCRIBE", "SUNSUBSCRIBE"),
            ],
        ),
        13 => match_upper(bytes, &[(b"ZRANGEBYSCORE", "ZRANGEBYSCORE")]),
        14 => match_upper(
            bytes,
            &[
                (b"GEOSEARCHSTORE", "GEOSEARCHSTORE"),
                (b"ZREMRANGEBYLEX", "ZREMRANGEBYLEX"),
                (b"ZREVRANGEBYLEX", "ZREVRANGEBYLEX"),
            ],
        ),
        15 => match_upper(bytes, &[(b"ZREMRANGEBYRANK", "ZREMRANGEBYRANK")]),
        16 => match_upper(
            bytes,
            &[
                (b"ZREMRANGEBYSCORE", "ZREMRANGEBYSCORE"),
                (b"ZREVRANGEBYSCORE", "ZREVRANGEBYSCORE"),
            ],
        ),
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
    ServerUnresponsive { node: Arc<str> },
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
    /// Number of inflight requests when the command was submitted.
    pub inflight_at_register: Option<usize>,
    /// Number of inflight requests when the timeout fired.
    pub inflight_at_timeout: Option<usize>,
    /// Number of retries attempted before this timeout.
    pub retry_count: u8,
}

impl std::fmt::Display for TimeoutEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Timeout: cmd={} node={} cause={:?} phase={:?} \
             elapsed={:?} configured={:?}",
            self.command,
            self.node,
            self.cause,
            self.phase,
            self.actual_elapsed,
            self.configured_timeout,
        )?;
        write!(
            f,
            " pending={} same_node={}",
            self.pending_commands, self.same_node_pending
        )?;
        if let (Some(at_reg), Some(at_fire)) = (self.inflight_at_register, self.inflight_at_timeout)
        {
            let trend = if at_fire > at_reg + 10 {
                "BUILDING (backpressure increasing during timeout window)"
            } else if at_reg > at_fire + 10 {
                "DRAINING (backpressure decreasing, system recovering)"
            } else {
                "STABLE (system was already saturated at submission)"
            };
            write!(f, " inflight={}→{} {}", at_reg, at_fire, trend)?;
        }
        if let Some(p99) = self.recent_p99_latency {
            write!(f, " p99={:?}", p99)?;
        }
        if let Some(suggested) = self.suggested_timeout {
            write!(f, " suggested_timeout={:?}", suggested)?;
        }
        if self.retry_count > 0 {
            write!(f, " retries={}", self.retry_count)?;
        }
        Ok(())
    }
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
        let _ = self
            .count
            .fetch_update(Ordering::Release, Ordering::Relaxed, |c| {
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

/// Returns the process RSS in bytes. Cached for 5 seconds on both Linux and macOS
/// to avoid repeated syscalls on the watchdog thread during timeout storms.
fn get_rss() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        use std::sync::Mutex;
        use std::time::Instant as StdInstant;

        static CACHED: Mutex<(Option<u64>, Option<StdInstant>)> = Mutex::new((None, None));
        const CACHE_TTL: Duration = Duration::from_secs(5);

        let mut cache = CACHED.lock().ok()?;
        if let (Some(val), Some(ts)) = *cache
            && ts.elapsed() < CACHE_TTL
        {
            return Some(val);
        }

        let rss_bytes = std::fs::read_to_string("/proc/self/status")
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
            });

        *cache = (rss_bytes, Some(StdInstant::now()));
        rss_bytes
    }
    #[cfg(target_os = "macos")]
    {
        use std::sync::Mutex;
        use std::time::Instant as StdInstant;

        static CACHED: Mutex<(Option<u64>, Option<StdInstant>)> = Mutex::new((None, None));
        const CACHE_TTL: Duration = Duration::from_secs(5);

        let mut cache = CACHED.lock().ok()?;
        if let (Some(val), Some(ts)) = *cache
            && ts.elapsed() < CACHE_TTL
        {
            return Some(val);
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
/// Holds a Weak<redis::Cmd> to read diagnostic state at fire time.
/// Per-client diagnostic context (latency tracker, inflight counter) is
/// stored here since it's set once at registration and only read at fire time.
struct DeadlineEntry {
    deadline: Instant,
    sender: oneshot::Sender<TimeoutEvent>,
    command: &'static str,
    cmd: std::sync::Weak<redis::Cmd>,
    submitted_at: Instant,
    inflight_at_register: Option<usize>,
    /// Per-client latency tracker. Set at registration, read at fire time.
    latency_tracker: Option<Arc<LatencyTracker>>,
    /// Per-client inflight counter. Set at registration, read at fire time.
    inflight_counter: Option<Arc<AtomicIsize>>,
    /// Per-client inflight limit. Set at registration, read at fire time.
    inflight_limit: isize,
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

/// Sentinel node address used when routing never completed.
/// Static to avoid repeated allocations at fire time.
static UNKNOWN_NODE: std::sync::LazyLock<Arc<str>> =
    std::sync::LazyLock::new(|| Arc::from("unknown"));

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

    /// Register a timeout with diagnostic context. Returns a
    /// `oneshot::Receiver<TimeoutEvent>` that resolves with diagnostics if the
    /// deadline fires.
    ///
    /// The caller must wrap the `Cmd` in an `Arc` before calling this, and pass
    /// `Arc::downgrade(&cmd)` as the `cmd` parameter. The watchdog reads
    /// diagnostic state (phase, node, retry_count) directly from the Cmd at
    /// fire time. If the Cmd has been dropped (command completed), the entry is
    /// skipped.
    ///
    /// Per-client diagnostic context (latency tracker, inflight counter) is stored
    /// on the DeadlineEntry — not on the Cmd — to keep the Cmd struct lean.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    pub fn register(
        &self,
        timeout: Duration,
        command: &'static str,
        cmd: std::sync::Weak<redis::Cmd>,
        latency_tracker: Option<Arc<LatencyTracker>>,
        inflight_count: Option<usize>,
        inflight_counter: Option<Arc<AtomicIsize>>,
        inflight_limit: isize,
    ) -> oneshot::Receiver<TimeoutEvent> {
        let (sender, rx) = oneshot::channel();
        let submitted_at = Instant::now();
        let deadline = submitted_at + timeout;

        let _ = self.tx.send(DeadlineEntry {
            deadline,
            sender,
            command,
            cmd,
            submitted_at,
            inflight_at_register: inflight_count,
            latency_tracker,
            inflight_counter,
            inflight_limit,
        });

        rx
    }

    /// Watchdog thread main loop.
    fn run(rx: mpsc::Receiver<DeadlineEntry>) {
        let mut deadlines: BTreeMap<Instant, Vec<DeadlineEntry>> = BTreeMap::new();
        let mut last_cleanup = Instant::now();
        let mut last_full_diagnostic = Instant::now() - Duration::from_secs(1); // allow first fire

        loop {
            let now = Instant::now();

            // Periodic cleanup of entries whose receivers were dropped (command completed)
            // Run at most once per second to amortize cost
            if now.duration_since(last_cleanup) > Duration::from_secs(1) {
                deadlines.retain(|_, entries| {
                    entries.retain(|e| !e.sender.is_closed());
                    !entries.is_empty()
                });
                last_cleanup = now;
            }

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
                    // Rate-limit full diagnostics: at most once per 100ms window.
                    // During a timeout storm many deadlines fire at once; running
                    // build_event (with its scan + syscall) for every single one
                    // would stall the watchdog thread and delay subsequent timeout
                    // delivery — the exact failure mode the watchdog exists to prevent.
                    let event =
                        if now.duration_since(last_full_diagnostic) >= Duration::from_millis(100) {
                            last_full_diagnostic = now;
                            Self::build_event(&e, &deadlines)
                        } else {
                            Self::build_bare_event(&e)
                        };
                    let _ = e.sender.send(event);
                }
            }

            // Drain new registrations (bounded to prevent starvation of deadline firing)
            const MAX_DRAIN_BATCH: usize = 256;
            for _ in 0..MAX_DRAIN_BATCH {
                match rx.try_recv() {
                    Ok(entry) => {
                        if !entry.sender.is_closed() {
                            deadlines.entry(entry.deadline).or_default().push(entry);
                        }
                    }
                    Err(_) => break,
                }
                // If a deadline is now due, stop draining and go fire it
                if deadlines
                    .keys()
                    .next()
                    .is_some_and(|d| *d <= Instant::now())
                {
                    break;
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

    /// Build a lightweight TimeoutEvent without expensive diagnostics (no scan,
    /// no syscall). Used during timeout storms when a full diagnostic was emitted
    /// recently. Still provides phase, node, command name, and elapsed time.
    fn build_bare_event(entry: &DeadlineEntry) -> TimeoutEvent {
        let now = Instant::now();
        let actual_elapsed = now.duration_since(entry.submitted_at);
        let configured_timeout = entry.deadline.duration_since(entry.submitted_at);

        // Try to read diagnostic state from the Cmd. If the Arc was dropped
        // (command completed), use defaults.
        let (phase_val, node, retry_count) = if let Some(cmd) = entry.cmd.upgrade() {
            let p = cmd.watchdog_phase.load(Ordering::Acquire);
            let n: Arc<str> = cmd
                .watchdog_node
                .get()
                .map(|s| Arc::from(s.as_str()))
                .unwrap_or_else(|| UNKNOWN_NODE.clone());
            let r = cmd.watchdog_retry_count.load(Ordering::Relaxed);
            (p, n, r)
        } else {
            (PHASE_QUEUED, UNKNOWN_NODE.clone(), 0)
        };

        let phase = if phase_val == PHASE_SENT {
            CommandPhase::Sent
        } else {
            CommandPhase::Queued
        };

        let cause = if phase == CommandPhase::Queued {
            TimeoutCause::ClientBackpressure {
                queue_depth: 0,
                scheduling_delay: actual_elapsed,
            }
        } else {
            TimeoutCause::ServerUnresponsive { node: node.clone() }
        };

        // p99() is a cheap sort over the ring buffer — no syscall, no scan.
        let recent_p99 = entry.latency_tracker.as_ref().and_then(|t| t.p99());
        let suggested_timeout = recent_p99.map(|p99| (p99 * 3).max(configured_timeout));

        TimeoutEvent {
            cause,
            command: entry.command,
            node,
            phase,
            configured_timeout,
            actual_elapsed,
            pending_commands: 0,
            same_node_pending: 0,
            recent_p99_latency: recent_p99,
            rss_bytes: None,
            suggested_timeout,
            inflight_at_register: entry.inflight_at_register,
            inflight_at_timeout: entry
                .inflight_counter
                .as_ref()
                .map(|counter| (entry.inflight_limit - counter.load(Ordering::Relaxed)) as usize),
            retry_count,
        }
    }

    /// Build a TimeoutEvent with full diagnostics. Only called at fire time (rare),
    /// and rate-limited to at most once per 100ms window.
    fn build_event(
        entry: &DeadlineEntry,
        deadlines: &BTreeMap<Instant, Vec<DeadlineEntry>>,
    ) -> TimeoutEvent {
        let now = Instant::now();
        let actual_elapsed = now.duration_since(entry.submitted_at);
        let configured_timeout = entry.deadline.duration_since(entry.submitted_at);

        // Try to read diagnostic state from the Cmd.
        let (phase_val, node, retry_count) = if let Some(cmd) = entry.cmd.upgrade() {
            let p = cmd.watchdog_phase.load(Ordering::Acquire);
            let n: Arc<str> = cmd
                .watchdog_node
                .get()
                .map(|s| Arc::from(s.as_str()))
                .unwrap_or_else(|| UNKNOWN_NODE.clone());
            let r = cmd.watchdog_retry_count.load(Ordering::Relaxed);
            (p, n, r)
        } else {
            (PHASE_QUEUED, UNKNOWN_NODE.clone(), 0)
        };

        let phase = if phase_val == PHASE_SENT {
            CommandPhase::Sent
        } else {
            CommandPhase::Queued
        };

        // Count pending commands (total and same-node)
        // Bounded scan to avoid O(n) stall during timeout storms
        let mut pending_total = 0usize;
        let mut same_node_pending = 0usize;
        const MAX_SCAN: usize = 1024;
        let mut scanned = 0;
        'outer: for entries in deadlines.values() {
            for e in entries {
                if scanned >= MAX_SCAN {
                    break 'outer;
                }
                scanned += 1;
                if !e.sender.is_closed() {
                    pending_total += 1;
                    if let Some(e_cmd) = e.cmd.upgrade()
                        && let Some(e_node) = e_cmd.watchdog_node.get()
                        && e_node.as_str() == node.as_ref()
                    {
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
            inflight_at_register: entry.inflight_at_register,
            inflight_at_timeout: entry
                .inflight_counter
                .as_ref()
                .map(|counter| (entry.inflight_limit - counter.load(Ordering::Relaxed)) as usize),
            retry_count,
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

    /// Helper: create an Arc<Cmd> for use in tests.
    fn make_cmd() -> Arc<redis::Cmd> {
        Arc::new(redis::cmd("GET"))
    }

    // ── Basic Firing Behavior ────────────────────────────────────────────

    #[tokio::test]
    async fn fires_with_diagnostic_event() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(50),
            "GET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

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
        let cmd = make_cmd();
        let mut rx = watchdog.register(
            Duration::from_millis(200),
            "GET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn multiple_deadlines_fire_in_order() {
        let watchdog = TimeoutWatchdog::start();
        let cmd1 = make_cmd();
        let cmd2 = Arc::new(redis::cmd("SET"));
        cmd1.mark_sent("127.0.0.1:6379");
        cmd2.mark_sent("127.0.0.1:6379");
        let rx1 = watchdog.register(
            Duration::from_millis(30),
            "GET",
            Arc::downgrade(&cmd1),
            None,
            None,
            None,
            0,
        );
        let rx2 = watchdog.register(
            Duration::from_millis(60),
            "SET",
            Arc::downgrade(&cmd2),
            None,
            None,
            None,
            0,
        );

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
        let cmd = make_cmd();
        let rx = watchdog.register(
            Duration::from_millis(200),
            "GET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );
        drop(rx);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // ── Phase Tracking ───────────────────────────────────────────────────

    #[tokio::test]
    async fn phase_defaults_to_queued() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        let rx = watchdog.register(
            Duration::from_millis(30),
            "SET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

        let event = rx.await.unwrap();
        assert_eq!(event.phase, CommandPhase::Queued);
    }

    #[tokio::test]
    async fn phase_transitions_to_sent() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(30),
            "SET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

        let event = rx.await.unwrap();
        assert_eq!(event.phase, CommandPhase::Sent);
    }

    #[tokio::test]
    async fn late_phase_transition_captured() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        let rx = watchdog.register(
            Duration::from_millis(80),
            "SET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

        tokio::time::sleep(Duration::from_millis(30)).await;
        cmd.mark_sent("127.0.0.1:6379");

        let event = rx.await.unwrap();
        assert_eq!(event.phase, CommandPhase::Sent);
    }

    // ── Classification Logic ─────────────────────────────────────────────

    #[tokio::test]
    async fn classifies_client_backpressure_when_queued() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        let rx = watchdog.register(
            Duration::from_millis(30),
            "SET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

        let event = rx.await.unwrap();
        assert!(matches!(
            event.cause,
            TimeoutCause::ClientBackpressure { .. }
        ));
    }

    #[tokio::test]
    async fn classifies_server_unresponsive_single_command() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(30),
            "GET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

        let event = rx.await.unwrap();
        assert!(matches!(
            event.cause,
            TimeoutCause::ServerUnresponsive { .. }
        ));
    }

    #[tokio::test]
    async fn classifies_node_degraded_many_same_node() {
        let watchdog = TimeoutWatchdog::start();
        let mut receivers = Vec::new();
        let mut cmds = Vec::new();
        for _ in 0..10 {
            let cmd = make_cmd();
            cmd.mark_sent("10.0.0.1:6379");
            let rx = watchdog.register(
                Duration::from_millis(50),
                "GET",
                Arc::downgrade(&cmd),
                None,
                None,
                None,
                0,
            );
            cmds.push(cmd);
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
        let mut receivers = Vec::new();
        let mut cmds = Vec::new();
        for i in 0..110 {
            let cmd = make_cmd();
            cmd.mark_sent(&format!("10.0.0.{}:6379", i % 50));
            let rx = watchdog.register(
                Duration::from_millis(50),
                "GET",
                Arc::downgrade(&cmd),
                None,
                None,
                None,
                0,
            );
            cmds.push(cmd);
            receivers.push(rx);
        }

        let event = receivers.remove(0).await.unwrap();
        assert!(matches!(event.cause, TimeoutCause::SystemOverload { .. }));
    }

    // ── Pending Command Counts ───────────────────────────────────────────

    #[tokio::test]
    async fn reports_pending_command_counts() {
        let watchdog = TimeoutWatchdog::start();
        let cmd_target = make_cmd();
        cmd_target.mark_sent("10.0.0.1:6379");
        let rx_target = watchdog.register(
            Duration::from_millis(50),
            "GET",
            Arc::downgrade(&cmd_target),
            None,
            None,
            None,
            0,
        );
        let mut _holders = Vec::new();
        let mut _cmd_holders = Vec::new();
        for _ in 0..2 {
            let cmd = make_cmd();
            cmd.mark_sent("10.0.0.1:6379");
            let rx = watchdog.register(
                Duration::from_millis(50),
                "GET",
                Arc::downgrade(&cmd),
                None,
                None,
                None,
                0,
            );
            _cmd_holders.push(cmd);
            _holders.push(rx);
        }
        for _ in 0..2 {
            let cmd = Arc::new(redis::cmd("SET"));
            cmd.mark_sent("10.0.0.2:6379");
            let rx = watchdog.register(
                Duration::from_millis(50),
                "SET",
                Arc::downgrade(&cmd),
                None,
                None,
                None,
                0,
            );
            _cmd_holders.push(cmd);
            _holders.push(rx);
        }

        let event = rx_target.await.unwrap();
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
        for i in 1..=20 {
            tracker.record(Duration::from_millis(i));
        }
        let p99 = tracker.p99().unwrap();
        assert!(p99 >= Duration::from_millis(19));
        assert!(p99 <= Duration::from_millis(20));
    }

    #[tokio::test]
    async fn timeout_event_includes_p99_from_tracker() {
        let watchdog = TimeoutWatchdog::start();
        let tracker = Arc::new(LatencyTracker::new(100));
        for i in 1..=100 {
            tracker.record(Duration::from_millis(i));
        }

        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(30),
            "GET",
            Arc::downgrade(&cmd),
            Some(tracker),
            None,
            None,
            0,
        );

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
        for _ in 0..100 {
            tracker.record(Duration::from_millis(10));
        }

        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(20),
            "GET",
            Arc::downgrade(&cmd),
            Some(tracker),
            None,
            None,
            0,
        );

        let event = rx.await.unwrap();
        let suggested = event.suggested_timeout.unwrap();
        assert!(suggested >= Duration::from_millis(28));
        assert!(suggested <= Duration::from_millis(35));
    }

    #[tokio::test]
    async fn suggested_timeout_at_least_configured() {
        let watchdog = TimeoutWatchdog::start();
        let tracker = Arc::new(LatencyTracker::new(100));
        for _ in 0..100 {
            tracker.record(Duration::from_millis(1));
        }

        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(30),
            "GET",
            Arc::downgrade(&cmd),
            Some(tracker),
            None,
            None,
            0,
        );

        let event = rx.await.unwrap();
        let suggested = event.suggested_timeout.unwrap();
        assert!(suggested >= Duration::from_millis(30));
    }

    #[tokio::test]
    async fn no_suggested_timeout_without_tracker() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(30),
            "GET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

        let event = rx.await.unwrap();
        assert!(event.suggested_timeout.is_none());
    }

    // ── RSS Diagnostics ──────────────────────────────────────────────────

    #[tokio::test]
    async fn rss_is_populated_on_supported_platforms() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(30),
            "GET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

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
        let mut cmds = Vec::with_capacity(10_000);
        for _ in 0..10_000 {
            let cmd = make_cmd();
            let rx = watchdog.register(
                Duration::from_secs(60),
                "GET",
                Arc::downgrade(&cmd),
                None,
                None,
                None,
                0,
            );
            cmds.push(cmd);
            receivers.push(rx);
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(100),
            "10K registrations took {:?} — possible contention",
            elapsed
        );
        drop(receivers);
        drop(cmds);
    }

    #[tokio::test]
    async fn completed_commands_dont_accumulate() {
        let watchdog = TimeoutWatchdog::start();
        for _ in 0..1000 {
            let cmd = make_cmd();
            let _rx = watchdog.register(
                Duration::from_secs(1),
                "GET",
                Arc::downgrade(&cmd),
                None,
                None,
                None,
                0,
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(30),
            "PING",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );
        let result = tokio::time::timeout(Duration::from_millis(200), rx).await;
        assert!(
            result.is_ok(),
            "Watchdog should still function after cleanup"
        );
    }

    #[tokio::test]
    async fn concurrent_register_from_multiple_tasks() {
        let watchdog = TimeoutWatchdog::start();
        let mut handles = Vec::new();
        for _ in 0..10 {
            let w = watchdog.clone();
            handles.push(tokio::spawn(async move {
                let mut rxs = Vec::new();
                let mut cmds = Vec::new();
                for _ in 0..100 {
                    let cmd = Arc::new(redis::cmd("GET"));
                    cmd.mark_sent("127.0.0.1:6379");
                    let rx = w.register(
                        Duration::from_millis(50),
                        "GET",
                        Arc::downgrade(&cmd),
                        None,
                        None,
                        None,
                        0,
                    );
                    cmds.push(cmd);
                    rxs.push(rx);
                }
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
        let cmd = make_cmd();
        let rx = watchdog.register(
            Duration::from_millis(100),
            "PING",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

        let blocker = tokio::spawn(async {
            let start = Instant::now();
            while start.elapsed() < Duration::from_secs(2) {
                tokio::task::yield_now().await;
                std::thread::sleep(Duration::from_millis(50));
            }
        });

        let result = tokio::time::timeout(Duration::from_secs(1), rx).await;
        assert!(
            result.is_ok(),
            "Watchdog should fire despite Tokio starvation"
        );
        blocker.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn starvation_produces_diagnostic_event() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        let rx = watchdog.register(
            Duration::from_millis(80),
            "CLUSTER SLOTS",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

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
        assert_eq!(event.phase, CommandPhase::Queued);
        assert!(matches!(
            event.cause,
            TimeoutCause::ClientBackpressure { .. }
        ));
        assert_eq!(event.command, "CLUSTER SLOTS");
        blocker.abort();
    }

    // ── Display / Debug ──────────────────────────────────────────────────

    #[tokio::test]
    async fn timeout_event_is_debug_printable() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(30),
            "GET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

        let event = rx.await.unwrap();
        let debug_str = format!("{:?}", event);
        assert!(debug_str.contains("GET"));
        assert!(debug_str.contains("127.0.0.1:6379"));
        assert!(debug_str.contains("ServerUnresponsive"));
    }

    // ── Wiring Verification ──────────────────────────────────────────────

    #[tokio::test]
    async fn inline_cmd_fields_set_node_and_phase() {
        let watchdog = TimeoutWatchdog::start();
        let cmd = Arc::new(redis::cmd("SET"));
        let rx = watchdog.register(
            Duration::from_millis(50),
            "SET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

        cmd.mark_sent("10.0.0.5:6379");

        let event = rx.await.unwrap();
        assert_eq!(event.command, "SET");
        assert_eq!(event.node.as_ref(), "10.0.0.5:6379");
        assert_eq!(event.phase, CommandPhase::Sent);
    }

    #[tokio::test]
    async fn mark_sent_updates_cmd_inline_fields() {
        let cmd = Arc::new(redis::cmd("GET"));
        cmd.mark_sent("192.168.1.1:6379");

        assert_eq!(
            cmd.watchdog_node.get().unwrap().as_str(),
            "192.168.1.1:6379"
        );
        assert_eq!(cmd.watchdog_phase.load(Ordering::Acquire), PHASE_SENT);
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
        let watchdog = TimeoutWatchdog::start();
        let cmd = make_cmd();
        let rx = watchdog.register(
            Duration::from_millis(30),
            "GET",
            Arc::downgrade(&cmd),
            None,
            None,
            None,
            0,
        );

        let event = rx.await.unwrap();
        assert_eq!(event.phase, CommandPhase::Queued);
        assert_eq!(event.node.as_ref(), "unknown");
        assert!(matches!(
            event.cause,
            TimeoutCause::ClientBackpressure { .. }
        ));
    }

    #[tokio::test]
    async fn inflight_count_propagated_to_event() {
        let watchdog = TimeoutWatchdog::start();
        let inflight_allowed = Arc::new(AtomicIsize::new(958));
        let cmd = make_cmd();
        cmd.mark_sent("127.0.0.1:6379");
        let rx = watchdog.register(
            Duration::from_millis(30),
            "GET",
            Arc::downgrade(&cmd),
            None,
            Some(42),
            Some(inflight_allowed.clone()),
            1000,
        );

        let event = rx.await.unwrap();
        assert_eq!(event.inflight_at_register, Some(42));
        assert_eq!(event.inflight_at_timeout, Some(42));
    }
}

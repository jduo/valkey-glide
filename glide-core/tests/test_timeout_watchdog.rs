// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Integration tests for the timeout watchdog diagnostics.
//!
//! These tests exercise the watchdog in scenarios that simulate real client
//! behavior: concurrent commands, mixed nodes, latency tracking across
//! command lifecycles, and interaction with the global singleton.

use glide_core::timeout_watchdog::{CommandPhase, LatencyTracker, TimeoutCause, TimeoutWatchdog};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ─── Global Singleton Tests ──────────────────────────────────────────────────

#[tokio::test]
async fn global_watchdog_is_singleton() {
    let w1 = TimeoutWatchdog::global();
    let w2 = TimeoutWatchdog::global();
    // Same pointer — only one instance
    assert!(std::ptr::eq(w1, w2));
}

#[tokio::test]
async fn global_watchdog_fires_timeout() {
    let watchdog = TimeoutWatchdog::global();
    let (rx, handle) = watchdog.register(Duration::from_millis(40), "PING", None, None, None, 0);
    handle.mark_sent("127.0.0.1:6379");

    let event = rx.await.unwrap();
    assert_eq!(event.command, "PING");
    assert_eq!(event.phase, CommandPhase::Sent);
}

// ─── Simulated Command Lifecycle ─────────────────────────────────────────────

/// Simulates a command that completes before the timeout — the watchdog should
/// not produce an event.
#[tokio::test]
async fn command_completes_before_timeout() {
    let watchdog = TimeoutWatchdog::start();
    let tracker = Arc::new(LatencyTracker::new(100));

    let (rx, handle) = watchdog.register(
        Duration::from_millis(200),
        "GET",
        Some(tracker.clone()),
        None,
        None,
        0,
    );
    handle.mark_sent("127.0.0.1:6379");

    // Simulate command completing after 50ms
    tokio::time::sleep(Duration::from_millis(50)).await;
    tracker.record(Duration::from_millis(50));
    drop(rx); // Command done — drop the receiver

    // Wait past the deadline — nothing should panic or leak
    tokio::time::sleep(Duration::from_millis(200)).await;
}

/// Simulates a burst of commands where some complete and some timeout.
#[tokio::test]
async fn mixed_completion_and_timeout() {
    let watchdog = TimeoutWatchdog::start();
    let tracker = Arc::new(LatencyTracker::new(100));

    // Pre-populate tracker with normal latencies
    for _ in 0..50 {
        tracker.record(Duration::from_millis(5));
    }

    // Register 5 commands: first 3 will "complete", last 2 will timeout
    let mut timeout_receivers = Vec::new();
    for i in 0..5 {
        let (rx, handle) = watchdog.register(
            Duration::from_millis(100),
            "GET",
            Some(tracker.clone()),
            None,
            None,
            0,
        );
        handle.mark_sent("127.0.0.1:6379");
        if i < 3 {
            // Simulate completion
            tracker.record(Duration::from_millis(10));
            drop(rx);
        } else {
            timeout_receivers.push(rx);
        }
    }

    // The 2 remaining should timeout with diagnostics
    for rx in timeout_receivers {
        let event = rx.await.unwrap();
        assert_eq!(event.command, "GET");
        assert_eq!(event.phase, CommandPhase::Sent);
        assert!(event.recent_p99_latency.is_some());
    }
}

// ─── Multi-Node Scenarios ────────────────────────────────────────────────────

/// When commands to multiple nodes timeout, the classification should reflect
/// the distribution.
#[tokio::test]
async fn multi_node_timeout_classification() {
    let watchdog = TimeoutWatchdog::start();

    // 3 commands to node A, 3 to node B — evenly distributed

    let mut receivers = Vec::new();
    for _ in 0..3 {
        let (rx, handle) = watchdog.register(Duration::from_millis(50), "GET", None, None, None, 0);
        handle.mark_sent("10.0.0.1:6379");
        receivers.push(rx);
    }
    for _ in 0..3 {
        let (rx, handle) = watchdog.register(Duration::from_millis(50), "SET", None, None, None, 0);
        handle.mark_sent("10.0.0.2:6379");
        receivers.push(rx);
    }

    let event = receivers.remove(0).await.unwrap();
    // With only 3 per node out of 6 total, neither dominates — should not be NodeDegraded
    assert!(!matches!(event.cause, TimeoutCause::NodeDegraded { .. }));
}

/// When one node has the majority of pending timeouts, it should be classified
/// as NodeDegraded.
#[tokio::test]
async fn single_node_dominates_pending() {
    let watchdog = TimeoutWatchdog::start();

    // 8 commands to bad_node, 2 to good_node
    let mut receivers = Vec::new();
    for _ in 0..8 {
        let (rx, handle) = watchdog.register(Duration::from_millis(50), "GET", None, None, None, 0);
        handle.mark_sent("10.0.0.99:6379");
        receivers.push(rx);
    }
    for _ in 0..2 {
        let (rx, handle) = watchdog.register(Duration::from_millis(50), "GET", None, None, None, 0);
        handle.mark_sent("10.0.0.1:6379");
        receivers.push(rx);
    }

    // First event should be for bad_node and classified as NodeDegraded
    let event = receivers.remove(0).await.unwrap();
    assert_eq!(event.node.as_ref(), "10.0.0.99:6379");
    assert!(matches!(event.cause, TimeoutCause::NodeDegraded { .. }));
}

// ─── Latency Tracker Integration ─────────────────────────────────────────────

/// Latency tracker shared across multiple commands accumulates data correctly.
#[tokio::test]
async fn shared_tracker_across_commands() {
    let watchdog = TimeoutWatchdog::start();
    let tracker = Arc::new(LatencyTracker::new(1024));

    // Simulate 200 successful commands with varying latency
    for i in 0..200 {
        let latency = Duration::from_millis(5 + (i % 20));
        tracker.record(latency);
    }

    // Now a command times out — should see the accumulated p99
    let (rx, handle) = watchdog.register(
        Duration::from_millis(30),
        "HGETALL",
        Some(tracker.clone()),
        None,
        None,
        0,
    );
    handle.mark_sent("127.0.0.1:6379");

    let event = rx.await.unwrap();
    let p99 = event.recent_p99_latency.unwrap();
    // Latencies range from 5ms to 24ms, p99 should be near the high end
    assert!(p99 >= Duration::from_millis(20));
    assert!(p99 <= Duration::from_millis(25));
    // Suggested timeout should be 3x p99
    let suggested = event.suggested_timeout.unwrap();
    assert!(suggested >= Duration::from_millis(60));
}

/// Latency tracker under concurrent writes doesn't corrupt data.
#[tokio::test]
async fn concurrent_latency_recording() {
    let tracker = Arc::new(LatencyTracker::new(1024));

    let mut handles = Vec::new();
    for t in 0..10 {
        let tr = tracker.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..100 {
                tr.record(Duration::from_micros(100 * (t * 100 + i)));
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Should have data and not panic
    let p99 = tracker.p99();
    assert!(p99.is_some());
}

// ─── Timing Accuracy ─────────────────────────────────────────────────────────

/// Verify that actual_elapsed in the event is reasonably accurate.
#[tokio::test]
async fn actual_elapsed_accuracy() {
    let watchdog = TimeoutWatchdog::start();
    let start = Instant::now();
    let (rx, handle) = watchdog.register(Duration::from_millis(75), "GET", None, None, None, 0);
    handle.mark_sent("127.0.0.1:6379");

    let event = rx.await.unwrap();
    let wall_elapsed = start.elapsed();

    // The event's actual_elapsed should be close to wall clock
    let diff = if event.actual_elapsed > wall_elapsed {
        event.actual_elapsed - wall_elapsed
    } else {
        wall_elapsed - event.actual_elapsed
    };
    assert!(
        diff < Duration::from_millis(20),
        "Elapsed drift too high: {:?}",
        diff
    );
}

/// Configured timeout in the event matches what was registered.
#[tokio::test]
async fn configured_timeout_matches_registration() {
    let watchdog = TimeoutWatchdog::start();
    let timeout = Duration::from_millis(42);
    let (rx, handle) = watchdog.register(timeout, "SET", None, None, None, 0);
    handle.mark_sent("127.0.0.1:6379");

    let event = rx.await.unwrap();
    assert_eq!(event.configured_timeout, timeout);
}

// ─── Stress / Reliability ────────────────────────────────────────────────────

/// Many short-lived registrations followed by a real timeout — watchdog stays healthy.
#[tokio::test]
async fn watchdog_survives_rapid_register_cancel_cycles() {
    let watchdog = TimeoutWatchdog::start();

    // Rapid register + cancel (simulates fast commands)
    for _ in 0..5000 {
        let (rx, _) = watchdog.register(Duration::from_secs(10), "GET", None, None, None, 0);
        drop(rx);
    }

    // Now register one that should actually fire
    let (rx, handle) = watchdog.register(Duration::from_millis(30), "PING", None, None, None, 0);
    handle.mark_sent("127.0.0.1:6379");

    let result = tokio::time::timeout(Duration::from_millis(200), rx).await;
    assert!(result.is_ok());
    let event = result.unwrap().unwrap();
    assert_eq!(event.command, "PING");
}

/// Watchdog handles zero-duration timeout gracefully.
#[tokio::test]
async fn zero_duration_timeout_fires_immediately() {
    let watchdog = TimeoutWatchdog::start();
    let (rx, handle) = watchdog.register(Duration::from_millis(0), "GET", None, None, None, 0);
    handle.mark_sent("127.0.0.1:6379");

    let result = tokio::time::timeout(Duration::from_millis(100), rx).await;
    assert!(
        result.is_ok(),
        "Zero-duration timeout should fire immediately"
    );
}

/// Very long timeout doesn't block other shorter timeouts.
#[tokio::test]
async fn long_timeout_doesnt_block_short() {
    let watchdog = TimeoutWatchdog::start();

    // Register a 10-second timeout first
    let (_long_rx, _) = watchdog.register(Duration::from_secs(10), "SLOWLOG", None, None, None, 0);

    // Then a 50ms timeout — should fire on time
    let start = Instant::now();
    let (short_rx, handle) =
        watchdog.register(Duration::from_millis(50), "GET", None, None, None, 0);
    handle.mark_sent("127.0.0.1:6379");

    let event = short_rx.await.unwrap();
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(150),
        "Short timeout blocked: {:?}",
        elapsed
    );
    assert_eq!(event.command, "GET");
}

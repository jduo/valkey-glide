// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use logger_core::log_debug;
use nanoid::nanoid;
use once_cell::sync::Lazy;
use redis::{RedisResult, ScanStateRC};
use std::collections::HashMap;
use tokio::sync::Mutex;

static CONTAINER: Lazy<Mutex<HashMap<String, ScanStateRC>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

// Internal async implementation - use this from async contexts
pub async fn insert_cluster_scan_cursor_async(scan_state: ScanStateRC) -> String {
    let id = nanoid!();
    CONTAINER.lock().await.insert(id.clone(), scan_state);
    log_debug(
        "scan_state_cursor insert",
        format!("Inserted to container scan_state_cursor with id: `{:?}`", id),
    );
    id
}

// Public blocking API - use this from sync contexts only
pub fn insert_cluster_scan_cursor(scan_state: ScanStateRC) -> String {
    tokio::runtime::Handle::current().block_on(insert_cluster_scan_cursor_async(scan_state))
}

// Internal async implementation - use this from async contexts
pub async fn get_cluster_scan_cursor_async(id: String) -> RedisResult<ScanStateRC> {
    let scan_state_rc = CONTAINER.lock().await.get(&id).cloned();
    log_debug(
        "scan_state_cursor get",
        format!("Retrieved from container scan_state_cursor with id: `{:?}`", id),
    );
    scan_state_rc.ok_or_else(|| {
        redis::RedisError::from((
            redis::ErrorKind::ResponseError,
            "Invalid scan state cursor",
        ))
    })
}

// Public blocking API - use this from sync contexts only
pub fn get_cluster_scan_cursor(id: String) -> RedisResult<ScanStateRC> {
    tokio::runtime::Handle::current().block_on(get_cluster_scan_cursor_async(id))
}

pub async fn remove_scan_state_cursor_async(id: String) {
    CONTAINER.lock().await.remove(&id);
    log_debug(
        "scan_state_cursor remove",
        format!("Removed from container scan_state_cursor with id: `{:?}`", id),
    );
}

// Public blocking API - use this from sync contexts only
pub fn remove_scan_state_cursor(id: String) {
    tokio::runtime::Handle::current().block_on(remove_scan_state_cursor_async(id))
}

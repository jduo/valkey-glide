// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use logger_core::log_debug;
use nanoid::nanoid;
use once_cell::sync::Lazy;
use redis::{RedisResult, ScanStateRC};
use std::collections::HashMap;
use tokio::sync::Mutex;

static CONTAINER: Lazy<Mutex<HashMap<String, ScanStateRC>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub async fn insert_cluster_scan_cursor(scan_state: ScanStateRC) -> String {
    let id = nanoid!();
    CONTAINER.lock().await.insert(id.clone(), scan_state);
    log_debug(
        "scan_state_cursor insert",
        format!("Inserted to container scan_state_cursor with id: `{:?}`", id),
    );
    id
}

pub async fn get_cluster_scan_cursor(id: String) -> RedisResult<ScanStateRC> {
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

pub async fn remove_scan_state_cursor(id: String) {
    CONTAINER.lock().await.remove(&id);
    log_debug(
        "scan_state_cursor remove",
        format!("Removed from container scan_state_cursor with id: `{:?}`", id),
    );
}

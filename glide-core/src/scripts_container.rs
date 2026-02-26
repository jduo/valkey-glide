// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use bytes::BytesMut;
use logger_core::log_debug;
use once_cell::sync::Lazy;
use sha1_smol::Sha1;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

struct ScriptEntry {
    code: Arc<BytesMut>,
    ref_count: usize,
}

static CONTAINER: Lazy<Mutex<HashMap<String, ScriptEntry>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub async fn add_script(script: &[u8]) -> String {
    let hash = Sha1::from(script).digest().to_string();
    let mut container = CONTAINER.lock().await;
    container
        .entry(hash.clone())
        .and_modify(|entry| entry.ref_count += 1)
        .or_insert_with(|| ScriptEntry {
            code: Arc::new(BytesMut::from(script)),
            ref_count: 1,
        });
    log_debug(
        "scripts_container add",
        format!("Added script with hash: `{:?}`", hash),
    );
    hash
}

pub async fn get_script(hash: &str) -> Option<Arc<BytesMut>> {
    CONTAINER.lock().await.get(hash).map(|entry| entry.code.clone())
}

pub async fn remove_script(hash: &str) {
    let mut container = CONTAINER.lock().await;
    if let Some(entry) = container.get_mut(hash) {
        entry.ref_count -= 1;
        if entry.ref_count == 0 {
            container.remove(hash);
            log_debug(
                "scripts_container remove",
                format!("Removed script with hash: `{:?}`", hash),
            );
        }
    }
}

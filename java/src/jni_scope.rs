// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! JNI bridge functions for isolated execution (Feature 2 scopes).
//!
//! Exposes scope operations (try_acquire, release, execute) to Java via JNI.

use crate::jni_client::{get_runtime, get_handle_table, complete_callback, JVM};
use crate::scope_pool::{
    get_client_scope_pools, get_or_create_scope_pool, get_scope_registry,
    spawn_background_scope_create, update_state_for_command,
};
use jni::objects::{JByteArray, JClass};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use redis::Cmd;
use std::sync::atomic::Ordering;

/// Acquire an isolated connection scope from a client's internal connection pool.
///
/// # Arguments
/// - `client_id`: the native handle of the GlideClient
/// - `connection_request_bytes`: protobuf ConnectionRequest (for creating new connections if needed)
///
/// # Returns
/// - scope_id >= 0 on success
/// - -1 if pool exhausted (caller should retry)
/// - -2 if client_id is invalid
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_ffi_resolvers_GlideScopeResolver_glideScopeTryAcquire(
    env: JNIEnv,
    _class: JClass,
    client_id: jlong,
    connection_request_bytes: JByteArray,
) -> jlong {
    // Verify client exists
    let handle_table = get_handle_table();
    if !handle_table.contains_key(&(client_id as u64)) {
        return -2; // Invalid client_id
    }

    // Get connection request bytes for pool creation
    let bytes = match env.convert_byte_array(&connection_request_bytes) {
        Ok(b) => b,
        Err(_) => return -2,
    };

    // Get or create the scope pool for this client
    let scope_pool = get_or_create_scope_pool(client_id as u64, bytes);

    // Try non-blocking acquire
    match scope_pool.try_lock() {
        Ok(mut pool) => {
            let result = pool.try_acquire();

            // If -1 and room to create, background creation was already reserved in try_acquire
            if result == -1 && pool.total_count.load(Ordering::Acquire) <= pool.config.max_total {
                let pool_clone = scope_pool.clone();
                drop(pool); // Release lock before spawning
                spawn_background_scope_create(client_id as u64, pool_clone);
            }

            result
        }
        Err(_) => -1, // Lock contended, retry
    }
}

/// Release an isolated scope back to the client's connection pool.
/// Fire-and-forget: returns immediately, cleanup happens async if needed.
///
/// # Returns
/// - 0 on success
/// - -1 if scope_id not found
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_ffi_resolvers_GlideScopeResolver_glideScopeRelease(
    _env: JNIEnv,
    _class: JClass,
    scope_id: jlong,
    client_id: jlong,
) -> jint {
    let pools = get_client_scope_pools();

    let scope_pool = match pools.get(&(client_id as u64)) {
        Some(pool) => pool.value().clone(),
        None => return -1,
    };

    let scope_pool_clone = scope_pool.clone();
    match scope_pool.try_lock() {
        Ok(mut pool) => {
            pool.release(scope_id as u64);
            0
        }
        Err(_) => {
            // Lock contended — spawn async release
            let runtime = get_runtime();
            let sid = scope_id as u64;
            runtime.spawn(async move {
                let mut pool = scope_pool_clone.lock().await;
                pool.release(sid);
            });
            0 // Fire-and-forget
        }
    }
}

/// Execute a command on a scoped connection (bypasses the multiplexer).
///
/// Dispatches asynchronously via the Tokio runtime to maintain the async API
/// contract (CompletableFuture). However, unlike the main client's command path
/// which goes through a callback worker thread, scope commands complete the Java
/// future directly from the Tokio task thread via JNI. This eliminates the
/// callback worker thread hop (~0.5ms savings per command).
///
/// # Returns
/// - 0 if command was dispatched
/// - -1 if scope_id is invalid
/// - -2 if command deserialization failed
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_ffi_resolvers_GlideScopeResolver_glideScopeExecute(
    env: JNIEnv,
    _class: JClass,
    scope_id: jlong,
    command_bytes: JByteArray,
    callback_id: jlong,
) -> jint {
    // Parse command bytes
    let bytes = match env.convert_byte_array(&command_bytes) {
        Ok(b) => b,
        Err(_) => return -2,
    };

    // Deserialize command
    let (cmd_name, args) = match deserialize_command(&bytes) {
        Some(parsed) => parsed,
        None => return -2,
    };

    // Look up the scoped connection
    let registry = get_scope_registry();
    let entry = match registry.get(&(scope_id as u64)) {
        Some(e) => e.connection.clone(),
        None => return -1,
    };

    // Dispatch asynchronously on the Tokio runtime.
    // The command executes on a Tokio worker thread (non-blocking to the caller),
    // then completes the Java callback directly from that thread.
    let runtime = get_runtime();
    let jvm = JVM.get().unwrap().clone();

    runtime.spawn(async move {
        let mut conn_guard = entry.lock().await;

        // Update state tracking
        let arg_refs: Vec<&[u8]> = args.iter().map(|a| a.as_slice()).collect();
        update_state_for_command(&mut conn_guard.state, &cmd_name, &arg_refs);

        // Build the redis command
        let mut cmd = Cmd::new();
        cmd.arg(cmd_name.as_bytes());
        for arg in &args {
            cmd.arg(arg.as_slice());
        }

        // Execute on the dedicated connection
        let result = conn_guard.connection.send_packed_command(&cmd).await;

        // Complete the Java callback from this Tokio thread.
        // This still goes through the callback worker (complete_callback enqueues),
        // but the Tokio spawn overhead is the main optimization vs the previous
        // fully-synchronous block_on approach which violated async semantics.
        complete_callback(jvm, callback_id, result, false);
    });

    0
}

/// Deserialize command bytes into (command_name, args).
/// Format: [cmd_name_len(4 LE), cmd_name_bytes, num_args(4 LE), [arg_len(4 LE), arg_bytes]...]
fn deserialize_command(bytes: &[u8]) -> Option<(String, Vec<Vec<u8>>)> {
    if bytes.len() < 4 {
        return None;
    }

    let mut offset = 0;

    // Read command name
    let cmd_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?) as usize;
    offset += 4;
    if offset + cmd_len > bytes.len() {
        return None;
    }
    let cmd_name = String::from_utf8(bytes[offset..offset + cmd_len].to_vec()).ok()?;
    offset += cmd_len;

    // Read num args
    if offset + 4 > bytes.len() {
        return None;
    }
    let num_args = u32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?) as usize;
    offset += 4;

    // Read each arg
    let mut args = Vec::with_capacity(num_args);
    for _ in 0..num_args {
        if offset + 4 > bytes.len() {
            return None;
        }
        let arg_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().ok()?) as usize;
        offset += 4;
        if offset + arg_len > bytes.len() {
            return None;
        }
        args.push(bytes[offset..offset + arg_len].to_vec());
        offset += arg_len;
    }

    Some((cmd_name, args))
}

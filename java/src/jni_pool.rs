// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! JNI bridge functions for the client-instance pool.
//!
//! Exposes pool operations (create, try_acquire, release, destroy, metrics)
//! to Java via JNI-callable extern "system" functions.

use crate::jni_client::get_runtime;
use crate::pool::{
    get_pool, register_pool, spawn_background_create, start_eviction_task, unregister_pool, Pool,
    PoolConfig,
};
use jni::objects::{JByteArray, JClass};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Create a new pool. Returns pool_id (positive) or error code (negative).
///
/// # Arguments
/// - `max_size`: maximum number of clients in the pool
/// - `min_idle`: minimum idle clients to pre-warm
/// - `idle_timeout_ms`: idle timeout in milliseconds
/// - `request_timeout_ms`: request timeout in milliseconds (used for cleanup: 2×)
/// - `connection_request_bytes`: protobuf-serialized ConnectionRequest
///
/// # Returns
/// - Positive pool_id on success
/// - -1 on invalid config
/// - -2 on other errors
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_ffi_resolvers_GlidePoolResolver_glidePoolCreate(
    env: JNIEnv,
    _class: JClass,
    max_size: jint,
    min_idle: jint,
    idle_timeout_ms: jlong,
    request_timeout_ms: jlong,
    connection_request_bytes: JByteArray,
) -> jlong {
    // Convert JByteArray to Vec<u8>
    let bytes = match env.convert_byte_array(&connection_request_bytes) {
        Ok(b) => b,
        Err(e) => {
            log::error!(
                "glide_pool_create: Failed to read connection_request_bytes: {}",
                e
            );
            return -2;
        }
    };

    // Build PoolConfig
    let config = PoolConfig {
        max_size: max_size as u32,
        min_idle: min_idle as u32,
        idle_timeout: Duration::from_millis(idle_timeout_ms as u64),
        request_timeout: Duration::from_millis(request_timeout_ms as u64),
        connection_request: bytes,
    };

    // Create pool with validation
    let pool = match Pool::new(config) {
        Ok(p) => p,
        Err(e) => {
            log::error!("glide_pool_create: Invalid config: {}", e);
            return -1;
        }
    };

    // Extract config values before registering (needed for eviction task)
    let idle_timeout = pool.config.idle_timeout;
    let min_idle_val = pool.config.min_idle;
    let max_size_val = pool.config.max_size;

    // Register in the global pool registry
    let pool_id = register_pool(pool);

    // Get the Arc reference to the registered pool
    let pool_arc = match get_pool(pool_id) {
        Some(arc) => arc,
        None => {
            log::error!("glide_pool_create: Failed to retrieve registered pool");
            return -2;
        }
    };

    // Start eviction task and spawn min_idle background client creation
    {
        let runtime = get_runtime();
        let pool_arc_eviction = pool_arc.clone();
        let eviction_handle =
            start_eviction_task(pool_arc_eviction, idle_timeout, min_idle_val, max_size_val);

        // Store the eviction handle and spawn min_idle background creations
        let pool_arc_init = pool_arc.clone();
        runtime.spawn(async move {
            let mut pool_guard = pool_arc_init.lock().await;
            pool_guard.eviction_handle = Some(eviction_handle);

            // Spawn min_idle background client creation tasks
            for _ in 0..min_idle_val {
                if pool_guard.total_count.load(Ordering::Acquire) < max_size_val {
                    pool_guard.total_count.fetch_add(1, Ordering::AcqRel);
                    spawn_background_create(get_pool(pool_id).unwrap());
                }
            }
        });
    }

    pool_id as jlong
}

/// Non-blocking acquire. Returns client_id >= 0 on success, -1 if exhausted.
/// If -1 returned and total < max_size, triggers background creation.
/// Returns -2 if pool_id is invalid.
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_ffi_resolvers_GlidePoolResolver_glidePoolTryAcquire(
    _env: JNIEnv,
    _class: JClass,
    pool_id: jlong,
) -> jlong {
    let pool_arc = match get_pool(pool_id as u64) {
        Some(arc) => arc,
        None => return -2, // Invalid pool_id
    };

    // Use try_lock to avoid blocking the JNI thread.
    // If the lock is contended, return -1 (caller will retry).
    let mut pool_guard = match pool_arc.try_lock() {
        Ok(guard) => guard,
        Err(_) => return -1, // Lock contended, retry
    };

    let result = pool_guard.try_acquire();

    // If no client available and room to create, trigger background creation
    if result == -1 && pool_guard.should_create_background() {
        pool_guard.total_count.fetch_add(1, Ordering::AcqRel);
        let pool_clone = pool_arc.clone();
        drop(pool_guard); // Release lock before spawning
        spawn_background_create(pool_clone);
    }

    result
}

/// Release a borrowed client back to the pool. Fire-and-forget.
/// Returns 0 on success, -1 if pool not found.
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_ffi_resolvers_GlidePoolResolver_glidePoolRelease(
    _env: JNIEnv,
    _class: JClass,
    pool_id: jlong,
    client_id: jlong,
) -> jint {
    let pool_arc = match get_pool(pool_id as u64) {
        Some(arc) => arc,
        None => return -1, // Invalid pool_id
    };

    // Use try_lock — if contended, spawn async release
    match pool_arc.try_lock() {
        Ok(mut pool_guard) => {
            pool_guard.release(client_id as u64, pool_arc.clone());
            0
        }
        Err(_) => {
            // Lock contended — spawn async release on runtime
            let runtime = get_runtime();
            let pool_clone = pool_arc.clone();
            runtime.spawn(async move {
                let mut pool_guard = pool_clone.lock().await;
                pool_guard.release(client_id as u64, pool_clone.clone());
            });
            0 // Fire-and-forget: always success from caller's perspective
        }
    }
}

/// Destroy the pool. Closes all idle and in-use clients.
/// Returns 0 on success, -1 if pool not found.
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_ffi_resolvers_GlidePoolResolver_glidePoolDestroy(
    _env: JNIEnv,
    _class: JClass,
    pool_id: jlong,
) -> jint {
    // Remove from registry (prevents future lookups)
    let pool_arc = match unregister_pool(pool_id as u64) {
        Some(arc) => arc,
        None => return -1, // Already destroyed or invalid
    };

    // Destroy the pool contents
    let runtime = get_runtime();
    runtime.spawn(async move {
        let mut pool_guard = pool_arc.lock().await;
        pool_guard.destroy();
    });

    0
}

/// Query pool metrics. Returns a jintArray with [idle_count, active_count, total_count].
/// Returns null on error (invalid pool_id).
#[unsafe(no_mangle)]
pub extern "system" fn Java_glide_ffi_resolvers_GlidePoolResolver_glidePoolMetrics(
    env: JNIEnv,
    _class: JClass,
    pool_id: jlong,
) -> jni::sys::jintArray {
    let pool_arc = match get_pool(pool_id as u64) {
        Some(arc) => arc,
        None => return std::ptr::null_mut(), // Invalid pool_id
    };

    // Use try_lock for non-blocking metrics query
    let (idle, active, total) = match pool_arc.try_lock() {
        Ok(pool_guard) => {
            let idle = pool_guard.idle.len() as i32;
            let active = pool_guard.in_use.len() as i32;
            let total = pool_guard.total_count.load(Ordering::Acquire) as i32;
            (idle, active, total)
        }
        Err(_) => {
            // Lock contended — return approximate metrics (zeros)
            (0i32, 0i32, 0i32)
        }
    };

    // Create jintArray with 3 elements
    let result = match env.new_int_array(3) {
        Ok(arr) => arr,
        Err(_) => return std::ptr::null_mut(),
    };

    let buf = [idle, active, total];
    if env.set_int_array_region(&result, 0, &buf).is_err() {
        return std::ptr::null_mut();
    }

    result.into_raw()
}

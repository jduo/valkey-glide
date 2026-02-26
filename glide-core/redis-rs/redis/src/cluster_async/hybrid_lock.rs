// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Hybrid lock that works in both sync and async contexts
//! Uses tokio::sync internally but provides blocking methods for sync contexts

use std::ops::{Deref, DerefMut};
use tokio::sync::{RwLock as TokioRwLock, RwLockReadGuard, RwLockWriteGuard};

/// A RwLock that can be used in both sync and async contexts
pub struct HybridRwLock<T> {
    inner: TokioRwLock<T>,
}

impl<T> HybridRwLock<T> {
    pub fn new(value: T) -> Self {
        Self {
            inner: TokioRwLock::new(value),
        }
    }

    /// Async read lock
    pub async fn read(&self) -> RwLockReadGuard<'_, T> {
        self.inner.read().await
    }

    /// Async write lock
    pub async fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.inner.write().await
    }

    /// Blocking read lock for sync contexts
    /// Uses try_read in a spin loop - only use in non-async contexts!
    pub fn blocking_read(&self) -> RwLockReadGuard<'_, T> {
        loop {
            if let Ok(guard) = self.inner.try_read() {
                return guard;
            }
            std::hint::spin_loop();
        }
    }

    /// Blocking write lock for sync contexts
    /// Uses try_write in a spin loop - only use in non-async contexts!
    pub fn blocking_write(&self) -> RwLockWriteGuard<'_, T> {
        loop {
            if let Ok(guard) = self.inner.try_write() {
                return guard;
            }
            std::hint::spin_loop();
        }
    }

    /// Try to acquire read lock without blocking
    pub fn try_read(&self) -> Result<RwLockReadGuard<'_, T>, tokio::sync::TryLockError> {
        self.inner.try_read()
    }

    /// Try to acquire write lock without blocking
    pub fn try_write(&self) -> Result<RwLockWriteGuard<'_, T>, tokio::sync::TryLockError> {
        self.inner.try_write()
    }
}

// Allow direct construction from value
impl<T> From<T> for HybridRwLock<T> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

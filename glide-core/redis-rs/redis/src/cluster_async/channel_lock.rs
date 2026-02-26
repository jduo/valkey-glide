// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Lock-free RwLock using channels for sync/async boundary

use tokio::sync::{mpsc, oneshot};

enum LockRequest<T> {
    Read(oneshot::Sender<ReadGuard<T>>),
    Write(oneshot::Sender<WriteGuard<T>>),
}

pub struct ChannelRwLock<T> {
    tx: mpsc::UnboundedSender<LockRequest<T>>,
}

pub struct ReadGuard<T> {
    value: *const T,
}

pub struct WriteGuard<T> {
    value: *mut T,
}

impl<T: Send + Sync + 'static> ChannelRwLock<T> {
    pub fn new(value: T) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<LockRequest<T>>();
        let value = Box::into_raw(Box::new(value));
        
        // Spawn lock manager task
        tokio::spawn(async move {
            while let Some(req) = rx.recv().await {
                match req {
                    LockRequest::Read(resp) => {
                        let guard = ReadGuard { value };
                        let _ = resp.send(guard);
                    }
                    LockRequest::Write(resp) => {
                        let guard = WriteGuard { value };
                        let _ = resp.send(guard);
                    }
                }
            }
            // Cleanup when channel closes
            unsafe { drop(Box::from_raw(value)) };
        });
        
        Self { tx }
    }

    pub async fn read(&self) -> ReadGuard<T> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(LockRequest::Read(tx)).unwrap();
        rx.await.unwrap()
    }

    pub async fn write(&self) -> WriteGuard<T> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(LockRequest::Write(tx)).unwrap();
        rx.await.unwrap()
    }

    pub fn blocking_read(&self) -> ReadGuard<T> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(LockRequest::Read(tx)).unwrap();
        rx.blocking_recv().unwrap()
    }

    pub fn blocking_write(&self) -> WriteGuard<T> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(LockRequest::Write(tx)).unwrap();
        rx.blocking_recv().unwrap()
    }
}

impl<T> std::ops::Deref for ReadGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.value }
    }
}

impl<T> std::ops::Deref for WriteGuard<T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { &*self.value }
    }
}

impl<T> std::ops::DerefMut for WriteGuard<T> {
    fn deref_mut(&mut self) -> &mut T {
        unsafe { &mut *self.value }
    }
}

unsafe impl<T: Send> Send for ReadGuard<T> {}
unsafe impl<T: Send> Send for WriteGuard<T> {}

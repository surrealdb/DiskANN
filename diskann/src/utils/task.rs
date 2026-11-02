/*
 * Copyright (c) Microsoft Corporation.
 * Licensed under the MIT license.
 */

//! Task spawning for the index's parallel phases.
//!
//! Where a tokio runtime exists, [`spawn`] and [`JoinSet`] are tokio's: the caller must be
//! inside a runtime context, as for [`tokio::spawn`].
//!
//! On `wasm32-unknown-unknown` (browsers and edge workers) there is no tokio runtime and no
//! threads. There a spawned future does not start on its own: it runs when its handle is
//! awaited (or, in a [`JoinSet`], when [`JoinSet::join_next`] reaches it), on the awaiting
//! task. Every call site already awaits its handles before using what they produce, and no
//! spawned future waits on another, so the work completes in the same order-independent way,
//! one task after another.

#[cfg(not(any(all(target_family = "wasm", target_os = "unknown"), diskann_inline_tasks)))]
pub use tokio::task::{JoinError, JoinHandle, JoinSet};

/// Spawns `future` as a task and returns its handle.
#[cfg(not(any(all(target_family = "wasm", target_os = "unknown"), diskann_inline_tasks)))]
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future)
}

#[cfg(any(all(target_family = "wasm", target_os = "unknown"), diskann_inline_tasks))]
pub use inline::{JoinError, JoinHandle, JoinSet, spawn};

#[cfg(any(all(target_family = "wasm", target_os = "unknown"), diskann_inline_tasks))]
mod inline {
    use std::{
        collections::VecDeque,
        fmt,
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    /// The error of a task that did not complete. Inline tasks always complete, so this is
    /// never constructed; it exists so call sites match on the same `Result` as with tokio.
    #[derive(Debug)]
    pub enum JoinError {}

    impl fmt::Display for JoinError {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            match *self {}
        }
    }

    impl std::error::Error for JoinError {}

    /// A spawned future that runs when this handle is awaited.
    pub struct JoinHandle<T> {
        future: Pin<Box<dyn Future<Output = T> + Send>>,
    }

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.future.as_mut().poll(cx).map(Ok)
        }
    }

    /// Wraps `future` in a handle that runs it when awaited.
    pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        JoinHandle {
            future: Box::pin(future),
        }
    }

    /// A set of spawned futures, run one at a time, in spawn order, by [`Self::join_next`].
    pub struct JoinSet<T> {
        pending: VecDeque<JoinHandle<T>>,
    }

    impl<T: Send + 'static> JoinSet<T> {
        pub fn new() -> Self {
            Self {
                pending: VecDeque::new(),
            }
        }

        pub fn spawn<F>(&mut self, future: F)
        where
            F: Future<Output = T> + Send + 'static,
        {
            self.pending.push_back(spawn(future));
        }

        /// Runs the next task to completion and returns its output, or `None` once the set
        /// is empty.
        pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
            let handle = self.pending.pop_front()?;
            Some(handle.await)
        }
    }
}

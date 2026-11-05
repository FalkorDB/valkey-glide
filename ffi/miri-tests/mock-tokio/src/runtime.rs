// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use crate::task::JoinHandle;
use std::{
    cell::{Cell, RefCell},
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

type DeferredTask = Pin<Box<dyn Future<Output = ()> + Send>>;

thread_local! {
    static DEFER_NEXT: Cell<bool> = const { Cell::new(false) };
    static DEFERRED: RefCell<Option<DeferredTask>> = const { RefCell::new(None) };
}

/// Keep one submission unpolled so ownership can be tested after caller release.
pub fn defer_next_spawn() {
    DEFER_NEXT.with(|flag| assert!(!flag.replace(true)));
}

/// Complete the deferred, locally mocked operation after its caller resources die.
pub fn complete_deferred_task() {
    let mut task = DEFERRED
        .with(|slot| slot.borrow_mut().take())
        .expect("missing deferred task");
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(task.as_mut().poll(&mut context), Poll::Ready(())));
}

pub struct Runtime;

pub struct EnterGuard;

#[derive(Clone)]
pub struct Handle;

#[derive(Debug)]
pub struct TryCurrentError;

impl Handle {
    pub fn enter(&self) -> EnterGuard {
        EnterGuard
    }

    /// The mock never establishes a runtime context, so there is never a
    /// current handle.
    pub fn try_current() -> Result<Handle, TryCurrentError> {
        Err(TryCurrentError)
    }
}

impl Runtime {
    pub fn enter(&self) -> EnterGuard {
        EnterGuard
    }

    pub fn handle(&self) -> Handle {
        Handle
    }

    pub fn shutdown_background(self) {}

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        let waker = std::task::Waker::noop();
        let context = &mut Context::from_waker(waker);
        match std::pin::pin!(future).poll(context) {
            Poll::Ready(result) => result,
            _ => panic!("No result found"),
        }
    }

    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if DEFER_NEXT.with(|flag| flag.replace(false)) {
            DEFERRED.with(|slot| {
                assert!(slot.borrow().is_none());
                *slot.borrow_mut() = Some(Box::pin(async move {
                    drop(future.await);
                }));
            });
        }
        // Don't poll the future - just return a handle
        // This allows long-running tasks like pubsub listeners to be spawned
        JoinHandle {
            _p: std::marker::PhantomData::<F::Output>,
        }
    }
}

pub struct Builder;

impl Builder {
    pub fn new_multi_thread() -> Builder {
        Builder
    }

    pub fn new_current_thread() -> Builder {
        Builder
    }

    pub fn enable_all(&mut self) -> &mut Self {
        self
    }

    pub fn worker_threads(&mut self, _val: usize) -> &mut Self {
        self
    }

    pub fn thread_stack_size(&mut self, _val: usize) -> &mut Self {
        self
    }

    pub fn thread_name(&mut self, _val: impl Into<String>) -> &mut Self {
        self
    }

    pub fn build(&mut self) -> Result<Runtime, std::io::Error> {
        Ok(Runtime)
    }
}

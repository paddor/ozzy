//! SDK owner executor and separately owned OMQ I/O threads.

use std::sync::Arc;
use std::thread::JoinHandle;

use omq_tokio::{Context, ContextConfig};
use tokio::runtime::Handle;

use crate::signal::CloseSignal;

thread_local! {
    // Bootstrap factories run through Tokio's Send handle on this same thread.
    // LocalSet::spawn_local also works outside a currently polled local task.
    static EXECUTOR: std::cell::RefCell<std::rc::Weak<tokio::task::LocalSet>> = const { std::cell::RefCell::new(std::rc::Weak::new()) };
}

/// Spawn on this thread's SDK executor; `None` when it has stopped or this
/// is not the SDK thread.
pub(in crate::replicated) fn spawn_local<F: Future + 'static>(
    future: F,
) -> Option<tokio::task::JoinHandle<F::Output>> {
    EXECUTOR.with(|executor| Some(executor.borrow().upgrade()?.spawn_local(future)))
}

/// Background execution shared by native writer clients. Callers may use any
/// executor, or poll writer futures with a plain-thread executor.
///
/// The SDK thread owns batching, compression, request state and confirmations.
/// OMQ owns only its configured I/O threads. No caller runtime is borrowed.
#[derive(Debug, Clone)]
pub struct WriterRuntime(Arc<Inner>);

#[derive(Debug)]
struct Inner {
    context: Context,
    sdk: Handle,
    stop: CloseSignal,
    thread: Option<JoinHandle<()>>,
}

impl WriterRuntime {
    /// Start one SDK thread and at least one separately owned OMQ I/O thread.
    pub fn new() -> std::io::Result<Self> {
        let mut config = ContextConfig::from_env();
        config.io_threads = config.io_threads.max(1);
        let context = Context::with_config_and_name(config, "ozzy/omq");
        Self::with_context(context)
    }

    /// Own a separate SDK thread using an existing OMQ context. Inproc brokers
    /// and independent SDK owners can share transport without sharing role state.
    pub fn with_context(context: Context) -> std::io::Result<Self> {
        let stop = CloseSignal::default();
        let (sdk, thread) = start("ozzy/sdk", stop.clone())?;
        Ok(Self(Arc::new(Inner {
            context,
            sdk,
            stop,
            thread: Some(thread),
        })))
    }

    /// Shared OMQ context. Embedded inproc brokers must use this context too.
    /// Its transport tasks run only on owned OMQ threads.
    pub fn context(&self) -> &Context {
        &self.0.context
    }

    pub(in crate::replicated) fn driver(&self) -> &Handle {
        &self.0.sdk
    }

    /// Create local owner state only after entering the SDK executor. `None`
    /// when that executor stopped first.
    pub(in crate::replicated) async fn on_owner<F, Fut, T>(&self, start: F) -> Option<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + 'static,
        T: Send + 'static,
    {
        let owner = self.driver().spawn(async move {
            match spawn_local(start()) {
                Some(local) => local.await.ok(),
                None => None,
            }
        });
        owner.await.ok().flatten()
    }

    /// Stop the SDK executor immediately, without draining producers. Fault
    /// tests retain the OMQ context and restart a distinct SDK owner.
    #[cfg(feature = "simulation")]
    #[doc(hidden)]
    pub fn abort_owner(&self) {
        self.0.stop.close();
    }
}

fn start(name: &str, stop: CloseSignal) -> std::io::Result<(Handle, JoinHandle<()>)> {
    // Bootstrap only. Record and completion traffic use OMQ and shared signals.
    let (thread, readiness) = ozzy_io::spawn_ready(name.into(), move |ready| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        match runtime {
            Ok(runtime) => {
                let executor = std::rc::Rc::new(tokio::task::LocalSet::new());
                EXECUTOR.with(|current| *current.borrow_mut() = std::rc::Rc::downgrade(&executor));
                runtime.block_on(executor.run_until(async move {
                    if ready.report(Ok(Handle::current())) {
                        stop.closed().await;
                    }
                }));
            }
            Err(error) => {
                let _ = ready.report(Err(error));
            }
        }
    })?;
    match readiness.started() {
        Ok(handle) => Ok((handle, thread)),
        Err(error) => {
            let _ = thread.join();
            Err(error)
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.stop.close();
        if let Some(thread) = self.thread.take() {
            // Last owner may be a finishing SDK task on this very thread.
            if thread.thread().id() != std::thread::current().id() {
                let _ = thread.join();
            }
        }
    }
}

impl std::ops::Deref for WriterRuntime {
    type Target = Context;
    fn deref(&self) -> &Context {
        self.context()
    }
}

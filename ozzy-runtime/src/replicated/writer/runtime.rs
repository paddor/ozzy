//! SDK owner executor and separately owned OMQ I/O threads.

use std::sync::Arc;
use std::thread::JoinHandle;

use omq_tokio::{Context, ContextConfig};
use tokio::runtime::Handle;

use crate::signal::CloseSignal;

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
    let (ready, receive) = std::sync::mpsc::sync_channel(1);
    let thread = std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build();
            match runtime {
                Ok(runtime) => runtime.block_on(async move {
                    if ready.send(Ok(Handle::current())).is_ok() {
                        stop.closed().await;
                    }
                }),
                Err(error) => {
                    let _ = ready.send(Err(error));
                }
            }
        })?;
    match receive.recv() {
        Ok(Ok(handle)) => Ok((handle, thread)),
        Ok(Err(error)) => {
            let _ = thread.join();
            Err(error)
        }
        Err(error) => {
            let _ = thread.join();
            Err(std::io::Error::other(error))
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

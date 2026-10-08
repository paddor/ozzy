use crate::StartupError;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
};
#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use tokio::sync::Notify;

type ThreadJoin = Shared<BoxFuture<'static, ()>>;

pub(crate) fn startup_groups(shards: usize) -> (Registration, Registration) {
    let application = State::new(shards);
    let mut frontend = State::new(1);
    frontend.stop = application.stop.clone();
    frontend.error = application.error.clone();
    (
        Registration::new(Arc::new(application)),
        Registration::new(Arc::new(frontend)),
    )
}

/// Coalesced shutdown signal, independent of the startup caller's executor.
#[derive(Clone, Debug, Default)]
pub struct Shutdown(Arc<Signal>);

#[derive(Debug, Default)]
struct Signal {
    requested: AtomicBool,
    changed: Notify,
}

impl Shutdown {
    /// Whether this owner has received a shutdown request.
    pub fn is_requested(&self) -> bool {
        self.0.requested.load(Ordering::Acquire)
    }
    pub(crate) fn request(&self) {
        self.0.requested.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
    /// Wait for shutdown; cancellation does not clear the request.
    pub async fn requested(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_requested() {
                return;
            }
            changed.await;
        }
    }
}

#[derive(Debug)]
pub(crate) struct State {
    pub(crate) stop: Shutdown,
    pub(crate) finished: Shutdown,
    pub(crate) threads: usize,
    active: AtomicUsize,
    error: Arc<OnceLock<Failure>>,
    next_thread: AtomicUsize,
    owned: Vec<OnceLock<ThreadJoin>>,
}

#[derive(Debug, Clone)]
enum Failure {
    Shard(u32, String),
    Runtime(String),
    Frontend(String),
}

impl State {
    pub(crate) fn new(threads: usize) -> Self {
        Self {
            stop: Shutdown::default(),
            finished: Shutdown::default(),
            threads,
            active: AtomicUsize::new(0),
            error: Arc::new(OnceLock::new()),
            next_thread: AtomicUsize::new(0),
            owned: (0..threads).map(|_| OnceLock::new()).collect(),
        }
    }
    pub(crate) fn own(&self, thread: std::thread::JoinHandle<()>) {
        let slot = self.next_thread.fetch_add(1, Ordering::Relaxed);
        self.owned[slot]
            .set(
                async move {
                    let _ = thread.join();
                }
                .boxed()
                .shared(),
            )
            .expect("thread handle published more than once");
    }
    /// Block until every owned thread has exited. A thread reports `finished`
    /// as its last action and can outlive that report briefly. Call only
    /// from a thread that may block and is not owned here.
    pub(crate) fn join(&self) {
        for thread in self.owned.iter().filter_map(OnceLock::get) {
            futures::executor::block_on(thread.clone());
        }
    }
    pub(crate) fn fail(&self, error: StartupError) {
        let error = match error {
            StartupError::Shard { shard, reason } => Failure::Shard(shard, reason),
            StartupError::Runtime(reason) => Failure::Runtime(reason),
            StartupError::Frontend(reason) => Failure::Frontend(reason),
            other => Failure::Runtime(other.to_string()),
        };
        let _ = self.error.set(error);
        self.stop.request();
    }
    pub(crate) fn result(&self) -> Result<(), StartupError> {
        match self.error.get() {
            Some(Failure::Shard(shard, reason)) => Err(StartupError::Shard {
                shard: *shard,
                reason: reason.clone(),
            }),
            Some(Failure::Runtime(reason)) => Err(StartupError::Runtime(reason.clone())),
            Some(Failure::Frontend(reason)) => Err(StartupError::Frontend(reason.clone())),
            None => Ok(()),
        }
    }
}

pub(crate) struct Registration(Arc<State>);
impl Registration {
    pub(crate) fn new(state: Arc<State>) -> Self {
        state.active.fetch_add(1, Ordering::AcqRel);
        Self(state)
    }

    pub(crate) fn state(&self) -> Arc<State> {
        self.0.clone()
    }
}
impl Drop for Registration {
    fn drop(&mut self) {
        if self.0.active.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.finished.request();
        }
    }
}

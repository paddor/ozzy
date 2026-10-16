use crate::StartupError;
use crate::signal::CloseSignal;
#[cfg(all(test, ozzy_loom))]
use loom::sync::atomic::{AtomicUsize, Ordering};
#[cfg(not(all(test, ozzy_loom)))]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

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

#[derive(Debug)]
pub(crate) struct State {
    pub(crate) stop: CloseSignal,
    pub(crate) finished: CloseSignal,
    pub(crate) threads: usize,
    active: AtomicUsize,
    error: Arc<OnceLock<Failure>>,
    owned: ozzy_io::OwnedThreads,
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
            stop: CloseSignal::default(),
            finished: CloseSignal::default(),
            threads,
            active: AtomicUsize::new(0),
            error: Arc::new(OnceLock::new()),
            owned: ozzy_io::OwnedThreads::new(threads),
        }
    }
    pub(crate) fn own(&self, thread: std::thread::JoinHandle<()>) -> Result<(), StartupError> {
        self.owned
            .own(thread)
            .map_err(|error| StartupError::Runtime(error.to_string()))
    }
    /// Block until every owned thread has exited. A thread reports `finished`
    /// as its last action and can outlive that report briefly. Call only
    /// from a thread that may block and is not owned here.
    pub(crate) fn join(&self) {
        self.owned.join();
    }
    pub(crate) fn fail(&self, error: StartupError) {
        let error = match error {
            StartupError::Shard { shard, reason } => Failure::Shard(shard, reason),
            StartupError::Runtime(reason) => Failure::Runtime(reason),
            StartupError::Frontend(reason) => Failure::Frontend(reason),
            other => Failure::Runtime(other.to_string()),
        };
        let _ = self.error.set(error);
        self.stop.close();
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
            self.0.finished.close();
        }
    }
}

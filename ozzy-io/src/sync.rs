//! Owned worker threads, their startup readiness, and locks shared across them.

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};
use std::task::{Context, Poll};

use futures::FutureExt;
use futures::future::{BoxFuture, Shared};
use tokio::sync::oneshot;

type Join = Shared<BoxFuture<'static, ()>>;

/// Join handles of up to a fixed number of owned threads. Any number of
/// callers may join; each waits for every thread owned so far.
#[derive(Debug)]
pub struct OwnedThreads {
    next: AtomicUsize,
    slots: Vec<OnceLock<Join>>,
}

impl OwnedThreads {
    /// Room for `count` threads.
    pub fn new(count: usize) -> Self {
        Self {
            next: AtomicUsize::new(0),
            slots: (0..count).map(|_| OnceLock::new()).collect(),
        }
    }

    /// Keep `thread` for a later join. Fails once every slot is taken.
    pub fn own(&self, thread: std::thread::JoinHandle<()>) -> io::Result<()> {
        let slot = self.next.fetch_add(1, Ordering::Relaxed);
        let join = async move {
            let _ = thread.join();
        };
        self.slots
            .get(slot)
            .and_then(|slot| slot.set(join.boxed().shared()).ok())
            .ok_or_else(|| io::Error::other("more owned threads than slots"))
    }

    /// Block until every owned thread has exited. Call only from a thread
    /// that may block and is not owned here.
    pub fn join(&self) {
        for thread in self.slots.iter().filter_map(OnceLock::get) {
            futures::executor::block_on(thread.clone());
        }
    }
}

/// A thread's single readiness report, moved into its startup code.
#[derive(Debug)]
pub struct Ready<T>(oneshot::Sender<T>);

impl<T> Ready<T> {
    /// Report once. False when nobody waits for the report any more.
    pub fn report(self, value: T) -> bool {
        self.0.send(value).is_ok()
    }
}

/// The spawner's side of a readiness report. A thread that exits or unwinds
/// before reporting resolves it to `None`.
#[derive(Debug)]
pub struct Readiness<T>(oneshot::Receiver<T>);

impl<T> Readiness<T> {
    /// Block for the report. Call from a thread that may block.
    pub fn wait(self) -> Option<T> {
        futures::executor::block_on(self)
    }
}

impl<T> Readiness<io::Result<T>> {
    /// Block for a startup result. A thread that never reported failed too.
    pub fn started(self) -> io::Result<T> {
        self.wait()
            .unwrap_or_else(|| Err(io::Error::other("thread exited before readiness")))
    }
}

impl<T> Future for Readiness<T> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        Pin::new(&mut self.0).poll(cx).map(Result::ok)
    }
}

/// Start a named thread whose startup reports readiness once through `run`'s
/// argument. The caller owns the handle and observes the report.
pub fn spawn_ready<T: Send + 'static>(
    name: String,
    run: impl FnOnce(Ready<T>) + Send + 'static,
) -> io::Result<(std::thread::JoinHandle<()>, Readiness<T>)> {
    let (sender, receiver) = oneshot::channel();
    let thread = std::thread::Builder::new()
        .name(name)
        .spawn(move || run(Ready(sender)))?;
    Ok((thread, Readiness(receiver)))
}

/// Lock state whose every update completes before unlocking, so a holder
/// that panicked left it consistent. Owners fence themselves after a panic.
pub fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

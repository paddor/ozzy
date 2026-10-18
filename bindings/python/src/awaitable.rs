//! Asyncio scheduling that never takes the GIL on SDK or transport threads.
//!
//! A waker queues its task once and signals one socket per event loop. The
//! loop's reader callback resolves every queued asyncio future in one pass.
use crate::execution::Slot;
use pyo3::{
    exceptions::{PyRuntimeError, PyStopIteration},
    intern,
    panic::PanicException,
    prelude::*,
    sync::PyOnceLock,
    types::PyIterator,
};
use std::{
    future::Future,
    io::{Read, Write},
    os::{fd::AsRawFd, unix::net::UnixStream},
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    task::{Context, Poll, Wake, Waker},
};
use sync_wrapper::SyncWrapper;

type Boxed = Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>;
type Work = SyncWrapper<Boxed>;

/// `None` for awaitables of unit results.
#[allow(
    clippy::redundant_closure_for_method_calls,
    reason = "Python::None is not general over the attach lifetime"
)]
pub(crate) fn none() -> Py<PyAny> {
    Python::attach(|py| py.None())
}

/// Cross-thread half of one event loop's notifier.
#[derive(Debug)]
struct Signal {
    tasks: mpsc::Sender<Arc<Task>>,
    /// Set from the first queued wake until the loop drains the socket.
    pending: AtomicBool,
    write: UnixStream,
}

/// One awaitable's wakeup state. Only the loop thread touches `future`.
#[derive(Debug)]
struct Task {
    queued: AtomicBool,
    future: Mutex<Option<Py<PyAny>>>,
    signal: Arc<Signal>,
}

impl Task {
    fn future(&self) -> std::sync::MutexGuard<'_, Option<Py<PyAny>>> {
        self.future.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Wake for Task {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self.queued.swap(true, Ordering::AcqRel) {
            return;
        }
        if self.signal.tasks.send(self.clone()).is_err() {
            // The loop and its notifier are gone; nothing can resume.
            return;
        }
        if !self.signal.pending.swap(true, Ordering::AcqRel) {
            // A full socket buffer already makes the loop's reader ready.
            let _ = (&self.signal.write).write(&[1]);
        }
    }
}

/// Event-loop reader for queued wakeups, registered once per loop.
#[pyclass(module = "ozzy._native")]
pub(crate) struct Notifier {
    signal: Arc<Signal>,
    tasks: SyncWrapper<mpsc::Receiver<Arc<Task>>>,
    read: UnixStream,
}

#[pymethods]
impl Notifier {
    /// Resolve the asyncio future of every task woken since the last call.
    fn drain(&mut self, py: Python<'_>) {
        let mut bytes = [0; 64];
        while matches!((&self.read).read(&mut bytes), Ok(count) if count > 0) {}
        // Acquire pairs with each waker's swap, so a wake that saw `pending`
        // set has its queued task visible below; a later wake writes again.
        self.signal.pending.swap(false, Ordering::AcqRel);
        while let Ok(task) = self.tasks.get_mut().try_recv() {
            task.queued.store(false, Ordering::Release);
            let future = task.future().take();
            if let Some(future) = future {
                resolve(future.bind(py));
            }
        }
    }
}

fn resolve(future: &Bound<'_, PyAny>) {
    let py = future.py();
    // Cancellation may already have completed the awaited future.
    if future
        .call_method0(intern!(py, "done"))
        .and_then(|done| done.is_truthy())
        .is_ok_and(|done| !done)
    {
        let _ = future.call_method1(intern!(py, "set_result"), (py.None(),));
    }
}

/// The running loop and its notifier, created and registered on first use.
fn notifier(py: Python<'_>) -> PyResult<(Py<PyAny>, Arc<Signal>)> {
    static RUNNING: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    static NOTIFIERS: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
    let running = RUNNING.get_or_try_init(py, || {
        py.import("asyncio")?
            .getattr("get_running_loop")
            .map(Bound::unbind)
    })?;
    let event_loop = running.call0(py)?;
    let notifiers = NOTIFIERS
        .get_or_try_init(py, || {
            py.import("weakref")?
                .getattr("WeakKeyDictionary")?
                .call0()
                .map(Bound::unbind)
        })?
        .bind(py);
    let found = notifiers.call_method1(intern!(py, "get"), (&event_loop,))?;
    if let Ok(found) = found.cast::<Notifier>() {
        return Ok((event_loop, found.try_borrow()?.signal.clone()));
    }
    let (read, write) = UnixStream::pair()?;
    read.set_nonblocking(true)?;
    write.set_nonblocking(true)?;
    let (sender, receiver) = mpsc::channel();
    let signal = Arc::new(Signal {
        tasks: sender,
        pending: AtomicBool::new(false),
        write,
    });
    let fd = read.as_raw_fd();
    let notifier = Bound::new(
        py,
        Notifier {
            signal: signal.clone(),
            tasks: SyncWrapper::new(receiver),
            read,
        },
    )?;
    event_loop.bind(py).call_method1(
        intern!(py, "add_reader"),
        (fd, notifier.getattr(intern!(py, "drain"))?),
    )?;
    notifiers.set_item(&event_loop, notifier)?;
    Ok((event_loop, signal))
}

enum Owned {
    /// Admission, receive, and confirmation futures: polled holding the GIL.
    Hot(Slot<Work, false>),
    /// May drop joining SDK owners: polled and dropped with the GIL released.
    Lifecycle(Slot<Work, true>),
}

type Polled = std::thread::Result<Poll<PyResult<Py<PyAny>>>>;

impl Owned {
    fn poll(&mut self, py: Python<'_>, waker: &Waker) -> PyResult<Polled> {
        let poll = |future: &mut Boxed| {
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                Pin::as_mut(future).poll(&mut Context::from_waker(waker))
            }))
        };
        Ok(match self {
            Self::Hot(work) => poll(work.get_mut()?.get_mut()),
            Self::Lifecycle(work) => {
                let future = work.get_mut()?.get_mut();
                py.detach(|| poll(future))
            }
        })
    }
}

fn panicked(payload: &(dyn std::any::Any + Send)) -> PyErr {
    let message = payload
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "SDK future panicked".into());
    PanicException::new_err(message)
}

/// Asyncio awaitable for one SDK future. It implements the coroutine
/// protocol, so `asyncio.create_task` accepts it and cancellation drops the
/// future on the loop thread.
#[pyclass(module = "ozzy._native")]
pub(crate) struct Awaitable {
    work: Option<Owned>,
    task: Option<Arc<Task>>,
    event_loop: Option<Py<PyAny>>,
}

impl Awaitable {
    fn new(work: Owned) -> Self {
        Self {
            work: Some(work),
            task: None,
            event_loop: None,
        }
    }

    /// An operation whose dropped future never joins SDK threads.
    pub(crate) fn hot<F>(future: F) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        Self::new(Owned::Hot(Slot::new(SyncWrapper::new(Box::pin(future)))))
    }

    /// An operation that may own and drop the last SDK runtime owner.
    pub(crate) fn lifecycle<F>(future: F) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        Self::new(Owned::Lifecycle(Slot::new(SyncWrapper::new(Box::pin(
            future,
        )))))
    }

    /// The waker task, registering this loop's notifier on the first poll.
    fn task(&mut self, py: Python<'_>) -> PyResult<Arc<Task>> {
        if let Some(task) = &self.task {
            return Ok(task.clone());
        }
        let (event_loop, signal) = notifier(py)?;
        self.event_loop = Some(event_loop);
        let task = Arc::new(Task {
            queued: AtomicBool::new(false),
            future: Mutex::new(None),
            signal,
        });
        Ok(self.task.insert(task).clone())
    }

    fn poll(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let reused = || PyRuntimeError::new_err("cannot reuse already awaited coroutine");
        if self.work.is_none() {
            return Err(reused());
        }
        let task = self.task(py)?;
        let waker = Waker::from(task.clone());
        let polled = self.work.as_mut().ok_or_else(reused)?.poll(py, &waker)?;
        match polled {
            Ok(Poll::Ready(result)) => {
                self.finish();
                Err(PyStopIteration::new_err((result?,)))
            }
            Err(panic) => {
                self.finish();
                Err(panicked(&*panic))
            }
            Ok(Poll::Pending) => self.wait(py, &task),
        }
    }

    /// Yield a fresh asyncio future that the notifier resolves on wake.
    fn wait(&self, py: Python<'_>, task: &Task) -> PyResult<Py<PyAny>> {
        let event_loop = self
            .event_loop
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err("event loop missing"))?;
        let future = event_loop.call_method0(py, intern!(py, "create_future"))?;
        // A wake queued during the poll resolves this future when the loop
        // runs the notifier; replacing an older future drops it here.
        drop(task.future().replace(future.clone_ref(py)));
        // `Future.__iter__` marks the future as awaited and yields it.
        PyIterator::from_object(future.bind(py))?
            .next()
            .unwrap_or_else(|| Ok(py.None().into_bound(py)))
            .map(Bound::unbind)
    }

    /// Drop the SDK future and any asyncio future while attached.
    fn finish(&mut self) {
        drop(self.work.take());
        if let Some(task) = &self.task {
            drop(task.future().take());
        }
    }
}

impl Drop for Awaitable {
    fn drop(&mut self) {
        self.finish();
    }
}

#[pymethods]
impl Awaitable {
    fn __await__(slf: Py<Self>) -> Py<Self> {
        slf
    }
    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.poll(py)
    }
    fn send(&mut self, py: Python<'_>, _value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.poll(py)
    }
    /// Cancellation and other thrown exceptions drop the SDK future first.
    fn throw(&mut self, py: Python<'_>, error: Py<PyAny>) -> PyResult<Py<PyAny>> {
        self.finish();
        Err(PyErr::from_value(error.into_bound(py)))
    }
    fn close(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dropping_an_unpolled_lifecycle_awaitable_releases_gil_while_joining() {
        Python::initialize();
        let (owner, ready) = crate::execution::tests::joining();
        let work = async move {
            let _owner = owner;
            std::future::pending::<PyResult<Py<PyAny>>>().await
        };
        Python::attach(|_| {
            let awaitable = Awaitable::lifecycle(work);
            ready.wait();
            drop(awaitable);
        });
    }
}

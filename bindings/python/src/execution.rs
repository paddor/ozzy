//! Interpreter scheduling only. Rust SDK handles exclusively own their state.
use crate::{BusyError, ClosedError, OzzyError};
use pyo3::{
    PyClass, exceptions::PyTimeoutError, exceptions::PyValueError, prelude::*,
    pyclass::boolean_struct::False,
};
use std::{
    future::Future,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

struct Caller(thread::Thread);
impl Wake for Caller {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}
thread_local! {
    static CALLER: Waker = Waker::from(Arc::new(Caller(thread::current())));
}

/// Interval between signal checks while a blocking call waits.
const SIGNALS: Duration = Duration::from_millis(50);

/// Convert an optional blocking timeout in seconds.
pub(crate) fn timeout(seconds: Option<f64>) -> PyResult<Option<Duration>> {
    seconds
        .map(|seconds| {
            Duration::try_from_secs_f64(seconds)
                .map_err(|_| PyValueError::new_err("timeout must be finite and nonnegative"))
        })
        .transpose()
}

/// Wait with the GIL released. The caller holds no Python borrow, so other
/// threads may use or reject the handle meanwhile. Signals and an optional
/// timeout cancel the SDK future, which every blocking operation permits.
pub(crate) fn wait<F>(py: Python<'_>, future: F, timeout: Option<Duration>) -> PyResult<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    // A deadline past what `Instant` holds never comes.
    let deadline = timeout.and_then(|timeout| Instant::now().checked_add(timeout));
    let waker = CALLER.with(Clone::clone);
    let mut future: Slot<_> = Slot::new(Box::pin(future));
    loop {
        py.check_signals()?;
        let park = match deadline {
            Some(deadline) => deadline
                .saturating_duration_since(Instant::now())
                .min(SIGNALS),
            None => SIGNALS,
        };
        let future = future.get_mut()?;
        let result = py.detach(|| {
            let result = future.as_mut().poll(&mut Context::from_waker(&waker));
            if result.is_pending() && !park.is_zero() {
                thread::park_timeout(park);
            }
            result
        });
        if let Poll::Ready(value) = result {
            return Ok(value);
        }
        if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(PyTimeoutError::new_err("operation timed out"));
        }
    }
}

/// Poll once holding the GIL, then wait with it released. Ready operations
/// cost no GIL handoff; their futures borrow leased handles, never joining owners.
pub(crate) fn ready<F>(py: Python<'_>, future: F, timeout: Option<Duration>) -> PyResult<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    let waker = CALLER.with(Clone::clone);
    let mut future = pin!(future);
    match future.as_mut().poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(value) => Ok(value),
        Poll::Pending => wait(py, future, timeout),
    }
}

/// Poll once without waiting. A pending future is dropped, which every probe
/// permits: admission assigns nothing and reads keep their position.
pub(crate) fn probe<F: Future>(future: F) -> Poll<F::Output> {
    pin!(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}

/// Children see a new generation; inherited SDK owners are never touched there.
static FORKS: AtomicU64 = AtomicU64::new(0);

/// Registered with `os.register_at_fork(after_in_child=...)`.
#[pyfunction]
pub(crate) fn forked() {
    FORKS.fetch_add(1, Ordering::Relaxed);
}

/// Never drop inherited SDK thread owners in a forked Python child.
#[derive(Debug)]
pub(crate) struct Slot<T: Send, const JOINS: bool = true> {
    value: Option<T>,
    generation: u64,
}

impl<T: Send, const JOINS: bool> Slot<T, JOINS> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            value: Some(value),
            generation: FORKS.load(Ordering::Relaxed),
        }
    }
    fn inherited(&self) -> bool {
        self.generation != FORKS.load(Ordering::Relaxed)
    }
    pub(crate) fn check(&self) -> PyResult<()> {
        if self.inherited() {
            return Err(OzzyError::new_err("create a new client after fork"));
        }
        Ok(())
    }
    pub(crate) fn get(&self) -> PyResult<&T> {
        self.check()?;
        self.value
            .as_ref()
            .ok_or_else(|| BusyError::new_err("operation already in progress"))
    }
    pub(crate) fn get_mut(&mut self) -> PyResult<&mut T> {
        self.check()?;
        self.value
            .as_mut()
            .ok_or_else(|| BusyError::new_err("operation already in progress"))
    }
}

impl<T: Send, const JOINS: bool> Drop for Slot<T, JOINS> {
    fn drop(&mut self) {
        let Some(value) = self.value.take() else {
            return;
        };
        if self.inherited() {
            std::mem::forget(value);
            return;
        }
        if !JOINS {
            // Confirmation observations own progress, never runtime threads.
            drop(value);
            return;
        }
        // Dropping the last SDK owner joins its threads; let Python run meanwhile.
        let mut value = Some(value);
        Python::try_attach(|py| py.detach(|| drop(value.take())));
        if let Some(value) = value {
            // Python is finalizing; inherited or exiting owners cannot be joined.
            std::mem::forget(value);
        }
    }
}

pub(crate) trait Owner: PyClass<Frozen = False> {
    type State: Send;
    fn slot(&mut self) -> &mut Slot<Self::State>;
}

/// Borrow an owner for Rust-only work. Never allocate Python objects or run
/// Python code inside this borrow: a collected lease could then not return.
pub(crate) fn borrow<'py, O: Owner>(owner: &Bound<'py, O>) -> PyResult<PyRefMut<'py, O>> {
    owner
        .try_borrow_mut()
        .map_err(|_| BusyError::new_err("operation already in progress"))
}

/// Exclusively lease a Rust handle while waiting, without a Python borrow.
/// Return it on completion, errors, timeouts, and cancellation.
pub(crate) struct Lease<O: Owner> {
    owner: Py<O>,
    value: Slot<O::State>,
}

impl<O: Owner> Lease<O> {
    pub(crate) fn take(owner: Py<O>) -> PyResult<Self> {
        let value = Python::attach(|py| {
            let mut owner = borrow(owner.bind(py))?;
            let slot = owner.slot();
            slot.check()?;
            slot.value
                .take()
                .ok_or_else(|| BusyError::new_err("operation already in progress"))
        })?;
        Ok(Self {
            owner,
            value: Slot::new(value),
        })
    }
    pub(crate) fn get_mut(&mut self) -> PyResult<&mut O::State> {
        self.value.get_mut()
    }
}

impl<O: Owner> Drop for Lease<O> {
    fn drop(&mut self) {
        // Owner borrows contain no Python code, so no lease drops inside one.
        // Finalization may prevent attachment; Slot then forgets joining owners.
        Python::try_attach(|py| {
            if let Ok(mut owner) = self.owner.try_borrow_mut(py) {
                owner.slot().value = self.value.value.take();
            }
        });
    }
}

pub(crate) fn opened<T>(value: &mut Option<T>) -> PyResult<&mut T> {
    value
        .as_mut()
        .ok_or_else(|| ClosedError::new_err("handle is closed"))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::sync::Barrier;

    #[derive(Debug)]
    pub(crate) struct Joining(Option<std::thread::JoinHandle<()>>);
    impl Drop for Joining {
        fn drop(&mut self) {
            if let Some(thread) = self.0.take() {
                assert!(thread.join().is_ok());
            }
        }
    }

    /// An owner whose drop joins a thread that needs the GIL first.
    pub(crate) fn joining() -> (Joining, Arc<Barrier>) {
        let ready = Arc::new(Barrier::new(2));
        let worker_ready = ready.clone();
        let thread = std::thread::spawn(move || {
            worker_ready.wait();
            Python::attach(|py| drop(py.None()));
        });
        (Joining(Some(thread)), ready)
    }

    #[test]
    fn last_owner_drop_releases_gil_while_joining_a_python_thread() {
        Python::initialize();
        let (owner, ready) = joining();
        Python::attach(|_| {
            let slot: Slot<_> = Slot::new(owner);
            ready.wait();
            drop(slot);
        });
    }

    #[test]
    fn blocking_wait_releases_gil_before_joining_a_python_thread() {
        Python::initialize();
        let (owner, ready) = joining();
        let work = async move {
            ready.wait();
            drop(owner);
        };
        Python::attach(|py| assert!(wait(py, work, None).is_ok()));
    }

    struct Dropped(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn blocking_wait_times_out_and_drops_its_future() {
        Python::initialize();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let owner = Dropped(dropped.clone());
        let work = async move {
            let _owner = owner;
            std::future::pending::<()>().await;
        };
        Python::attach(|py| {
            let timed_out = wait(py, work, Some(Duration::from_millis(1)));
            assert!(timed_out.is_err_and(|error| error.is_instance_of::<PyTimeoutError>(py)));
        });
        assert!(dropped.load(Ordering::Relaxed));
    }

    #[test]
    fn blocking_wait_treats_an_unrepresentable_deadline_as_none() {
        Python::initialize();
        let timeout = super::timeout(Some(1e19));
        Python::attach(|py| {
            assert!(timeout.is_ok_and(|timeout| wait(py, async {}, timeout).is_ok()));
        });
    }

    #[test]
    fn inherited_slots_reject_access_and_forget_their_value() {
        Python::initialize();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut slot: Slot<_, false> = Slot::new(Dropped(dropped.clone()));
        assert!(slot.get().is_ok());
        // Model a fork without changing the generation other tests observe.
        slot.generation = slot.generation.wrapping_add(1);
        Python::attach(|py| {
            assert!(
                slot.get()
                    .is_err_and(|error| error.is_instance_of::<OzzyError>(py))
            );
        });
        drop(slot);
        assert!(!dropped.load(Ordering::Relaxed));
    }
}

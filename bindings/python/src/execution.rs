//! Interpreter scheduling only. Rust SDK handles exclusively own their state.
use crate::{BusyError, ClosedError, OzzyError};
use pyo3::{PyClass, prelude::*, pyclass::boolean_struct::False};
use std::{
    future::{Future, poll_fn},
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Wake, Waker},
    thread,
    time::Duration,
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

/// Release the GIL for lifecycle polls, which may drop joining SDK owners.
pub(crate) fn wait<F>(py: Python<'_>, future: F) -> PyResult<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    let waker = CALLER.with(Clone::clone);
    let mut future: Slot<_> = Slot::new(Box::pin(future));
    loop {
        py.check_signals()?;
        let future = future.get_mut()?;
        let result = py.detach(|| {
            let result = future.as_mut().poll(&mut Context::from_waker(&waker));
            if result.is_pending() {
                thread::park_timeout(Duration::from_millis(50));
            }
            result
        });
        if let Poll::Ready(value) = result {
            return Ok(value);
        }
    }
}

/// Hot operations borrow their SDK owner; no joining owner can drop here.
pub(crate) fn ready<F>(py: Python<'_>, future: F) -> PyResult<F::Output>
where
    F: Future + Send,
    F::Output: Send,
{
    let waker = CALLER.with(Clone::clone);
    let mut future = pin!(future);
    py.check_signals()?;
    match future.as_mut().poll(&mut Context::from_waker(&waker)) {
        Poll::Ready(value) => Ok(value),
        Poll::Pending => wait(py, future),
    }
}

/// `PyO3` polls directly from asyncio. Cancellation drops the SDK future there.
pub(crate) fn drive<F>(future: F) -> impl Future<Output = PyResult<F::Output>> + Send
where
    F: Future + Send,
    F::Output: Send,
{
    // Construct the drop guard before polling, including unpolled cancellation.
    let mut future: Slot<_> = Slot::new(Box::pin(future));
    async move {
        poll_fn(|cx| {
            let future = match future.get_mut() {
                Ok(future) => future,
                Err(error) => return Poll::Ready(Err(error)),
            };
            let waker = cx.waker();
            Python::attach(|py| py.detach(|| future.as_mut().poll(&mut Context::from_waker(waker))))
                .map(Ok)
        })
        .await
    }
}

/// Never drop inherited SDK thread owners in a forked Python child.
#[derive(Debug)]
pub(crate) struct Slot<T: Send, const JOINS: bool = true> {
    value: Option<T>,
    process: u32,
}

impl<T: Send, const JOINS: bool> Slot<T, JOINS> {
    pub(crate) fn new(value: T) -> Self {
        Self {
            value: Some(value),
            process: std::process::id(),
        }
    }
    pub(crate) fn check(&self) -> PyResult<()> {
        if self.process != std::process::id() {
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
        if self.process != std::process::id() {
            std::mem::forget(value);
            return;
        }
        if !JOINS {
            // Confirmation observations own progress, never runtime threads.
            drop(value);
            return;
        }
        // Last SDK owners may join a thread whose asyncio waker needs the GIL.
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

/// Exclusively lease a Rust handle without holding a Python borrow across an
/// await. Return it on completion, errors, and asyncio cancellation.
pub(crate) struct Lease<O: Owner> {
    owner: Py<O>,
    value: Slot<O::State>,
}

impl<O: Owner> Lease<O> {
    pub(crate) fn take(owner: Py<O>) -> PyResult<Self> {
        let value = Python::attach(|py| {
            let mut owner = owner.try_borrow_mut(py)?;
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
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[derive(Debug)]
    struct Joining(Option<std::thread::JoinHandle<()>>);
    impl Drop for Joining {
        fn drop(&mut self) {
            if let Some(thread) = self.0.take() {
                assert!(thread.join().is_ok());
            }
        }
    }

    #[test]
    fn last_owner_drop_releases_gil_while_joining_a_python_waker() {
        Python::initialize();
        let ready = Arc::new(Barrier::new(2));
        let worker_ready = ready.clone();
        let thread = std::thread::spawn(move || {
            worker_ready.wait();
            Python::attach(|py| drop(py.None()));
        });
        Python::attach(|_| {
            let slot: Slot<_> = Slot::new(Joining(Some(thread)));
            ready.wait();
            drop(slot);
        });
    }

    fn joining_future() -> impl Future<Output = ()> + Send {
        let ready = Arc::new(Barrier::new(2));
        let worker_ready = ready.clone();
        let thread = Joining(Some(std::thread::spawn(move || {
            worker_ready.wait();
            Python::attach(|py| drop(py.None()));
        })));
        async move {
            ready.wait();
            drop(thread);
        }
    }

    #[test]
    fn lifecycle_wait_releases_gil_before_joining_a_python_waker() {
        Python::initialize();
        Python::attach(|py| assert!(wait(py, joining_future()).is_ok()));
    }

    #[test]
    fn lifecycle_async_poll_releases_gil_before_joining_a_python_waker() {
        Python::initialize();
        Python::attach(|py| {
            assert!(ready(py, drive(joining_future())).is_ok_and(|result| result.is_ok()));
        });
    }

    #[test]
    fn lifecycle_future_cancellation_releases_gil_before_joining_a_python_waker() {
        Python::initialize();
        for polled in [false, true] {
            let ready = Arc::new(Barrier::new(2));
            let worker_ready = ready.clone();
            let owner = Joining(Some(std::thread::spawn(move || {
                worker_ready.wait();
                Python::attach(|py| drop(py.None()));
            })));
            let work = async move {
                let _owner = owner;
                std::future::pending::<()>().await;
            };
            Python::attach(|_| {
                let mut future = Box::pin(drive(work));
                if polled {
                    assert!(
                        future
                            .as_mut()
                            .poll(&mut Context::from_waker(Waker::noop()))
                            .is_pending()
                    );
                }
                ready.wait();
                drop(future);
            });
        }
    }
}

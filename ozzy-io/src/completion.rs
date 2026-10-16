use fanring::{mpsc, teardown::Coordinated};
use futures::task::AtomicWaker;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    fmt,
    future::Future,
    io,
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use crate::{Charge, Outcome};

#[derive(Debug, Default)]
struct Signal {
    finished: AtomicBool,
    wake: AtomicWaker,
}

struct Packet {
    result: io::Result<Outcome>,
    charge: Charge,
}

/// Owns admitted resource charge until the backend sends its physical result.
pub struct Reply {
    // First, so an unsent reply marks the observer finished before its sender
    // and charge drop.
    finish: Finish,
    sender: mpsc::Sender<Packet, Coordinated>,
    charge: Charge,
}

/// Marks the observer finished when its reply goes, sent or not.
struct Finish(Arc<Signal>);

impl Drop for Finish {
    fn drop(&mut self) {
        self.0.finished.store(true, Ordering::Release);
        self.0.wake.wake();
    }
}

/// Dropping this observer is not physical cancellation.
pub struct Completion {
    receiver: mpsc::Receiver<Packet, Coordinated>,
    signal: Arc<Signal>,
}

/// Retaining a result retains its count/byte charge. Borrow its data, or keep
/// this value in shared ownership for zero-copy readers. Handles may be cloned;
/// backend handle limits account for their independent lifetime.
#[derive(Debug)]
pub struct Completed {
    outcome: Outcome,
    _charge: Charge,
}

impl Deref for Completed {
    type Target = Outcome;
    fn deref(&self) -> &Outcome {
        &self.outcome
    }
}

impl Completed {
    /// Transfer a bounded directory listing into caller-owned state. The caller
    /// must account for it after this completion releases its admission charge.
    pub fn take_directory(&mut self) -> Option<Vec<crate::Entry>> {
        if let Outcome::Directory(entries) = &mut self.outcome {
            Some(std::mem::take(entries))
        } else {
            None
        }
    }
}

/// Create one physical-result sender and observer under an admitted charge.
pub fn completion(charge: Charge) -> (Reply, Completion) {
    let (sender, receiver) = mpsc::channel_with_policy(1);
    let signal = Arc::new(Signal::default());
    (
        Reply {
            finish: Finish(signal.clone()),
            sender,
            charge,
        },
        Completion { receiver, signal },
    )
}

impl Reply {
    /// Call only once no kernel or worker can access the operation's resources.
    pub fn finish(self, result: io::Result<Outcome>) {
        let Self {
            finish,
            mut sender,
            charge,
        } = self;
        // A canceled observer discards the result here, on the backend thread.
        let _ = sender.try_send(Packet { result, charge });
        drop(finish);
    }
}

impl Future for Completion {
    type Output = io::Result<Completed>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.signal.wake.register(cx.waker());
        if !self.signal.finished.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        Poll::Ready(match self.receiver.try_recv() {
            Ok(Packet {
                result: Ok(outcome),
                charge,
            }) => Ok(Completed {
                outcome,
                _charge: charge,
            }),
            Ok(Packet {
                result: Err(error), ..
            }) => Err(error),
            Err(_) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "I/O worker ended without a result",
            )),
        })
    }
}

impl fmt::Debug for Completion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Completion").finish_non_exhaustive()
    }
}
impl fmt::Debug for Reply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reply").finish_non_exhaustive()
    }
}

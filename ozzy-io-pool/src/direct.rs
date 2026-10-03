//! Backend extension for direct writes. Only execution workers see descriptors;
//! application clients keep the same opaque handles and owned operations.

use fanring::{mpmc, teardown::Coordinated};
use futures::task::AtomicWaker;
use ozzy_io::{Class, Limits, Operation, Outcome, WriteBuffer};
use std::{
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::Waker,
};

pub use crate::files::handles::OwnedFile;

type Sender = mpmc::Sender<Write, Coordinated>;
type Receiver = mpmc::Receiver<Write, Coordinated>;
pub(crate) type Senders = [Sender; 2];
pub(crate) type Receivers = [Receiver; 2];

/// One fixed execution thread. Create and destroy kernel state inside `run`.
/// Finish accepted writes only after the kernel releases their buffers. On
/// failure, settle all kernel work before returning or unwinding.
pub trait Worker: Send + 'static {
    fn run(self: Box<Self>, queue: &mut Queue) -> io::Result<()>;
}

pub(crate) struct Shared {
    closed: AtomicBool,
    pub(crate) wake: AtomicWaker,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectShared").finish_non_exhaustive()
    }
}

fn index(class: Class) -> usize {
    match class {
        Class::Data => 0,
        Class::Progress => 1,
    }
}

impl Shared {
    pub(crate) fn new(limits: Limits) -> io::Result<(Self, Receivers, Senders)> {
        let queue = |capacity| {
            mpmc::try_channel_with_policy(capacity)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))
        };
        let (data, data_rx) = queue(limits.data.operations)?;
        let (progress, progress_rx) = queue(limits.progress.operations)?;
        Ok((
            Self {
                closed: AtomicBool::new(false),
                wake: AtomicWaker::new(),
            },
            [data_rx, progress_rx],
            [data, progress],
        ))
    }

    pub(crate) fn send(&self, senders: &mut Senders, write: Write) {
        let result = if self.closed.load(Ordering::Acquire) {
            Err(write)
        } else {
            senders[index(write.class())]
                .try_send(write)
                .map_err(|error| match error {
                    mpmc::TrySendError::Full(write) | mpmc::TrySendError::Disconnected(write) => {
                        write
                    }
                })
        };
        if let Err(write) = result {
            write.finish(Err(io::ErrorKind::BrokenPipe.into()));
        }
        self.wake.wake();
    }
}

/// Owns the admitted operation, its descriptor reservation and completion.
/// Moving to a kernel request does not release any admission or handle budget.
pub struct Write {
    pub(crate) file: Arc<OwnedFile>,
    pub(crate) job: crate::Job,
}

impl fmt::Debug for Write {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectWrite")
            .field("operation", &self.job.operation)
            .finish_non_exhaustive()
    }
}

impl Write {
    pub fn file(&self) -> Arc<OwnedFile> {
        self.file.clone()
    }
    pub fn class(&self) -> Class {
        self.job.class
    }
    pub fn offset(&self) -> u64 {
        let Operation::Write { offset, .. } = self
            .job
            .operation
            .as_ref()
            .expect("unrun write")
            .unprotected()
        else {
            unreachable!()
        };
        *offset
    }
    pub fn data(&self) -> &WriteBuffer {
        let Operation::Write { data, .. } = self
            .job
            .operation
            .as_ref()
            .expect("unrun write")
            .unprotected()
        else {
            unreachable!()
        };
        data
    }

    /// Call after physical completion and after releasing staging allocations.
    pub fn finish(self, result: io::Result<usize>) {
        let Self { file, mut job } = self;
        drop(file);
        drop(job.operation.take());
        job.reply
            .take()
            .expect("one reply")
            .finish(result.map(Outcome::Written));
    }
}

/// Data and progress queues share the parent pool's budgets and drain fence.
pub struct Queue {
    receivers: Receivers,
    shared: Arc<crate::Shared>,
}

impl fmt::Debug for Queue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectQueue").finish_non_exhaustive()
    }
}

impl Queue {
    /// Register before inspecting queues/drain state. The worker must turn this
    /// wake into its own readiness mechanism, alongside kernel completions.
    pub fn register(&self, waker: &Waker) {
        self.shared
            .direct
            .as_ref()
            .expect("direct worker")
            .wake
            .register(waker);
    }
    pub fn try_recv(&mut self, class: Class) -> Option<Write> {
        self.receivers[index(class)].try_recv().ok()
    }
    pub fn drained(&self) -> bool {
        self.shared.drained()
    }
}

pub(crate) fn spawn(
    worker: Box<dyn Worker>,
    receivers: Receivers,
    shared: &Arc<crate::Shared>,
    initialize: crate::Initializer,
) -> io::Result<std::thread::JoinHandle<()>> {
    let owner = shared.clone();
    let (started, ready) = std::sync::mpsc::sync_channel(1);
    shared.state.lock().expect("worker state poisoned").workers += 1;
    let thread = match std::thread::Builder::new()
        .name("ozzy_io-direct".into())
        .spawn(move || {
            if !crate::startup::initialize(&initialize, crate::Worker::Direct, &started) {
                owner.fail();
                owner.worker_finished();
                return;
            }
            let mut queue = Queue {
                receivers,
                shared: owner.clone(),
            };
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| worker.run(&mut queue)));
            if !matches!(result, Ok(Ok(()))) || !queue.drained() {
                owner.fail();
            }
            // A helper may have read the open flag before this fence. Its job
            // stays active until it forwards or rejects the write, so drain
            // until no such producer can publish behind the final receive.
            owner
                .direct
                .as_ref()
                .expect("direct worker")
                .closed
                .store(true, Ordering::Release);
            loop {
                let mut progressed = false;
                for class in [Class::Progress, Class::Data] {
                    while let Some(write) = queue.try_recv(class) {
                        progressed = true;
                        write.finish(Err(io::ErrorKind::BrokenPipe.into()));
                    }
                }
                if owner.drained() {
                    break;
                }
                if !progressed {
                    std::thread::yield_now();
                }
            }
            drop(owner.direct.as_ref().expect("direct worker").wake.take());
            owner.worker_finished();
        }) {
        Ok(thread) => thread,
        Err(error) => {
            shared.worker_finished();
            return Err(error);
        }
    };
    crate::startup::ready(&ready)?;
    Ok(thread)
}

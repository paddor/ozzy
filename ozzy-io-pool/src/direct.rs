//! Backend extension for direct writes. Only execution workers see descriptors;
//! application clients keep the same opaque handles and owned operations.
//! Local raw-ring exception: physical jobs retain buffers/handles through canceled
//! observation. Replacing this channel must preserve that exact ownership.

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

type Sender = mpmc::Sender<crate::Job, Coordinated>;
type Receiver = mpmc::Receiver<crate::Job, Coordinated>;
pub(crate) type Senders = [Sender; 2];
pub(crate) type Receivers = [Receiver; 2];

/// One fixed execution thread. Create and destroy kernel state inside `run`.
/// Finish accepted writes only after the kernel releases their buffers. On
/// failure, settle all kernel work before returning or unwinding.
pub trait Worker: Send + 'static {
    /// Own the worker event loop until admitted writes and shutdown have settled.
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
    /// Retain the backend-owned descriptor for physical submission.
    pub fn file(&self) -> Arc<OwnedFile> {
        self.file.clone()
    }
    /// Independent data or reserved-progress admission class.
    pub fn class(&self) -> Class {
        self.job.class
    }
    /// Explicit starting byte offset of this physical write.
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
    /// Borrow owned scatter/gather payload while the write remains admitted.
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
    pending: Option<crate::Job>,
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
    /// Take one eligible write without waiting; execution limits may return none.
    pub fn try_recv(&mut self, class: Class) -> Option<Write> {
        loop {
            let job = if class == Class::Data {
                self.pending
                    .take()
                    .or_else(|| self.receivers[0].try_recv().ok())
            } else {
                self.receivers[1].try_recv().ok()
            };
            let mut job = job?;
            let executing = class == Class::Data && !self.shared.failed.load(Ordering::Acquire);
            if executing
                && self
                    .shared
                    .executing
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                        (used < self.shared.max_inflight).then_some(used + 1)
                    })
                    .is_err()
            {
                self.pending = Some(job);
                return None;
            }
            job.executing = executing;
            let file = if self.shared.failed.load(Ordering::Acquire) {
                Err(io::ErrorKind::BrokenPipe.into())
            } else {
                crate::files::direct_file(
                    job.operation.as_ref().expect("unrun write"),
                    &self.shared,
                )
            };
            match file {
                Ok(Some(file)) => return Some(Write { file, job }),
                Ok(None) => {
                    job.reply
                        .take()
                        .expect("one reply")
                        .finish(Err(io::ErrorKind::InvalidInput.into()));
                }
                Err(error) => {
                    job.reply.take().expect("one reply").finish(Err(error));
                }
            }
        }
    }
    /// Whether admission is fenced and all queued/running jobs have settled.
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
                pending: None,
                shared: owner.clone(),
            };
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| worker.run(&mut queue)));
            if !matches!(result, Ok(Ok(()))) || !queue.drained() {
                owner.fail();
            }
            // Client publication is fenced by the shared active reservation.
            // Drain every accepted write before releasing the backend owner.
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

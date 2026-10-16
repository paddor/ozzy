//! Backend extension for direct writes. Only execution workers see descriptors;
//! application clients keep the same opaque handles and owned operations.
//! Local raw-ring exception: physical jobs retain buffers/handles through canceled
//! observation. Replacing this channel must preserve that exact ownership.

use fanring::{mpmc, teardown::Coordinated};
use futures::task::AtomicWaker;
use ozzy_io::{Class, Handle, Limits, Operation, Outcome, Reply, WriteBuffer};
use std::{
    fmt, io,
    sync::{Arc, atomic::Ordering},
    task::Waker,
};

pub use crate::files::handles::OwnedFile;
use crate::parking::Unpark;

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
                wake: AtomicWaker::new(),
            },
            [data_rx, progress_rx],
            [data, progress],
        ))
    }
}

/// Owns the admitted operation, its descriptor reservation and completion.
/// Moving to a kernel request does not release any admission or handle budget.
/// Fields drop in order: descriptor, write handles and payload, reply, slot.
pub struct Write {
    file: Arc<OwnedFile>,
    parts: WriteParts,
    reply: Reply,
    slot: crate::Slot,
}

/// A direct write's payload at its offset, with the handles it keeps alive.
struct WriteParts {
    offset: u64,
    data: WriteBuffer,
    _target: Handle,
    _protection: Vec<Handle>,
}

impl WriteParts {
    /// Take apart a plain or protected write; any other operation is `None`.
    fn take(operation: Operation) -> Option<Self> {
        let (operation, protection) = match operation {
            Operation::Protected { operation, handles } => (*operation, handles),
            operation => (operation, Vec::new()),
        };
        let Operation::Write {
            handle,
            offset,
            data,
        } = operation
        else {
            return None;
        };
        Some(Self {
            offset,
            data,
            _target: handle,
            _protection: protection,
        })
    }
}

impl fmt::Debug for Write {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DirectWrite")
            .field("offset", &self.parts.offset)
            .field("bytes", &self.parts.data.len())
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
        self.slot.class
    }
    /// Explicit starting byte offset of this physical write.
    pub fn offset(&self) -> u64 {
        self.parts.offset
    }
    /// Borrow owned scatter/gather payload while the write remains admitted.
    pub fn data(&self) -> &WriteBuffer {
        &self.parts.data
    }

    /// Call after physical completion and after releasing staging allocations.
    pub fn finish(self, result: io::Result<usize>) {
        let Self {
            file,
            parts,
            reply,
            slot,
        } = self;
        drop(file);
        drop(parts);
        slot.settle(reply, result.map(Outcome::Written));
    }
}

/// Data and progress queues share the parent pool's budgets and drain fence.
pub struct Queue {
    receivers: Receivers,
    pending: Option<crate::Job>,
    shared: Arc<crate::Shared>,
    direct: Arc<Shared>,
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
        self.direct.wake.register(waker);
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
            let job = job?;
            let executing = class == Class::Data && !self.shared.failed.load(Ordering::Acquire);
            if executing && !self.shared.reserve_execution() {
                self.pending = Some(job);
                return None;
            }
            let crate::Job {
                operation,
                reply,
                mut slot,
            } = job;
            slot.executing = executing;
            slot.observe_start(&operation);
            let file = if self.shared.failed.load(Ordering::Acquire) {
                Err(io::ErrorKind::BrokenPipe.into())
            } else {
                crate::files::direct_file(&operation, &self.shared)
            };
            let error = match file.map(|file| file.zip(WriteParts::take(operation))) {
                Ok(Some((file, parts))) => {
                    return Some(Write {
                        file,
                        parts,
                        reply,
                        slot,
                    });
                }
                Ok(None) => io::ErrorKind::InvalidInput.into(),
                Err(error) => error,
            };
            slot.settle(reply, Err(error));
        }
    }
    /// Whether admission is fenced and all queued/running jobs have settled.
    pub fn drained(&self) -> bool {
        self.shared.drained()
    }
}

/// The direct worker's thread body after its placement initialized: run the
/// driver, then finish every accepted write before releasing the pool.
pub(crate) fn run(
    worker: Box<dyn Worker>,
    (direct, receivers): (Arc<Shared>, Receivers),
    owner: &Arc<crate::Shared>,
) {
    let mut queue = Queue {
        receivers,
        pending: None,
        shared: owner.clone(),
        direct,
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| worker.run(&mut queue)));
    if !matches!(result, Ok(Ok(()))) || !queue.drained() {
        owner.fail();
    }
    // Client publication is fenced by the shared active reservation.
    // Drain every accepted write before releasing the backend owner.
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    loop {
        queue.register(&waker);
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
            std::thread::park();
        }
    }
    drop(queue.direct.wake.take());
    owner.worker_finished();
}

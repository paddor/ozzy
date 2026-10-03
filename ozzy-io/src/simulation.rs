//! Controlled byte-backed implementation of the production file-I/O contract.
//!
//! Submission, physical execution and completion delivery are separate events.
//! The harness chooses their schedule explicitly. No OS files, threads, clocks,
//! or automatic background writeback participate. This models allowed crash
//! images, not every behavior of every filesystem.

mod disk;
mod execute;
#[cfg(test)]
mod tests;

pub use disk::{Image, ImageLimits};

use crate::{
    Admission, Backend, Class, Completion, HandleOwner, Lane, Limits, Operation, Outcome, Rejected,
    Reply, completion,
};
use fanring::{mpsc, teardown::Coordinated};
use std::{
    collections::BTreeMap,
    fmt, io,
    ops::Range,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

/// Fixed admission, handle, media-image, and trace limits for one simulated device.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Device admission shares, identical to production backends.
    pub limits: Limits,
    /// Maximum open handles, divided across configured shards.
    pub handles: usize,
    /// Independent bounds on dirty and durable simulated media.
    pub image: ImageLimits,
    /// Maximum recorded schedule events; overflow refuses further schedule changes.
    pub trace_events: usize,
}

/// Physical job identity fenced by its simulated process incarnation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct JobId {
    /// Device boot incarnation; old jobs cannot resolve after restart.
    pub boot: u64,
    /// Monotonic submission number within this boot.
    pub number: u64,
}

/// An explicit physical outcome, separate from when its result is delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Effect {
    /// Execute the complete physical operation normally.
    Normal,
    /// Report failure without applying any physical side effect.
    FailBefore(io::ErrorKind),
    /// Execute normally, then report failure. Side effects are not rolled back.
    FailAfter(io::ErrorKind),
    /// Only for reads and writes, including zero-byte results.
    Short(usize),
    /// Write a prefix, then report failure. It has no implicit durability.
    WriteThenError {
        /// Physical prefix length written before reporting failure.
        bytes: usize,
        /// Reported failure after the physical prefix write.
        error: io::ErrorKind,
    },
}

/// Whether a job awaits physical execution or only result delivery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Accepted operation whose physical effect has not run.
    Queued,
    /// Physical effect has run; its result remains undelivered.
    Executed,
}

/// Recorded device schedule and crash events for deterministic replay.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// Apply one job with an explicit physical effect.
    Execute {
        /// Accepted job selected for physical execution.
        job: JobId,
        /// Normal, partial, or failing physical outcome.
        effect: Effect,
    },
    /// Expose one previously executed result to its observer.
    Deliver(JobId),
    /// Persist selected dirty bytes without an implied file barrier.
    PersistRange {
        /// File path whose simulated media is selected.
        path: std::path::PathBuf,
        /// Byte range selected to reach simulated media.
        range: Range<usize>,
    },
    /// Persist the current logical file length independently of its bytes.
    PersistLength {
        /// File path whose simulated media is selected.
        path: std::path::PathBuf,
    },
    /// Fence new admissions and begin draining accepted physical work.
    Shutdown,
    /// Drop the process and handles while retaining dirty cached media.
    ProcessCrash,
    /// Drop the process and restore only durable bytes and namespace.
    PowerLoss,
}

#[derive(Debug)]
struct Intake {
    closed: bool,
    next: u64,
    active: usize,
    drained: bool,
}

#[derive(Debug)]
struct Shared {
    admission: Admission,
    boot: u64,
    intake: Mutex<Intake>,
    changed: Notify,
}

struct Job {
    id: JobId,
    origin: usize,
    operation: Operation,
    reply: Reply,
}

/// One shard-owned submission lane into the controlled device schedule.
pub struct Client {
    sender: mpsc::Sender<Job, Coordinated>,
    shared: Arc<Shared>,
    shard: usize,
    admission: Lane,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimulatedIoClient")
            .field("shard", &self.shard)
            .finish_non_exhaustive()
    }
}

enum Pending {
    Queued(Job),
    Executed {
        result: io::Result<Outcome>,
        reply: Reply,
    },
}

/// Owns the device image and its explicit execution schedule. Keep one instance
/// per simulated device. Broker restarts create new clients over the returned
/// image, never reactivate old clients or old handles.
pub struct Controller {
    shared: Arc<Shared>,
    receiver: mpsc::Receiver<Job, Coordinated>,
    pending: BTreeMap<JobId, Pending>,
    files: execute::Files,
    image: Image,
    config: Config,
    trace: Vec<Event>,
    reclaim_needed: bool,
}

impl fmt::Debug for Controller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IoController")
            .field("boot", &self.shared.boot)
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

/// Observes a shutdown without borrowing the controller needed to execute its
/// remaining jobs. Readiness means physical drain, not delivery of every result.
#[derive(Debug)]
pub struct Drain(Arc<Shared>);

impl Drain {
    /// Wait for physical shutdown drain while the harness continues scheduling jobs.
    pub async fn wait(self) {
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .0
                .intake
                .lock()
                .expect("simulation intake poisoned")
                .drained
            {
                return;
            }
            notified.await;
        }
    }
}

impl Controller {
    /// Start one device incarnation and return exactly one submission client per shard.
    pub fn new(config: Config, mut image: Image) -> io::Result<(Self, Vec<Client>)> {
        config.limits.validate()?;
        config.image.validate()?;
        image.validate(config.image)?;
        if config.handles < config.limits.shards || config.trace_events == 0 {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let boot = image.next_boot;
        image.next_boot = boot.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
        let shared = Arc::new(Shared {
            admission: Admission::new(config.limits)?,
            boot,
            intake: Mutex::new(Intake {
                closed: false,
                next: 0,
                active: 0,
                drained: false,
            }),
            changed: Notify::new(),
        });
        let capacity = config
            .limits
            .share(0, Class::Data)
            .operations
            .checked_add(config.limits.share(0, Class::Progress).operations)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let (sender, receiver) = mpsc::try_channel_with_policy(capacity)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        let mut clients = Vec::with_capacity(config.limits.shards);
        for shard in 1..config.limits.shards {
            clients.push(Client {
                sender: sender.try_clone().ok_or(io::ErrorKind::BrokenPipe)?,
                shared: shared.clone(),
                shard,
                admission: shared.admission.lane(shard)?,
            });
        }
        clients.insert(
            0,
            Client {
                sender,
                shared: shared.clone(),
                shard: 0,
                admission: shared.admission.lane(0)?,
            },
        );
        Ok((
            Self {
                shared,
                receiver,
                pending: BTreeMap::new(),
                files: execute::Files::new(
                    HandleOwner::default(),
                    config.handles,
                    config.limits.shards,
                ),
                image,
                config,
                trace: Vec::new(),
                reclaim_needed: true,
            },
            clients,
        ))
    }

    fn collect(&mut self) {
        while let Ok(job) = self.receiver.try_recv() {
            assert!(self.pending.insert(job.id, Pending::Queued(job)).is_none());
        }
        self.files.reclaim();
        // Reads, writes, and idle polls cannot change namespace reachability.
        // Dropped handles and namespace/barrier jobs can retire inode images.
        if self.files.take_closed() || self.reclaim_needed {
            self.image.reclaim(self.files.inodes());
            self.reclaim_needed = false;
        }
    }

    /// Collect accepted submissions and list their current execution stages.
    pub fn jobs(&mut self) -> Vec<(JobId, Stage)> {
        self.collect();
        self.pending
            .iter()
            .map(|(id, pending)| {
                (
                    *id,
                    match pending {
                        Pending::Queued(_) => Stage::Queued,
                        Pending::Executed { .. } => Stage::Executed,
                    },
                )
            })
            .collect()
    }

    /// Borrow a queued operation for schedule decisions; executed jobs return none.
    pub fn operation(&mut self, id: JobId) -> Option<&Operation> {
        self.collect();
        match self.pending.get(&id)? {
            Pending::Queued(job) => Some(&job.operation),
            Pending::Executed { .. } => None,
        }
    }

    /// Physically apply a selected queued operation. The awaiting actor still
    /// sees no result until `deliver`. Invalid schedules leave the job untouched.
    pub fn execute(&mut self, id: JobId, effect: Effect) -> io::Result<()> {
        self.collect();
        let Some(Pending::Queued(job)) = self.pending.get(&id) else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
        effect.validate(&job.operation)?;
        self.record(Event::Execute { job: id, effect })?;
        let Pending::Queued(job) = self.pending.remove(&id).expect("queued job") else {
            unreachable!()
        };
        self.reclaim_needed |= matches!(
            job.operation.unprotected(),
            Operation::Sync { .. }
                | Operation::Rename { .. }
                | Operation::RemoveFile { .. }
                | Operation::RemoveDirectory { .. }
        );
        let result = self.files.execute(
            &mut self.image,
            self.config.image,
            job.origin,
            job.operation,
            effect,
        );
        self.pending.insert(
            id,
            Pending::Executed {
                result,
                reply: job.reply,
            },
        );
        self.shared
            .intake
            .lock()
            .expect("simulation intake poisoned")
            .active -= 1;
        self.finish_drain();
        Ok(())
    }

    /// Deliver one executed result without changing its prior physical side effects.
    pub fn deliver(&mut self, id: JobId) -> io::Result<()> {
        if !matches!(self.pending.get(&id), Some(Pending::Executed { .. })) {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.record(Event::Deliver(id))?;
        let Pending::Executed { result, reply } = self.pending.remove(&id).expect("executed job")
        else {
            unreachable!()
        };
        reply.finish(result);
        self.files.reclaim();
        Ok(())
    }

    /// Select bytes that reached media without a successful file barrier. This
    /// may persist later writes while leaving a hole from an earlier write.
    pub fn persist_range(&mut self, path: &Path, range: Range<usize>) -> io::Result<()> {
        self.image.check_range(path, &range, self.config.image)?;
        self.record(Event::PersistRange {
            path: path.into(),
            range: range.clone(),
        })?;
        self.image.persist_range(path, range, self.config.image)
    }

    /// Independently persist the current file length, including truncation.
    pub fn persist_length(&mut self, path: &Path) -> io::Result<()> {
        self.image.check_length(path, self.config.image)?;
        self.record(Event::PersistLength { path: path.into() })?;
        self.image.persist_length(path, self.config.image)
    }

    /// Inspect dirty and durable media independently of actor observations.
    pub fn image(&self) -> &Image {
        &self.image
    }
    /// Recorded schedule events in application order.
    pub fn trace(&self) -> &[Event] {
        &self.trace
    }
    /// Production admission counters and wakeups backing these simulated lanes.
    pub fn admission(&self) -> &Admission {
        &self.shared.admission
    }

    /// Fence new submissions and return a drain observer; the harness executes remaining jobs.
    pub fn begin_shutdown(&mut self) -> io::Result<Drain> {
        self.record(Event::Shutdown)?;
        self.stop();
        self.collect();
        self.finish_drain();
        Ok(Drain(self.shared.clone()))
    }

    fn stop(&self) {
        self.shared
            .intake
            .lock()
            .expect("simulation intake poisoned")
            .closed = true;
        self.shared.admission.close();
    }

    fn finish_drain(&mut self) {
        let mut intake = self
            .shared
            .intake
            .lock()
            .expect("simulation intake poisoned");
        if intake.closed && intake.active == 0 && !intake.drained {
            self.files.clear();
            intake.drained = true;
            drop(intake);
            self.shared.changed.notify_waiters();
        }
    }

    /// Process death preserves dirty cached bytes. Power loss restores only
    /// durable inode contents and directory entries. Neither delivers success
    /// for a completed but unobserved old operation.
    pub fn crash(mut self, power_loss: bool) -> io::Result<(Image, Vec<Event>)> {
        self.record(if power_loss {
            Event::PowerLoss
        } else {
            Event::ProcessCrash
        })?;
        self.stop();
        self.collect();
        self.pending.clear();
        self.files.clear();
        if power_loss {
            self.image.power_loss();
        } else {
            self.image.reclaim(std::iter::empty());
        }
        Ok((
            std::mem::take(&mut self.image),
            std::mem::take(&mut self.trace),
        ))
    }

    fn record(&mut self, event: Event) -> io::Result<()> {
        if self.trace.len() == self.config.trace_events {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "simulation trace limit",
            ));
        }
        self.trace.push(event);
        Ok(())
    }
}

impl Drop for Controller {
    fn drop(&mut self) {
        self.stop();
        self.collect();
        self.pending.clear();
        self.files.clear();
        let mut intake = self
            .shared
            .intake
            .lock()
            .expect("simulation intake poisoned");
        intake.active = 0;
        intake.drained = true;
        drop(intake);
        self.shared.changed.notify_waiters();
    }
}

impl Backend for Client {
    fn submit(&mut self, class: Class, operation: Operation) -> Result<Completion, Rejected> {
        let charge = match operation
            .retained_bytes()
            .and_then(|bytes| self.admission.try_charge(class, bytes))
        {
            Ok(charge) => charge,
            Err(error) => return Err(Rejected::new(error, operation)),
        };
        let mut intake = self
            .shared
            .intake
            .lock()
            .expect("simulation intake poisoned");
        if intake.closed {
            return Err(Rejected::new(io::ErrorKind::BrokenPipe.into(), operation));
        }
        let Some(next) = intake.next.checked_add(1) else {
            return Err(Rejected::new(io::ErrorKind::InvalidInput.into(), operation));
        };
        let id = JobId {
            boot: self.shared.boot,
            number: intake.next,
        };
        let (reply, completion) = completion(charge);
        match self.sender.try_send(Job {
            id,
            origin: self.shard,
            operation,
            reply,
        }) {
            Ok(()) => {
                intake.next = next;
                intake.active += 1;
                Ok(completion)
            }
            Err(error) => {
                let (job, kind) = match error {
                    mpsc::TrySendError::Full(job) => (job, io::ErrorKind::WouldBlock),
                    mpsc::TrySendError::Disconnected(job) => (job, io::ErrorKind::BrokenPipe),
                };
                Err(Rejected::new(kind.into(), job.operation))
            }
        }
    }

    fn admission(&self) -> &Admission {
        &self.shared.admission
    }
    fn shard(&self) -> usize {
        self.shard
    }
}

impl Effect {
    fn validate(self, operation: &Operation) -> io::Result<()> {
        match (self, operation.unprotected()) {
            (Self::Short(_), Operation::Read { .. } | Operation::Write { .. })
            | (Self::WriteThenError { .. }, Operation::Write { .. })
            | (Self::Normal | Self::FailBefore(_) | Self::FailAfter(_), _) => Ok(()),
            _ => Err(io::ErrorKind::InvalidInput.into()),
        }
    }
}

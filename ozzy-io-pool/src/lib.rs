//! Fixed workers shared by a device/controller. Application threads hold only
//! submission lanes and opaque handles. No Tokio blocking pool or per-request
//! threads are used. Dropping the pool requests a detached drain; `shutdown`
//! additionally waits for every admitted operation and file close. `join`
//! then blocks until the worker threads have exited.
#![warn(missing_docs)]
#![forbid(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

pub mod direct;
mod files;
mod parking;
mod shared;
mod startup;
#[cfg(test)]
mod tests;
mod worker;

use std::fmt;
use std::io;
use std::sync::{Arc, atomic::Ordering};
use std::task::Waker;

use fanring::{mpmc, teardown::Coordinated};
use ozzy_io::{
    Admission, Backend, Class, Completion, Lane, Limits, Operation, Outcome, OwnedThreads,
    Rejected, Reply, completion,
};

use parking::{DATA, PROGRESS, Seat};
use shared::Shared;
pub use startup::{Initializer, Worker};
use worker::run;

type Sender = mpmc::Sender<Job, Coordinated>;
type Receiver = mpmc::Receiver<Job, Coordinated>;

/// Fixed worker, physical-job, handle, and admission bounds for one device.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Ordinary execution threads. Up to two more serve progress jobs.
    pub threads: usize,
    /// Ordinary physical jobs, including writes forwarded to a direct driver.
    /// Progress execution has independent reserved capacity.
    pub max_inflight: usize,
    /// Aggregate open handles, divided fairly across application shards.
    pub handles: usize,
    /// Aggregate shard admission limits with reserved progress capacity.
    pub limits: Limits,
}

/// Pool owner. Clients do not keep admission open after this is dropped.
#[derive(Debug)]
pub struct Pool {
    shared: Arc<Shared>,
    threads: OwnedThreads,
}

/// One shard-owned submission lane into the fixed backend worker pool.
pub struct Client {
    data: Sender,
    progress: Sender,
    shared: Arc<Shared>,
    shard: usize,
    admission: Lane,
    direct: Option<direct::Senders>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("shard", &self.shard)
            .finish_non_exhaustive()
    }
}

const CLOSED: usize = 1 << (usize::BITS - 1);

/// One admitted operation on its way to a worker. Fields drop in order:
/// retained handles first, then the reply, then the admission slot, whose
/// release takes the state lock that final-handle drops also signal through.
struct Job {
    operation: Operation,
    reply: Reply,
    slot: Slot,
}

/// Admission reservation and telemetry of one job. Dropping it releases the
/// reservation and wakes the workers.
struct Slot {
    shared: Arc<Shared>,
    shard: usize,
    class: Class,
    executing: bool,
    enqueued: Option<std::time::Instant>,
    started: Option<std::time::Instant>,
    diagnostic_operation: &'static str,
}

impl Pool {
    /// Startup allocates queues and launches workers but performs no filesystem
    /// access on this calling thread. Exactly one client lane per shard.
    pub fn new(config: Config) -> io::Result<(Self, Vec<Client>)> {
        Self::with_initializer(config, Arc::new(|_| Ok(())))
    }

    /// Initialize placement on each worker before it starts executing jobs.
    /// Startup waits for initialization and returns any placement failure.
    pub fn with_initializer(
        config: Config,
        initialize: Initializer,
    ) -> io::Result<(Self, Vec<Client>)> {
        Self::start(config, None, initialize)
    }

    /// Install one backend-owned direct-write driver, sharing this pool's
    /// admission, descriptors and drain. Driver startup runs on its worker.
    pub fn with_direct_worker(
        config: Config,
        worker: impl direct::Worker,
    ) -> io::Result<(Self, Vec<Client>)> {
        Self::with_direct_worker_and_initializer(config, worker, Arc::new(|_| Ok(())))
    }

    /// Install a direct-write worker and initialize placement before execution.
    pub fn with_direct_worker_and_initializer(
        config: Config,
        worker: impl direct::Worker,
        initialize: Initializer,
    ) -> io::Result<(Self, Vec<Client>)> {
        Self::start(config, Some(Box::new(worker)), initialize)
    }

    fn start(
        config: Config,
        direct_worker: Option<Box<dyn direct::Worker>>,
        initialize: Initializer,
    ) -> io::Result<(Self, Vec<Client>)> {
        let limits = config.validate()?;
        let (direct, receivers, forward) = if direct_worker.is_some() {
            let (shared, receivers, senders) = direct::Shared::new(limits)?;
            (Some(Arc::new(shared)), Some(receivers), Some(senders))
        } else {
            (None, None, None)
        };
        let maximum_threads =
            config.threads + config.threads.min(2) + usize::from(direct.is_some());
        let shared = Arc::new(Shared::new(config, limits, direct.clone())?);
        let queue = |class| {
            mpmc::try_channel_with_policy(match class {
                Class::Data => limits.data.operations,
                Class::Progress => limits.progress.operations,
            })
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))
        };
        let (data, data_rx) = queue(Class::Data)?;
        let (progress, progress_rx) = queue(Class::Progress)?;
        let mut clients = Vec::with_capacity(limits.shards);
        for shard in 1..limits.shards {
            let senders = [
                data.try_clone().ok_or(io::ErrorKind::BrokenPipe)?,
                progress.try_clone().ok_or(io::ErrorKind::BrokenPipe)?,
            ];
            clients.push(Client::new(&shared, shard, senders, forward.as_ref())?);
        }
        clients.insert(
            0,
            Client::new(&shared, 0, [data, progress], forward.as_ref())?,
        );
        let pool = Self {
            shared,
            threads: OwnedThreads::new(maximum_threads),
        };
        // The startup guard prevents a failing driver from finishing the pool
        // while helper threads are still being created.
        pool.shared.state().workers = 1;
        let startup = Startup(pool.shared.clone());
        if let (Some(worker), Some(direct), Some(receivers)) = (direct_worker, direct, receivers) {
            pool.spawn(
                "ozzy/disk/aio".into(),
                Worker::Direct,
                initialize.clone(),
                move |shared| direct::run(worker, (direct, receivers), shared),
            )?;
        }
        pool.start_helpers(config.threads, &data_rx, &progress_rx, &initialize)?;
        drop(forward);
        drop(initialize);
        drop(startup);
        Ok((pool, clients))
    }

    fn start_helpers(
        &self,
        threads: usize,
        data_rx: &Receiver,
        progress_rx: &Receiver,
        initialize: &Initializer,
    ) -> io::Result<()> {
        for index in 0..threads {
            let receiver = (*data_rx).clone();
            let (role, seat) = (Worker::Data(index), Seat::new(DATA, index));
            self.spawn(
                format!("ozzy/disk/{index}"),
                role,
                initialize.clone(),
                move |shared| run(receiver, shared, role, seat),
            )?;
        }
        // Partitions submit independent metadata barriers. Progress workers
        // settle separate partitions without consuming data-worker slots.
        for index in 0..threads.min(2) {
            let receiver = (*progress_rx).clone();
            let seat = Seat::new(PROGRESS, index);
            self.spawn(
                format!("ozzy/disk/p{index}"),
                Worker::Progress,
                initialize.clone(),
                move |shared| run(receiver, shared, Worker::Progress, seat),
            )?;
        }
        Ok(())
    }

    /// Start one worker, initialize its placement on it, and wait for that.
    /// `body` runs only after a successful initialization; a failed one fences
    /// the pool. The thread is owned before its readiness is awaited.
    fn spawn(
        &self,
        name: String,
        role: Worker,
        initialize: Initializer,
        body: impl FnOnce(&Arc<Shared>) + Send + 'static,
    ) -> io::Result<()> {
        let shared = self.shared.clone();
        shared.state().workers += 1;
        let spawned = ozzy_io::spawn_ready(name, move |ready| {
            if startup::initialize(&initialize, role, ready) {
                body(&shared);
            } else {
                shared.abandon();
            }
        });
        let (thread, readiness) = spawned.inspect_err(|_| self.shared.worker_finished())?;
        self.threads.own(thread)?;
        readiness.started()
    }

    /// Fence publication, finish admitted work, then close all descriptors on
    /// workers. Unobserved results may remain charged after shutdown returns.
    pub async fn shutdown(&self) {
        self.shared.stop();
        self.shared.finished.closed().await;
    }

    /// Block until every worker thread has exited. Workers report the end of
    /// `shutdown` as their last action, so their threads can outlive that
    /// report briefly. Call after `shutdown` from a thread that may block,
    /// never from an application shard or a worker.
    pub fn join(&self) {
        self.threads.join();
    }

    /// Device-wide admission observations and capacity wakeups.
    pub fn admission(&self) -> &Admission {
        &self.shared.admission
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.shared.stop();
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
        // One atomic reservation fences shutdown against queue publication.
        // The closed bit and active count change together, so a worker cannot
        // finish while a producer still owns an accepted job.
        if self.shared.reserve_job().is_err() {
            return Err(Rejected::new(io::ErrorKind::BrokenPipe.into(), operation));
        }
        let direct_write = matches!(
            operation.unprotected(),
            Operation::Write { handle, data, .. } if handle.is_direct() && !data.is_empty()
        );
        let (reply, completion) = completion(charge);
        let job = Job {
            operation,
            reply,
            slot: Slot {
                shared: self.shared.clone(),
                shard: self.shard,
                class,
                executing: false,
                enqueued: ozzy_telemetry::start(),
                started: None,
                diagnostic_operation: "unstarted",
            },
        };
        let index = usize::from(class == Class::Progress);
        let result = match self.direct.as_mut() {
            Some(direct) if direct_write => direct[index].try_send(job),
            _ => {
                // Count before publishing: a worker that finds the queue empty
                // keeps retrying while this count says a job is on its way.
                self.shared.queued[index].fetch_add(1, Ordering::AcqRel);
                let sender = match class {
                    Class::Data => &mut self.data,
                    Class::Progress => &mut self.progress,
                };
                let result = sender.try_send(job);
                if result.is_err() {
                    self.shared.queued[index].fetch_sub(1, Ordering::AcqRel);
                }
                result
            }
        };
        match result {
            Ok(()) => {
                self.shared
                    .published(index, direct_write && self.direct.is_some());
                Ok(completion)
            }
            Err(error) => {
                let (job, kind) = match error {
                    mpmc::TrySendError::Full(job) => (job, io::ErrorKind::WouldBlock),
                    mpmc::TrySendError::Disconnected(job) => (job, io::ErrorKind::BrokenPipe),
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

impl Config {
    /// Admission limits, once workers, handles, and in-flight jobs fit them.
    fn validate(self) -> io::Result<Limits> {
        let limits = self.limits.validate()?;
        if !(1..=64).contains(&self.threads)
            || self.handles < limits.shards
            || self.max_inflight == 0
            || self.max_inflight > limits.data.operations
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid I/O worker or handle limit",
            ));
        }
        Ok(limits)
    }
}

impl Client {
    /// Shard client sending ordinary and progress jobs through `[data, progress]`.
    fn new(
        pool: &Arc<Shared>,
        shard: usize,
        [data, progress]: [Sender; 2],
        direct: Option<&direct::Senders>,
    ) -> io::Result<Self> {
        Ok(Self {
            data,
            progress,
            shared: pool.clone(),
            shard,
            admission: pool.admission.lane(shard)?,
            direct: direct_senders(direct)?,
        })
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let prior = self.shared.active.fetch_sub(1, Ordering::AcqRel);
        assert!(prior & !CLOSED != 0, "worker job underflow");
        if self.executing && self.shared.release_execution() {
            self.shared.parking.wake_one(DATA);
            self.shared.wake_direct();
        }
        if prior == CLOSED | 1 {
            self.shared.wake_all();
        }
    }
}

struct Startup(Arc<Shared>);

impl Drop for Startup {
    fn drop(&mut self) {
        let mut state = self.0.state();
        state.workers -= 1;
        state.starting = false;
        // Helpers cannot exit before this guard releases. Zero workers means
        // no helper was started, hence no descriptor was ever opened.
        if state.workers == 0 {
            self.0.finished.close();
        }
        drop(state);
        self.0.wake_all();
    }
}

impl Slot {
    /// Report the result, then release the reservation.
    fn settle(mut self, reply: Reply, result: io::Result<Outcome>) {
        self.observe_finish(&result);
        reply.finish(result);
    }

    /// Counters and the operation name always; stage timing only when this
    /// thread enabled stage diagnostics. The name tells probe misses apart.
    fn observe_start(&mut self, operation: &Operation) {
        ozzy_telemetry::finish(ozzy_telemetry::Stage::IoQueue, self.enqueued.take());
        self.started = ozzy_telemetry::start();
        ozzy_telemetry::event(ozzy_telemetry::Event::IoStarted);
        if self.class == Class::Progress {
            ozzy_telemetry::event(ozzy_telemetry::Event::IoProgressStarted);
        }
        self.diagnostic_operation = match operation.unprotected() {
            Operation::Open { .. } | Operation::OpenDirectory { .. } => "open",
            Operation::Metadata { .. } => "metadata",
            Operation::Read { .. } => {
                ozzy_telemetry::event(ozzy_telemetry::Event::IoRead);
                "read"
            }
            Operation::Write { .. } => {
                ozzy_telemetry::event(ozzy_telemetry::Event::IoWrite);
                "write"
            }
            Operation::Sync { .. } => {
                ozzy_telemetry::event(ozzy_telemetry::Event::IoSync);
                "sync"
            }
            Operation::RemoveFile { .. } | Operation::RemoveDirectory { .. } => "remove",
            Operation::CreateDirectory { .. } => "create_directory",
            _ => "other",
        };
    }

    fn observe_finish(&mut self, result: &io::Result<Outcome>) {
        ozzy_telemetry::finish(ozzy_telemetry::Stage::IoExecution, self.started.take());
        match result {
            Ok(outcome) => {
                ozzy_telemetry::event(ozzy_telemetry::Event::IoCompleted);
                if let ozzy_io::Outcome::Written(bytes) = outcome {
                    ozzy_telemetry::count(ozzy_telemetry::Event::IoWriteBytes, *bytes as u64);
                }
                if let ozzy_io::Outcome::Read(bytes) = outcome {
                    ozzy_telemetry::count(
                        ozzy_telemetry::Event::IoReadBytes,
                        bytes.as_slice().len() as u64,
                    );
                }
            }
            Err(error) => {
                let probe = matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::AlreadyExists
                ) && matches!(
                    self.diagnostic_operation,
                    "open" | "metadata" | "remove" | "create_directory"
                );
                let (event, level) = if probe {
                    (
                        ozzy_telemetry::Event::IoProbeMissed,
                        ozzy_telemetry::Level::Info,
                    )
                } else {
                    (
                        ozzy_telemetry::Event::IoFailed,
                        ozzy_telemetry::Level::Error,
                    )
                };
                ozzy_telemetry::event(event);
                // Probe misses are expected; they would crowd the recent log.
                if !probe || ozzy_telemetry::enabled() {
                    ozzy_telemetry::log(
                        level,
                        &format!(
                            "storage {} returned {:?}",
                            self.diagnostic_operation,
                            error.kind()
                        ),
                    );
                }
            }
        }
    }
}

fn direct_senders(seed: Option<&direct::Senders>) -> io::Result<Option<direct::Senders>> {
    seed.map(|[data, progress]| {
        Ok([
            data.try_clone().ok_or(io::ErrorKind::BrokenPipe)?,
            progress.try_clone().ok_or(io::ErrorKind::BrokenPipe)?,
        ])
    })
    .transpose()
}

fn handle_waker(shared: &Arc<Shared>) -> Waker {
    Waker::from(shared.clone())
}

//! Fixed workers shared by a device/controller. Application threads hold only
//! submission lanes and opaque handles. No Tokio blocking pool or per-request
//! threads are used. Dropping the pool requests a detached drain; `shutdown`
//! additionally waits for every admitted operation and file close. `join`
//! then blocks until the worker threads have exited.
#![warn(missing_docs)]
#![forbid(unsafe_code)]

pub mod direct;
mod files;
mod startup;
#[cfg(test)]
mod tests;

use std::fmt;
use std::io;
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::task::{Wake, Waker};

use fanring::{mpmc, teardown::Coordinated};
use futures::{
    FutureExt,
    future::{BoxFuture, Shared as SharedFuture},
};
use ozzy_io::{
    Admission, Backend, Class, Completion, HandleOwner, Lane, Limits, Operation, Rejected, Reply,
    completion,
};
use tokio::sync::Notify;

pub use startup::{Initializer, Worker};

type ThreadJoin = SharedFuture<BoxFuture<'static, ()>>;
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
    next_thread: AtomicUsize,
    threads: Vec<OnceLock<ThreadJoin>>,
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

#[derive(Debug)]
struct Shared {
    admission: Admission,
    state: Mutex<State>,
    active: AtomicUsize,
    queued: [AtomicUsize; 2],
    executing: AtomicUsize,
    failed: AtomicBool,
    reclaim: AtomicBool,
    parked: Vec<OnceLock<std::thread::Thread>>,
    finished: Notify,
    handles: HandleOwner,
    files: Mutex<files::Files>,
    reclamation: Mutex<()>,
    direct: Option<direct::Shared>,
    max_inflight: usize,
    #[cfg(test)]
    gate: Mutex<Option<Arc<tests::Gate>>>,
}

#[derive(Debug, Default)]
struct State {
    lifecycle: Lifecycle,
    workers: usize,
    starting: bool,
}

const CLOSED: usize = 1 << (usize::BITS - 1);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Lifecycle {
    #[default]
    Running,
    Draining,
    Finished,
}

struct Job {
    operation: Option<Operation>,
    reply: Option<Reply>,
    shared: Arc<Shared>,
    shard: usize,
    class: Class,
    executing: bool,
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
        let limits = config.limits.validate()?;
        if !(1..=64).contains(&config.threads)
            || config.handles < limits.shards
            || config.max_inflight == 0
            || config.max_inflight > limits.data.operations
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid I/O worker or handle limit",
            ));
        }
        let (direct, receivers, forward) = if direct_worker.is_some() {
            let (shared, receivers, senders) = direct::Shared::new(limits)?;
            (Some(shared), Some(receivers), Some(senders))
        } else {
            (None, None, None)
        };
        let maximum_threads =
            config.threads + config.threads.min(2) + usize::from(direct.is_some());
        let handles = HandleOwner::default();
        let shared = Arc::new(Shared {
            admission: Admission::new(limits)?,
            state: Mutex::new(State {
                starting: true,
                ..State::default()
            }),
            active: AtomicUsize::new(0),
            queued: [AtomicUsize::new(0), AtomicUsize::new(0)],
            executing: AtomicUsize::new(0),
            failed: AtomicBool::new(false),
            reclaim: AtomicBool::new(false),
            parked: (0..config.threads + config.threads.min(2))
                .map(|_| OnceLock::new())
                .collect(),
            finished: Notify::new(),
            handles: handles.clone(),
            files: Mutex::new(files::Files::new(config.handles, limits.shards, handles)),
            reclamation: Mutex::new(()),
            direct,
            max_inflight: config.max_inflight,
            #[cfg(test)]
            gate: Mutex::new(None),
        });
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
            clients.push(Client {
                data: data.try_clone().ok_or(io::ErrorKind::BrokenPipe)?,
                progress: progress.try_clone().ok_or(io::ErrorKind::BrokenPipe)?,
                shared: shared.clone(),
                shard,
                admission: shared.admission.lane(shard)?,
                direct: direct_senders(forward.as_ref())?,
            });
        }
        clients.insert(
            0,
            Client {
                data,
                progress,
                shared: shared.clone(),
                shard: 0,
                admission: shared.admission.lane(0)?,
                direct: direct_senders(forward.as_ref())?,
            },
        );
        let pool = Self {
            shared,
            next_thread: AtomicUsize::new(0),
            threads: (0..maximum_threads).map(|_| OnceLock::new()).collect(),
        };
        // The startup guard prevents a failing driver from finishing the pool
        // while helper threads are still being created.
        pool.shared
            .state
            .lock()
            .expect("worker state poisoned")
            .workers = 1;
        let startup = Startup(pool.shared.clone());
        if let (Some(worker), Some(receivers)) = (direct_worker, receivers) {
            let thread = direct::spawn(worker, receivers, &pool.shared, initialize.clone())?;
            pool.own(thread);
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
            self.spawn(
                index,
                (*data_rx).clone(),
                Worker::Data(index),
                initialize.clone(),
            )?;
        }
        // Partitions submit independent metadata barriers. Progress workers
        // settle separate partitions without consuming data-worker slots.
        for index in 0..threads.min(2) {
            self.spawn(
                threads + index,
                (*progress_rx).clone(),
                Worker::Progress,
                initialize.clone(),
            )?;
        }
        Ok(())
    }

    /// Start one worker and wait for its initialization.
    fn spawn(
        &self,
        index: usize,
        receiver: Receiver,
        role: Worker,
        initialize: Initializer,
    ) -> io::Result<()> {
        let shared = self.shared.clone();
        let (started, ready) = std::sync::mpsc::sync_channel(1);
        shared.state.lock().expect("worker state poisoned").workers += 1;
        match std::thread::Builder::new()
            .name(format!("ozzy_io-{index}"))
            .spawn(move || {
                shared.parked[index]
                    .set(std::thread::current())
                    .expect("worker registered once");
                if startup::initialize(&initialize, role, &started) {
                    run(receiver, &shared, role);
                } else {
                    shared.fail();
                    shared.worker_finished();
                }
            }) {
            Ok(thread) => self.own(thread),
            Err(error) => {
                self.shared.worker_finished();
                return Err(error);
            }
        }
        startup::ready(&ready)
    }

    /// Fence publication, finish admitted work, then close all descriptors on
    /// workers. Unobserved results may remain charged after shutdown returns.
    pub async fn shutdown(&self) {
        self.shared.stop();
        loop {
            let notified = self.shared.finished.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .shared
                .state
                .lock()
                .expect("worker state poisoned")
                .lifecycle
                == Lifecycle::Finished
            {
                return;
            }
            notified.await;
        }
    }

    /// Block until every worker thread has exited. Workers report the end of
    /// `shutdown` as their last action, so their threads can outlive that
    /// report briefly. Call after `shutdown` from a thread that may block,
    /// never from an application shard or a worker.
    pub fn join(&self) {
        for thread in self.threads.iter().filter_map(OnceLock::get) {
            futures::executor::block_on(thread.clone());
        }
    }

    fn own(&self, thread: std::thread::JoinHandle<()>) {
        let slot = self.next_thread.fetch_add(1, Ordering::Relaxed);
        self.threads[slot]
            .set(
                async move {
                    let _ = thread.join();
                }
                .boxed()
                .shared(),
            )
            .expect("worker handle published more than once");
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
        let (reply, completion) = completion(charge);
        let job = Job {
            operation: Some(operation),
            reply: Some(reply),
            shared: self.shared.clone(),
            shard: self.shard,
            class,
            executing: false,
        };
        let index = usize::from(class == Class::Progress);
        let direct_write = self.direct.is_some()
            && matches!(
                job.operation
                    .as_ref()
                    .expect("unrun operation")
                    .unprotected(),
                Operation::Write { handle, data, .. } if handle.is_direct() && !data.is_empty()
            );
        let result = if direct_write {
            self.direct.as_mut().expect("direct sender")[index].try_send(job)
        } else {
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
        };
        self.shared.notify();
        match result {
            Ok(()) => Ok(completion),
            Err(error) => {
                let (mut job, kind) = match error {
                    mpmc::TrySendError::Full(job) => (job, io::ErrorKind::WouldBlock),
                    mpmc::TrySendError::Disconnected(job) => (job, io::ErrorKind::BrokenPipe),
                };
                Err(Rejected::new(
                    kind.into(),
                    job.operation.take().expect("unrun operation"),
                ))
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

impl Shared {
    #[allow(
        deprecated,
        reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
    )]
    fn reserve_job(&self) -> Result<(), ()> {
        self.active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| {
                (active & CLOSED == 0)
                    .then(|| active.checked_add(1))
                    .flatten()
            })
            .map(|_| ())
            .map_err(|_| ())
    }

    fn drained(&self) -> bool {
        let active = self.active.load(Ordering::Acquire);
        active & CLOSED != 0 && active & !CLOSED == 0
    }

    fn notify(&self) {
        for thread in self.parked.iter().filter_map(OnceLock::get) {
            thread.unpark();
        }
        if let Some(direct) = &self.direct {
            direct.wake.wake();
        }
    }

    fn fail(&self) {
        self.failed.store(true, Ordering::Release);
        self.stop();
    }
    fn files(&self) -> std::sync::MutexGuard<'_, files::Files> {
        // After an execution panic the device is fenced and only drains. A
        // poisoned registry must still be reclaimable on these worker threads.
        self.files
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn reclaim_files(&self) {
        self.with_reclaimed_files(|_| ());
    }

    fn with_reclaimed_files<T>(&self, action: impl FnOnce(&mut files::Files) -> T) -> T {
        // Another worker must not reserve between registry removal and physical
        // close. This lock belongs only to storage workers, never shard handles.
        let _reclamation = self.reclamation.lock().expect("file reclamation poisoned");
        let retired = self.files().reclaim();
        // Closing a descriptor may block. Never hold the lookup lock for it.
        drop(retired);
        action(&mut self.files())
    }

    fn stop(&self) {
        self.active.fetch_or(CLOSED, Ordering::AcqRel);
        let mut state = self.state.lock().expect("worker state poisoned");
        if state.lifecycle == Lifecycle::Running {
            state.lifecycle = Lifecycle::Draining;
        }
        drop(state);
        self.admission.close();
        self.notify();
    }

    fn worker_finished(&self) {
        let last = {
            let mut state = self.state.lock().expect("worker state poisoned");
            state.workers -= 1;
            state.workers == 0
        };
        if last {
            // Only worker threads have ever inserted actual descriptors.
            self.files().clear();
            self.state.lock().expect("worker state poisoned").lifecycle = Lifecycle::Finished;
            self.finished.notify_waiters();
        }
    }
}

impl Wake for Shared {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.reclaim.store(true, Ordering::Release);
        self.notify();
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        // Drop retained handles before taking the state lock: final-handle drop
        // signals reclamation through that same lock.
        drop(self.operation.take());
        drop(self.reply.take());
        let prior = self.shared.active.fetch_sub(1, Ordering::AcqRel);
        assert!(prior & !CLOSED != 0, "worker job underflow");
        if self.executing {
            self.shared.executing.fetch_sub(1, Ordering::AcqRel);
        }
        self.shared.notify();
    }
}

struct Startup(Arc<Shared>);

impl Drop for Startup {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().expect("worker state poisoned");
        state.workers -= 1;
        state.starting = false;
        // Helpers cannot exit before this guard releases. Zero workers means
        // no helper was started, hence no descriptor was ever opened.
        if state.workers == 0 {
            state.lifecycle = Lifecycle::Finished;
            self.0.finished.notify_waiters();
        }
        drop(state);
        self.0.notify();
    }
}

#[allow(
    deprecated,
    reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
)]
fn run(mut receiver: Receiver, shared: &Arc<Shared>, role: Worker) {
    let class = usize::from(role == Worker::Progress);
    loop {
        if shared.reclaim.swap(false, Ordering::AcqRel) {
            shared.reclaim_files();
        }
        let execution_slot = if role == Worker::Progress {
            false
        } else {
            shared
                .executing
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                    (used < shared.max_inflight).then_some(used + 1)
                })
                .is_ok()
        };
        let next_job = if role == Worker::Progress || execution_slot {
            receiver.try_recv().ok()
        } else {
            None
        };
        let Some(mut job) = next_job else {
            if execution_slot {
                shared.executing.fetch_sub(1, Ordering::AcqRel);
                // A direct writer may have observed this temporary reservation.
                // Wake it after releasing room, even when this queue was empty.
                if let Some(direct) = &shared.direct {
                    direct.wake.wake();
                }
            }
            // A producer may have reserved its queue count immediately before
            // publication. Do not park until that entry becomes visible.
            if shared.queued[class].load(Ordering::Acquire) != 0
                && (role == Worker::Progress
                    || shared.executing.load(Ordering::Acquire) < shared.max_inflight)
            {
                std::thread::yield_now();
                continue;
            }
            if shared.drained() && !shared.state.lock().expect("worker state poisoned").starting {
                shared.worker_finished();
                return;
            }
            std::thread::park();
            continue;
        };
        shared.queued[class].fetch_sub(1, Ordering::AcqRel);
        job.executing = execution_slot;
        let failed = shared.failed.load(Ordering::Acquire);
        let result = if failed {
            Err(io::ErrorKind::BrokenPipe.into())
        } else if let Ok(result) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            #[cfg(test)]
            {
                let gate = shared.gate.lock().expect("gate lock").clone();
                if let Some(gate) = gate {
                    gate.enter(job.operation.as_ref().expect("unrun operation"));
                }
            }
            files::execute(
                job.operation.take().expect("one execution"),
                shared,
                job.shard,
            )
        })) {
            result
        } else {
            shared.fail();
            Err(io::Error::other("file worker panicked"))
        };
        drop(job.operation.take());
        job.reply.take().expect("one reply").finish(result);
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

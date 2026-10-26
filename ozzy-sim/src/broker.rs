//! Reusable real-broker harness over OMQ and memory-only physical storage.

use ozzy_broker::{Broker, CheckedConfig, JournalPlan, ServingContext, ShardIo, StorageOwner};
use ozzy_config::BrokerIdentity;
use ozzy_io::simulation::{Client, Config, Controller, Effect, Image, ImageLimits, Stage};
use ozzy_io::{Class, Limits, Local, Operation, Quota};
use ozzy_replication::JournalGeneration;
use ozzy_runtime::replicated::WriterRuntime;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

mod config;
pub use config::{Shape, deployment_with_resources};
mod cluster;
pub use cluster::Cluster;
pub mod network;

/// Backend owner joining admitted memory-file work during broker shutdown.
#[derive(Debug)]
pub struct Storage {
    control: Arc<Control>,
    done: Arc<tokio::sync::Semaphore>,
}

impl Storage {
    fn stop(&self) {
        self.control.stop.store(true, Ordering::Release);
        self.control.wake.mark();
    }
}

/// External fault controls; protocol and journal authority remain broker-owned.
#[derive(Debug, Default)]
pub struct Control {
    stop: AtomicBool,
    wake: ozzy_io::signal::DataSignal,
    crash: std::sync::atomic::AtomicU8,
    hold_writes: AtomicBool,
    hold_completions: AtomicBool,
    fail_write: AtomicBool,
    short_write: std::sync::atomic::AtomicUsize,
    held: std::sync::atomic::AtomicUsize,
    failed: std::sync::atomic::AtomicUsize,
    pending_completions: std::sync::atomic::AtomicUsize,
    physical_events: std::sync::atomic::AtomicU64,
    space_changed: AtomicBool,
    /// Free device bytes to leave; `usize::MAX` lifts the limit.
    free_space: std::sync::atomic::AtomicUsize,
    recent_trace: std::sync::Mutex<std::collections::VecDeque<ozzy_io::simulation::Event>>,
    snapshot: std::sync::Mutex<Snapshot>,
}

#[derive(Debug, Default)]
struct Snapshot {
    pending: Option<tokio::sync::oneshot::Sender<Image>>,
    finished: Option<Image>,
}

impl Control {
    /// Cut the memory device without draining old physical work. Process death
    /// retains dirty bytes; power loss restores only explicit durable effects.
    pub fn crash(&self, power_loss: bool) {
        self.crash
            .store(if power_loss { 2 } else { 1 }, Ordering::Release);
        self.wake.mark();
    }

    fn record(&self, events: Vec<ozzy_io::simulation::Event>) {
        self.physical_events
            .fetch_add(events.len() as u64, Ordering::Release);
        let mut recent = self.recent_trace.lock().unwrap();
        for event in events {
            if recent.len() == 4096 {
                recent.pop_front();
            }
            recent.push_back(event);
        }
    }

    /// Cut the device and hand its image to a waiting snapshot and the drain.
    fn finish(&self, device: Controller, power_loss: bool, done: &tokio::sync::Semaphore) -> Image {
        let (image, events) = device.crash(power_loss).unwrap();
        self.record(events);
        let mut snapshot = self.snapshot.lock().unwrap();
        snapshot.finished = Some(image.clone());
        if let Some(reply) = snapshot.pending.take() {
            let _ = reply.send(image.clone());
        }
        done.add_permits(1);
        image
    }

    /// Capture the current memory image between physical events.
    pub async fn image(&self) -> Image {
        let (send, receive) = tokio::sync::oneshot::channel();
        {
            let mut snapshot = self.snapshot.lock().unwrap();
            if let Some(image) = &snapshot.finished {
                return image.clone();
            }
            assert!(snapshot.pending.replace(send).is_none());
        }
        self.wake.mark();
        receive.await.unwrap()
    }

    /// Hold segment data execution until released; metadata can progress.
    pub fn hold_record_writes(&self, hold: bool) {
        self.hold_writes.store(hold, Ordering::Release);
        self.wake.mark();
    }
    /// Number of segment writes currently held by the pump.
    pub fn held_writes(&self) -> usize {
        self.held.load(Ordering::Acquire)
    }
    /// Hold result delivery independently of physical execution.
    pub fn hold_completions(&self, hold: bool) {
        self.hold_completions.store(hold, Ordering::Release);
        self.wake.mark();
    }
    /// Physically finished jobs whose result has not reached its observer.
    pub fn pending_completions(&self) -> usize {
        self.pending_completions.load(Ordering::Acquire)
    }
    /// Total physical execution and delivery events across bounded trace windows.
    pub fn physical_events(&self) -> u64 {
        self.physical_events.load(Ordering::Acquire)
    }
    /// Most recent physical schedule, bounded to 4096 events per device.
    pub fn recent_trace(&self) -> Vec<ozzy_io::simulation::Event> {
        self.recent_trace.lock().unwrap().iter().cloned().collect()
    }
    /// Fail the next admitted segment write before changing its bytes.
    pub fn fail_next_record_write(&self) {
        self.fail_write.store(true, Ordering::Release);
        self.wake.mark();
    }
    /// Number of injected write failures observed by the pump.
    pub fn failures(&self) -> usize {
        self.failed.load(Ordering::Acquire)
    }

    /// Leave the device this many free bytes from the next pump pass on, as
    /// a file system that other tenants fill. `None` lifts the limit.
    pub fn limit_free_space(&self, bytes: Option<usize>) {
        self.free_space
            .store(bytes.unwrap_or(usize::MAX), Ordering::Release);
        self.space_changed.store(true, Ordering::Release);
        self.wake.mark();
    }

    /// Apply only this many bytes of the next segment entry write.
    pub fn short_next_record_write(&self, bytes: usize) {
        assert!(bytes > 0);
        self.short_write.store(bytes, Ordering::Release);
        self.wake.mark();
    }

    /// One pass over the device jobs: execute queued jobs and deliver
    /// executed results unless held. Returns whether any job advanced.
    fn schedule(&self, device: &mut Controller, reverse: bool, draining: bool) -> bool {
        let mut jobs = device.jobs();
        if reverse {
            jobs.reverse();
        }
        let mut held = 0;
        let mut completions = 0;
        let mut progressed = false;
        for (job, stage) in jobs {
            match stage {
                Stage::Queued => match self.effect(device.operation(job), draining) {
                    Some(effect) => {
                        device.execute(job, effect).unwrap();
                        progressed = true;
                    }
                    None => held += 1,
                },
                Stage::Executed => {
                    if !draining && self.hold_completions.load(Ordering::Acquire) {
                        completions += 1;
                    } else {
                        device.deliver(job).unwrap();
                        progressed = true;
                    }
                }
            }
        }
        self.held.store(held, Ordering::Release);
        self.pending_completions
            .store(completions, Ordering::Release);
        if !device.trace().is_empty() {
            self.record(device.take_trace());
        }
        progressed
    }

    /// The injected effect for one queued job, or none while record writes
    /// are held. A drain never holds writes.
    fn effect(&self, operation: Option<&Operation>, draining: bool) -> Option<Effect> {
        let record = operation.is_some_and(is_record_write);
        if record && !draining && self.hold_writes.load(Ordering::Acquire) {
            return None;
        }
        Some(if record && self.fail_write.swap(false, Ordering::AcqRel) {
            self.failed.fetch_add(1, Ordering::Release);
            Effect::FailBefore(std::io::ErrorKind::Other)
        } else if operation.is_some_and(is_entry_write) {
            match self.short_write.swap(0, Ordering::AcqRel) {
                0 => Effect::Normal,
                bytes => Effect::Short(bytes),
            }
        } else {
            Effect::Normal
        })
    }

    /// Deliver the results left after physical drain, then stop the device.
    fn finish_drained(&self, mut device: Controller, done: &tokio::sync::Semaphore) -> Image {
        // Results remain deliverable even after physical drain.
        for (job, stage) in device.jobs() {
            assert_eq!(stage, Stage::Executed);
            device.deliver(job).unwrap();
        }
        self.finish(device, false, done)
    }
}

/// Segment record writes start past the segment header.
fn is_record_write(operation: &Operation) -> bool {
    matches!(operation.unprotected(), Operation::Write { offset, .. } if *offset >= 4096)
}

/// A record write that starts a journal entry.
fn is_entry_write(operation: &Operation) -> bool {
    is_record_write(operation)
        && matches!(operation.unprotected(), Operation::Write { data, .. }
            if data.parts().first().is_some_and(|bytes| bytes.starts_with(b"OZJE")))
}

impl StorageOwner for Storage {
    fn drain(&self) {
        self.stop();
        futures::executor::block_on(self.done.acquire())
            .unwrap()
            .forget();
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Execution and delivery alternate. Reversed schedules deliver newer jobs
/// first; awaiting one partition cannot prevent another from submitting work.
/// An idle pump waits for a submission or an external control change.
pub fn pump(
    mut device: Controller,
    reverse: bool,
) -> (Storage, Arc<Control>, tokio::task::JoinHandle<Image>) {
    let control = Arc::new(Control::default());
    let storage = Storage {
        control: control.clone(),
        done: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let done = storage.done.clone();
    let scheduling = control.clone();
    let task = tokio::spawn(async move {
        let mut drain = None;
        loop {
            // Clear before inspecting controls; a later change marks again.
            scheduling.wake.drain(|| ());
            let crash = scheduling.crash.swap(0, Ordering::AcqRel);
            if crash != 0 {
                return scheduling.finish(device, crash == 2, &done);
            }
            if scheduling.space_changed.swap(false, Ordering::AcqRel) {
                let free = scheduling.free_space.load(Ordering::Acquire);
                let used = device.image().used_space();
                device.limit_space((free != usize::MAX).then(|| used + free));
            }
            if scheduling.stop.load(Ordering::Acquire) && drain.is_none() {
                drain = Some(Box::pin(device.begin_shutdown().unwrap().wait()));
            }
            if let Some(reply) = scheduling.snapshot.lock().unwrap().pending.take() {
                let _ = reply.send(device.image().clone());
            }
            let progressed = scheduling.schedule(&mut device, reverse, drain.is_some());
            let drained = if let Some(drain) = drain.as_mut() {
                futures::poll!(drain.as_mut()).is_ready()
            } else {
                false
            };
            if drained {
                return scheduling.finish_drained(device, &done);
            }
            if progressed {
                tokio::task::yield_now().await;
            } else {
                // Physical drain only advances through this pump's execution.
                tokio::select! {
                    () = device.submitted() => {}
                    () = scheduling.wake.ready() => {}
                }
            }
        }
    });
    (storage, control, task)
}

/// Default media bounds for short integration and simulator schedules.
pub const IMAGE_LIMITS: ImageLimits = ImageLimits {
    nodes: 4096,
    directory_entries: 16384,
    file_bytes: 16 * 1024 * 1024,
    total_bytes: 256 * 1024 * 1024,
};

/// One bounded production file controller with an explicitly supplied image.
pub fn device(
    checked: &CheckedConfig,
    shards: usize,
    image: Image,
    limits: ImageLimits,
) -> (Controller, Vec<Client>) {
    assert_eq!(checked.plan.controllers.len(), 1);
    let workers = &checked.plan.controllers[0].workers;
    Controller::new(
        Config {
            limits: Limits {
                shards,
                data: Quota {
                    operations: workers.queued_jobs,
                    bytes: workers.queued_bytes as usize,
                },
                progress: Quota {
                    operations: workers.progress_jobs,
                    bytes: workers.progress_bytes as usize,
                },
            },
            handles: workers.open_handles,
            image: limits,
            trace_events: 2_000_000,
        },
        image,
    )
    .unwrap()
}

/// Format broker partitions in the memory image through production journal jobs.
pub async fn provision(checked: &CheckedConfig, local: &BrokerIdentity, reverse: bool) -> Image {
    Box::pin(provision_sized(checked, local, reverse, IMAGE_LIMITS)).await
}

/// Format partitions in a memory image with explicit media bounds.
pub async fn provision_sized(
    checked: &CheckedConfig,
    local: &BrokerIdentity,
    reverse: bool,
    limits: ImageLimits,
) -> Image {
    let plan = JournalPlan::from_trusted_deployment(checked, local).unwrap();
    let (controller, mut clients) = device(checked, 1, Image::default(), limits);
    let (storage, _, task) = pump(controller, reverse);
    let io = Local::new(clients.pop().unwrap());
    for partition in plan.partitions {
        let parent = partition.placement.directory.parent().unwrap();
        let mut path = std::path::PathBuf::from("/");
        for component in parent.components().skip(1) {
            path.push(component);
            match io
                .execute(
                    Class::Progress,
                    Operation::CreateDirectory { path: path.clone() },
                )
                .await
            {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => panic!("virtual directory {}: {error}", path.display()),
            }
            sync_directory(&io, path.parent().unwrap()).await;
        }
        let opened = Box::pin(partition.format(io.clone(), JournalGeneration(1)))
            .await
            .unwrap();
        opened.journal.shutdown().await.unwrap();
    }
    drop(io);
    storage.stop();
    task.await.unwrap()
}

async fn sync_directory(io: &Local, path: &std::path::Path) {
    let opened = io
        .execute(
            Class::Progress,
            Operation::OpenDirectory {
                path: path.to_owned(),
            },
        )
        .await
        .unwrap();
    let ozzy_io::Outcome::Opened(handle) = &*opened else {
        panic!("directory open returned no handle");
    };
    let handle = handle.clone();
    drop(opened);
    io.execute(
        Class::Progress,
        Operation::Sync {
            handle: handle.clone(),
            mode: ozzy_io::SyncMode::All,
        },
    )
    .await
    .unwrap();
    io.execute(Class::Progress, Operation::Close { handle })
        .await
        .unwrap();
}

/// Start the real brokers on the runtime context with controlled memory storage.
pub async fn start_brokers(
    runtime: &WriterRuntime,
    deployment: Vec<(CheckedConfig, BrokerIdentity)>,
    reverse: bool,
) -> Vec<Broker> {
    Box::pin(start_controlled(runtime, deployment, reverse))
        .await
        .0
}

/// Start brokers and retain their external storage fault controls.
pub async fn start_controlled(
    runtime: &WriterRuntime,
    deployment: Vec<(CheckedConfig, BrokerIdentity)>,
    reverse: bool,
) -> (Vec<Broker>, Vec<Arc<Control>>) {
    let (brokers, controls, _) = start_images(runtime.context().clone(), deployment, reverse).await;
    (brokers, controls)
}

/// Start brokers, fault controls, and tasks yielding their shutdown images.
pub async fn start_images(
    context: impl Into<ServingContext>,
    deployment: Vec<(CheckedConfig, BrokerIdentity)>,
    reverse: bool,
) -> (
    Vec<Broker>,
    Vec<Arc<Control>>,
    Vec<tokio::task::JoinHandle<Image>>,
) {
    start_images_sized(context, deployment, reverse, IMAGE_LIMITS).await
}

/// Start the same broker/storage pipeline with explicit bounded media capacity.
pub async fn start_images_sized(
    context: impl Into<ServingContext>,
    deployment: Vec<(CheckedConfig, BrokerIdentity)>,
    reverse: bool,
    limits: ImageLimits,
) -> (
    Vec<Broker>,
    Vec<Arc<Control>>,
    Vec<tokio::task::JoinHandle<Image>>,
) {
    let context = context.into();
    let mut brokers = Vec::new();
    let mut controls = Vec::new();
    let mut images = Vec::new();
    for (checked, local) in deployment {
        let name = checked.plan.name.clone();
        let image = tokio::time::timeout(
            Duration::from_secs(5),
            Box::pin(provision_sized(&checked, &local, reverse, limits)),
        )
        .await
        .unwrap_or_else(|_| panic!("virtual provision stalled: {name}"));
        let (broker, control, task) = start_image(
            context.clone(),
            checked,
            local,
            Memory {
                image,
                limits,
                reverse,
            },
        )
        .await;
        brokers.push(broker);
        controls.push(control);
        images.push(task);
    }
    (brokers, controls, images)
}

/// The memory device one broker starts on.
#[derive(Debug)]
pub struct Memory {
    /// Image the device boots from.
    pub image: Image,
    /// Bounds on dirty and durable media.
    pub limits: ImageLimits,
    /// Whether the pump delivers newer jobs first.
    pub reverse: bool,
}

impl Memory {
    /// An image under the default test media bounds.
    pub fn new(image: Image, reverse: bool) -> Self {
        Self {
            image,
            limits: IMAGE_LIMITS,
            reverse,
        }
    }
}

/// Restart one broker from an image under ordinary startup selection.
pub async fn start_image(
    context: impl Into<ServingContext>,
    checked: CheckedConfig,
    local: BrokerIdentity,
    memory: Memory,
) -> (Broker, Arc<Control>, tokio::task::JoinHandle<Image>) {
    start_image_selected(context, checked, local, &[], memory).await
}

/// Restart one broker with explicit production recovery selections.
pub async fn start_image_selected(
    context: impl Into<ServingContext>,
    checked: CheckedConfig,
    local: BrokerIdentity,
    selections: &[ozzy_broker::RecoverySelection],
    memory: Memory,
) -> (Broker, Arc<Control>, tokio::task::JoinHandle<Image>) {
    let name = checked.plan.name.clone();
    tokio::time::timeout(
        Duration::from_secs(5),
        Box::pin(start_image_sized(
            context, checked, local, selections, memory,
        )),
    )
    .await
    .unwrap_or_else(|_| panic!("virtual startup stalled: {name}"))
    .unwrap()
}

/// Start one broker over a memory image with explicit media bounds.
pub async fn start_image_sized(
    context: impl Into<ServingContext>,
    checked: CheckedConfig,
    local: BrokerIdentity,
    selections: &[ozzy_broker::RecoverySelection],
    memory: Memory,
) -> Result<(Broker, Arc<Control>, tokio::task::JoinHandle<Image>), ozzy_broker::StartupError> {
    let (controller, clients) = device(
        &checked,
        checked.plan.shards.len(),
        memory.image,
        memory.limits,
    );
    let lanes = checked
        .plan
        .shards
        .iter()
        .zip(clients)
        .map(|(shard, client)| ShardIo {
            shard: shard.id,
            controller: checked.plan.controllers[0].name.clone(),
            client,
        })
        .collect();
    let (owner, control, task) = pump(controller, memory.reverse);
    let storage = ozzy_broker::ShardStorage { lanes, owner };
    let broker =
        Broker::start_trusted_with_storage(checked, local, selections, context, storage).await?;
    Ok((broker, control, task))
}

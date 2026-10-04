//! Reusable real-broker harness over OMQ and memory-only physical storage.

use ozzy_broker::{Broker, CheckedConfig, JournalPlan, ShardIo, StorageOwner};
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
pub use config::deployment_with_resources;
mod cluster;
pub use cluster::Cluster;

/// Backend owner joining admitted memory-file work during broker shutdown.
#[derive(Debug)]
pub struct Storage {
    stop: Arc<AtomicBool>,
    done: Arc<tokio::sync::Semaphore>,
}

/// External fault controls; protocol and journal authority remain broker-owned.
#[derive(Debug, Default)]
pub struct Control {
    hold_writes: AtomicBool,
    hold_completions: AtomicBool,
    fail_write: AtomicBool,
    short_write: std::sync::atomic::AtomicUsize,
    held: std::sync::atomic::AtomicUsize,
    failed: std::sync::atomic::AtomicUsize,
    pending_completions: std::sync::atomic::AtomicUsize,
    physical_events: std::sync::atomic::AtomicU64,
    recent_trace: std::sync::Mutex<std::collections::VecDeque<ozzy_io::simulation::Event>>,
    snapshot: std::sync::Mutex<Snapshot>,
}

#[derive(Debug, Default)]
struct Snapshot {
    pending: Option<tokio::sync::oneshot::Sender<Image>>,
    finished: Option<Image>,
}

impl Control {
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
        receive.await.unwrap()
    }

    /// Hold segment data execution until released; metadata can progress.
    pub fn hold_record_writes(&self, hold: bool) {
        self.hold_writes.store(hold, Ordering::Release);
    }
    /// Number of segment writes currently held by the pump.
    pub fn held_writes(&self) -> usize {
        self.held.load(Ordering::Acquire)
    }
    /// Hold result delivery independently of physical execution.
    pub fn hold_completions(&self, hold: bool) {
        self.hold_completions.store(hold, Ordering::Release);
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
    }
    /// Number of injected write failures observed by the pump.
    pub fn failures(&self) -> usize {
        self.failed.load(Ordering::Acquire)
    }

    /// Apply only this many bytes of the next segment entry write.
    pub fn short_next_record_write(&self, bytes: usize) {
        assert!(bytes > 0);
        self.short_write.store(bytes, Ordering::Release);
    }
}

impl StorageOwner for Storage {
    fn drain(&self) {
        self.stop.store(true, Ordering::Release);
        futures::executor::block_on(self.done.acquire())
            .unwrap()
            .forget();
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

/// Execution and delivery alternate. Reversed schedules deliver newer jobs
/// first; awaiting one partition cannot prevent another from submitting work.
pub fn pump(
    mut device: Controller,
    reverse: bool,
) -> (Storage, Arc<Control>, tokio::task::JoinHandle<Image>) {
    let storage = Storage {
        stop: Arc::new(AtomicBool::new(false)),
        done: Arc::new(tokio::sync::Semaphore::new(0)),
    };
    let stop = storage.stop.clone();
    let done = storage.done.clone();
    let control = Arc::new(Control::default());
    let scheduling = control.clone();
    let task = tokio::spawn(async move {
        let mut drain = None;
        loop {
            if stop.load(Ordering::Acquire) && drain.is_none() {
                drain = Some(Box::pin(device.begin_shutdown().unwrap().wait()));
            }
            if let Some(reply) = scheduling.snapshot.lock().unwrap().pending.take() {
                let _ = reply.send(device.image().clone());
            }
            let mut jobs = device.jobs();
            if reverse {
                jobs.reverse();
            }
            let mut held = 0;
            let mut completions = 0;
            for (job, stage) in jobs {
                match stage {
                    Stage::Queued => {
                        let record = device.operation(job).is_some_and(|op| matches!(op.unprotected(), Operation::Write { offset, .. } if *offset >= 4096));
                        let entry = device.operation(job).is_some_and(|op| matches!(op.unprotected(), Operation::Write { offset, data, .. } if *offset >= 4096 && data.parts().first().is_some_and(|bytes| bytes.starts_with(b"OZJE"))));
                        if record
                            && drain.is_none()
                            && scheduling.hold_writes.load(Ordering::Acquire)
                        {
                            held += 1;
                            continue;
                        }
                        let effect =
                            if record && scheduling.fail_write.swap(false, Ordering::AcqRel) {
                                scheduling.failed.fetch_add(1, Ordering::Release);
                                Effect::FailBefore(std::io::ErrorKind::Other)
                            } else if entry {
                                match scheduling.short_write.swap(0, Ordering::AcqRel) {
                                    0 => Effect::Normal,
                                    bytes => Effect::Short(bytes),
                                }
                            } else {
                                Effect::Normal
                            };
                        device.execute(job, effect).unwrap();
                    }
                    Stage::Executed => {
                        if drain.is_none() && scheduling.hold_completions.load(Ordering::Acquire) {
                            completions += 1;
                        } else {
                            device.deliver(job).unwrap();
                        }
                    }
                }
            }
            scheduling.held.store(held, Ordering::Release);
            scheduling
                .pending_completions
                .store(completions, Ordering::Release);
            if !device.trace().is_empty() {
                let events = device.take_trace();
                scheduling
                    .physical_events
                    .fetch_add(events.len() as u64, Ordering::Release);
                let mut recent = scheduling.recent_trace.lock().unwrap();
                for event in events {
                    if recent.len() == 4096 {
                        recent.pop_front();
                    }
                    recent.push_back(event);
                }
            }
            let drained = if let Some(drain) = drain.as_mut() {
                futures::poll!(drain.as_mut()).is_ready()
            } else {
                false
            };
            if drained {
                // Results remain deliverable even after physical drain.
                for (job, stage) in device.jobs() {
                    assert_eq!(stage, Stage::Executed);
                    device.deliver(job).unwrap();
                }
                let image = device.crash(false).unwrap().0;
                let mut snapshot = scheduling.snapshot.lock().unwrap();
                snapshot.finished = Some(image.clone());
                if let Some(reply) = snapshot.pending.take() {
                    let _ = reply.send(image.clone());
                }
                done.add_permits(1);
                return image;
            }
            tokio::task::yield_now().await;
        }
    });
    (storage, control, task)
}

/// One bounded production file controller with an explicitly supplied image.
pub fn device(checked: &CheckedConfig, shards: usize, image: Image) -> (Controller, Vec<Client>) {
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
            image: ImageLimits {
                nodes: 4096,
                directory_entries: 16384,
                file_bytes: 16 * 1024 * 1024,
                total_bytes: 256 * 1024 * 1024,
            },
            trace_events: 2_000_000,
        },
        image,
    )
    .unwrap()
}

/// Format broker partitions in the memory image through production journal jobs.
pub async fn provision(checked: &CheckedConfig, local: &BrokerIdentity, reverse: bool) -> Image {
    let plan = JournalPlan::from_trusted_deployment(checked, local).unwrap();
    let (controller, mut clients) = device(checked, 1, Image::default());
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
        }
        let opened = Box::pin(partition.format(io.clone(), JournalGeneration(1)))
            .await
            .unwrap();
        opened.journal.shutdown().await.unwrap();
    }
    drop(io);
    storage.stop.store(true, Ordering::Release);
    task.await.unwrap()
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
    let (brokers, controls, _) = start_images(runtime, deployment, reverse).await;
    (brokers, controls)
}

/// Start brokers, fault controls, and tasks yielding their shutdown images.
pub async fn start_images(
    runtime: &WriterRuntime,
    deployment: Vec<(CheckedConfig, BrokerIdentity)>,
    reverse: bool,
) -> (
    Vec<Broker>,
    Vec<Arc<Control>>,
    Vec<tokio::task::JoinHandle<Image>>,
) {
    let mut brokers = Vec::new();
    let mut controls = Vec::new();
    let mut images = Vec::new();
    for (checked, local) in deployment {
        let name = checked.plan.name.clone();
        let image = tokio::time::timeout(
            Duration::from_secs(5),
            Box::pin(provision(&checked, &local, reverse)),
        )
        .await
        .unwrap_or_else(|_| panic!("virtual provision stalled: {name}"));
        let (broker, control, task) = start_image(runtime, checked, local, reverse, image).await;
        brokers.push(broker);
        controls.push(control);
        images.push(task);
    }
    (brokers, controls, images)
}

/// Restart one broker from an image under ordinary startup selection.
pub async fn start_image(
    runtime: &WriterRuntime,
    checked: CheckedConfig,
    local: BrokerIdentity,
    reverse: bool,
    image: Image,
) -> (Broker, Arc<Control>, tokio::task::JoinHandle<Image>) {
    start_image_selected(runtime, checked, local, reverse, image, &[]).await
}

/// Restart one broker with explicit production recovery selections.
pub async fn start_image_selected(
    runtime: &WriterRuntime,
    checked: CheckedConfig,
    local: BrokerIdentity,
    reverse: bool,
    image: Image,
    selections: &[ozzy_broker::RecoverySelection],
) -> (Broker, Arc<Control>, tokio::task::JoinHandle<Image>) {
    let name = checked.plan.name.clone();
    let (controller, clients) = device(&checked, checked.plan.shards.len(), image);
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
    let (storage, control, task) = pump(controller, reverse);
    let broker = tokio::time::timeout(
        Duration::from_secs(5),
        Broker::start_trusted_with_storage(
            checked,
            local,
            selections,
            runtime.context().clone(),
            lanes,
            storage,
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("virtual startup stalled: {name}"))
    .unwrap();
    (broker, control, task)
}

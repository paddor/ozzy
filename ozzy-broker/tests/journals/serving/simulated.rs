//! Memory-only storage driving the production broker and OMQ inproc owners.

use super::*;
use ozzy_broker::{ShardIo, StorageOwner};
use ozzy_io::simulation::{Client, Config, Controller, Effect, Image, ImageLimits, Stage};
use ozzy_io::{Class, Limits, Local, Operation, Quota};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

mod tests {
    use super::*;
    use futures::FutureExt;

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "one held-I/O scenario checks all confirmation policies"
    )]
    async fn held_record_io_keeps_control_live_and_respects_confirmation_policy() {
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            let runtime = WriterRuntime::new().unwrap();
            let deployment = fixture(policy);
            let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
            let consumer = role_links(&runtime, &deployment[0].0, handshake::CONSUMER).await;
            let (brokers, controls) = Box::pin(start_controlled(&runtime, deployment, true)).await;
            let mut writer = live_many(
                &brokers,
                "open writer",
                SharedTopicWriter::open(
                    &producer,
                    "orders",
                    SharedTopicWriterConfig::new(limits()),
                    RetryPolicy::default(),
                ),
            )
            .await
            .unwrap();
            let warmup = writer
                .send(
                    RecordInput::single(
                        MessageId::from_bytes([26; 16]),
                        bytes::Bytes::from_static(b"warmup"),
                    ),
                    None,
                )
                .await
                .unwrap();
            live_many(
                &brokers,
                "establish producer before holding disk",
                warmup.confirmed(),
            )
            .await
            .unwrap();
            for device in &controls {
                device.hold_record_writes(true);
            }
            let id = MessageId::from_bytes([27; 16]);
            let pending = writer
                .send(
                    RecordInput::single(id, bytes::Bytes::from_static(b"held record")),
                    None,
                )
                .await
                .unwrap();
            live_many(&brokers, "observe held physical record write", async {
                while !controls.iter().any(|device| device.held_writes() > 0) {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            if policy == Confirmation::ReplicatedPersisting {
                live_many(
                    &brokers,
                    "two RAM copies confirm RP while disk is held",
                    pending.confirmed(),
                )
                .await
                .unwrap();
            } else {
                assert!(pending.confirmed().now_or_never().is_none());
            }
            let mut reader = live_many(
                &brokers,
                "metadata over separate control socket while disk is held",
                TopicReader::open(consumer.clone(), "orders", TopicReaderConfig::default()),
            )
            .await
            .unwrap();
            if policy != Confirmation::ReplicatedPersisting {
                assert!(pending.confirmed().now_or_never().is_none());
            }
            for device in &controls {
                device.hold_record_writes(false);
            }
            let confirmation = live_many(
                &brokers,
                "release physical record write",
                pending.confirmed(),
            )
            .await
            .unwrap();
            assert_eq!(confirmation.record.message_id, id);
            let warmup = live_many(&brokers, "read warmup", reader.next())
                .await
                .unwrap();
            assert_eq!(warmup.message_id, MessageId::from_bytes([26; 16]));
            drop(warmup);
            let record = live_many(&brokers, "read confirmed physical record", reader.next())
                .await
                .unwrap();
            assert_eq!(record.message_id, id);
            assert_eq!(record.payload[0].as_ref(), b"held record");
            reader.close().await.unwrap();
            writer.close().await.unwrap();
            producer.shutdown().await.unwrap();
            consumer.shutdown().await.unwrap();
            for broker in brokers {
                broker.shutdown().await.unwrap();
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn failed_physical_record_write_stops_the_real_broker_without_confirmation() {
        let runtime = WriterRuntime::new().unwrap();
        let deployment = fixture(Confirmation::LocalDurable);
        let producer = role_links(&runtime, &deployment[0].0, handshake::PRODUCER).await;
        let (brokers, controls) = Box::pin(start_controlled(&runtime, deployment, false)).await;
        let mut writer = live_many(
            &brokers,
            "open writer",
            SharedTopicWriter::open(
                &producer,
                "orders",
                SharedTopicWriterConfig::new(limits()),
                RetryPolicy::default(),
            ),
        )
        .await
        .unwrap();
        controls[0].fail_next_record_write();
        let pending = writer
            .send(
                RecordInput::single(
                    MessageId::from_bytes([28; 16]),
                    bytes::Bytes::from_static(b"failed record"),
                ),
                None,
            )
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), brokers[0].closed())
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(controls[0].failures(), 1);
        assert!(
            pending
                .confirmed()
                .now_or_never()
                .is_none_or(|result| result.is_err())
        );
        drop(writer);
        producer.shutdown().await.unwrap();
    }

    fn fixture(policy: Confirmation) -> Vec<(CheckedConfig, BrokerIdentity)> {
        let root = std::path::PathBuf::from(format!("/ozzy-simulation-{}", Uuid::now_v7()));
        deployment(
            &root,
            if policy == Confirmation::LocalDurable {
                DeploymentMode::Single
            } else {
                DeploymentMode::Three
            },
            policy,
            1,
        )
    }
}

#[derive(Debug)]
struct Storage {
    stop: Arc<AtomicBool>,
    done: Arc<tokio::sync::Semaphore>,
}

#[derive(Debug, Default)]
pub(super) struct Control {
    hold_writes: AtomicBool,
    fail_write: AtomicBool,
    held: std::sync::atomic::AtomicUsize,
    failed: std::sync::atomic::AtomicUsize,
}

impl Control {
    pub(super) fn hold_record_writes(&self, hold: bool) {
        self.hold_writes.store(hold, Ordering::Release);
    }
    pub(super) fn held_writes(&self) -> usize {
        self.held.load(Ordering::Acquire)
    }
    pub(super) fn fail_next_record_write(&self) {
        self.fail_write.store(true, Ordering::Release);
    }
    pub(super) fn failures(&self) -> usize {
        self.failed.load(Ordering::Acquire)
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
fn pump(
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
            let mut jobs = device.jobs();
            if reverse {
                jobs.reverse();
            }
            let mut held = 0;
            for (job, stage) in jobs {
                match stage {
                    Stage::Queued => {
                        let record = device.operation(job).is_some_and(|op| matches!(op.unprotected(), Operation::Write { offset, .. } if *offset >= 4096));
                        if record && scheduling.hold_writes.load(Ordering::Acquire) {
                            held += 1;
                            continue;
                        }
                        let effect =
                            if record && scheduling.fail_write.swap(false, Ordering::AcqRel) {
                                scheduling.failed.fetch_add(1, Ordering::Release);
                                Effect::FailBefore(std::io::ErrorKind::Other)
                            } else {
                                Effect::Normal
                            };
                        device.execute(job, effect).unwrap();
                    }
                    Stage::Executed => device.deliver(job).unwrap(),
                }
            }
            scheduling.held.store(held, Ordering::Release);
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
                done.add_permits(1);
                return image;
            }
            tokio::task::yield_now().await;
        }
    });
    (storage, control, task)
}

fn device(checked: &CheckedConfig, shards: usize, image: Image) -> (Controller, Vec<Client>) {
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

async fn provision(checked: &CheckedConfig, local: &BrokerIdentity, reverse: bool) -> Image {
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

pub(super) async fn start_brokers(
    runtime: &WriterRuntime,
    deployment: Vec<(CheckedConfig, BrokerIdentity)>,
    reverse: bool,
) -> Vec<Broker> {
    Box::pin(start_controlled(runtime, deployment, reverse))
        .await
        .0
}

pub(super) async fn start_controlled(
    runtime: &WriterRuntime,
    deployment: Vec<(CheckedConfig, BrokerIdentity)>,
    reverse: bool,
) -> (Vec<Broker>, Vec<Arc<Control>>) {
    let mut brokers = Vec::new();
    let mut controls = Vec::new();
    for (checked, local) in deployment {
        let name = checked.plan.name.clone();
        let image = tokio::time::timeout(
            Duration::from_secs(5),
            Box::pin(provision(&checked, &local, reverse)),
        )
        .await
        .unwrap_or_else(|_| panic!("virtual provision stalled: {name}"));
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
        let (storage, control, _task) = pump(controller, reverse);
        controls.push(control);
        brokers.push(
            tokio::time::timeout(
                Duration::from_secs(5),
                Broker::start_trusted_with_storage(
                    checked,
                    local,
                    runtime.context().clone(),
                    lanes,
                    storage,
                ),
            )
            .await
            .unwrap_or_else(|_| panic!("virtual startup stalled: {name}"))
            .unwrap(),
        );
    }
    (brokers, controls)
}

use super::*;
mod donor;
mod installation;
mod local;
mod maintenance;
mod memory;
mod proposal;
mod read;
mod receiving;
mod replay;
mod shard;
mod sync;
mod validation;
mod writeback;
use ozzy_io::{
    Operation, Quota,
    simulation::{self, Controller, Effect, Image, ImageLimits, JobId},
};
use ozzy_journal_segment::{
    AsyncSegmentOptions, CheckpointLimits, DecodeLimits, GroupIdentity, MetadataLimits,
    SegmentWriteMode,
};
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};
use ozzy_replication::{
    ConfigurationRecord, ConfiguredVoter, NormalReplica, PipelineLimits, driver::Timing,
};
use std::{
    future::Future,
    path::PathBuf,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

fn config(root: &str, id: u8, policy: QuorumPolicy) -> OwnedConfig {
    let configuration = ConfigurationRecord::with_policy(
        GroupId::from_bytes([id; 16]),
        1,
        std::array::from_fn(|n| ConfiguredVoter {
            node_id: NodeId::from_bytes([n as u8 + 1; 16]),
            principal: Digest::from_bytes([n as u8 + 20; 32]),
        }),
        policy,
    )
    .unwrap();
    OwnedConfig {
        root: root.into(),
        identity: GroupIdentity {
            group_id: configuration.configuration().scope().group_id,
            replica_node_id: NodeId::from_bytes([1; 16]),
            volume_id: VolumeId::from_bytes([9; 16]),
            store_id: StoreId::from_bytes([id + 10; 16]),
            store_generation: 1,
        },
        configuration,
        limits: AsyncJournalLimits {
            metadata: MetadataLimits {
                max_manifest_bytes: 256 * 1024,
                max_segments: 16,
            },
            decode: DecodeLimits::default(),
            operations: ozzy_journal::operation::OperationLimits::default(),
            checkpoint: CheckpointLimits {
                max_manifest_bytes: 64 * 1024,
                max_chunks: 64,
                max_chunk_bytes: 4096,
                max_state_bytes: 256 * 1024,
            },
            io: AsyncSegmentOptions {
                max_segment_bytes: 32768,
                chunk_bytes: 4096,
                direct: false,
                write_mode: if policy == QuorumPolicy::Replicated {
                    SegmentWriteMode::Buffered
                } else {
                    SegmentWriteMode::DataSync
                },
            },
            directory_entries: 128,
            directory_name_bytes: 16384,
        },
        recovery: CanonicalRecoveryLimits {
            retained_identities: 64,
            accepted_transitions: 8,
            ..CanonicalRecoveryLimits::default()
        },
        append_buffers: 4,
        append_limits: pipeline(),
        writeback: pipeline(),
        write_group_bytes: 8192,
        reads: ozzy_journal_segment::AsyncPartitionReadLimits {
            index: ozzy_journal_segment::IndexBuildLimits::default(),
            cached_index_bytes: 8192,
            cached_indexes: 2,
            concurrent_reads: 2,
        },
    }
}

fn device_limits() -> ozzy_io::Limits {
    ozzy_io::Limits {
        shards: 1,
        data: Quota {
            operations: 8,
            bytes: 8 * 1024 * 1024,
        },
        progress: Quota {
            operations: 8,
            bytes: 8 * 1024 * 1024,
        },
    }
}

fn setup() -> (Controller, Local) {
    setup_image(Image::default())
}

fn setup_image(image: Image) -> (Controller, Local) {
    let (controller, mut clients) = Controller::new(
        simulation::Config {
            limits: device_limits(),
            handles: 96,
            image: ImageLimits {
                nodes: 512,
                directory_entries: 2048,
                file_bytes: 256 * 1024,
                total_bytes: 8 * 1024 * 1024,
            },
            trace_events: 100_000,
        },
        image,
    )
    .unwrap();
    (controller, Local::new(clients.remove(0)))
}

fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn drive<T>(controller: &mut Controller, future: impl Future<Output = T>) -> T {
    drive_except(controller, future, None, |_| Effect::Normal)
}

fn drive_except<T>(
    controller: &mut Controller,
    future: impl Future<Output = T>,
    held: Option<JobId>,
    mut effect: impl FnMut(&Operation) -> Effect,
) -> T {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct WakeFlag(AtomicBool);
    impl std::task::Wake for WakeFlag {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.store(true, Ordering::Relaxed);
        }
    }
    let ready = Arc::new(WakeFlag(AtomicBool::new(false)));
    let waker = Waker::from(ready.clone());
    let mut future = std::pin::pin!(future);
    for _ in 0..10_000 {
        ready.0.store(false, Ordering::Relaxed);
        if let Poll::Ready(result) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
            return result;
        }
        let jobs = controller.jobs();
        assert!(
            ready.0.load(Ordering::Relaxed) || jobs.iter().any(|(id, _)| Some(*id) != held),
            "no independent work"
        );
        for (id, _) in jobs {
            if Some(id) != held {
                let effect = effect(controller.operation(id).unwrap().unprotected());
                controller.execute(id, effect).unwrap();
                controller.deliver(id).unwrap();
            }
        }
    }
    panic!("owned journal stalled")
}

#[test]
fn owned_driver_observes_cooperative_yields_without_storage_jobs() {
    let (mut controller, _) = setup();
    drive(&mut controller, crate::replica_journal::shard::yield_turn());
    assert!(controller.jobs().is_empty());
}

fn pipeline() -> PipelineLimits {
    PipelineLimits {
        max_operations: 8,
        max_body_bytes: 8192,
    }
}

fn timing() -> Timing {
    Timing {
        heartbeat: Duration::from_millis(10),
        retransmit: Duration::from_millis(10),
        primary_timeout: Duration::from_secs(1),
        election_timeout: Duration::from_secs(2),
        max_election_timeout: Duration::from_secs(8),
    }
}

fn promise(config: &OwnedConfig, generation: u128) -> PromiseTicket {
    NormalReplica::bootstrap(
        config.configuration.configuration(),
        config.identity.replica_node_id,
        JournalGeneration(generation),
        pipeline(),
    )
    .unwrap()
    .into_view_change(1)
    .unwrap()
    .begin_promise()
    .unwrap()
}

#[test]
fn owned_bootstrap_and_empty_restart_supply_distinct_driver_authority() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let config = config("/partition", 7, policy);
        let (journal, startup) = drive(
            &mut controller,
            OwnedJournal::format_new(config.clone(), io.clone(), JournalGeneration(1), 32768),
        )
        .unwrap();
        assert_eq!(journal.images().unwrap().committed().revision(), 0);
        assert!(startup.recovered().is_none());
        let driver = startup
            .into_driver(Duration::ZERO, timing(), pipeline())
            .unwrap();
        assert_eq!(driver.scope().view, 0);
        assert!(driver.normal().unwrap().snapshot().ready_for_appends);
        drive(&mut controller, journal.shutdown()).unwrap();
        let (journal, startup) = drive(
            &mut controller,
            OwnedJournal::open(config, io, JournalGeneration(2)),
        )
        .unwrap();
        assert!(journal.images().is_err());
        assert_eq!(startup.recovered().unwrap().log.accepted, Prefix::GENESIS);
        assert_eq!(startup.generation(), JournalGeneration(2));
        let driver = startup
            .into_driver(Duration::ZERO, timing(), pipeline())
            .unwrap();
        assert_eq!(driver.scope().view, 1);
        assert!(driver.normal().is_none());
        drive(&mut controller, journal.shutdown()).unwrap();
    }
}

#[test]
fn owned_promise_waits_for_observation_and_duplicate_needs_no_io() {
    let (mut controller, io) = setup();
    let config = config("/partition", 7, QuorumPolicy::Durable);
    let (mut journal, _) = drive(
        &mut controller,
        OwnedJournal::format_new(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    let ticket = promise(&config, 1);
    let returned = {
        let mut future = std::pin::pin!(journal.persist_promise(ticket));
        loop {
            if let Poll::Ready(result) = poll(future.as_mut()) {
                break result.unwrap();
            }
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                assert!(
                    poll(future.as_mut()).is_pending(),
                    "physical effect is not observation"
                );
                controller.deliver(id).unwrap();
            }
        }
    };
    assert_eq!(returned, ticket);
    assert_eq!(journal.scope().view, 1);
    assert!(journal.images().is_err());
    assert_eq!(
        drive(&mut controller, journal.persist_promise(ticket)).unwrap(),
        ticket
    );
    assert!(controller.jobs().is_empty());
    drop(journal);
    let (_, startup) = drive(
        &mut controller,
        OwnedJournal::open(config, io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(startup.recovered().unwrap().scope.view, 1);
}

#[test]
fn canceled_owned_promise_fences_owner_and_keeps_group_lock_until_physical_completion() {
    let (mut controller, io) = setup();
    let config = config("/partition", 7, QuorumPolicy::Durable);
    let (mut journal, _) = drive(
        &mut controller,
        OwnedJournal::format_new(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    let mut future = Box::pin(journal.persist_promise(promise(&config, 1)));
    assert!(poll(future.as_mut()).is_pending());
    let held = controller.jobs()[0].0;
    drop(future);
    assert!(journal.is_faulted());
    assert!(journal.images().is_err());
    assert!(
        drive_except(
            &mut controller,
            journal.persist_promise(promise(&config, 1)),
            Some(held),
            |_| Effect::Normal
        )
        .is_err()
    );
    drop(journal);
    assert!(
        drive_except(
            &mut controller,
            OwnedJournal::open(config.clone(), io.clone(), JournalGeneration(2)),
            Some(held),
            |_| Effect::Normal
        )
        .is_err()
    );
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    let (journal, _) = drive(
        &mut controller,
        OwnedJournal::open(config, io, JournalGeneration(3)),
    )
    .unwrap();
    assert!(journal.images().is_err());
}

#[test]
fn failed_owned_promise_after_current_rename_never_grants_normal_authority() {
    let (mut controller, io) = setup();
    let config = config("/partition", 7, QuorumPolicy::Durable);
    let (mut journal, _) = drive(
        &mut controller,
        OwnedJournal::format_new(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    let mut failed = false;
    let result = drive_except(
        &mut controller,
        journal.persist_promise(promise(&config, 1)),
        None,
        |operation| {
            if matches!(operation, Operation::Rename { destination, .. }
            if destination.ends_with("CURRENT"))
            {
                failed = true;
                Effect::FailAfter(std::io::ErrorKind::Other)
            } else {
                Effect::Normal
            }
        },
    );
    assert!(failed && result.is_err());
    assert!(journal.is_faulted());
    assert_eq!(journal.scope().view, 0);
    drop(journal);
    let (journal, startup) = drive(
        &mut controller,
        OwnedJournal::open(config, io, JournalGeneration(2)),
    )
    .unwrap();
    assert!(journal.images().is_err());
    assert_eq!(startup.recovered().unwrap().scope.view, 1);
}

#[test]
fn owned_memory_voting_without_drained_shutdown_cannot_restart_intact() {
    let (mut controller, io) = setup();
    let config = config("/partition", 7, QuorumPolicy::Replicated);
    let (journal, _) = drive(
        &mut controller,
        OwnedJournal::format_new(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    drop(journal);
    assert!(matches!(
        drive(
            &mut controller,
            OwnedJournal::open(config, io, JournalGeneration(2))
        ),
        Err(JournalError::Directory(
            ozzy_journal_segment::DirectoryError::MemoryHistoryUnproven
        ))
    ));
}

#[test]
fn stalled_partition_startup_does_not_block_another_partition_on_the_same_shard_lane() {
    let (mut controller, io) = setup();
    let left = config("/left", 7, QuorumPolicy::Durable);
    let right = config("/right", 8, QuorumPolicy::Durable);
    let mut stalled = Box::pin(OwnedJournal::format_new(
        left,
        io.clone(),
        JournalGeneration(1),
        32768,
    ));
    assert!(poll(stalled.as_mut()).is_pending());
    let held = controller.jobs()[0].0;
    let (mut journal, startup) = drive_except(
        &mut controller,
        OwnedJournal::format_new(right.clone(), io, JournalGeneration(2), 32768),
        Some(held),
        |_| Effect::Normal,
    )
    .unwrap();
    assert!(startup.recovered().is_none());
    drive_except(
        &mut controller,
        journal.persist_promise(promise(&right, 2)),
        Some(held),
        |_| Effect::Normal,
    )
    .unwrap();
    assert_eq!(journal.scope().view, 1);
    assert!(poll(stalled.as_mut()).is_pending());
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    let (left, _) = drive(&mut controller, stalled).unwrap();
    drive(&mut controller, left.shutdown()).unwrap();
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn unsupported_owned_profile_is_rejected_before_mutable_recovery() {
    let (mut controller, io) = setup();
    let config = config("/partition", 7, QuorumPolicy::Durable);
    let journal = drive(
        &mut controller,
        AsyncGroupJournal::format(
            config.root.clone(),
            io.clone(),
            AsyncJournalFormat {
                identity: config.identity,
                configuration_epoch: 1,
                commit_mode: CommitMode::LocalDurable,
                first_segment: SegmentHeader::new(
                    config.identity.group_id,
                    1,
                    None,
                    Digest::ZERO,
                    32768,
                )
                .unwrap(),
                configuration: config.configuration.encode().to_vec(),
            },
            JournalGeneration(1),
            config.limits,
        ),
    )
    .unwrap();
    drop(journal);
    let mut writes = 0;
    let result = drive_except(
        &mut controller,
        OwnedJournal::open(config, io, JournalGeneration(2)),
        None,
        |operation| {
            if matches!(
                operation,
                Operation::Write { .. }
                    | Operation::Rename { .. }
                    | Operation::Allocate { .. }
                    | Operation::SetLength { .. }
                    | Operation::RemoveFile { .. }
            ) {
                writes += 1;
            }
            Effect::Normal
        },
    );
    assert!(matches!(result, Err(JournalError::UnsupportedHistory)));
    assert_eq!(writes, 0);
}

#[test]
fn foreign_generation_promise_faults_without_submitting_io() {
    let (mut controller, io) = setup();
    let config = config("/partition", 7, QuorumPolicy::Durable);
    let (mut journal, _) = drive(
        &mut controller,
        OwnedJournal::format_new(config.clone(), io, JournalGeneration(1), 32768),
    )
    .unwrap();
    assert!(matches!(
        drive(
            &mut controller,
            journal.persist_promise(promise(&config, 2))
        ),
        Err(JournalError::PromiseMismatch)
    ));
    assert!(journal.is_faulted());
    assert!(controller.jobs().is_empty());
    assert_eq!(journal.scope().view, 0);
}

#[tokio::test(flavor = "current_thread")]
async fn owned_journals_share_real_backend_and_close_independently() {
    let temporary = tempfile::tempdir().unwrap();
    let (pool, mut clients) = ozzy_io_pool::Pool::new(ozzy_io_pool::Config {
        threads: 1,
        max_inflight: 1,
        handles: 96,
        limits: device_limits(),
    })
    .unwrap();
    let io = Local::new(clients.remove(0));
    let mut first = config("", 7, QuorumPolicy::Durable);
    first.root = temporary.path().join("first");
    let mut second = config("", 8, QuorumPolicy::Durable);
    second.root = temporary.path().join("second");
    let (left, right) = tokio::join!(
        OwnedJournal::format_new(first.clone(), io.clone(), JournalGeneration(1), 32768),
        OwnedJournal::format_new(second.clone(), io.clone(), JournalGeneration(2), 32768),
    );
    left.unwrap().0.shutdown().await.unwrap();
    let (mut right, _) = right.unwrap();
    right.persist_promise(promise(&second, 2)).await.unwrap();
    assert_eq!(right.scope().view, 1);
    right.shutdown().await.unwrap();
    let (left, _) = OwnedJournal::open(first, io, JournalGeneration(3))
        .await
        .unwrap();
    assert!(left.images().is_err());
    left.shutdown().await.unwrap();
    pool.shutdown().await;
}

fn payload_owner(bytes: usize) -> crate::memory::Owner {
    crate::memory::Domain::new(None, bytes)
        .unwrap()
        .owner(crate::memory::Limits {
            bytes,
            buffers: 64,
            cache_bytes: bytes,
        })
        .unwrap()
}

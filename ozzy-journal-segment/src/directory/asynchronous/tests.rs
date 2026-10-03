use super::*;
mod canonical;
mod checkpoints;
mod cooperation;
mod history;
mod indexes;
mod lifecycle;
mod memory_voting;
mod metadata_cleanup;
mod partition_read;
mod pipeline;
mod prepared_roll;
mod recovery;
mod repair;
mod retention;
mod snapshots;
mod suffix;
use crate::{
    Digest, GroupDirectory, OperationKind, SegmentWriteMode,
    test_io::{drive, drive_with, poll},
};
use ozzy_io::simulation::{self, Controller, Effect, Image, ImageLimits, Stage};
use ozzy_io::{Class, Local, Operation, Quota, WriteBuffer};
use ozzy_journal::progress::JournalGeneration;
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};
use std::{
    io,
    path::{Path, PathBuf},
    task::Poll,
};

fn limits() -> Limits {
    Limits {
        metadata: MetadataLimits {
            max_manifest_bytes: 256 * 1024,
            max_segments: 16,
        },
        decode: DecodeLimits::default(),
        operations: OperationLimits::default(),
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
            write_mode: SegmentWriteMode::DataSync,
        },
        directory_entries: 128,
        directory_name_bytes: 16384,
    }
}
fn io_limits() -> ozzy_io::Limits {
    ozzy_io::Limits {
        shards: 1,
        data: Quota {
            operations: 4,
            bytes: 2 * 1024 * 1024,
        },
        progress: Quota {
            operations: 4,
            bytes: 2 * 1024 * 1024,
        },
    }
}
fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([1; 16]),
        replica_node_id: NodeId::from_bytes([2; 16]),
        volume_id: VolumeId::from_bytes([3; 16]),
        store_id: StoreId::from_bytes([4; 16]),
        store_generation: 1,
    }
}
fn spec(mode: CommitMode) -> Format {
    Format {
        identity: identity(),
        configuration_epoch: 1,
        commit_mode: mode,
        first_segment: SegmentHeader::new(identity().group_id, 1, None, Digest::ZERO, 32768)
            .unwrap(),
        configuration: b"test deployment".to_vec(),
    }
}
fn setup(image: Image) -> (Controller, Local) {
    let (controller, mut clients) = Controller::new(
        simulation::Config {
            limits: io_limits(),
            handles: 32,
            image: ImageLimits {
                nodes: 512,
                directory_entries: 2048,
                file_bytes: 256 * 1024,
                total_bytes: 8 * 1024 * 1024,
            },
            trace_events: 50_000,
        },
        image,
    )
    .unwrap();
    (controller, Local::new(clients.remove(0)))
}
fn operation(journal: &Journal) -> CanonicalOperation<'static> {
    let chain = journal.writer.written_position().next_chain();
    CanonicalOperation {
        group_id: identity().group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: chain.next_op_number(),
        previous_digest: chain.previous_digest(),
        kind: OperationKind::Barrier,
        body: &[1; 16],
    }
}

fn empty_journal() -> (Controller, Journal) {
    let (mut controller, io) = setup(Image::default());
    let journal = drive(
        &mut controller,
        Journal::format(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        ),
    )
    .unwrap();
    (controller, journal)
}
async fn append_confirmed(journal: &mut Journal) -> WriterPosition {
    let operation = operation(journal);
    let written = journal
        .append(&[operation], BodyEncoding::Raw)
        .await
        .unwrap();
    journal.sync_through(written).await.unwrap();
    journal.publish_durable_progress().await.unwrap();
    written
}
async fn open(io: Local, generation: u128) -> Result<Journal, DirectoryError> {
    Journal::open(
        "/group".into(),
        io,
        identity(),
        Some(b"test deployment"),
        JournalGeneration(generation),
        limits(),
    )
    .await
}
fn baseline() -> Image {
    let (mut controller, io) = setup(Image::default());
    let mut journal = drive(
        &mut controller,
        Journal::format(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        ),
    )
    .unwrap();
    drive(&mut controller, append_confirmed(&mut journal));
    drop(journal);
    controller.crash(true).unwrap().0
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn pool_and_aio_journals_exchange_rolls_and_progress_with_legacy_recovery() {
    for aio in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("group");
        let config = ozzy_io_pool::Config {
            threads: 1,
            max_inflight: 1,
            handles: 32,
            limits: io_limits(),
        };
        let (pool, aio_backend, mut clients) = if aio {
            let (backend, clients) = ozzy_io_aio::Aio::new(ozzy_io_aio::Config {
                pool: config,
                depth: 1,
            })
            .unwrap();
            (None, Some(backend), clients)
        } else {
            let (pool, clients) = ozzy_io_pool::Pool::new(config).unwrap();
            (Some(pool), None, clients)
        };
        let io = Local::new(clients.remove(0));
        let mut limits = limits();
        limits.io.direct = aio;
        let mut journal = Journal::format(
            path.clone(),
            io.clone(),
            spec(CommitMode::External),
            JournalGeneration(1),
            limits,
        )
        .await
        .unwrap();
        let first = append_confirmed(&mut journal).await;
        let mut next = journal.next_manifest().unwrap();
        next.accepted = position_before(first.next_chain()).unwrap();
        next.committed = next.accepted;
        journal.install_metadata(next).await.unwrap();
        let journal = Box::pin(prepared_roll::roll_real(journal)).await;
        let mut journal = Box::pin(pipeline::append_real_shared(journal)).await;
        let mut seen = Vec::new();
        journal
            .replay_accepted(|item| {
                seen.push((item.operation.op_number, item.committed));
                Ok::<_, io::Error>(())
            })
            .await
            .unwrap();
        assert_eq!(seen, [(1, true), (2, false)]);
        history::check_real_history(&mut journal).await;
        let journal = Box::pin(suffix::check_real_suffix(journal)).await;
        drop(journal);
        if let Some(pool) = pool {
            pool.shutdown().await;
        }
        if let Some(aio) = aio_backend {
            aio.shutdown().await;
        }
        let directory = GroupDirectory::open_with_configuration(
            &path,
            identity(),
            limits.metadata,
            b"test deployment",
        )
        .unwrap();
        let mut legacy = directory
            .recover(JournalGeneration(2), limits.decode, limits.operations)
            .unwrap();
        assert_eq!(legacy.accepted_position().unwrap().op_number, 2);
        assert_eq!(legacy.committed_position().unwrap().op_number, 1);
        let chain = legacy.writer().written_position().next_chain();
        let third = CanonicalOperation {
            op_number: chain.next_op_number(),
            previous_digest: chain.previous_digest(),
            group_id: identity().group_id,
            configuration_epoch: 1,
            original_view: 0,
            kind: OperationKind::Barrier,
            body: &[2; 16],
        };
        let written = legacy.append(&[third]).unwrap();
        legacy.sync_through(written).unwrap();
        legacy.publish_durable_progress().unwrap();
        drop(legacy);
        let (pool, mut clients) = ozzy_io_pool::Pool::new(config).unwrap();
        let journal = Journal::open(
            path,
            Local::new(clients.remove(0)),
            identity(),
            Some(b"test deployment"),
            JournalGeneration(3),
            limits,
        )
        .await
        .unwrap();
        assert_eq!(journal.accepted_position().unwrap().op_number, 3);
        assert_eq!(journal.manifest.segments.len(), 2);
        drop(journal);
        pool.shutdown().await;
    }
}

#[test]
fn every_roll_execution_and_observation_cut_recovers_confirmed_history() {
    let image = baseline();
    for immediate_delivery in [false, true] {
        let mut finished = false;
        for cut in 0..200 {
            let (mut controller, io) = setup(image.clone());
            let mut journal = drive(&mut controller, open(io, 2)).unwrap();
            let done = {
                let mut future = std::pin::pin!(journal.roll_active(32768, 4));
                let mut done = false;
                for _ in 0..cut {
                    if let Poll::Ready(result) = poll(future.as_mut()) {
                        result.unwrap();
                        done = true;
                        break;
                    }
                    let (id, stage) = controller.jobs()[0];
                    match stage {
                        Stage::Queued => {
                            controller.execute(id, Effect::Normal).unwrap();
                            if immediate_delivery {
                                controller.deliver(id).unwrap();
                            }
                        }
                        Stage::Executed => controller.deliver(id).unwrap(),
                    }
                }
                done
            };
            if cut != 0 && !done {
                assert!(journal.is_faulted());
            }
            drop(journal);
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let journal = drive(&mut controller, open(io, 3)).unwrap();
            assert_eq!(journal.accepted_position().unwrap().op_number, 1);
            assert!([1, 2].contains(&journal.manifest.segments.len()));
            let mut replayed = Vec::new();
            drive(
                &mut controller,
                journal.replay_accepted(|item| {
                    replayed.push(item.operation.op_number);
                    Ok::<_, io::Error>(())
                }),
            )
            .unwrap();
            assert_eq!(replayed, [1]);
            if done {
                assert_eq!(journal.manifest.segments.len(), 2);
                finished = true;
                break;
            }
        }
        assert!(finished, "roll did not finish within test cut bound");
    }
}

#[test]
fn failed_progress_publication_fences_until_recovery_without_claiming_success() {
    for after in [false, true] {
        let (mut controller, io) = setup(baseline());
        let mut journal = drive(&mut controller, open(io, 2)).unwrap();
        let op = operation(&journal);
        let written = drive(&mut controller, journal.append(&[op], BodyEncoding::Raw)).unwrap();
        drive(&mut controller, journal.sync_through(written)).unwrap();
        let result = drive_with(
            &mut controller,
            journal.publish_durable_progress(),
            |operation| {
                if matches!(operation, Operation::Sync { .. }) {
                    if after {
                        Effect::FailAfter(io::ErrorKind::Other)
                    } else {
                        Effect::FailBefore(io::ErrorKind::Other)
                    }
                } else {
                    Effect::Normal
                }
            },
        );
        assert!(result.is_err() && journal.is_faulted());
        assert!(matches!(
            drive(&mut controller, journal.roll_active(32768, 4)),
            Err(DirectoryError::Writer(WriterError::Faulted))
        ));
        drop(journal);
        let (mut controller, io) = setup(controller.crash(true).unwrap().0);
        let journal = drive(&mut controller, open(io, 3)).unwrap();
        assert_eq!(journal.accepted_position().unwrap().op_number, 2);
    }
}

#[test]
fn local_durable_progress_never_protects_unsynchronized_groups() {
    let (mut controller, io) = setup(Image::default());
    let mut chosen = limits();
    chosen.io.write_mode = SegmentWriteMode::Buffered;
    let mut journal = drive(
        &mut controller,
        Journal::format(
            "/group".into(),
            io,
            spec(CommitMode::LocalDurable),
            JournalGeneration(1),
            chosen,
        ),
    )
    .unwrap();
    let op = operation(&journal);
    let written = drive(&mut controller, journal.append(&[op], BodyEncoding::Raw)).unwrap();
    assert_eq!(journal.accepted_position().unwrap().op_number, 1);
    assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
    assert!(matches!(
        drive(&mut controller, journal.publish_durable_progress()),
        Err(DirectoryError::PositionNotDurable(1))
    ));
    assert!(!journal.is_faulted());
    drive(&mut controller, journal.sync_through(written)).unwrap();
    drive(&mut controller, journal.publish_durable_progress()).unwrap();
    drop(journal);
    let (mut controller, io) = setup(controller.crash(true).unwrap().0);
    let journal = drive(&mut controller, open(io, 2)).unwrap();
    assert_eq!(journal.committed_position().unwrap().op_number, 1);
}

#[test]
fn sealed_corruption_is_refused_before_active_recovery_can_modify_bytes() {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io.clone(), 2)).unwrap();
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    let access = journal.access.clone();
    let file = drive(
        &mut controller,
        access.open(
            PathBuf::from("/group/segments/1.log"),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        access.execute(
            Class::Progress,
            Operation::Write {
                handle: file.clone(),
                offset: crate::SEGMENT_HEADER_BYTES as u64 + 16,
                data: WriteBuffer::from_vec(vec![99]),
            },
        ),
    )
    .unwrap();
    drive(&mut controller, access.sync(&file)).unwrap();
    drop(journal);
    drop(access);
    drop(file);
    let before = controller
        .image()
        .bytes(Path::new("/group/segments/2.log"), false)
        .unwrap()
        .to_vec();
    assert!(drive(&mut controller, open(io, 3)).is_err());
    assert_eq!(
        controller
            .image()
            .bytes(Path::new("/group/segments/2.log"), false)
            .unwrap(),
        before
    );
}

#[test]
fn impossible_limits_reject_format_before_any_file_effect() {
    for invalid in 0..5 {
        let (mut controller, io) = setup(Image::default());
        let mut limits = limits();
        match invalid {
            0 => limits.decode.max_groups = 0,
            1 => limits.directory_entries = usize::MAX,
            2 => limits.io.max_segment_bytes = 8192,
            3 => limits.checkpoint.max_chunks = 0,
            4 => limits.metadata.max_segments = 0,
            _ => unreachable!(),
        }
        let mut future = std::pin::pin!(Journal::format(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits,
        ));
        assert!(matches!(poll(future.as_mut()), Poll::Ready(Err(_))));
        assert!(controller.jobs().is_empty());
        assert!(
            controller
                .image()
                .bytes(Path::new("/group/identity"), false)
                .is_err()
        );
        // No queued work is needed to observe the refusal.
        controller.crash(true).unwrap();
    }
}

#[test]
fn append_cannot_write_groups_its_own_recovery_limits_reject() {
    let (mut controller, io) = setup(Image::default());
    let mut journal = drive(
        &mut controller,
        Journal::format(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        ),
    )
    .unwrap();
    let original = journal.written_position().unwrap();
    for kind in 0..4 {
        journal.limits.decode = limits().decode;
        match kind {
            0 => journal.limits.decode.max_entries = 1,
            1 => journal.limits.decode.max_decoded_body_bytes = 15,
            2 => {
                journal.limits.decode.max_decoded_body_bytes = 16;
                journal.limits.decode.max_group_decoded_body_bytes = 16;
            }
            3 => journal.limits.decode.max_entry_bytes = crate::ENTRY_HEADER_BYTES,
            _ => unreachable!(),
        }
        let count = if kind == 0 || kind == 2 { 2 } else { 1 };
        let operations: Vec<_> = (0..count).map(|_| operation(&journal)).collect();
        assert!(
            drive(
                &mut controller,
                journal.append(&operations, BodyEncoding::Raw)
            )
            .is_err()
        );
        assert_eq!(journal.written_position().unwrap(), original);
        assert!(!journal.is_faulted());
    }
    journal.limits.decode = limits().decode;
    journal.limits.decode.max_groups = 1;
    drive(&mut controller, append_confirmed(&mut journal));
    let op = operation(&journal);
    assert!(matches!(
        drive(&mut controller, journal.append(&[op], BodyEncoding::Raw)),
        Err(DirectoryError::Codec(CodecError::LimitExceeded {
            kind: "physical group count",
            ..
        }))
    ));
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    drive(&mut controller, append_confirmed(&mut journal));
    assert_eq!(journal.accepted_position().unwrap().op_number, 2);
}

async fn pool_open(path: PathBuf) -> Result<(), DirectoryError> {
    let (pool, mut clients) = ozzy_io_pool::Pool::new(ozzy_io_pool::Config {
        threads: 1,
        max_inflight: 1,
        handles: 32,
        limits: io_limits(),
    })
    .unwrap();
    let result = Journal::open(
        path,
        Local::new(clients.remove(0)),
        identity(),
        Some(b"test deployment"),
        JournalGeneration(9),
        limits(),
    )
    .await;
    let result = result.map(drop);
    pool.shutdown().await;
    result
}

#[tokio::test]
async fn selected_checkpoint_is_read_and_validated_before_active_tail_repair() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("group");
    let (pool, mut clients) = ozzy_io_pool::Pool::new(ozzy_io_pool::Config {
        threads: 1,
        max_inflight: 1,
        handles: 32,
        limits: io_limits(),
    })
    .unwrap();
    let mut journal = Journal::format(
        path.clone(),
        Local::new(clients.remove(0)),
        spec(CommitMode::LocalDurable),
        JournalGeneration(1),
        limits(),
    )
    .await
    .unwrap();
    append_confirmed(&mut journal).await;
    journal.publish_progress().await.unwrap();
    drop(journal);
    pool.shutdown().await;
    let legacy = GroupDirectory::open_with_configuration(
        &path,
        identity(),
        limits().metadata,
        b"test deployment",
    )
    .unwrap()
    .recover(JournalGeneration(2), limits().decode, limits().operations)
    .unwrap();
    let id = ozzy_proto::CheckpointId::from_bytes([0x55; 16]);
    let state = b"selected checkpoint state";
    let image = legacy
        .checkpoint_plan(id, Digest::from_bytes([0x56; 32]), 8)
        .unwrap()
        .build(state, limits().checkpoint)
        .unwrap();
    let checkpoint_path = image.root().to_path_buf();
    drop(image);
    drop(legacy.install_checkpoint(id, limits().checkpoint).unwrap());

    let (pool, mut clients) = ozzy_io_pool::Pool::new(ozzy_io_pool::Config {
        threads: 1,
        max_inflight: 1,
        handles: 32,
        limits: io_limits(),
    })
    .unwrap();
    let journal = Journal::open(
        path.clone(),
        Local::new(clients.remove(0)),
        identity(),
        Some(b"test deployment"),
        JournalGeneration(3),
        limits(),
    )
    .await
    .unwrap();
    assert_eq!(
        journal.read_checkpoint_state().await.unwrap().unwrap(),
        state
    );
    journal
        .replay_accepted(|_| -> Result<(), io::Error> {
            panic!("checkpoint already covers history")
        })
        .await
        .unwrap();
    drop(journal);
    pool.shutdown().await;

    let segment_path = path.join("segments/1.log");
    let original = std::fs::read(&segment_path).unwrap();
    // This tail would be erased by recovery if checkpoint validation were late.
    let mut damaged_tail = original;
    damaged_tail[8192] = 99;
    std::fs::write(&segment_path, &damaged_tail).unwrap();
    let chunk = checkpoint_path.join(crate::checkpoint::chunk_name(0));
    let bytes = std::fs::read(&chunk).unwrap();
    std::fs::write(&chunk, [0; 8]).unwrap();
    assert!(pool_open(path.clone()).await.is_err());
    assert_eq!(std::fs::read(&segment_path).unwrap(), damaged_tail);
    std::fs::write(&chunk, &bytes).unwrap();
    let extra = checkpoint_path.join("unexpected");
    std::fs::write(&extra, b"extra").unwrap();
    assert!(pool_open(path.clone()).await.is_err());
    std::fs::remove_file(extra).unwrap();
    std::fs::remove_file(&chunk).unwrap();
    assert!(pool_open(path).await.is_err());
    assert_eq!(std::fs::read(&segment_path).unwrap(), damaged_tail);
}

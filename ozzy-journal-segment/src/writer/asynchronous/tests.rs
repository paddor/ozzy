use super::*;
use crate::SegmentWriter;
use crate::test_io::{drive, drive_with, poll};
use crate::{Digest, OperationKind, OperationLimits};
use ozzy_io::simulation::{self, Controller, Effect, Image, ImageLimits};
use ozzy_io::{Outcome, Quota, SyncMode};
use ozzy_proto::GroupId;
use std::{path::Path, task::Poll};

mod cooperation;

const CAPACITY: u64 = 32768;

fn options() -> Options {
    Options {
        max_segment_bytes: CAPACITY,
        chunk_bytes: 4096,
        direct: false,
        write_mode: SegmentWriteMode::Buffered,
    }
}
fn start(generation: u128) -> Start {
    Start {
        generation: JournalGeneration(generation),
        first_group_number: 1,
        initial_chain: ChainPosition::GENESIS,
    }
}
fn header() -> SegmentHeader {
    SegmentHeader::new(
        GroupId::from_bytes([1; 16]),
        1,
        None,
        Digest::ZERO,
        CAPACITY,
    )
    .unwrap()
}
fn operation(chain: ChainPosition) -> CanonicalOperation<'static> {
    CanonicalOperation {
        group_id: header().group_id(),
        configuration_epoch: 1,
        original_view: 1,
        op_number: chain.next_op_number(),
        previous_digest: chain.previous_digest(),
        kind: OperationKind::Barrier,
        body: &[3; 16],
    }
}
fn limits() -> ozzy_io::Limits {
    ozzy_io::Limits {
        shards: 1,
        data: Quota {
            operations: 4,
            bytes: 1024 * 1024,
        },
        progress: Quota {
            operations: 4,
            bytes: 1024 * 1024,
        },
    }
}
fn setup(image: Image) -> (Controller, Local) {
    let (controller, mut clients) = Controller::new(
        simulation::Config {
            limits: limits(),
            handles: 32,
            image: ImageLimits {
                nodes: 32,
                directory_entries: 64,
                file_bytes: CAPACITY as usize,
                total_bytes: 1024 * 1024,
            },
            trace_events: 10000,
        },
        image,
    )
    .unwrap();
    (controller, Local::new(clients.remove(0)))
}
fn create(controller: &mut Controller, io: Local, path: &str) -> Writer {
    let writer = drive(
        controller,
        Writer::create(path.into(), io.clone(), None, header(), start(1), options()),
    )
    .unwrap();
    // Publish the file's directory entry independently of its synchronized data.
    let directory = drive(
        controller,
        io.execute(
            Class::Progress,
            Operation::OpenDirectory { path: "/".into() },
        ),
    )
    .unwrap();
    let Outcome::Opened(handle) = &*directory else {
        panic!("directory")
    };
    drive(
        controller,
        io.execute(
            Class::Progress,
            Operation::Sync {
                handle: handle.clone(),
                mode: SyncMode::All,
            },
        ),
    )
    .unwrap();
    drop(io);
    writer
}
fn append(controller: &mut Controller, writer: &mut Writer) -> WriterPosition {
    let operations = [operation(writer.written_position().next_chain())];
    drive(controller, writer.append(&operations, BodyEncoding::Raw)).unwrap()
}
fn requirements(protected: ChainPosition) -> CanonicalRecoveryRequirements {
    CanonicalRecoveryRequirements {
        operation_limits: OperationLimits::default(),
        protected: [Some(protected), None],
        configuration_epoch: Some(1),
        promised_view: Some(1),
        discard_damaged_tail: true,
    }
}

#[tokio::test]
#[cfg(target_os = "linux")]
async fn real_backends_exchange_segments_with_the_legacy_writer() {
    for (aio, direct, mode) in [
        (false, false, SegmentWriteMode::Buffered),
        (false, false, SegmentWriteMode::DataSync),
        (false, true, SegmentWriteMode::DataSync),
        (true, true, SegmentWriteMode::DataSync),
    ] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("segment");
        let pool_config = ozzy_io_pool::Config {
            threads: 1,
            max_inflight: 1,
            handles: 16,
            limits: limits(),
        };
        let (pool, aio_backend, mut clients) = if aio {
            let (backend, clients) = ozzy_io_aio::Aio::new(ozzy_io_aio::Config {
                pool: pool_config,
                depth: 1,
            })
            .unwrap();
            (None, Some(backend), clients)
        } else {
            let (backend, clients) = ozzy_io_pool::Pool::new(pool_config).unwrap();
            (Some(backend), None, clients)
        };
        let io = Local::new(clients.remove(0));
        let selected = Options {
            direct,
            write_mode: mode,
            ..options()
        };
        let mut writer =
            Writer::create(path.clone(), io.clone(), None, header(), start(1), selected)
                .await
                .unwrap();
        let first = writer
            .append(&[operation(ChainPosition::GENESIS)], BodyEncoding::Raw)
            .await
            .unwrap();
        assert_eq!(writer.durable_position().group_number(), 0);
        assert_eq!(writer.sync_through(first).await.unwrap(), first);
        writer.zero_remainder().await.unwrap();
        writer.close().await.unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut legacy = SegmentWriter::recover(
            file,
            JournalGeneration(2),
            1,
            ChainPosition::GENESIS,
            DecodeLimits::default(),
            CAPACITY,
        )
        .unwrap();
        assert_eq!(legacy.written_position().next_chain(), first.next_chain());
        let second = legacy.append(&[operation(first.next_chain())]).unwrap();
        legacy.sync_through(second).unwrap();
        drop(legacy);
        let mut reopened = Writer::recover(
            path,
            io,
            None,
            start(3),
            selected,
            DecodeLimits::default(),
            Some(requirements(second.next_chain())),
        )
        .await
        .unwrap();
        assert_eq!(
            reopened.durable_position().next_chain(),
            second.next_chain()
        );
        assert!(matches!(
            reopened.sync_through(first).await,
            Err(WriterError::InvalidSyncPosition)
        ));
        reopened.close().await.unwrap();
        if let Some(pool) = pool {
            pool.shutdown().await;
        }
        if let Some(aio) = aio_backend {
            aio.shutdown().await;
        }
    }
}

#[test]
fn canceled_write_never_installs_a_result_and_other_partitions_keep_running() {
    for execute_before_cancel in [false, true] {
        let (mut controller, io) = setup(Image::default());
        let mut first = create(&mut controller, io.clone(), "/first");
        let mut second = create(&mut controller, io, "/second");
        let before = first.written_position();
        let operations = [operation(before.next_chain())];
        let mut pending = Box::pin(first.append(&operations, BodyEncoding::Raw));
        assert!(poll(pending.as_mut()).is_pending());
        let (first_id, _) = controller.jobs()[0];
        let mut independent = Box::pin(second.append(&operations, BodyEncoding::Raw));
        assert!(poll(independent.as_mut()).is_pending());
        let second_id = controller
            .jobs()
            .into_iter()
            .find(|(id, _)| *id != first_id)
            .unwrap()
            .0;
        controller.execute(second_id, Effect::Normal).unwrap();
        controller.deliver(second_id).unwrap();
        assert!(matches!(poll(independent.as_mut()), Poll::Ready(Ok(_))));
        drop(independent);
        assert_eq!(second.written_position().group_number(), 1);
        if execute_before_cancel {
            controller.execute(first_id, Effect::Normal).unwrap();
        }
        drop(pending);
        assert!(first.is_faulted());
        assert_eq!(first.written_position(), before);
        assert!(matches!(
            drive(
                &mut controller,
                first.append(&operations, BodyEncoding::Raw)
            ),
            Err(WriterError::Faulted)
        ));
        if !execute_before_cancel {
            controller.execute(first_id, Effect::Normal).unwrap();
        }
        controller.deliver(first_id).unwrap();
        assert_eq!(
            controller.admission().used(0, Class::Data),
            Quota::default()
        );
    }
}

#[test]
fn failed_or_partial_group_write_fences_without_advancing_state() {
    for effect in [
        Effect::Short(0),
        Effect::Short(80),
        Effect::FailBefore(io::ErrorKind::StorageFull),
        Effect::FailAfter(io::ErrorKind::Other),
    ] {
        let (mut controller, io) = setup(Image::default());
        let mut writer = create(&mut controller, io, "/segment");
        let before = writer.written_position();
        let operations = [operation(before.next_chain())];
        let result = drive_with(
            &mut controller,
            writer.append(&operations, BodyEncoding::Raw),
            |_| effect,
        );
        assert!(result.is_err());
        assert_eq!(writer.written_position(), before);
        assert_eq!(writer.durable_position(), before);
        assert!(writer.is_faulted());
    }
}

fn image_with_missing_middle_group() -> (Image, WriterPosition, WriterPosition) {
    let (mut controller, io) = setup(Image::default());
    let mut writer = create(&mut controller, io, "/segment");
    let first = append(&mut controller, &mut writer);
    drive(&mut controller, writer.sync_through(first)).unwrap();
    let second = append(&mut controller, &mut writer);
    let third = append(&mut controller, &mut writer);
    controller
        .persist_range(
            Path::new("/segment"),
            second.end_offset() as usize..third.end_offset() as usize,
        )
        .unwrap();
    drop(writer);
    (controller.crash(true).unwrap().0, first, third)
}

#[test]
fn power_loss_hole_is_discarded_only_beyond_every_protected_position() {
    let (image, first, third) = image_with_missing_middle_group();
    for protected in [first.next_chain(), third.next_chain()] {
        let (mut controller, io) = setup(image.clone());
        let before = controller
            .image()
            .bytes(Path::new("/segment"), false)
            .unwrap()
            .to_vec();
        let result = drive(
            &mut controller,
            Writer::recover(
                "/segment".into(),
                io,
                None,
                start(2),
                options(),
                DecodeLimits::default(),
                Some(requirements(protected)),
            ),
        );
        if protected == third.next_chain() {
            assert!(matches!(
                result,
                Err(WriterError::ProtectedPrefixMismatch(_))
            ));
            assert_eq!(
                controller
                    .image()
                    .bytes(Path::new("/segment"), false)
                    .unwrap(),
                before
            );
        } else {
            let writer = result.unwrap();
            assert_eq!(writer.written_position().next_chain(), first.next_chain());
            assert_eq!(writer.durable_position(), writer.written_position());
            assert!(
                controller
                    .image()
                    .bytes(Path::new("/segment"), true)
                    .unwrap()[first.end_offset() as usize..]
                    .iter()
                    .all(|byte| *byte == 0)
            );
        }
    }
}

#[test]
fn interrupted_tail_repair_and_failed_barrier_never_report_recovery_success() {
    let (image, first, _) = image_with_missing_middle_group();
    for fail_write in [false, true] {
        let (mut controller, io) = setup(image.clone());
        let mut injected = false;
        let result = drive_with(
            &mut controller,
            Writer::recover(
                "/segment".into(),
                io,
                None,
                start(2),
                options(),
                DecodeLimits::default(),
                Some(requirements(first.next_chain())),
            ),
            |operation| {
                let selected = if fail_write {
                    matches!(operation, Operation::Write { .. })
                } else {
                    matches!(operation, Operation::Sync { .. })
                };
                if selected && !injected {
                    injected = true;
                    Effect::FailBefore(io::ErrorKind::Other)
                } else {
                    Effect::Normal
                }
            },
        );
        assert!(injected && result.is_err());
        let (image, _) = controller.crash(true).unwrap();
        let (mut controller, io) = setup(image);
        let recovered = drive(
            &mut controller,
            Writer::recover(
                "/segment".into(),
                io,
                None,
                start(3),
                options(),
                DecodeLimits::default(),
                Some(requirements(first.next_chain())),
            ),
        )
        .unwrap();
        assert_eq!(
            recovered.durable_position().next_chain(),
            first.next_chain()
        );
    }
}

#[test]
fn failed_sync_fences_without_advancing_evidence_or_stable_record_bytes() {
    for effect in [
        Effect::FailBefore(io::ErrorKind::Other),
        Effect::FailAfter(io::ErrorKind::Other),
    ] {
        let (mut controller, io) = setup(Image::default());
        let mut writer = create(&mut controller, io, "/segment");
        let before = writer.durable_position();
        let written = append(&mut controller, &mut writer);
        assert!(matches!(
            drive_with(&mut controller, writer.sync_through(written), |op| {
                assert!(matches!(op, Operation::Sync { .. }));
                effect
            }),
            Err(WriterError::Io(_))
        ));
        assert!(writer.is_faulted());
        assert_eq!(writer.written_position(), written);
        assert_eq!(writer.durable_position(), before);
        assert!(matches!(
            drive(&mut controller, writer.sync_through(written)),
            Err(WriterError::Faulted)
        ));
        let (image, _) = controller.crash(true).unwrap();
        let stable = image.bytes(Path::new("/segment"), true).unwrap();
        if matches!(effect, Effect::FailBefore(_)) {
            assert_eq!(
                crate::scan_segment(stable, 1, ChainPosition::GENESIS, DecodeLimits::default())
                    .unwrap()
                    .groups
                    .len(),
                0
            );
            assert!(stable[SEGMENT_HEADER_BYTES..].iter().all(|byte| *byte == 0));
        } else {
            assert_eq!(
                crate::scan_segment(stable, 1, ChainPosition::GENESIS, DecodeLimits::default())
                    .unwrap()
                    .next_chain,
                written.next_chain()
            );
        }
    }
}

#[test]
fn pending_sync_cancellation_keeps_the_installed_durable_boundary() {
    let (mut controller, io) = setup(Image::default());
    let mut writer = create(&mut controller, io, "/segment");
    let before = writer.durable_position();
    let written = append(&mut controller, &mut writer);
    let mut pending = Box::pin(writer.sync_through(written));
    assert!(poll(pending.as_mut()).is_pending());
    let (id, _) = controller.jobs()[0];
    controller.execute(id, Effect::Normal).unwrap();
    drop(pending);
    assert!(writer.is_faulted());
    assert_eq!(writer.durable_position(), before);
    controller.deliver(id).unwrap();
}

#[test]
fn completed_writes_reuse_encoding_memory_and_recovery_handles_short_reads() {
    let (mut controller, io) = setup(Image::default());
    let mut writer = create(&mut controller, io.clone(), "/segment");
    writer
        .state
        .reserve_encode_buffer(8192, BodyEncoding::Raw)
        .unwrap();
    let allocation = writer.state.encode_buffer.as_ptr();
    for _ in 0..3 {
        append(&mut controller, &mut writer);
        assert_eq!(writer.state.encode_buffer.as_ptr(), allocation);
        assert_eq!(writer.state.encode_buffer.capacity(), 8192);
    }
    let written = writer.written_position();
    drive(&mut controller, writer.close()).unwrap();
    let recovered = drive_with(
        &mut controller,
        Writer::recover(
            "/segment".into(),
            io,
            None,
            start(2),
            options(),
            DecodeLimits::default(),
            Some(requirements(written.next_chain())),
        ),
        |operation| {
            if matches!(operation, Operation::Read { .. }) {
                Effect::Short(100)
            } else {
                Effect::Normal
            }
        },
    )
    .unwrap();
    assert_eq!(
        recovered.durable_position().next_chain(),
        written.next_chain()
    );
}

#[test]
fn truncated_tail_is_repaired_only_after_canonical_protection_checks() {
    let (mut controller, io) = setup(Image::default());
    let mut writer = create(&mut controller, io.clone(), "/segment");
    let first = append(&mut controller, &mut writer);
    let second = append(&mut controller, &mut writer);
    drive(
        &mut controller,
        writer.access.done(Operation::SetLength {
            handle: writer.buffered.clone(),
            length: first.end_offset() + 100,
        }),
    )
    .unwrap();
    drive(&mut controller, writer.close()).unwrap();
    let image = controller.crash(false).unwrap().0;
    for protected in [first.next_chain(), second.next_chain()] {
        let (mut controller, io) = setup(image.clone());
        let result = drive(
            &mut controller,
            Writer::recover(
                "/segment".into(),
                io,
                None,
                start(2),
                options(),
                DecodeLimits::default(),
                Some(requirements(protected)),
            ),
        );
        let length = controller
            .image()
            .bytes(Path::new("/segment"), false)
            .unwrap()
            .len() as u64;
        if protected == first.next_chain() {
            assert_eq!(
                result.unwrap().written_position().next_chain(),
                first.next_chain()
            );
            assert_eq!(length, first.end_offset());
        } else {
            assert!(matches!(
                result,
                Err(WriterError::ProtectedPrefixMismatch(_))
            ));
            assert_eq!(length, first.end_offset() + 100);
        }
    }
}

#[test]
fn read_handles_reuse_exact_sources_and_eviction_keeps_running_leases() {
    let (mut controller, io) = setup(Image::default());
    let access = crate::async_files::Access::new(io.clone(), None);
    let source = crate::IndexSource {
        group_id: header().group_id(),
        segment_id: 1,
        valid_bytes: 4096,
        segment_digest: Digest::ZERO,
        first_op_number: 1,
        last_op_number: 1,
        last_operation_digest: Digest::ZERO,
    };
    for number in 0..5 {
        drop(create(
            &mut controller,
            io.clone(),
            &format!("/segment-{number}"),
        ));
    }
    let retained = drive(
        &mut controller,
        access.read_handle("/segment-0".into(), source),
    )
    .unwrap();
    let observed = controller.trace().len();
    drop(
        drive(
            &mut controller,
            access.read_handle("/segment-0".into(), source),
        )
        .unwrap(),
    );
    assert_eq!(
        controller.trace().len(),
        observed,
        "a cache hit must issue no file job"
    );
    for number in 1..5 {
        drop(
            drive(
                &mut controller,
                access.read_handle(format!("/segment-{number}").into(), source),
            )
            .unwrap(),
        );
        assert!(access.readers.borrow().len() <= 4);
    }
    assert!(
        !access
            .readers
            .borrow()
            .iter()
            .any(|(path, _, _)| path == Path::new("/segment-0"))
    );
    assert_eq!(
        drive(&mut controller, access.length(&retained)).unwrap(),
        4096
    );
    let observed = controller.trace().len();
    let changed = crate::IndexSource {
        valid_bytes: source.valid_bytes + 4096,
        ..source
    };
    drop(
        drive(
            &mut controller,
            access.read_handle("/segment-4".into(), changed),
        )
        .unwrap(),
    );
    assert!(
        controller.trace().len() > observed,
        "changed source must reopen"
    );
    assert_eq!(access.readers.borrow().len(), 4);
}

use super::*;
mod cooperation;
use crate::{
    IndexBuildError, IndexBuildLimits, IndexLimits, IndexSource, SegmentIndex, build_segment_index,
    scan_segment, segment_index_name,
};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, encode_operation_body,
};
use ozzy_proto::{
    MessageId, Offset, OperationId, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};

pub(super) fn build_limits() -> IndexBuildLimits {
    IndexBuildLimits {
        max_entry_buffer_bytes: 200,
        max_merge_fan_in: 2,
        max_run_files: 16,
        file: IndexLimits {
            max_file_bytes: 65536,
            max_offset_entries: 64,
            max_message_entries: 64,
            max_operation_entries: 64,
        },
        max_resident_bytes: 0,
    }
}
pub(super) fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([0x20; 16])
}
pub(super) fn append_body(number: u8) -> Vec<u8> {
    encode_operation_body(
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: partition(),
                owner_epoch: OwnerEpoch::new(1),
                producer_id: ProducerId::from_bytes([0x30; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(u64::from(number)),
                first_offset: Offset::new(u64::from(number)),
                append_timestamp_millis: 100,
                records: vec![AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([0x40 + number % 2; 16]),
                    parts: vec![b"payload".as_slice()].into(),
                }]
                .into(),
            }],
        }),
        OperationLimits::default(),
    )
    .unwrap()
}

pub(super) async fn populate(journal: &mut Journal) {
    populate_encoded(journal, BodyEncoding::Raw).await;
}

async fn populate_encoded(journal: &mut Journal, encoding: BodyEncoding) {
    for number in 0..6 {
        let body = append_body(number);
        let template = operation(journal);
        let op = CanonicalOperation {
            kind: OperationKind::Append,
            body: &body,
            ..template
        };
        journal.append(&[op], encoding).await.unwrap();
    }
    append_confirmed(journal).await;
    journal.roll_active(32768, 4).await.unwrap();
}

fn check_index(index: &SegmentIndex) {
    assert_eq!(index.offset_count(), 6);
    assert_eq!(index.message_count(), 6);
    assert_eq!(index.operation_count(), 1);
    for number in 0..6 {
        let entry = index.find_offset(partition(), Offset::new(number)).unwrap();
        assert_eq!(entry.location.operation.op_number, number + 1);
    }
    // Repeated message IDs remain ordered by their first offset across runs.
    assert_eq!(
        index
            .find_message(partition(), MessageId::from_bytes([0x41; 16]))
            .unwrap()
            .offset,
        Offset::new(1)
    );
    assert_eq!(
        index
            .find_operation(OperationId::from_bytes([1; 16]))
            .unwrap()
            .location
            .op_number,
        7
    );
}

#[tokio::test]
async fn async_multi_pass_index_is_byte_identical_to_existing_builder() {
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
        path,
        Local::new(clients.remove(0)),
        spec(CommitMode::External),
        JournalGeneration(1),
        limits(),
    )
    .await
    .unwrap();
    populate(&mut journal).await;
    let index = journal.build_sealed_index(1, build_limits()).await.unwrap();
    check_index(&index);
    assert_eq!(
        journal
            .build_sealed_index(1, build_limits())
            .await
            .unwrap()
            .as_bytes(),
        index.as_bytes()
    );
    assert_eq!(
        journal
            .open_sealed_index(1, build_limits().file)
            .await
            .unwrap()
            .as_bytes(),
        index.as_bytes()
    );
    let source = index.source();
    let captured = journal.capture_sealed_segments(&[1]).unwrap();
    let record = captured
        .read_record(
            source,
            index.find_offset(partition(), Offset::new(1)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(record.parts[0], b"payload"[..]);
    assert_eq!(record.producer_sequence, ProducerSequence::new(1));
    drop(captured);
    check_snapshot(&journal).await;
    let reference = journal.manifest.segments[0];
    let image = journal.segment_image(reference).await.unwrap();
    drop(journal);
    pool.shutdown().await;
    let indexes = root.path().join("legacy-indexes");
    let staging = root.path().join("legacy-staging");
    std::fs::create_dir(&indexes).unwrap();
    std::fs::create_dir(&staging).unwrap();
    let scan = scan_segment(
        &image,
        reference.first_group_number,
        reference.first_chain,
        limits().decode,
    )
    .unwrap();
    let legacy = build_segment_index(
        &scan,
        source,
        indexes,
        staging,
        limits().operations,
        build_limits(),
    )
    .unwrap();
    assert_eq!(index.as_bytes(), legacy.as_bytes());
}

async fn check_snapshot(journal: &Journal) {
    let snapshot = journal
        .open_index_snapshot(crate::JournalIndexBoundary::Accepted, build_limits().file)
        .await
        .unwrap();
    assert_eq!(
        snapshot
            .read_range(
                partition(),
                Offset::new(0),
                Offset::new(6),
                ozzy_journal::ReadLimits {
                    max_records: 6,
                    max_bytes: 100,
                }
            )
            .await
            .unwrap()
            .len(),
        6
    );
    assert_eq!(
        snapshot
            .find_operation(OperationId::from_bytes([1; 16]))
            .await
            .unwrap()
            .unwrap()
            .entry
            .location
            .op_number,
        7
    );
}

async fn retire_first_segment(journal: &mut Journal) {
    let mut next = journal.next_manifest().unwrap();
    next.accepted = journal.accepted_position().unwrap();
    next.committed = next.accepted;
    journal.install_metadata(next).await.unwrap();
    let id = ozzy_proto::CheckpointId::from_bytes([0x55; 16]);
    journal
        .build_checkpoint(id, Digest::from_bytes([0x56; 32]), 8, b"checkpoint")
        .await
        .unwrap();
    journal.install_checkpoint(id).await.unwrap();
    let floors = crate::RetentionFloors::new(vec![(partition(), Offset::new(6))]).unwrap();
    journal
        .retire_sealed_prefix(
            &floors,
            crate::AsyncRetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        )
        .await
        .unwrap();
}

#[test]
fn indexed_readers_validate_selectors_and_remain_readable_after_retirement() {
    for encoding in [
        BodyEncoding::Raw,
        BodyEncoding::Lz4 {
            min_savings_bytes: 0,
        },
    ]
    .into_iter()
    .filter(|encoding| encoding.is_supported())
    {
        let (mut controller, mut journal) = empty_journal();
        drive(&mut controller, populate_encoded(&mut journal, encoding));
        let index = drive(
            &mut controller,
            journal.build_sealed_index(1, build_limits()),
        )
        .unwrap();
        let captured = journal.capture_sealed_segments(&[1]).unwrap();
        let snapshot = drive(
            &mut controller,
            journal.open_index_snapshot(crate::JournalIndexBoundary::Accepted, build_limits().file),
        )
        .unwrap();
        let entry = index.find_offset(partition(), Offset::new(3)).unwrap();
        let record = drive(&mut controller, captured.read_record(index.source(), entry)).unwrap();
        assert_eq!(record.parts[0], b"payload"[..]);
        let mut wrong = entry;
        wrong.location.record_index = 20;
        assert!(drive(&mut controller, captured.read_record(index.source(), wrong)).is_err());
        let mut wrong_source = index.source();
        wrong_source.first_op_number += 1;
        assert!(drive(&mut controller, captured.read_record(wrong_source, entry)).is_err());
        drive(&mut controller, retire_first_segment(&mut journal));
        assert_eq!(
            drive(&mut controller, journal.reclaim_unreferenced_segments(1))
                .unwrap()
                .pinned_segment_ids,
            [1]
        );
        assert_eq!(
            drive(&mut controller, captured.read_record(index.source(), entry)).unwrap(),
            record
        );
        drop(captured);
        assert_eq!(
            drive(&mut controller, journal.reclaim_unreferenced_segments(1))
                .unwrap()
                .pinned_segment_ids,
            [1]
        );
        assert_eq!(
            drive(
                &mut controller,
                snapshot.read_offset(partition(), Offset::new(3))
            )
            .unwrap()
            .unwrap(),
            record
        );
        let file = drive(
            &mut controller,
            journal.access.open(
                journal.root().join("segments/1.log"),
                ozzy_io::OpenMode::ReadWrite,
                false,
                false,
            ),
        )
        .unwrap();
        drive(
            &mut controller,
            journal.access.write_all(
                &file,
                entry.location.operation.entry_offset + crate::ENTRY_HEADER_BYTES as u64,
                &[99],
            ),
        )
        .unwrap();
        assert!(
            drive(
                &mut controller,
                snapshot.read_offset(partition(), Offset::new(3))
            )
            .is_err()
        );
        drop(snapshot);
        assert_eq!(
            drive(&mut controller, journal.reclaim_unreferenced_segments(1))
                .unwrap()
                .removed_segment_ids,
            [1]
        );
    }
}

fn sealed_image() -> Image {
    let (mut controller, io) = setup(baseline());
    let mut journal = drive(&mut controller, open(io, 2)).unwrap();
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    drop(journal);
    controller.crash(true).unwrap().0
}

async fn corrupt_index(journal: &Journal, source: IndexSource) {
    let file = journal
        .access
        .open(
            journal
                .root()
                .join("indexes")
                .join(segment_index_name(source)),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        )
        .await
        .unwrap();
    journal.access.write_all(&file, 0, &[99]).await.unwrap();
    journal.access.sync(&file).await.unwrap();
}

#[test]
fn explicit_index_repair_checks_source_before_replacing_derived_bytes() {
    let (mut controller, io) = setup(sealed_image());
    let mut journal = drive(&mut controller, open(io.clone(), 3)).unwrap();
    let original = drive(
        &mut controller,
        journal.build_sealed_index(1, build_limits()),
    )
    .unwrap();
    drive(&mut controller, corrupt_index(&journal, original.source()));
    assert!(
        drive(
            &mut controller,
            journal.build_sealed_index(1, build_limits())
        )
        .is_err()
    );
    drop(journal);
    let mut journal = drive(&mut controller, open(io.clone(), 4)).unwrap();
    let repaired = drive(
        &mut controller,
        journal.repair_sealed_index(1, build_limits()),
    )
    .unwrap();
    assert_eq!(repaired.as_bytes(), original.as_bytes());
    let file = drive(
        &mut controller,
        journal.access.open(
            journal.root().join("segments/1.log"),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal
            .access
            .write_all(&file, crate::SEGMENT_HEADER_BYTES as u64, &[99]),
    )
    .unwrap();
    assert!(
        drive(
            &mut controller,
            journal.repair_sealed_index(1, build_limits())
        )
        .is_err()
    );
    let bytes = controller
        .image()
        .bytes(
            &journal
                .root()
                .join("indexes")
                .join(segment_index_name(original.source())),
            false,
        )
        .unwrap();
    assert_eq!(bytes, original.as_bytes());
}

#[test]
fn sort_and_workspace_limits_bound_failed_builds_without_touching_history() {
    for workspace_limit in [false, true] {
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
        drive(&mut controller, populate(&mut journal));
        let before = journal.committed_position().unwrap();
        let mut limits = build_limits();
        if workspace_limit {
            journal.limits.directory_entries = 2;
        } else {
            limits.max_run_files = 1;
        }
        let error = drive(&mut controller, journal.build_sealed_index(1, limits)).unwrap_err();
        assert!(matches!(
            error,
            DirectoryError::Index(IndexBuildError::RunLimitExceeded(_))
        ));
        assert_eq!(journal.manifest.committed, before);
    }
}

#[test]
fn index_publication_crash_cuts_rebuild_or_reuse_without_changing_history() {
    let image = sealed_image();
    for immediate in [false, true] {
        let mut finished = false;
        for cut in 0..400 {
            let (mut controller, io) = setup(image.clone());
            let mut journal = drive(&mut controller, open(io, 3)).unwrap();
            let done = {
                let mut future = std::pin::pin!(journal.build_sealed_index(1, build_limits()));
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
                            if immediate {
                                controller.deliver(id).unwrap();
                            }
                        }
                        Stage::Executed => controller.deliver(id).unwrap(),
                    }
                }
                done
            };
            drop(journal);
            let (mut controller, io) = setup(controller.crash(true).unwrap().0);
            let mut journal = drive(&mut controller, open(io, 4)).unwrap();
            assert_eq!(journal.accepted_position().unwrap().op_number, 1);
            let index = drive(
                &mut controller,
                journal.build_sealed_index(1, build_limits()),
            )
            .unwrap();
            assert_eq!(index.operation_count(), 1);
            assert_eq!(
                index
                    .find_operation(OperationId::from_bytes([1; 16]))
                    .unwrap()
                    .location
                    .op_number,
                1
            );
            if done {
                finished = true;
                break;
            }
        }
        assert!(finished);
    }
}

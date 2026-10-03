use std::io::{Seek, SeekFrom, Write};

use ozzy_journal::operation::{Barrier, OperationBody, encode_operation_body};
use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    BodyEncoding, CanonicalOperation, ChainPosition, DecodeLimits, Digest, GroupDirectory,
    GroupIdentity, HistoryError, JournalHistory, LogPosition, OpenGroupJournal, OperationKind,
    OperationLimits, SEGMENT_HEADER_BYTES, SegmentHeader, SuffixReplacement, SuffixStreamLimits,
    encode_group, logical_operation_digest,
};
use ozzy_proto::{GroupId, NodeId, OperationId, StoreId, VolumeId};
use tempfile::TempDir;

const CAPACITY: u64 = 12 * 1024;

fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([1; 16]),
        replica_node_id: NodeId::from_bytes([2; 16]),
        volume_id: VolumeId::from_bytes([3; 16]),
        store_id: StoreId::from_bytes([4; 16]),
        store_generation: 1,
    }
}

fn empty() -> (TempDir, OpenGroupJournal) {
    let temporary = tempfile::Builder::new()
        .prefix(".history-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let header = SegmentHeader::new(identity().group_id, 1, None, Digest::ZERO, CAPACITY).unwrap();
    let journal =
        GroupDirectory::format_new(temporary.path().join("group"), identity(), 1, &header)
            .unwrap()
            .recover(
                JournalGeneration(1),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
    (temporary, journal)
}

fn bodies(count: u8) -> Vec<Vec<u8>> {
    (1..=count)
        .map(|index| {
            encode_operation_body(
                &OperationBody::Barrier(Barrier {
                    operation_id: OperationId::from_bytes([index; 16]),
                }),
                OperationLimits::default(),
            )
            .unwrap()
        })
        .collect()
}

fn operations(bytes: &[Vec<u8>], view: u64) -> Vec<CanonicalOperation<'_>> {
    let mut previous = LogPosition::GENESIS;
    bytes
        .iter()
        .map(|body| {
            let operation = CanonicalOperation {
                group_id: identity().group_id,
                configuration_epoch: 1,
                original_view: view,
                op_number: previous.op_number + 1,
                previous_digest: previous.digest,
                kind: OperationKind::Barrier,
                body,
            };
            previous = position(operation);
            operation
        })
        .collect()
}

fn position(operation: CanonicalOperation<'_>) -> LogPosition {
    LogPosition {
        op_number: operation.op_number,
        digest: logical_operation_digest(&operation),
    }
}

fn append(
    journal: &mut OpenGroupJournal,
    operations: &[CanonicalOperation<'_>],
    encoding: BodyEncoding,
) {
    let written = journal
        .append_with_body_encoding(operations, encoding)
        .unwrap();
    journal.sync_through(written).unwrap();
}

fn assert_chunks(history: &mut JournalHistory, expected: &[CanonicalOperation<'_>]) {
    let mut predecessor = LogPosition::GENESIS;
    let mut visited = 0usize;
    while predecessor != history.through() {
        let chunk = history.read_after(predecessor, 2, 32).unwrap();
        assert_eq!(chunk.predecessor(), predecessor);
        assert!((1..=2).contains(&chunk.operations().len()));
        for operation in chunk.operations() {
            assert_eq!(operation, expected[visited]);
            visited += 1;
        }
        predecessor = chunk.end();
    }
    assert_eq!(visited, expected.len());
}

#[test]
fn captures_stable_tail_and_never_exports_later_active_appends() {
    let (_temporary, mut journal) = empty();
    let bytes = bodies(6);
    let operations = operations(&bytes, 0);
    let pending = journal.append(&operations[..3]).unwrap();
    assert!(matches!(
        journal.freeze_history(CAPACITY as usize),
        Err(HistoryError::Unsettled)
    ));
    journal.sync_through(pending).unwrap();
    assert!(matches!(
        journal.freeze_history(CAPACITY as usize - 1),
        Err(HistoryError::Capacity)
    ));
    let mut history = journal.freeze_history(CAPACITY as usize).unwrap();
    assert_eq!(history.generation(), JournalGeneration(1));
    assert_eq!(history.identity(), identity());
    journal.append(&operations[3..]).unwrap(); // Deliberately leave the later group unsynced.
    assert_eq!(history.through(), position(operations[2]));
    assert_eq!(history.position(4).unwrap(), None);
    assert_chunks(&mut history, &operations[..3]);
}

#[test]
fn chunks_and_selection_lookups_span_many_source_segments() {
    let (_temporary, mut journal) = empty();
    let bytes = bodies(21);
    let operations = operations(&bytes, 0);
    for (index, group) in operations.chunks(3).enumerate() {
        if index != 0 && index % 2 == 0 {
            journal = journal.roll_active(CAPACITY).unwrap();
        }
        append(&mut journal, group, BodyEncoding::Raw);
    }
    assert_eq!(journal.directory().manifest().segments.len(), 4);
    let mut history = journal.freeze_history(CAPACITY as usize).unwrap();
    assert_chunks(&mut history, &operations);
    for operation in operations.iter().rev() {
        assert_eq!(
            history.position(operation.op_number).unwrap(),
            Some(position(*operation))
        );
    }
    assert_eq!(history.position(0).unwrap(), Some(LogPosition::GENESIS));
    assert_eq!(history.position(22).unwrap(), None);
    assert!(matches!(
        history.read_after(
            LogPosition {
                op_number: 3,
                digest: Digest::ZERO
            },
            2,
            32
        ),
        Err(HistoryError::Predecessor)
    ));
    assert!(matches!(
        history.read_after(LogPosition::GENESIS, 0, 32),
        Err(HistoryError::Capacity)
    ));
    assert!(matches!(
        history.read_after(LogPosition::GENESIS, 2, 1),
        Err(HistoryError::BodyBudget { required, available: 1 }) if required > 1
    ));
    assert!(matches!(
        history.read_after(history.through(), 2, 32),
        Err(HistoryError::Predecessor)
    ));
}

#[test]
fn captured_prefix_can_end_inside_a_later_synced_group_without_exporting_its_tail() {
    let (_temporary, mut journal) = empty();
    let bytes = bodies(9);
    let operations = operations(&bytes, 0);
    // Protocol captured op 3 while this physical group was still pending.
    let target = position(operations[2]);
    let written = journal.append(&operations[..6]).unwrap();
    assert!(matches!(
        journal.freeze_history_through(target, CAPACITY as usize),
        Err(HistoryError::Unsettled)
    ));
    journal.sync_through(written).unwrap();
    let mut history = journal
        .freeze_history_through(target, CAPACITY as usize)
        .unwrap();
    assert_eq!(history.through(), target);
    assert_eq!(history.position(3).unwrap(), Some(target));
    assert_eq!(history.position(4).unwrap(), None);
    // Even an oversized chunk must stop at the captured canonical prefix.
    let chunk = history.read_after(LogPosition::GENESIS, 100, 1000).unwrap();
    assert_eq!(chunk.operations().collect::<Vec<_>>(), operations[..3]);
    assert_eq!(chunk.end(), target);
    journal = journal.roll_active(CAPACITY).unwrap();
    journal.append(&operations[6..]).unwrap();
    assert_chunks(&mut history, &operations[..3]);
    assert!(matches!(
        history.read_after(target, 1, 16),
        Err(HistoryError::Predecessor)
    ));
}

#[test]
fn exact_prefix_capture_checks_every_anchor_across_sealed_and_active_segments() {
    let (_temporary, mut journal) = empty();
    let bytes = bodies(12);
    let operations = operations(&bytes, 0);
    for (index, group) in operations.chunks(3).enumerate() {
        if index != 0 {
            journal = journal.roll_active(CAPACITY).unwrap();
        }
        append(&mut journal, group, BodyEncoding::Raw);
    }
    for end in 0..=operations.len() {
        let target = if end == 0 {
            LogPosition::GENESIS
        } else {
            position(operations[end - 1])
        };
        let mut history = journal
            .freeze_history_through(target, CAPACITY as usize)
            .unwrap();
        assert_eq!(history.through(), target);
        assert_chunks(&mut history, &operations[..end]);
        assert_eq!(history.position(target.op_number + 1).unwrap(), None);
        let wrong = LogPosition {
            digest: Digest::from_bytes([19; 32]),
            ..target
        };
        assert!(matches!(
            journal.freeze_history_through(wrong, CAPACITY as usize),
            Err(HistoryError::Source)
        ));
    }
    let missing = LogPosition {
        op_number: 13,
        digest: Digest::from_bytes([20; 32]),
    };
    assert!(matches!(
        journal.freeze_history_through(missing, CAPACITY as usize),
        Err(HistoryError::Source)
    ));
}

#[test]
fn pinned_history_survives_divergent_suffix_installation_and_cleanup() {
    let (_temporary, mut journal) = empty();
    let bytes = bodies(6);
    let old = operations(&bytes, 0);
    append(&mut journal, &old, BodyEncoding::Raw);
    let mut history = journal.freeze_history(CAPACITY as usize).unwrap();
    let mut prefix = journal
        .freeze_history_through(position(old[2]), CAPACITY as usize)
        .unwrap();
    let new = operations(&bytes[..3], 1);
    let request = SuffixReplacement {
        expected_current: journal.directory().current(),
        protected_committed: LogPosition::GENESIS,
        promised_view: 1,
        last_normal_view: 1,
        committed: LogPosition::GENESIS,
        writer_generation: JournalGeneration(2),
        segment_capacity: CAPACITY,
        body_encoding: BodyEncoding::Raw,
    };
    let mut install = journal
        .begin_suffix_replacement(
            request,
            position(new[2]),
            SuffixStreamLimits {
                max_group_operations: 3,
                max_group_body_bytes: 64,
                max_segments: 8,
                max_staged_bytes: 8 * CAPACITY,
                max_source_segment_bytes: CAPACITY,
                max_orphan_probes: 8,
            },
        )
        .unwrap();
    install.append_chunk(&new).unwrap();
    let journal = install.finish().unwrap();
    assert_eq!(
        journal
            .reclaim_unreferenced_segments()
            .unwrap()
            .pinned_segment_ids,
        [1]
    );
    assert_chunks(&mut history, &old);
    assert_chunks(&mut prefix, &old[..3]);
    assert_ne!(history.through(), journal.accepted_position().unwrap());
    drop(history);
    assert_eq!(
        journal
            .reclaim_unreferenced_segments()
            .unwrap()
            .pinned_segment_ids,
        [1]
    );
    drop(prefix);
    assert_eq!(
        journal
            .reclaim_unreferenced_segments()
            .unwrap()
            .removed_segment_ids,
        [1]
    );
}

#[test]
fn valid_but_wrong_active_group_is_rejected_by_captured_external_anchor() {
    let (temporary, mut journal) = empty();
    let bytes = bodies(3);
    let expected = operations(&bytes, 0);
    append(&mut journal, &expected, BodyEncoding::Raw);
    let mut history = journal.freeze_history(CAPACITY as usize).unwrap();
    let target = position(expected[0]);
    let mut changed = bytes;
    changed[2] = changed[1].clone(); // First operation remains canonically identical.
    let alternate = operations(&changed, 0);
    let replacement = encode_group(
        journal.writer().header(),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &alternate,
    )
    .unwrap();
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(temporary.path().join("group/segments/1.log"))
        .unwrap();
    file.seek(SeekFrom::Start(SEGMENT_HEADER_BYTES as u64))
        .unwrap();
    file.write_all(replacement.as_bytes()).unwrap();
    file.sync_data().unwrap();
    assert!(matches!(
        journal.freeze_history_through(target, CAPACITY as usize),
        Err(HistoryError::Source)
    ));
    assert!(matches!(history.position(1), Err(HistoryError::Source)));
    assert!(matches!(
        history.read_after(LogPosition::GENESIS, 2, 32),
        Err(HistoryError::Source)
    ));
}

#[cfg(feature = "lz4")]
#[test]
fn lz4_history_exports_identical_canonical_bodies() {
    assert_compressed_history(BodyEncoding::Lz4 {
        min_savings_bytes: 0,
    });
}

#[cfg(feature = "lz4")]
fn assert_compressed_history(encoding: BodyEncoding) {
    use ozzy_journal::operation::{CreatePartition, RetentionPolicy};
    use ozzy_proto::{OwnerEpoch, PartitionId, PartitionIncarnation};
    let (_temporary, mut journal) = empty();
    let name = "structured-topic-".repeat(14);
    let bytes: Vec<_> = (1..=8)
        .map(|id| {
            encode_operation_body(
                &OperationBody::CreatePartition(CreatePartition {
                    partition: PartitionIncarnation::from_bytes([id; 16]),
                    stream: &name,
                    topic: &name,
                    partition_id: PartitionId::new(u32::from(id)),
                    owner_epoch: OwnerEpoch::INITIAL,
                    retention: RetentionPolicy::default(),
                }),
                OperationLimits::default(),
            )
            .unwrap()
        })
        .collect();
    let mut previous = LogPosition::GENESIS;
    let operations: Vec<_> = bytes
        .iter()
        .map(|body| {
            let operation = CanonicalOperation {
                group_id: identity().group_id,
                configuration_epoch: 1,
                original_view: 0,
                op_number: previous.op_number + 1,
                previous_digest: previous.digest,
                kind: OperationKind::CreatePartition,
                body,
            };
            previous = position(operation);
            operation
        })
        .collect();
    let raw = encode_group(
        journal.writer().header(),
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &operations,
    )
    .unwrap();
    append(&mut journal, &operations, encoding);
    assert!(journal.writer().written_position().end_offset() < raw.end_offset());
    for end in [1, 4, 7] {
        let mut prefix = journal
            .freeze_history_through(position(operations[end - 1]), CAPACITY as usize)
            .unwrap();
        let chunk = prefix.read_after(LogPosition::GENESIS, 64, 8192).unwrap();
        assert_eq!(chunk.operations().collect::<Vec<_>>(), operations[..end]);
        assert_eq!(chunk.end(), position(operations[end - 1]));
    }
    let mut history = journal.freeze_history(CAPACITY as usize).unwrap();
    let mut predecessor = LogPosition::GENESIS;
    for operation in operations {
        let chunk = history
            .read_after(predecessor, 1, operation.body.len())
            .unwrap();
        assert_eq!(chunk.operations().next().unwrap(), operation);
        predecessor = chunk.end();
    }
    assert_eq!(predecessor, history.through());
}

use std::convert::Infallible;

use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, encode_operation_body,
    logical_operation_digest,
};
use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    BodyEncoding, DecodeLimits, Digest, GroupDirectory, GroupIdentity, LogPosition, MetadataLimits,
    OpenGroupJournal, OperationKind, SegmentHeader, SuffixReplacement,
    SuffixReplacementError as Error, SuffixStreamLimits,
};
use ozzy_proto::{GroupId, NodeId, OperationId, StoreId, VolumeId};
use tempfile::TempDir;

fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([1; 16]),
        replica_node_id: NodeId::from_bytes([2; 16]),
        volume_id: VolumeId::from_bytes([3; 16]),
        store_id: StoreId::from_bytes([4; 16]),
        store_generation: 1,
    }
}

fn bodies(count: u8) -> Vec<Vec<u8>> {
    (1..=count)
        .map(|number| {
            encode_operation_body(
                &OperationBody::Barrier(Barrier {
                    operation_id: OperationId::from_bytes([number; 16]),
                }),
                OperationLimits::default(),
            )
            .unwrap()
        })
        .collect()
}

fn operations(bytes: &[Vec<u8>], previous: LogPosition, view: u64) -> Vec<CanonicalOperation<'_>> {
    typed_operations(bytes, previous, view, OperationKind::Barrier)
}

fn typed_operations(
    bytes: &[Vec<u8>],
    mut previous: LogPosition,
    view: u64,
    kind: OperationKind,
) -> Vec<CanonicalOperation<'_>> {
    bytes
        .iter()
        .map(|body| {
            let operation = CanonicalOperation {
                group_id: identity().group_id,
                configuration_epoch: 1,
                original_view: view,
                op_number: previous.op_number + 1,
                previous_digest: previous.digest,
                kind,
                body,
            };
            previous = position(operation);
            operation
        })
        .collect()
}

fn partition_bodies(count: u8, name: &str, operation_limits: OperationLimits) -> Vec<Vec<u8>> {
    use ozzy_journal::operation::{CreatePartition, RetentionPolicy};
    use ozzy_proto::{OwnerEpoch, PartitionId, PartitionIncarnation};
    (1..=count)
        .map(|id| {
            encode_operation_body(
                &OperationBody::CreatePartition(CreatePartition {
                    partition: PartitionIncarnation::from_bytes([id; 16]),
                    stream: name,
                    topic: name,
                    partition_id: PartitionId::new(u32::from(id)),
                    owner_epoch: OwnerEpoch::new(1),
                    retention: RetentionPolicy::default(),
                }),
                operation_limits,
            )
            .unwrap()
        })
        .collect()
}

fn position(operation: CanonicalOperation<'_>) -> LogPosition {
    LogPosition {
        op_number: operation.op_number,
        digest: logical_operation_digest(&operation),
    }
}

fn empty() -> (TempDir, OpenGroupJournal) {
    let temp = tempfile::Builder::new()
        .prefix(".suffix-stream-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let header = SegmentHeader::new(identity().group_id, 1, None, Digest::ZERO, 12 * 1024).unwrap();
    let journal = GroupDirectory::format_new(temp.path().join("group"), identity(), 1, &header)
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    (temp, journal)
}

fn limits() -> SuffixStreamLimits {
    SuffixStreamLimits {
        max_group_operations: 2,
        max_group_body_bytes: 128,
        max_segments: 16,
        max_staged_bytes: 192 * 1024,
        max_source_segment_bytes: 12 * 1024,
        max_orphan_probes: 16,
    }
}

fn request(journal: &OpenGroupJournal, committed: LogPosition) -> SuffixReplacement {
    SuffixReplacement {
        expected_current: journal.directory().current(),
        protected_committed: journal.committed_position().unwrap(),
        promised_view: 2,
        last_normal_view: 2,
        committed,
        writer_generation: JournalGeneration(2),
        segment_capacity: 12 * 1024,
        body_encoding: BodyEncoding::Raw,
    }
}

fn reopen(temp: &TempDir, generation: u128) -> OpenGroupJournal {
    GroupDirectory::open(
        temp.path().join("group"),
        identity(),
        MetadataLimits::default(),
    )
    .unwrap()
    .recover(
        JournalGeneration(generation),
        DecodeLimits::default(),
        OperationLimits::default(),
    )
    .unwrap()
}

#[test]
fn staging_rejects_chunk_limits_incompatible_with_reopen_before_creating_files() {
    let (temp, journal) = empty();
    drop(journal);
    let decode = DecodeLimits {
        max_entries: 2,
        max_groups: 1,
        max_decoded_body_bytes: 16,
        max_group_decoded_body_bytes: 32,
        max_segment_decoded_body_bytes: 32,
        ..DecodeLimits::default()
    };
    let open = |generation| {
        GroupDirectory::open(
            temp.path().join("group"),
            identity(),
            MetadataLimits::default(),
        )
        .unwrap()
        .recover(
            JournalGeneration(generation),
            decode,
            OperationLimits::default(),
        )
        .unwrap()
    };
    let journal = open(1);
    let bytes = bodies(8);
    let operations = operations(&bytes, LogPosition::GENESIS, 1);
    let target = position(operations[7]);
    let replacement = request(&journal, LogPosition::GENESIS);
    assert!(matches!(
        journal.begin_suffix_replacement(
            replacement,
            target,
            SuffixStreamLimits {
                max_group_operations: 8,
                ..limits()
            }
        ),
        Err(Error::InvalidLimits)
    ));
    assert!(!temp.path().join("group/segments/2.log").exists());
    assert_eq!(open(3).directory().current(), replacement.expected_current);
}

#[test]
fn interrupted_staging_keeps_old_selection_and_retry_skips_unselected_files() {
    let (temp, journal) = empty();
    let bytes = bodies(8);
    let operations = operations(&bytes, LogPosition::GENESIS, 1);
    let target = position(operations[7]);
    let replacement = request(&journal, LogPosition::GENESIS);
    let mut staging = journal
        .begin_suffix_replacement(replacement, target, limits())
        .unwrap();
    for chunk in operations[..6].chunks(2) {
        staging.append_chunk(chunk).unwrap();
    }
    drop(staging);
    let journal = reopen(&temp, 3);
    assert_eq!(journal.directory().current(), replacement.expected_current);
    assert_eq!(journal.accepted_position().unwrap(), LogPosition::GENESIS);
    assert_eq!(journal.directory().manifest().last_normal_view, 0);
    assert!(temp.path().join("group/segments/2.log").exists());
    assert!(temp.path().join("group/segments/3.log").exists());
    let replacement = request(&journal, LogPosition::GENESIS);
    let mut staging = journal
        .begin_suffix_replacement(replacement, target, limits())
        .unwrap();
    for chunk in operations.chunks(2) {
        staging.append_chunk(chunk).unwrap();
    }
    let journal = staging.finish().unwrap();
    assert_eq!(journal.directory().manifest().segments[0].segment_id, 4);
    assert_eq!(journal.accepted_position().unwrap(), target);
    assert_eq!(
        journal
            .reclaim_unreferenced_segments()
            .unwrap()
            .removed_segment_ids,
        [1, 2, 3]
    );
}

#[test]
fn incomplete_or_quota_failed_staging_cannot_publish_or_resume_in_place() {
    for fail_quota in [false, true] {
        let (temp, journal) = empty();
        let bytes = bodies(6);
        let operations = operations(&bytes, LogPosition::GENESIS, 1);
        let replacement = request(&journal, LogPosition::GENESIS);
        let bounds = SuffixStreamLimits {
            max_staged_bytes: 12 * 1024,
            ..limits()
        };
        let mut staging = journal
            .begin_suffix_replacement(replacement, position(operations[5]), bounds)
            .unwrap();
        staging.append_chunk(&operations[..2]).unwrap();
        staging.append_chunk(&operations[2..4]).unwrap();
        if fail_quota {
            assert!(matches!(
                staging.append_chunk(&operations[4..]),
                Err(Error::StagingQuota { .. })
            ));
            assert!(matches!(
                staging.append_chunk(&operations[4..]),
                Err(Error::StagingFaulted)
            ));
            assert!(matches!(staging.finish(), Err(Error::StagingFaulted)));
        } else {
            assert!(matches!(staging.finish(), Err(Error::IncompleteSuffix)));
        }
        let journal = reopen(&temp, 3);
        assert_eq!(journal.directory().current(), replacement.expected_current);
        assert_eq!(journal.accepted_position().unwrap(), LogPosition::GENESIS);
        assert!(!temp.path().join("group/segments/3.log").exists());
    }
}

#[test]
fn repeated_abort_preserves_pinned_history_and_removes_only_its_staging_files() {
    let (temp, mut journal) = empty();
    let bytes = bodies(10);
    let old = operations(&bytes[..2], LogPosition::GENESIS, 0);
    let written = journal.append(&old).unwrap();
    journal.sync_through(written).unwrap();
    let accepted = position(old[1]);
    let mut manifest = journal.directory().manifest().clone();
    manifest.parent_generation = manifest.generation;
    manifest.generation += 1;
    manifest.accepted = accepted;
    manifest.committed = accepted;
    manifest.promised_view = 1;
    journal = journal.install_metadata(manifest).unwrap();
    let original = std::fs::read(temp.path().join("group/segments/1.log")).unwrap();
    let pin = journal.pin_segments(&[1]).unwrap();
    let suffix = operations(&bytes[2..], accepted, 1);
    // An unrelated orphan must survive. Exclusive creation skips this name.
    std::fs::write(temp.path().join("group/segments/2.log"), b"existing orphan").unwrap();
    for generation in 10..14 {
        let mut replacement = request(&journal, accepted);
        replacement.writer_generation = JournalGeneration(generation);
        let old_current = replacement.expected_current;
        let mut staging = journal
            .begin_suffix_replacement(replacement, position(suffix[7]), limits())
            .unwrap();
        for chunk in suffix[..6].chunks(2) {
            staging.append_chunk(chunk).unwrap();
        }
        assert!(temp.path().join("group/segments/4.log").exists());
        journal = staging.abort().unwrap();
        assert_eq!(journal.directory().current(), old_current);
        assert_eq!(journal.directory().manifest().promised_view, 1);
        assert_eq!(journal.directory().manifest().last_normal_view, 0);
        assert_eq!(journal.accepted_position().unwrap(), accepted);
        assert_eq!(journal.committed_position().unwrap(), accepted);
        assert_eq!(
            std::fs::read(temp.path().join("group/segments/1.log")).unwrap(),
            original
        );
        assert_eq!(
            std::fs::read(temp.path().join("group/segments/2.log")).unwrap(),
            b"existing orphan"
        );
        assert_eq!(
            std::fs::read_dir(temp.path().join("group/segments"))
                .unwrap()
                .count(),
            2
        );
    }
    drop(pin);
    drop(journal);
    assert_eq!(reopen(&temp, 30).committed_position().unwrap(), accepted);
}

#[test]
fn faulted_staging_cannot_return_old_writer_through_abort() {
    let (temp, journal) = empty();
    let bytes = bodies(6);
    let operations = operations(&bytes, LogPosition::GENESIS, 1);
    let replacement = request(&journal, LogPosition::GENESIS);
    let mut staging = journal
        .begin_suffix_replacement(
            replacement,
            position(operations[5]),
            SuffixStreamLimits {
                max_staged_bytes: 12 * 1024,
                ..limits()
            },
        )
        .unwrap();
    staging.append_chunk(&operations[..2]).unwrap();
    staging.append_chunk(&operations[2..4]).unwrap();
    assert!(staging.append_chunk(&operations[4..]).is_err());
    assert!(matches!(staging.abort(), Err(Error::StagingFaulted)));
    assert_eq!(
        reopen(&temp, 3).directory().current(),
        replacement.expected_current
    );
}

#[test]
fn invalid_chunks_leave_prefix_unchanged_and_valid_retry_completes() {
    let (temp, journal) = empty();
    let bytes = bodies(5);
    let operations = operations(&bytes, LogPosition::GENESIS, 1);
    let target = position(operations[3]);
    let replacement = request(&journal, position(operations[2]));
    let mut staging = journal
        .begin_suffix_replacement(replacement, target, limits())
        .unwrap();
    assert!(matches!(
        staging.append_chunk(&operations[..3]),
        Err(Error::LimitExceeded { .. })
    ));
    assert!(matches!(
        staging.append_chunk(&[operations[0], operations[2]]),
        Err(Error::SelectedSuffixMismatch)
    ));
    let wrong_body = CanonicalOperation {
        body: b"",
        ..operations[0]
    };
    assert!(matches!(
        staging.append_chunk(&[wrong_body]),
        Err(Error::Operation(_))
    ));
    staging.append_chunk(&operations[..2]).unwrap();
    let wrong_tail = CanonicalOperation {
        body: operations[4].body,
        ..operations[3]
    };
    assert!(matches!(
        staging.append_chunk(&[operations[2], wrong_tail]),
        Err(Error::SelectedSuffixMismatch)
    ));
    staging.append_chunk(&operations[2..4]).unwrap();
    assert!(matches!(
        staging.append_chunk(&operations[4..]),
        Err(Error::SelectedSuffixMismatch)
    ));
    let journal = staging.finish().unwrap();
    assert_eq!(journal.accepted_position().unwrap(), target);
    assert_eq!(
        journal.committed_position().unwrap(),
        position(operations[2])
    );
    drop(journal);
    assert_eq!(reopen(&temp, 3).accepted_position().unwrap(), target);
}

#[test]
fn replacement_preserves_earlier_sealed_segments_and_supports_empty_selected_suffix() {
    for inside_group in [false, true] {
        let (temp, mut journal) = empty();
        let bytes = bodies(4);
        let operations = operations(&bytes, LogPosition::GENESIS, 0);
        let written = journal.append(&operations[..2]).unwrap();
        journal.sync_through(written).unwrap();
        let mut journal = journal.roll_active(12 * 1024).unwrap();
        let earlier = journal.directory().manifest().segments[0];
        let original_bytes = std::fs::read(temp.path().join("group/segments/1.log")).unwrap();
        let written = journal.append(&operations[2..]).unwrap();
        journal.sync_through(written).unwrap();
        let protected = position(operations[if inside_group { 2 } else { 1 }]);
        let mut manifest = journal.directory().manifest().clone();
        manifest.parent_generation = manifest.generation;
        manifest.generation += 1;
        manifest.accepted = position(operations[3]);
        manifest.committed = protected;
        let journal = journal.install_metadata(manifest).unwrap();
        let replacement = request(&journal, protected);
        let journal = journal
            .begin_suffix_replacement(replacement, protected, limits())
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(journal.directory().manifest().segments[0], earlier);
        assert_eq!(
            std::fs::read(temp.path().join("group/segments/1.log")).unwrap(),
            original_bytes
        );
        assert_eq!(journal.accepted_position().unwrap(), protected);
        assert_eq!(journal.committed_position().unwrap(), protected);
        drop(journal);
        assert_eq!(reopen(&temp, 3).accepted_position().unwrap(), protected);
    }
}

#[test]
fn segment_count_and_orphan_probe_limits_stop_staging_without_publication() {
    let (temp, journal) = empty();
    let bytes = bodies(6);
    let operations = operations(&bytes, LogPosition::GENESIS, 1);
    let target = position(operations[5]);
    let replacement = request(&journal, LogPosition::GENESIS);
    let bounds = SuffixStreamLimits {
        max_segments: 1,
        ..limits()
    };
    let mut staging = journal
        .begin_suffix_replacement(replacement, target, bounds)
        .unwrap();
    staging.append_chunk(&operations[..2]).unwrap();
    staging.append_chunk(&operations[2..4]).unwrap();
    assert!(matches!(
        staging.append_chunk(&operations[4..]),
        Err(Error::LimitExceeded {
            kind: "selected manifest segments",
            ..
        })
    ));
    drop(staging);
    let journal = reopen(&temp, 3);
    assert_eq!(journal.directory().current(), replacement.expected_current);
    let replacement = request(&journal, LogPosition::GENESIS);
    let bounds = SuffixStreamLimits {
        max_orphan_probes: 1,
        ..limits()
    };
    assert!(matches!(
        journal.begin_suffix_replacement(replacement, target, bounds),
        Err(Error::ReplacementSegmentConflict)
    ));
    assert!(!temp.path().join("group/segments/3.log").exists());
    assert_eq!(
        reopen(&temp, 4).accepted_position().unwrap(),
        LogPosition::GENESIS
    );
}

#[test]
fn outstanding_buffered_roll_blocks_both_suffix_installation_paths() {
    for streamed in [false, true] {
        let (temp, mut journal) = empty();
        let bytes = bodies(1);
        let operations = operations(&bytes, LogPosition::GENESIS, 0);
        let written = journal.append(&operations).unwrap();
        journal.sync_through(written).unwrap();
        let replacement = request(&journal, LogPosition::GENESIS);
        let (journal, publication) = journal.begin_buffered_roll(12 * 1024).unwrap();
        if streamed {
            assert!(matches!(
                journal.begin_suffix_replacement(replacement, position(operations[0]), limits()),
                Err(Error::SourceNotDurable)
            ));
        } else {
            assert!(matches!(
                journal.replace_suffix(
                    replacement,
                    &operations,
                    ozzy_journal_segment::SuffixReplacementLimits::default()
                ),
                Err(Error::SourceNotDurable)
            ));
        }
        drop(publication);
        let journal = reopen(&temp, 3);
        assert_eq!(
            journal.accepted_position().unwrap(),
            position(operations[0])
        );
        assert_eq!(journal.directory().manifest().last_normal_view, 0);
    }
}

#[test]
fn transfer_chunk_can_split_between_operations_across_small_physical_segments() {
    let (temp, journal) = empty();
    drop(journal);
    let operation_limits = OperationLimits {
        max_name_bytes: 1024,
        ..OperationLimits::default()
    };
    let journal = GroupDirectory::open(
        temp.path().join("group"),
        identity(),
        MetadataLimits::default(),
    )
    .unwrap()
    .recover(
        JournalGeneration(10),
        DecodeLimits::default(),
        operation_limits,
    )
    .unwrap();
    let bytes = partition_bodies(2, &"orders".repeat(160), operation_limits);
    let operations = typed_operations(
        &bytes,
        LogPosition::GENESIS,
        1,
        OperationKind::CreatePartition,
    );
    let accepted = position(operations[1]);
    let mut replacement = request(&journal, accepted);
    replacement.segment_capacity = 8192;
    let mut staging = journal
        .begin_suffix_replacement(
            replacement,
            accepted,
            SuffixStreamLimits {
                max_group_body_bytes: 8192,
                ..limits()
            },
        )
        .unwrap();
    staging.append_chunk(&operations).unwrap();
    let journal = staging.finish().unwrap();
    assert_eq!(journal.directory().manifest().segments.len(), 2);
    drop(journal);
    let journal = GroupDirectory::open(
        temp.path().join("group"),
        identity(),
        MetadataLimits::default(),
    )
    .unwrap()
    .recover(
        JournalGeneration(11),
        DecodeLimits::default(),
        operation_limits,
    )
    .unwrap();
    assert_eq!(journal.accepted_position().unwrap(), accepted);
    assert_eq!(journal.committed_position().unwrap(), accepted);
}

#[test]
fn oversized_canonical_operation_is_never_split_or_published() {
    let (temp, journal) = empty();
    drop(journal);
    let operation_limits = OperationLimits {
        max_name_bytes: 2048,
        ..OperationLimits::default()
    };
    let journal = GroupDirectory::open(
        temp.path().join("group"),
        identity(),
        MetadataLimits::default(),
    )
    .unwrap()
    .recover(
        JournalGeneration(10),
        DecodeLimits::default(),
        operation_limits,
    )
    .unwrap();
    let bytes = partition_bodies(1, &"orders".repeat(320), operation_limits);
    let operations = typed_operations(
        &bytes,
        LogPosition::GENESIS,
        1,
        OperationKind::CreatePartition,
    );
    let accepted = position(operations[0]);
    let mut replacement = request(&journal, accepted);
    replacement.segment_capacity = 8192;
    let mut staging = journal
        .begin_suffix_replacement(
            replacement,
            accepted,
            SuffixStreamLimits {
                max_group_body_bytes: 8192,
                ..limits()
            },
        )
        .unwrap();
    assert!(matches!(
        staging.append_chunk(&operations),
        Err(Error::Writer(ozzy_journal_segment::WriterError::Codec(
            ozzy_journal_segment::CodecError::GroupExceedsSegment
        )))
    ));
    assert!(matches!(staging.finish(), Err(Error::StagingFaulted)));
    let journal = reopen(&temp, 11);
    assert_eq!(journal.directory().current(), replacement.expected_current);
    assert_eq!(journal.accepted_position().unwrap(), LogPosition::GENESIS);
}

#[cfg(feature = "lz4")]
#[test]
fn compressed_staging_rolls_at_decoded_limit_and_reopens_with_identical_bounds() {
    let (temp, journal) = empty();
    drop(journal);
    let decode = DecodeLimits {
        max_decoded_body_bytes: 8192,
        max_group_decoded_body_bytes: 8192,
        max_segment_decoded_body_bytes: 8192,
        ..DecodeLimits::default()
    };
    let operation_limits = OperationLimits {
        max_name_bytes: 4096,
        ..OperationLimits::default()
    };
    let journal = GroupDirectory::open(
        temp.path().join("group"),
        identity(),
        MetadataLimits::default(),
    )
    .unwrap()
    .recover(JournalGeneration(10), decode, operation_limits)
    .unwrap();
    let name = "orders".repeat(500);
    let bytes = partition_bodies(6, &name, operation_limits);
    let selected = typed_operations(
        &bytes,
        LogPosition::GENESIS,
        1,
        OperationKind::CreatePartition,
    );
    let previous = position(*selected.last().unwrap());
    let mut replacement = request(&journal, previous);
    replacement.body_encoding = BodyEncoding::Lz4 {
        min_savings_bytes: 0,
    };
    let mut staging = journal
        .begin_suffix_replacement(
            replacement,
            previous,
            SuffixStreamLimits {
                max_group_body_bytes: 8192,
                ..limits()
            },
        )
        .unwrap();
    for operation in &selected {
        staging
            .append_chunk(std::slice::from_ref(operation))
            .unwrap();
    }
    let journal = staging.finish().unwrap();
    // Each decoded body exceeds half the per-segment bound. Compressed bytes
    // fit physically, but writing two bodies would make recovery reject them.
    assert_eq!(
        journal.directory().manifest().segments.len(),
        selected.len()
    );
    for segment in &journal.directory().manifest().segments {
        let data = std::fs::read(
            temp.path()
                .join(format!("group/segments/{}.log", segment.segment_id)),
        )
        .unwrap();
        let scan = ozzy_journal_segment::scan_segment(
            &data,
            segment.first_group_number,
            segment.first_chain,
            decode,
        )
        .unwrap();
        assert!(matches!(
            scan.groups[0].operations[0].body,
            std::borrow::Cow::Owned(_)
        ));
    }
    drop(journal);
    let journal = GroupDirectory::open(
        temp.path().join("group"),
        identity(),
        MetadataLimits::default(),
    )
    .unwrap()
    .recover(JournalGeneration(11), decode, operation_limits)
    .unwrap();
    assert_eq!(journal.accepted_position().unwrap(), previous);
    let recovered = journal
        .recover_canonical_images(ozzy_journal_segment::CanonicalRecoveryLimits::default())
        .unwrap();
    assert_eq!(recovered.committed().revision(), 6);
}

#[test]
fn streamed_suffix_spans_segments_and_preserves_exact_committed_fragment_on_reopen() {
    let (temp, mut journal) = empty();
    let bytes = bodies(16);
    let old = operations(&bytes[..3], LogPosition::GENESIS, 0);
    let written = journal.append(&old).unwrap();
    journal.sync_through(written).unwrap();
    let protected = position(old[0]);
    let mut manifest = journal.directory().manifest().clone();
    manifest.parent_generation = manifest.generation;
    manifest.generation += 1;
    manifest.accepted = position(old[2]);
    manifest.committed = protected;
    let journal = journal.install_metadata(manifest).unwrap();
    let old_current = journal.directory().current();
    let pin = journal.pin_segments(&[1]).unwrap();
    let selected = operations(&bytes[3..15], protected, 1);
    let accepted = position(selected[11]);
    let committed = position(selected[6]);
    let replacement = request(&journal, committed);
    let mut staging = journal
        .begin_suffix_replacement(replacement, accepted, limits())
        .unwrap();
    for chunk in selected.chunks(2) {
        staging.append_chunk(chunk).unwrap();
        let bytes = std::fs::read(temp.path().join("group/CURRENT")).unwrap();
        assert_eq!(
            ozzy_journal_segment::decode_current(&bytes).unwrap(),
            old_current
        );
    }
    let journal = staging.finish().unwrap();
    assert!(journal.directory().manifest().segments.len() >= 4);
    assert_eq!(journal.accepted_position().unwrap(), accepted);
    assert_eq!(journal.committed_position().unwrap(), committed);
    assert_eq!(journal.directory().manifest().last_normal_view, 2);
    assert!(
        journal
            .reclaim_unreferenced_segments()
            .unwrap()
            .pinned_segment_ids
            .contains(&1)
    );
    drop(pin);
    drop(journal);
    let mut journal = GroupDirectory::open(
        temp.path().join("group"),
        identity(),
        MetadataLimits::default(),
    )
    .unwrap()
    .recover(
        JournalGeneration(3),
        DecodeLimits::default(),
        OperationLimits::default(),
    )
    .unwrap();
    let expected: Vec<_> = std::iter::once(old[0])
        .chain(selected.iter().copied())
        .map(position)
        .collect();
    let mut replayed = Vec::new();
    journal
        .replay_accepted(|item| {
            replayed.push(LogPosition {
                op_number: item.operation.op_number,
                digest: item.operation.digest,
            });
            assert_eq!(
                item.committed,
                item.operation.op_number <= committed.op_number
            );
            Ok::<_, Infallible>(())
        })
        .unwrap();
    assert_eq!(replayed, expected);
    let fresh = operations(&bytes[15..], accepted, 2);
    let written = journal.append(&fresh).unwrap();
    journal.sync_through(written).unwrap();
    assert_eq!(journal.accepted_position().unwrap(), position(fresh[0]));
}

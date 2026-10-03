use std::fs;

use ozzy_journal::operation::{
    Barrier, CanonicalOperation, ChainPosition, OperationBody, OperationLimits,
    encode_operation_body, logical_operation_digest,
};
use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    BodyEncoding, CanonicalRecoveryLimits, DecodeLimits, Digest, GroupDirectory, GroupIdentity,
    IndexBuildLimits, IndexLimits, JournalIndexBoundary, LogPosition, MetadataLimits,
    OperationKind, SegmentHeader, SuffixReplacement, SuffixReplacementLimits, encode_group,
    encode_segment_header,
};
use ozzy_proto::{GroupId, NodeId, OperationId, StoreId, VolumeId};
use tempfile::TempDir;

fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([0x11; 16]),
        replica_node_id: NodeId::from_bytes([0x12; 16]),
        volume_id: VolumeId::from_bytes([0x13; 16]),
        store_id: StoreId::from_bytes([0x14; 16]),
        store_generation: 1,
    }
}

fn barrier_body(byte: u8) -> Vec<u8> {
    encode_operation_body(
        &OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes([byte; 16]),
        }),
        OperationLimits::default(),
    )
    .unwrap()
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "linear suffix installation lifecycle"
)]
fn replacement_preserves_commit_inside_group_and_discards_old_suffix() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("group");
    let identity = identity();
    let capacity = 16 * 1024;
    let header = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, capacity).unwrap();
    let directory = GroupDirectory::format_new(&root, identity, 1, &header).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let first_body = barrier_body(0x20);
    let first = CanonicalOperation {
        group_id: identity.group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Barrier,
        body: &first_body,
    };
    let first_digest = logical_operation_digest(&first);
    let discarded_body = barrier_body(0x21);
    let discarded = CanonicalOperation {
        op_number: 2,
        previous_digest: first_digest,
        body: &discarded_body,
        ..first
    };
    let discarded_digest = logical_operation_digest(&discarded);
    let written = journal.append(&[first, discarded]).unwrap();
    journal.sync_through(written).unwrap();
    let mut manifest = journal.directory().manifest().clone();
    manifest.generation += 1;
    manifest.parent_generation = journal.directory().manifest().generation;
    manifest.accepted = LogPosition {
        op_number: 2,
        digest: discarded_digest,
    };
    manifest.committed = LogPosition {
        op_number: 1,
        digest: first_digest,
    };
    let journal = journal.install_metadata(manifest).unwrap();
    let source_snapshot = journal
        .build_index_snapshot(JournalIndexBoundary::Accepted, IndexBuildLimits::default())
        .unwrap();

    let selected_body = barrier_body(0x30);
    let selected = CanonicalOperation {
        original_view: 2,
        op_number: 2,
        previous_digest: first_digest,
        body: &selected_body,
        ..first
    };
    let selected_digest = logical_operation_digest(&selected);
    let tail_body = barrier_body(0x31);
    let tail = CanonicalOperation {
        original_view: 2,
        op_number: 3,
        previous_digest: selected_digest,
        body: &tail_body,
        ..first
    };
    let tail_digest = logical_operation_digest(&tail);

    // A different abandoned view occupies 2; the matching interrupted retry is 3.
    fs::write(root.join("segments/2.log"), b"conflicting abandoned suffix").unwrap();
    let replacement_header =
        SegmentHeader::new(identity.group_id, 3, None, Digest::ZERO, capacity).unwrap();
    let group = encode_group(
        &replacement_header,
        1,
        ozzy_journal_segment::SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &[first, selected, tail],
    )
    .unwrap();
    let mut expected = encode_segment_header(&replacement_header).to_vec();
    expected.extend_from_slice(group.as_bytes());
    fs::write(root.join("segments/3.log"), &expected[..5_000]).unwrap();

    let current = journal.directory().current();
    let journal = journal
        .replace_suffix(
            SuffixReplacement {
                expected_current: current,
                protected_committed: LogPosition {
                    op_number: 1,
                    digest: first_digest,
                },
                promised_view: 2,
                last_normal_view: 2,
                committed: LogPosition {
                    op_number: 2,
                    digest: selected_digest,
                },
                writer_generation: JournalGeneration(2),
                segment_capacity: capacity,
                body_encoding: BodyEncoding::Raw,
            },
            &[selected, tail],
            SuffixReplacementLimits::default(),
        )
        .unwrap();
    assert_eq!(journal.directory().manifest().segments.len(), 1);
    assert_eq!(journal.directory().manifest().segments[0].segment_id, 3);
    assert_eq!(journal.accepted_position().unwrap().op_number, 3);
    assert_eq!(journal.committed_position().unwrap().op_number, 2);
    assert_eq!(journal.directory().manifest().promised_view, 2);
    assert_eq!(journal.directory().manifest().last_normal_view, 2);
    assert!(root.join("segments/1.log").is_file());

    let cleanup = journal.reclaim_unreferenced_segments().unwrap();
    assert_eq!(cleanup.removed_segment_ids, [2]);
    assert_eq!(cleanup.pinned_segment_ids, [1]);
    assert!(cleanup.reclaimed_bytes > 0);
    assert!(root.join("segments/1.log").is_file());
    assert!(!root.join("segments/2.log").exists());
    drop(source_snapshot);
    let cleanup = journal.reclaim_unreferenced_segments().unwrap();
    assert_eq!(cleanup.removed_segment_ids, [1]);
    assert!(cleanup.pinned_segment_ids.is_empty());
    assert!(cleanup.reclaimed_bytes > 0);
    assert!(!root.join("segments/1.log").exists());

    let index = journal
        .build_index_snapshot(JournalIndexBoundary::Accepted, IndexBuildLimits::default())
        .unwrap();
    assert!(
        index
            .find_operation(OperationId::from_bytes([0x21; 16]))
            .unwrap()
            .is_none()
    );
    assert!(
        index
            .find_operation(OperationId::from_bytes([0x30; 16]))
            .unwrap()
            .is_some()
    );
    drop(index);

    let recovered = journal
        .recover_canonical_images(CanonicalRecoveryLimits {
            retained_identities: 4,
            accepted_transitions: 4,
            ..CanonicalRecoveryLimits::default()
        })
        .unwrap();
    assert_eq!(recovered.committed().revision(), 2);
    assert_eq!(recovered.speculative().revision(), 3);
    drop(recovered);
    drop(journal);

    let journal = GroupDirectory::open(&root, identity, MetadataLimits::default())
        .unwrap()
        .recover(
            JournalGeneration(3),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(journal.accepted_position().unwrap().digest, tail_digest);
    assert_eq!(
        journal
            .open_index_snapshot(JournalIndexBoundary::Accepted, IndexLimits::default())
            .unwrap()
            .through()
            .op_number,
        3
    );
}

use std::fs::{self, OpenOptions};
use std::io;

use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, encode_operation_body,
};
use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    CanonicalOperation, ChainPosition, CheckpointLimits, CommitMode, CurrentReference,
    DecodeLimits, Digest, DirectoryError, GroupDirectory, GroupIdentity, IndexBuildLimits,
    LogPosition, Manifest, MetadataLimits, OperationKind, OperationLimits, ReplayError,
    RetentionFloors, SEGMENT_HEADER_BYTES, SegmentHeader, SegmentWriter, WriterError,
    encode_current, encode_manifest, manifest_digest, scan_segment,
};
use ozzy_proto::{
    CheckpointId, GroupId, MessageId, NodeId, Offset, OperationId, OwnerEpoch,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, StoreId, VolumeId,
};
use tempfile::TempDir;

#[path = "directory/maintenance.rs"]
mod maintenance;

fn identity(byte: u8) -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([byte; 16]),
        replica_node_id: NodeId::from_bytes([byte + 1; 16]),
        volume_id: VolumeId::from_bytes([byte + 2; 16]),
        store_id: StoreId::from_bytes([byte + 3; 16]),
        store_generation: 1,
    }
}

fn first_segment(identity: GroupIdentity) -> SegmentHeader {
    SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap()
}

fn next_manifest(directory: &GroupDirectory) -> Manifest {
    let mut next = directory.manifest().clone();
    next.generation += 1;
    next.parent_generation = directory.manifest().generation;
    next.promised_view += 1;
    next
}

fn operation(
    identity: GroupIdentity,
    number: u64,
    previous_digest: Digest,
) -> CanonicalOperation<'static> {
    CanonicalOperation {
        group_id: identity.group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: number,
        previous_digest,
        kind: OperationKind::Barrier,
        body: &[0xaa; 16],
    }
}

fn corrupt_first_body_byte(root: &std::path::Path) {
    let path = root.join("segments/1.log");
    let mut bytes = fs::read(&path).unwrap();
    bytes[SEGMENT_HEADER_BYTES + 192] ^= 1;
    fs::write(path, bytes).unwrap();
}

#[test]
fn arbitrary_segment_pins_preserve_request_order_with_sparse_ids() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x39);
    let mut journal = GroupDirectory::format_new(&root, expected, 1, &first_segment(expected))
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    for id in [2, 3, 4, 6, 7] {
        fs::write(root.join(format!("segments/{id}.log")), b"occupied orphan").unwrap();
    }
    let mut previous = Digest::ZERO;
    for number in 1..=2 {
        let written = journal
            .append(&[operation(expected, number, previous)])
            .unwrap();
        journal.sync_through(written).unwrap();
        previous = written.next_chain().previous_digest();
        journal = journal.roll_active(8 * 1024).unwrap();
    }
    let references = &journal.directory().manifest().segments;
    assert_eq!(
        references.iter().map(|r| r.segment_id).collect::<Vec<_>>(),
        [1, 5, 8]
    );
    let pin = journal.pin_segments(&[8, 1, 5]).unwrap();
    assert_eq!(
        pin.references(),
        &[references[2], references[0], references[1]]
    );
    assert!(pin.segment_path(2).is_none());
    for ids in [&[0][..], &[1, 999, 1], &[999, 999]] {
        assert!(matches!(
            journal.pin_segments(ids),
            Err(DirectoryError::SegmentMismatch(_))
        ));
    }
    assert!(matches!(
        journal.pin_segments(&[1, 5, 1]),
        Err(DirectoryError::Retention(
            ozzy_journal_segment::RetentionError::DuplicateSegment
        ))
    ));
    assert!(matches!(
        journal.pin_segments(&[]),
        Err(DirectoryError::Retention(
            ozzy_journal_segment::RetentionError::EmptyPin
        ))
    ));
    drop(journal);
    assert!(GroupDirectory::open(&root, expected, MetadataLimits::default()).is_err());
    drop(pin);
    GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
}

fn append_unchecked(root: &std::path::Path, operation: CanonicalOperation<'_>) {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(root.join("segments/1.log"))
        .unwrap();
    let mut writer = SegmentWriter::recover(
        file,
        JournalGeneration(99),
        1,
        ChainPosition::GENESIS,
        DecodeLimits::default(),
        8 * 1024,
    )
    .unwrap();
    let written = writer.append(&[operation]).unwrap();
    writer.sync_through(written).unwrap();
}

fn append_body(partition: PartitionIncarnation, first_offset: u64, message_byte: u8) -> Vec<u8> {
    encode_operation_body(
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition,
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([0x62; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(first_offset),
                first_offset: Offset::new(first_offset),
                append_timestamp_millis: 123,
                records: vec![
                    AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes([message_byte; 16]),
                        parts: vec![b"one".as_slice()].into(),
                    },
                    AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes([message_byte + 1; 16]),
                        parts: vec![b"two".as_slice()].into(),
                    },
                ]
                .into(),
            }],
        }),
        OperationLimits::default(),
    )
    .unwrap()
}

#[test]
fn format_is_explicit_and_open_never_creates() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x10);
    let segment = first_segment(expected);

    let directory = GroupDirectory::format_new(&root, expected, 7, &segment).unwrap();
    assert_eq!(directory.root(), root);
    assert_eq!(directory.identity(), expected);
    assert_eq!(directory.current().generation, 1);
    assert_eq!(directory.manifest().segments.len(), 1);
    assert_eq!(
        fs::metadata(root.join("segments/1.log")).unwrap().len(),
        segment.capacity()
    );
    for path in [
        "group.lock",
        "identity",
        "MANIFEST.1",
        "CURRENT",
        "segments/1.log",
    ] {
        assert!(root.join(path).is_file(), "missing {path}");
    }
    for path in ["segments", "checkpoints", "indexes", "staging"] {
        assert!(root.join(path).is_dir(), "missing {path}");
    }

    assert!(matches!(
        GroupDirectory::open(&root, expected, MetadataLimits::default()),
        Err(DirectoryError::Locked)
    ));
    drop(directory);

    let reopened = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert_eq!(reopened.current().generation, 1);
    drop(reopened);
    assert!(matches!(
        GroupDirectory::format_new(&root, expected, 7, &segment),
        Err(DirectoryError::StoreAlreadyExists)
    ));

    let absent = volume.path().join("absent");
    assert!(GroupDirectory::open(&absent, expected, MetadataLimits::default()).is_err());
    assert!(!absent.exists());
}

#[test]
fn recovery_removes_only_recognized_abandoned_staging_workspaces() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x18);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let staging = root.join("staging");
    let checkpoint = "31".repeat(16);
    let index_workspace = staging.join("index-1-123-0");
    let checkpoint_workspace = staging.join(format!(".checkpoint-{checkpoint}-123-0.tmp"));
    let operator_directory = staging.join("index-01-123-0");
    fs::create_dir(&index_workspace).unwrap();
    fs::write(index_workspace.join("partial.run"), b"partial").unwrap();
    fs::create_dir(&checkpoint_workspace).unwrap();
    fs::write(checkpoint_workspace.join("partial"), b"partial").unwrap();
    fs::create_dir(&operator_directory).unwrap();

    let journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert!(!index_workspace.exists());
    assert!(!checkpoint_workspace.exists());
    assert!(operator_directory.is_dir());
    drop(journal);
}

#[test]
fn open_requires_configured_identity_and_all_referenced_files() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x20);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    drop(directory);

    assert!(matches!(
        GroupDirectory::open(&root, identity(0x30), MetadataLimits::default()),
        Err(DirectoryError::IdentityMismatch)
    ));
    fs::remove_file(root.join("segments/1.log")).unwrap();
    assert!(GroupDirectory::open(&root, expected, MetadataLimits::default()).is_err());
}

#[test]
fn manifest_install_publishes_current_last() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x30);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let next = next_manifest(journal.directory());
    let journal = journal.install_metadata(next.clone()).unwrap();
    assert_eq!(journal.directory().current().generation, 2);
    assert_eq!(journal.directory().manifest(), &next);
    assert!(root.join("MANIFEST.1").is_file());
    assert!(root.join("MANIFEST.2").is_file());
    drop(journal);

    let reopened = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert_eq!(reopened.manifest(), &next);
}

#[test]
fn local_commit_mode_needs_one_data_sync_and_recovers_complete_groups() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x37);
    let segment = SegmentHeader::new(expected.group_id, 1, None, Digest::ZERO, 16 * 1024).unwrap();
    let directory = GroupDirectory::format_new_with_commit_mode(
        &root,
        expected,
        1,
        CommitMode::LocalDurable,
        &segment,
    )
    .unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let first = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(first).unwrap();
    assert_eq!(journal.committed_position().unwrap().op_number, 1);
    assert_eq!(
        journal.directory().manifest().committed,
        LogPosition::GENESIS
    );
    assert!(matches!(
        journal.checkpoint_plan(
            CheckpointId::from_bytes([0x73; 16]),
            Digest::from_bytes([0x74; 32]),
            1024,
        ),
        Err(DirectoryError::LocalProgressUnpublished)
    ));
    drop(journal);

    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(journal.directory().current().generation, 2);
    assert_eq!(journal.directory().manifest().committed.op_number, 1);
    let mut replayed = Vec::new();
    journal
        .replay_accepted(|operation| {
            replayed.push((operation.operation.op_number, operation.committed));
            Ok::<_, io::Error>(())
        })
        .unwrap();
    assert_eq!(replayed, [(1, true)]);

    let second = journal
        .append(&[operation(expected, 2, first.next_chain().previous_digest())])
        .unwrap();
    journal.sync_through(second).unwrap();
    assert_eq!(journal.directory().current().generation, 2);
    let journal = journal.publish_progress().unwrap();
    assert_eq!(journal.directory().current().generation, 3);
    assert_eq!(journal.directory().manifest().committed.op_number, 2);
}

#[test]
fn checkpoint_build_installs_exact_committed_state_and_replay_starts_after_it() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x38);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(written).unwrap();
    let mut next = next_manifest(journal.directory());
    next.accepted = LogPosition {
        op_number: 1,
        digest: written.next_chain().previous_digest(),
    };
    next.committed = next.accepted;
    let journal = journal.install_metadata(next).unwrap();

    let checkpoint_id = CheckpointId::from_bytes([0x71; 16]);
    let plan = journal
        .checkpoint_plan(checkpoint_id, Digest::from_bytes([0x72; 32]), 5)
        .unwrap();
    let image = plan
        .build(b"canonical state bytes", CheckpointLimits::default())
        .unwrap();
    assert_eq!(image.manifest().position.op_number, 1);
    let journal = journal
        .install_checkpoint(checkpoint_id, CheckpointLimits::default())
        .unwrap();
    assert_eq!(journal.directory().current().generation, 3);
    assert_eq!(
        journal
            .directory()
            .manifest()
            .checkpoint
            .unwrap()
            .manifest_digest,
        image.manifest_digest()
    );
    drop(image);
    let mut replayed = 0;
    journal
        .replay_accepted(|_| {
            replayed += 1;
            Ok::<_, io::Error>(())
        })
        .unwrap();
    assert_eq!(replayed, 0);
    drop(journal);

    let reopened = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert_eq!(
        reopened.manifest().checkpoint.unwrap().position.op_number,
        1
    );
    drop(reopened);

    let chunk = root.join(format!(
        "checkpoints/{}/CHUNK.00000000",
        ozzy_journal_segment::checkpoint_name(checkpoint_id)
    ));
    let mut bytes = fs::read(&chunk).unwrap();
    bytes[0] ^= 1;
    fs::write(chunk, bytes).unwrap();
    assert!(matches!(
        GroupDirectory::open(&root, expected, MetadataLimits::default()),
        Err(DirectoryError::Checkpoint(_))
    ));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "linear manifest-first retention and interrupted-cleanup lifecycle"
)]
fn retention_drops_manifest_prefix_first_and_respects_record_floors_and_pins() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x39);
    let partition = PartitionIncarnation::from_bytes([0x61; 16]);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let first_body = append_body(partition, 0, 0x70);
    let first = journal
        .append(&[CanonicalOperation {
            body: &first_body,
            kind: OperationKind::Append,
            ..operation(expected, 1, Digest::ZERO)
        }])
        .unwrap();
    journal.sync_through(first).unwrap();
    let mut journal = journal.roll_active(8 * 1024).unwrap();
    let second_body = append_body(partition, 2, 0x72);
    let second = journal
        .append(&[CanonicalOperation {
            body: &second_body,
            kind: OperationKind::Append,
            ..operation(expected, 2, first.next_chain().previous_digest())
        }])
        .unwrap();
    journal.sync_through(second).unwrap();
    let mut next = next_manifest(journal.directory());
    next.accepted = LogPosition {
        op_number: 2,
        digest: second.next_chain().previous_digest(),
    };
    next.committed = next.accepted;
    let journal = journal.install_metadata(next).unwrap();
    let checkpoint_id = CheckpointId::from_bytes([0x75; 16]);
    journal
        .checkpoint_plan(checkpoint_id, Digest::from_bytes([0x76; 32]), 8)
        .unwrap()
        .build(b"committed canonical state", CheckpointLimits::default())
        .unwrap();
    let journal = journal
        .install_checkpoint(checkpoint_id, CheckpointLimits::default())
        .unwrap();
    let journal = journal.roll_active(8 * 1024).unwrap();
    journal
        .build_sealed_index(1, IndexBuildLimits::default())
        .unwrap();
    let index_path = fs::read_dir(root.join("indexes"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let old_segment = fs::read(root.join("segments/1.log")).unwrap();
    let old_index = fs::read(&index_path).unwrap();

    let pin = journal.pin_segments(&[1]).unwrap();
    let floors = RetentionFloors::new(vec![(partition, Offset::new(2))]).unwrap();
    let (journal, blocked) = journal.trim_sealed_prefix(&floors).unwrap();
    assert_eq!(blocked.blocked_by_pin, Some(1));
    assert_eq!(blocked.unreferenced_segment_ids, [1]);
    assert!(blocked.removed_segment_ids.is_empty());
    assert_eq!(journal.directory().manifest().segments[0].segment_id, 2);
    assert!(root.join("segments/1.log").exists());
    drop(pin);

    let removed = journal.reclaim_unreferenced_segments().unwrap();
    assert_eq!(removed.removed_segment_ids, [1]);
    assert!(removed.reclaimed_bytes > 0);
    assert!(!root.join("segments/1.log").exists());
    assert!(!index_path.exists());

    // Model garbage restored by a crash before prior unlinks became durable.
    fs::write(root.join("segments/1.log"), &old_segment).unwrap();
    fs::write(&index_path, &old_index).unwrap();
    let cleanup = journal.reclaim_unreferenced_segments().unwrap();
    assert_eq!(cleanup.removed_segment_ids, [1]);
    assert!(cleanup.pinned_segment_ids.is_empty());
    assert_eq!(
        cleanup.reclaimed_bytes,
        u64::try_from(old_segment.len() + old_index.len()).unwrap()
    );
    assert!(!root.join("segments/1.log").exists());
    assert!(!index_path.exists());
    drop(journal);

    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    let journal = directory
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let mut replayed = 0;
    journal
        .replay_accepted(|_| {
            replayed += 1;
            Ok::<_, io::Error>(())
        })
        .unwrap();
    assert_eq!(replayed, 0);
}

#[test]
fn detached_segment_pin_retains_exclusive_group_ownership() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x3a);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let pin = journal.pin_segments(&[1]).unwrap();
    assert_eq!(pin.segment_path(1), Some(root.join("segments/1.log")));
    drop(journal);

    assert!(matches!(
        GroupDirectory::open(&root, expected, MetadataLimits::default()),
        Err(DirectoryError::Locked)
    ));
    drop(pin);
    GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
}

#[test]
fn install_resumes_matching_or_skips_conflicting_immutable_generation() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("matching");
    let expected = identity(0x40);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let next = next_manifest(journal.directory());
    fs::write(root.join("MANIFEST.2"), encode_manifest(&next).unwrap()).unwrap();
    let journal = journal.install_metadata(next).unwrap();
    assert_eq!(journal.directory().current().generation, 2);
    drop(journal);

    let root = volume.path().join("conflicting");
    let expected = identity(0x50);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let next = next_manifest(journal.directory());
    let mut conflicting = next.clone();
    conflicting.promised_view += 1;
    fs::write(
        root.join("MANIFEST.2"),
        encode_manifest(&conflicting).unwrap(),
    )
    .unwrap();
    let conflicting_bytes = encode_manifest(&conflicting).unwrap();
    let journal = journal.install_metadata(next).unwrap();
    assert_eq!(journal.directory().current().generation, 3);
    assert_eq!(
        fs::read(root.join("MANIFEST.2")).unwrap(),
        conflicting_bytes
    );
    drop(journal);
    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert_eq!(directory.current().generation, 3);
}

#[test]
fn malformed_current_never_falls_back_silently() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x60);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    drop(directory);
    let mut current = fs::read(root.join("CURRENT")).unwrap();
    current[48] ^= 1;
    fs::write(root.join("CURRENT"), current).unwrap();

    assert!(GroupDirectory::open(&root, expected, MetadataLimits::default()).is_err());
}

#[cfg(unix)]
#[test]
fn open_rejects_symlinked_identity_file() {
    use std::os::unix::fs::symlink;

    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x70);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    drop(directory);
    fs::rename(root.join("identity"), root.join("real-identity")).unwrap();
    symlink("real-identity", root.join("identity")).unwrap();

    assert!(matches!(
        GroupDirectory::open(&root, expected, MetadataLimits::default()),
        Err(DirectoryError::NotRegularFile("identity"))
    ));
}

#[test]
fn recovered_writer_retains_lock_and_protects_manifest_prefix() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x80);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(written).unwrap();
    let mut next = next_manifest(journal.directory());
    next.accepted = LogPosition {
        op_number: 1,
        digest: written.next_chain().previous_digest(),
    };
    next.committed = next.accepted;
    journal = journal.install_metadata(next).unwrap();

    assert!(matches!(
        GroupDirectory::open(&root, expected, MetadataLimits::default()),
        Err(DirectoryError::Locked)
    ));
    drop(journal);

    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    let recovered = directory
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(recovered.writer().durable_position().group_number(), 1);
    drop(recovered);

    OpenOptions::new()
        .write(true)
        .open(root.join("segments/1.log"))
        .unwrap()
        .set_len(SEGMENT_HEADER_BYTES as u64)
        .unwrap();
    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert!(matches!(
        directory.recover(
            JournalGeneration(3),
            DecodeLimits::default(),
            OperationLimits::default(),
        ),
        Err(DirectoryError::Writer(
            WriterError::ProtectedPrefixMismatch(1)
        ))
    ));
}

#[test]
fn recovery_independently_protects_committed_digest() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x88);
    let segment = SegmentHeader::new(expected.group_id, 1, None, Digest::ZERO, 16 * 1024).unwrap();
    let directory = GroupDirectory::format_new(&root, expected, 1, &segment).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let first = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    let second = journal
        .append(&[operation(expected, 2, first.next_chain().previous_digest())])
        .unwrap();
    journal.sync_through(second).unwrap();
    let mut next = next_manifest(journal.directory());
    next.accepted = LogPosition {
        op_number: 2,
        digest: second.next_chain().previous_digest(),
    };
    next.committed = LogPosition {
        op_number: 1,
        digest: Digest::from_bytes([0xff; 32]),
    };
    drop(journal);

    let limits = MetadataLimits::default();
    let manifest_bytes = encode_manifest(&next).unwrap();
    let current = CurrentReference {
        group_id: expected.group_id,
        store_id: expected.store_id,
        generation: next.generation,
        manifest_digest: manifest_digest(&manifest_bytes, limits).unwrap(),
    };
    fs::write(root.join("MANIFEST.2"), manifest_bytes).unwrap();
    fs::write(root.join("CURRENT"), encode_current(current).unwrap()).unwrap();
    let directory = GroupDirectory::open(&root, expected, limits).unwrap();
    assert!(matches!(
        directory.recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        ),
        Err(DirectoryError::Writer(
            WriterError::ProtectedPrefixMismatch(1)
        ))
    ));
}

#[test]
fn metadata_install_requires_an_exact_durable_position() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("wrong-digest");
    let expected = identity(0x89);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(written).unwrap();
    let mut next = next_manifest(journal.directory());
    next.accepted = LogPosition {
        op_number: 1,
        digest: Digest::from_bytes([0xfe; 32]),
    };
    assert!(matches!(
        journal.install_metadata(next),
        Err(DirectoryError::PositionMismatch(1))
    ));
    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert_eq!(directory.current().generation, 1);
    drop(directory);

    let root = volume.path().join("not-durable");
    let expected = identity(0x8a);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    let mut next = next_manifest(journal.directory());
    next.accepted = LogPosition {
        op_number: 1,
        digest: written.next_chain().previous_digest(),
    };
    assert!(matches!(
        journal.install_metadata(next),
        Err(DirectoryError::PositionNotDurable(1))
    ));
    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert_eq!(directory.current().generation, 1);
}

#[test]
fn ordinary_metadata_install_never_regresses_hard_state() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x8b);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(written).unwrap();
    let mut next = next_manifest(journal.directory());
    next.accepted = LogPosition {
        op_number: 1,
        digest: written.next_chain().previous_digest(),
    };
    next.committed = next.accepted;
    let journal = journal.install_metadata(next).unwrap();

    let mut regressed = next_manifest(journal.directory());
    regressed.accepted = LogPosition::GENESIS;
    regressed.committed = LogPosition::GENESIS;
    assert!(matches!(
        journal.install_metadata(regressed),
        Err(DirectoryError::HardStateRegression)
    ));
    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert_eq!(directory.current().generation, 2);
}

#[test]
fn recovery_validates_sealed_segments_before_opening_active_writer() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0x90);
    let first = first_segment(expected);
    let directory = GroupDirectory::format_new(&root, expected, 1, &first).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(written).unwrap();
    let mut next = next_manifest(journal.directory());
    next.accepted = LogPosition {
        op_number: 1,
        digest: written.next_chain().previous_digest(),
    };
    next.committed = next.accepted;
    let journal = journal.install_metadata(next).unwrap();
    let journal = journal.roll_active(8 * 1024).unwrap();
    assert_eq!(journal.writer().header().segment_id(), 2);
    drop(journal);

    corrupt_first_body_byte(&root);
    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert!(
        directory
            .recover(
                JournalGeneration(4),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .is_err()
    );
}

#[test]
fn roll_seals_durable_active_segment_and_reopens_successor() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0xa0);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(written).unwrap();

    let mut journal = journal.roll_active(12 * 1024).unwrap();
    assert_eq!(journal.directory().current().generation, 2);
    assert_eq!(journal.writer().header().segment_id(), 2);
    assert_eq!(journal.writer().written_position().group_number(), 1);
    assert_eq!(
        journal.writer().written_position().next_chain(),
        written.next_chain()
    );
    let references = &journal.directory().manifest().segments;
    assert_eq!(references.len(), 2);
    assert!(references[0].sealed.is_some());
    assert!(references[1].sealed.is_none());
    assert_eq!(references[1].first_group_number, 2);
    assert_eq!(references[1].first_chain, written.next_chain());

    let written = journal
        .append(&[operation(
            expected,
            2,
            written.next_chain().previous_digest(),
        )])
        .unwrap();
    journal.sync_through(written).unwrap();
    drop(journal);

    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    let recovered = directory
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(recovered.writer().header().segment_id(), 2);
    assert_eq!(recovered.writer().durable_position().group_number(), 2);
    assert_eq!(
        recovered.writer().durable_position().next_chain(),
        written.next_chain()
    );
}

#[test]
fn sealed_segment_index_build_and_open_are_bound_to_manifest_source() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0xa1);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(written).unwrap();
    let journal = journal.roll_active(8 * 1024).unwrap();

    let index = journal
        .build_sealed_index(
            1,
            IndexBuildLimits {
                max_entry_buffer_bytes: 256,
                max_merge_fan_in: 2,
                ..IndexBuildLimits::default()
            },
        )
        .unwrap();
    assert_eq!(index.operation_count(), 1);
    assert_eq!(
        index
            .find_operation(OperationId::from_bytes([0xaa; 16]))
            .unwrap()
            .location
            .op_number,
        1
    );
    assert_eq!(
        journal
            .open_sealed_index(1, IndexBuildLimits::default().file)
            .unwrap()
            .source(),
        index.source()
    );
    let catalog = journal
        .open_sealed_index_catalog(IndexBuildLimits::default().file)
        .unwrap();
    assert!(
        catalog
            .find_operation(OperationId::from_bytes([0xaa; 16]), 0)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        catalog
            .find_operation(OperationId::from_bytes([0xaa; 16]), 1)
            .unwrap()
            .unwrap()
            .entry
            .location
            .op_number,
        1
    );
    let index_path = fs::read_dir(root.join("indexes"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut corrupt = fs::read(&index_path).unwrap();
    corrupt[256] ^= 1;
    fs::write(&index_path, corrupt).unwrap();
    assert!(
        journal
            .open_sealed_index(1, IndexBuildLimits::default().file)
            .is_err()
    );
    assert_eq!(
        journal
            .repair_sealed_index(1, IndexBuildLimits::default())
            .unwrap()
            .operation_count(),
        1
    );
    assert!(matches!(
        journal.build_sealed_index(2, IndexBuildLimits::default()),
        Err(DirectoryError::SegmentNotSealed(2))
    ));
}

#[test]
fn roll_rejects_unsynchronized_or_empty_active_segment() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("unsynchronized");
    let expected = identity(0xb0);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    assert!(matches!(
        journal.roll_active(8 * 1024),
        Err(DirectoryError::ActiveSegmentNotDurable)
    ));
    assert!(!root.join("segments/2.log").exists());

    let root = volume.path().join("empty");
    let expected = identity(0xc0);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert!(matches!(
        journal.roll_active(8 * 1024),
        Err(DirectoryError::EmptyActiveSegment)
    ));
    assert!(!root.join("segments/2.log").exists());
}

#[test]
fn roll_resumes_exact_empty_orphan_and_skips_data_bearing_one() {
    let volume = TempDir::new().unwrap();
    for (name, byte, data_bearing) in [("empty", 0xd0, false), ("data", 0xe0, true)] {
        let root = volume.path().join(name);
        let expected = identity(byte);
        let first = first_segment(expected);
        let directory = GroupDirectory::format_new(&root, expected, 1, &first).unwrap();
        let mut journal = directory
            .recover(
                JournalGeneration(1),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        let written = journal
            .append(&[operation(expected, 1, Digest::ZERO)])
            .unwrap();
        journal.sync_through(written).unwrap();

        let image = fs::read(root.join("segments/1.log")).unwrap();
        let scan =
            scan_segment(&image, 1, ChainPosition::GENESIS, DecodeLimits::default()).unwrap();
        let header = SegmentHeader::new(
            expected.group_id,
            2,
            Some(1),
            scan.next_chain.previous_digest(),
            8 * 1024,
        )
        .unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(root.join("segments/2.log"))
            .unwrap();
        let mut orphan = SegmentWriter::initialize_at(
            file,
            header,
            JournalGeneration(1),
            scan.next_group_number,
            scan.next_chain,
        )
        .unwrap();
        if data_bearing {
            orphan
                .append(&[operation(expected, 2, scan.next_chain.previous_digest())])
                .unwrap();
        }
        let orphan = orphan.into_inner();
        rustix::fs::fallocate(&orphan, rustix::fs::FallocateFlags::empty(), 0, 8 * 1024).unwrap();
        orphan.sync_data().unwrap();
        drop(orphan);

        let result = journal.roll_active(8 * 1024);
        if data_bearing {
            let journal = result.unwrap();
            assert_eq!(journal.directory().current().generation, 2);
            assert_eq!(journal.writer().header().segment_id(), 3);
            assert!(root.join("segments/2.log").is_file());
            let cleanup = journal.reclaim_unreferenced_segments().unwrap();
            assert_eq!(cleanup.removed_segment_ids, [2]);
        } else {
            let journal = result.unwrap();
            assert_eq!(journal.directory().current().generation, 2);
            assert_eq!(journal.writer().header().segment_id(), 2);
        }
    }
}

#[test]
fn replay_streams_durable_accepted_lineage_and_marks_committed_prefix() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0xf0);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let first = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(first).unwrap();
    let mut journal = journal.roll_active(16 * 1024).unwrap();
    let second = journal
        .append(&[operation(expected, 2, first.next_chain().previous_digest())])
        .unwrap();
    journal.sync_through(second).unwrap();
    let mut next = next_manifest(journal.directory());
    next.accepted = LogPosition {
        op_number: 2,
        digest: second.next_chain().previous_digest(),
    };
    next.committed = LogPosition {
        op_number: 1,
        digest: first.next_chain().previous_digest(),
    };
    let mut journal = journal.install_metadata(next).unwrap();
    let third = journal
        .append(&[operation(
            expected,
            3,
            second.next_chain().previous_digest(),
        )])
        .unwrap();
    let mut replayed = Vec::new();
    journal
        .replay_accepted::<io::Error>(|item| {
            replayed.push((item.operation.op_number, item.committed));
            Ok(())
        })
        .unwrap();
    assert_eq!(replayed, [(1, true), (2, false)]);

    journal.sync_through(third).unwrap();

    let mut replayed = Vec::new();
    journal
        .replay_accepted::<io::Error>(|item| {
            replayed.push((item.operation.op_number, item.committed));
            Ok(())
        })
        .unwrap();
    assert_eq!(replayed, [(1, true), (2, false), (3, false)]);

    assert!(matches!(
        journal.replay_accepted(|_| Err(io::Error::other("stop"))),
        Err(ReplayError::Visitor(_))
    ));
    assert_eq!(journal.directory().manifest().accepted.op_number, 2);
    drop(journal);

    let journal = GroupDirectory::open(&root, expected, MetadataLimits::default())
        .unwrap()
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(journal.directory().manifest().accepted.op_number, 3);
    assert_eq!(journal.directory().manifest().committed.op_number, 1);
}

#[test]
fn decoded_body_limit_forces_a_recoverable_segment_roll() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0xe0);
    let header = SegmentHeader::new(expected.group_id, 1, None, Digest::ZERO, 16 * 1024).unwrap();
    let limits = DecodeLimits {
        max_decoded_body_bytes: 16,
        max_group_decoded_body_bytes: 16,
        max_segment_decoded_body_bytes: 16,
        ..DecodeLimits::default()
    };
    let directory = GroupDirectory::format_new(&root, expected, 1, &header).unwrap();
    let mut journal = directory
        .recover(JournalGeneration(1), limits, OperationLimits::default())
        .unwrap();
    let first = journal
        .append(&[operation(expected, 1, Digest::ZERO)])
        .unwrap();
    journal.sync_through(first).unwrap();
    let second = operation(expected, 2, first.next_chain().previous_digest());
    assert!(matches!(
        journal.append(&[second]),
        Err(DirectoryError::Codec(
            ozzy_journal_segment::CodecError::SegmentDecodedBodyLimit {
                actual: 32,
                limit: 16
            }
        ))
    ));
    assert_eq!(journal.writer().written_position(), first);

    let mut journal = journal.roll_active(16 * 1024).unwrap();
    let second = journal.append(&[second]).unwrap();
    journal.sync_through(second).unwrap();
    drop(journal);

    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    let recovered = directory
        .recover(JournalGeneration(2), limits, OperationLimits::default())
        .unwrap();
    assert_eq!(recovered.writer().written_position().group_number(), 2);
    assert_eq!(recovered.directory().manifest().segments.len(), 2);
}

#[test]
fn recovery_rejects_physically_valid_noncanonical_operation_body() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0xa0);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let invalid = CanonicalOperation {
        body: b"bad",
        ..operation(expected, 1, Digest::ZERO)
    };
    assert!(matches!(
        journal.append(&[invalid]),
        Err(DirectoryError::Operation(_))
    ));
    assert_eq!(
        journal.writer().written_position().end_offset(),
        SEGMENT_HEADER_BYTES as u64
    );
    drop(journal);
    append_unchecked(&root, invalid);

    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert!(matches!(
        directory.recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        ),
        Err(DirectoryError::Writer(WriterError::Operation(_)))
    ));
}

#[test]
fn recovery_enforces_operation_configuration_and_promised_view() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("configuration");
    let expected = identity(0xa1);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let invalid = CanonicalOperation {
        configuration_epoch: 2,
        ..operation(expected, 1, Digest::ZERO)
    };
    assert!(matches!(
        journal.append(&[invalid]),
        Err(DirectoryError::Writer(
            WriterError::ConfigurationMismatch { .. }
        ))
    ));
    drop(journal);
    append_unchecked(&root, invalid);
    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert!(matches!(
        directory.recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        ),
        Err(DirectoryError::Writer(WriterError::ConfigurationMismatch {
            op_number: 1,
            ..
        }))
    ));

    let root = volume.path().join("view");
    let expected = identity(0xa2);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let invalid = CanonicalOperation {
        original_view: 1,
        ..operation(expected, 1, Digest::ZERO)
    };
    assert!(matches!(
        journal.append(&[invalid]),
        Err(DirectoryError::Writer(
            WriterError::ViewBeyondPromise { .. }
        ))
    ));
    drop(journal);
    append_unchecked(&root, invalid);
    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert!(matches!(
        directory.recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        ),
        Err(DirectoryError::Writer(WriterError::ViewBeyondPromise {
            op_number: 1,
            ..
        }))
    ));
}

#[test]
fn generic_metadata_cannot_change_configuration() {
    let volume = TempDir::new().unwrap();
    let root = volume.path().join("group");
    let expected = identity(0xa3);
    let directory =
        GroupDirectory::format_new(&root, expected, 1, &first_segment(expected)).unwrap();
    let journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let mut next = next_manifest(journal.directory());
    next.configuration_epoch = 2;
    assert!(matches!(
        journal.install_metadata(next),
        Err(DirectoryError::ConfigurationChangeRequiresInstall)
    ));
    let directory = GroupDirectory::open(&root, expected, MetadataLimits::default()).unwrap();
    assert_eq!(directory.current().generation, 1);
}

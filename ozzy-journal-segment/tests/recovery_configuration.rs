//! A replacement store stays inadmissible until complete history is published.

use ozzy_journal::operation::{Barrier, CanonicalOperation, OperationBody, encode_operation_body};
use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    BodyEncoding, CanonicalRecoveryLimits, DecodeLimits, Digest, DirectoryError, GroupDirectory,
    GroupIdentity, LogPosition, MetadataLimits, OperationKind, OperationLimits,
    RecoveryPublication, SegmentHeader, SuffixReplacement, SuffixStreamLimits,
    logical_operation_digest,
};
use ozzy_proto::{GroupId, NodeId, OperationId, StoreId, VolumeId};

const CONFIGURATION: &[u8] = b"prevalidated fixed voter configuration";
const CAPACITY: u64 = 16 * 1024;

fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([1; 16]),
        replica_node_id: NodeId::from_bytes([2; 16]),
        volume_id: VolumeId::from_bytes([3; 16]),
        store_id: StoreId::from_bytes([4; 16]),
        store_generation: 1,
    }
}

fn header() -> SegmentHeader {
    SegmentHeader::new(identity().group_id, 1, None, Digest::ZERO, CAPACITY).unwrap()
}

#[test]
fn unfinished_replacement_fails_ordinary_configured_open_after_owner_exits() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("replacement");
    let directory =
        GroupDirectory::format_recovering(&root, identity(), 1, &header(), CONFIGURATION).unwrap();
    let marker = directory.configuration().unwrap().to_vec();
    assert_ne!(marker, CONFIGURATION);
    assert_eq!(marker.len(), 64);
    assert_eq!(&marker[..8], b"OZYRECOV");
    drop(directory);
    assert!(matches!(
        GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION
        ),
        Err(DirectoryError::ConfigurationMismatch)
    ));
    let recovery = GroupDirectory::open_recovering(
        &root,
        identity(),
        MetadataLimits::default(),
        CONFIGURATION,
    )
    .unwrap();
    assert_eq!(recovery.configuration(), Some(marker.as_slice()));
    drop(recovery);
    assert!(matches!(
        GroupDirectory::open_recovering(
            &root,
            identity(),
            MetadataLimits::default(),
            b"wrong membership"
        ),
        Err(DirectoryError::ConfigurationMismatch)
    ));
    assert!(matches!(
        GroupDirectory::format_recovering(&root, identity(), 1, &header(), CONFIGURATION),
        Err(DirectoryError::StoreAlreadyExists)
    ));
}

#[test]
fn invalid_intended_configuration_never_creates_replacement_directory() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("replacement");
    for bytes in [
        b"".as_slice(),
        b"OZYRECOV reserved namespace".as_slice(),
        &vec![0; ozzy_journal_segment::GROUP_CONFIGURATION_MAX_BYTES + 1],
    ] {
        assert!(GroupDirectory::format_recovering(&root, identity(), 1, &header(), bytes).is_err());
        assert!(!root.exists());
    }
}

#[test]
fn incomplete_or_corrupt_marker_is_never_repaired_into_voting_configuration() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("replacement");
    let directory =
        GroupDirectory::format_recovering(&root, identity(), 1, &header(), CONFIGURATION).unwrap();
    let marker = directory.configuration().unwrap().to_vec();
    drop(directory);
    for index in 0..marker.len() {
        let mut corrupt = marker.clone();
        corrupt[index] ^= 1;
        for bytes in [&marker[..index], &corrupt] {
            std::fs::write(root.join("CONFIGURATION"), bytes).unwrap();
            assert!(
                GroupDirectory::open_recovering(
                    &root,
                    identity(),
                    MetadataLimits::default(),
                    CONFIGURATION
                )
                .is_err()
            );
            assert!(
                GroupDirectory::open_with_configuration(
                    &root,
                    identity(),
                    MetadataLimits::default(),
                    CONFIGURATION
                )
                .is_err()
            );
            assert_eq!(std::fs::read(root.join("CONFIGURATION")).unwrap(), bytes);
        }
    }
}

#[test]
fn publication_preserves_full_current_view_tail_without_claiming_it_committed() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("replacement");
    let journal = GroupDirectory::format_recovering(&root, identity(), 1, &header(), CONFIGURATION)
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let body = encode_operation_body(
        &OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes([5; 16]),
        }),
        OperationLimits::default(),
    )
    .unwrap();
    let operation = CanonicalOperation {
        group_id: identity().group_id,
        configuration_epoch: 1,
        original_view: 2,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Barrier,
        body: &body,
    };
    let accepted = LogPosition {
        op_number: 1,
        digest: logical_operation_digest(&operation),
    };
    let replacement = SuffixReplacement {
        expected_current: journal.directory().current(),
        protected_committed: LogPosition::GENESIS,
        promised_view: 2,
        last_normal_view: 2,
        committed: LogPosition::GENESIS,
        writer_generation: JournalGeneration(2),
        segment_capacity: CAPACITY,
        body_encoding: BodyEncoding::Raw,
    };
    let mut staging = journal
        .begin_suffix_replacement(replacement, accepted, SuffixStreamLimits::default())
        .unwrap();
    staging.append_chunk(&[operation]).unwrap();
    let journal = staging.finish().unwrap();
    let publication = RecoveryPublication {
        current: journal.directory().current(),
        generation: JournalGeneration(2),
        view: 2,
        accepted,
        committed: LogPosition::GENESIS,
    };
    let (journal, candidate) = journal
        .publish_recovered_configuration(
            CONFIGURATION,
            publication,
            CanonicalRecoveryLimits::default(),
        )
        .unwrap();
    assert_eq!(journal.directory().configuration(), Some(CONFIGURATION));
    assert_eq!(candidate.accepted_position(), accepted);
    assert_eq!(candidate.committed_images().committed().revision(), 0);
    assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
    assert!(matches!(
        GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION
        ),
        Err(DirectoryError::Locked)
    ));
    drop(candidate);
    drop(journal);
    let opened = GroupDirectory::open_with_configuration(
        &root,
        identity(),
        MetadataLimits::default(),
        CONFIGURATION,
    )
    .unwrap();
    assert_eq!(opened.manifest().accepted, accepted);
    assert_eq!(opened.manifest().promised_view, 2);
    assert_eq!(opened.manifest().last_normal_view, 2);
    drop(opened);
    assert!(matches!(
        GroupDirectory::open_recovering(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION
        ),
        Err(DirectoryError::ConfigurationMismatch)
    ));
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep interrupted transfer and ordinary-voter rejection in one chronological scenario"
)]
fn fresh_recovery_replaces_unadmitted_attempt_not_ordinary_voter_history() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("replacement");
    let journal = GroupDirectory::format_recovering(&root, identity(), 1, &header(), CONFIGURATION)
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let body = encode_operation_body(
        &OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes([6; 16]),
        }),
        OperationLimits::default(),
    )
    .unwrap();
    let operation = CanonicalOperation {
        group_id: identity().group_id,
        configuration_epoch: 1,
        original_view: 2,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Barrier,
        body: &body,
    };
    let accepted = LogPosition {
        op_number: 1,
        digest: logical_operation_digest(&operation),
    };
    let mut replacement = SuffixReplacement {
        expected_current: journal.directory().current(),
        protected_committed: LogPosition::GENESIS,
        promised_view: 2,
        last_normal_view: 2,
        committed: accepted,
        writer_generation: JournalGeneration(2),
        segment_capacity: CAPACITY,
        body_encoding: BodyEncoding::Raw,
    };
    let mut staging = journal
        .begin_suffix_replacement(replacement, accepted, SuffixStreamLimits::default())
        .unwrap();
    staging.append_chunk(&[operation]).unwrap();
    drop(staging.finish().unwrap()); // Crash before publishing the voter configuration.
    let reopen = || {
        GroupDirectory::open_recovering(&root, identity(), MetadataLimits::default(), CONFIGURATION)
            .unwrap()
            .recover(
                JournalGeneration(3),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap()
    };
    let journal = reopen();
    replacement.expected_current = journal.directory().current();
    replacement.writer_generation = JournalGeneration(4);
    replacement.promised_view = 3;
    replacement.last_normal_view = 3;
    replacement.committed = LogPosition::GENESIS;
    // Ordinary suffix installation still cannot lower even this local commit floor.
    assert!(matches!(
        journal.begin_suffix_replacement(replacement, accepted, SuffixStreamLimits::default()),
        Err(ozzy_journal_segment::SuffixReplacementError::ProtectedCommitMismatch)
    ));
    let mut staging = reopen()
        .begin_recovery_replacement(
            CONFIGURATION,
            replacement,
            accepted,
            SuffixStreamLimits::default(),
        )
        .unwrap();
    staging.append_chunk(&[operation]).unwrap();
    let journal = staging.finish().unwrap();
    assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
    let publication = RecoveryPublication {
        current: journal.directory().current(),
        generation: JournalGeneration(4),
        view: 3,
        accepted,
        committed: LogPosition::GENESIS,
    };
    let (journal, candidate) = journal
        .publish_recovered_configuration(
            CONFIGURATION,
            publication,
            CanonicalRecoveryLimits::default(),
        )
        .unwrap();
    assert_eq!(candidate.accepted_position(), accepted);
    drop(candidate);
    replacement.expected_current = journal.directory().current();
    replacement.writer_generation = JournalGeneration(5);
    assert!(matches!(
        journal.begin_recovery_replacement(
            CONFIGURATION,
            replacement,
            accepted,
            SuffixStreamLimits::default()
        ),
        Err(ozzy_journal_segment::SuffixReplacementError::Directory(
            DirectoryError::ConfigurationMismatch
        ))
    ));
}

#[test]
fn replacement_rechecks_disk_marker_before_creating_any_staged_file() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("replacement");
    let journal = GroupDirectory::format_recovering(&root, identity(), 1, &header(), CONFIGURATION)
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let replacement = SuffixReplacement {
        expected_current: journal.directory().current(),
        protected_committed: LogPosition::GENESIS,
        promised_view: 0,
        last_normal_view: 0,
        committed: LogPosition::GENESIS,
        writer_generation: JournalGeneration(2),
        segment_capacity: CAPACITY,
        body_encoding: BodyEncoding::Raw,
    };
    // Simulate changed media after the in-memory marker was loaded.
    std::fs::write(root.join("CONFIGURATION"), CONFIGURATION).unwrap();
    assert!(
        journal
            .begin_recovery_replacement(
                CONFIGURATION,
                replacement,
                LogPosition::GENESIS,
                SuffixStreamLimits::default()
            )
            .is_err()
    );
    assert_eq!(std::fs::read_dir(root.join("segments")).unwrap().count(), 1);
    assert_eq!(
        std::fs::read(root.join("CONFIGURATION")).unwrap(),
        CONFIGURATION
    );
}

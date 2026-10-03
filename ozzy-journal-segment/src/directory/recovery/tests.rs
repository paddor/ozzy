//! Error cuts through the real configuration publication sequence.

use super::*;
use crate::{
    BodyEncoding, DecodeLimits, Digest, OperationLimits, SuffixReplacement, SuffixStreamLimits,
};
use ozzy_journal::operation::{CanonicalOperation, OperationKind, logical_operation_digest};
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};

const CONFIGURATION: &[u8] = b"recovery publication error cuts";

#[test]
fn nonvoting_inspection_preserves_metadata_and_cannot_open_as_a_configured_store() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("replacement");
    let (journal, _) = installed(&root);
    drop(journal);
    let marker = fs::read(root.join("CONFIGURATION")).unwrap();
    let current = fs::read(root.join("CURRENT")).unwrap();
    let inspected = GroupDirectory::inspect_recovering(
        &root,
        identity(),
        MetadataLimits::default(),
        CONFIGURATION,
    )
    .unwrap();
    assert_eq!(inspected.configuration(), Some(marker.as_slice()));
    drop(inspected);
    assert_eq!(fs::read(root.join("CONFIGURATION")).unwrap(), marker);
    assert_eq!(fs::read(root.join("CURRENT")).unwrap(), current);
    assert!(matches!(
        GroupDirectory::inspect_recovering(
            &root,
            identity(),
            MetadataLimits::default(),
            b"different configuration",
        ),
        Err(DirectoryError::ConfigurationMismatch)
    ));
    assert!(matches!(
        GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION,
        ),
        Err(DirectoryError::ConfigurationMismatch)
    ));
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

fn installed(root: &Path) -> (OpenGroupJournal, RecoveryPublication) {
    installed_operations(root, &[barrier()])
}

fn barrier() -> CanonicalOperation<'static> {
    CanonicalOperation {
        group_id: identity().group_id,
        configuration_epoch: 1,
        original_view: 7,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Barrier,
        body: &[5; 16],
    }
}

fn installed_operations(
    root: &Path,
    operations: &[CanonicalOperation<'_>],
) -> (OpenGroupJournal, RecoveryPublication) {
    let header = SegmentHeader::new(identity().group_id, 1, None, Digest::ZERO, 16 * 1024).unwrap();
    let journal = GroupDirectory::format_recovering(root, identity(), 1, &header, CONFIGURATION)
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let last = operations.last().unwrap();
    let accepted = LogPosition {
        op_number: last.op_number,
        digest: logical_operation_digest(last),
    };
    let replacement = SuffixReplacement {
        expected_current: journal.directory().current(),
        protected_committed: LogPosition::GENESIS,
        promised_view: 7,
        last_normal_view: 7,
        committed: LogPosition::GENESIS,
        writer_generation: JournalGeneration(2),
        segment_capacity: 16 * 1024,
        body_encoding: BodyEncoding::Raw,
    };
    let mut staging = journal
        .begin_suffix_replacement(replacement, accepted, SuffixStreamLimits::default())
        .unwrap();
    staging.append_chunk(operations).unwrap();
    let journal = staging.finish().unwrap();
    let publication = RecoveryPublication {
        current: journal.directory().current(),
        generation: JournalGeneration(2),
        view: 7,
        accepted,
        committed: LogPosition::GENESIS,
    };
    (journal, publication)
}

#[test]
fn syntactically_valid_duplicate_identity_cannot_publish_voter_configuration() {
    let temporary = tempfile::tempdir().unwrap();
    let root = temporary.path().join("replacement");
    let first = barrier();
    let second = CanonicalOperation {
        op_number: 2,
        previous_digest: logical_operation_digest(&first),
        ..first
    };
    let (journal, publication) = installed_operations(&root, &[first, second]);
    assert_eq!(journal.accepted_position().unwrap(), publication.accepted);
    let result = journal.publish_recovered_configuration(
        CONFIGURATION,
        publication,
        CanonicalRecoveryLimits::default(),
    );
    assert!(
        matches!(result, Err(RecoveryPublicationError::Canonical(_))),
        "{result:?}"
    );
    assert!(!root.join(".CONFIGURATION.recovered.2.tmp").exists());
    assert!(
        GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION
        )
        .is_err()
    );
    let recovery = GroupDirectory::open_recovering(
        &root,
        identity(),
        MetadataLimits::default(),
        CONFIGURATION,
    )
    .unwrap();
    assert_eq!(recovery.current(), publication.current);
    assert_eq!(recovery.manifest().accepted, publication.accepted);
}

#[test]
fn stale_publication_cannot_replace_marker_or_selected_metadata() {
    for field in 0..8 {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("replacement");
        let (journal, publication) = installed(&root);
        let marker = fs::read(root.join("CONFIGURATION")).unwrap();
        let mut wrong = publication;
        match field {
            0 => wrong.current.generation += 1,
            1 => wrong.current.manifest_digest = Digest::ZERO,
            2 => wrong.generation = JournalGeneration(99),
            3 => wrong.view += 1,
            4 => wrong.accepted.op_number += 1,
            5 => wrong.accepted.digest = Digest::ZERO,
            6 => wrong.committed = publication.accepted,
            7 => wrong.committed.digest = publication.accepted.digest,
            _ => unreachable!(),
        }
        assert!(
            matches!(
                journal.publish_recovered_configuration(
                    CONFIGURATION,
                    wrong,
                    CanonicalRecoveryLimits::default()
                ),
                Err(RecoveryPublicationError::ImageMismatch)
            ),
            "field {field}"
        );
        assert_eq!(fs::read(root.join("CONFIGURATION")).unwrap(), marker);
        assert!(!root.join(".CONFIGURATION.recovered.2.tmp").exists());
        let directory = GroupDirectory::open_recovering(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION,
        )
        .unwrap();
        assert_eq!(directory.current(), publication.current);
        assert_eq!(directory.manifest().accepted, publication.accepted);
        assert_eq!(directory.manifest().committed, publication.committed);
    }
}

#[test]
fn conflicting_temporary_configuration_is_preserved_without_admitting_store() {
    for bytes in [b"conflicting membership".as_slice(), &CONFIGURATION[..8]] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("replacement");
        let (journal, publication) = installed(&root);
        let marker = fs::read(root.join("CONFIGURATION")).unwrap();
        fs::write(root.join(".CONFIGURATION.recovered.2.tmp"), bytes).unwrap();
        assert!(matches!(
            journal.publish_recovered_configuration(
                CONFIGURATION,
                publication,
                CanonicalRecoveryLimits::default()
            ),
            Err(RecoveryPublicationError::Directory(_))
        ));
        assert_eq!(
            fs::read(root.join(".CONFIGURATION.recovered.2.tmp")).unwrap(),
            bytes
        );
        assert_eq!(fs::read(root.join("CONFIGURATION")).unwrap(), marker);
        assert!(
            GroupDirectory::open_with_configuration(
                &root,
                identity(),
                MetadataLimits::default(),
                CONFIGURATION
            )
            .is_err()
        );
        assert!(
            GroupDirectory::open_recovering(
                &root,
                identity(),
                MetadataLimits::default(),
                CONFIGURATION
            )
            .is_ok()
        );
        // A crash may leave a truncated temp. A fresh writer must make progress
        // without deleting or overwriting the previous attempt's bytes.
        let journal = GroupDirectory::open_recovering(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION,
        )
        .unwrap()
        .recover(
            JournalGeneration(3),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
        let (journal, _) = journal
            .publish_recovered_configuration(
                CONFIGURATION,
                RecoveryPublication {
                    generation: JournalGeneration(3),
                    ..publication
                },
                CanonicalRecoveryLimits::default(),
            )
            .unwrap();
        assert_eq!(journal.directory().configuration(), Some(CONFIGURATION));
        assert_eq!(
            fs::read(root.join(".CONFIGURATION.recovered.2.tmp")).unwrap(),
            bytes
        );
    }
}

#[test]
fn completed_configuration_cannot_be_republished_or_changed() {
    for bytes in [CONFIGURATION, b"new membership".as_slice()] {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("replacement");
        let (journal, publication) = installed(&root);
        let (journal, candidate) = journal
            .publish_recovered_configuration(
                CONFIGURATION,
                publication,
                CanonicalRecoveryLimits::default(),
            )
            .unwrap();
        drop(candidate);
        assert!(matches!(
            journal.publish_recovered_configuration(
                bytes,
                publication,
                CanonicalRecoveryLimits::default()
            ),
            Err(RecoveryPublicationError::Directory(
                DirectoryError::ConfigurationMismatch
            ))
        ));
        assert_eq!(fs::read(root.join("CONFIGURATION")).unwrap(), CONFIGURATION);
        let directory = GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION,
        )
        .unwrap();
        assert_eq!(directory.current(), publication.current);
    }
}

#[test]
fn publication_error_cuts_leave_marker_or_complete_validated_image() {
    let phases = [
        PublicationPhase::Validated,
        PublicationPhase::ConfigurationSynced,
        PublicationPhase::ConfigurationRenamed,
        PublicationPhase::DirectorySynced,
    ];
    for (cut, phase) in phases.into_iter().enumerate() {
        let temporary = tempfile::tempdir().unwrap();
        let root = temporary.path().join("replacement");
        let (journal, publication) = installed(&root);
        let mut observed = Vec::new();
        let result = journal.publish_recovered_configuration_observing(
            CONFIGURATION,
            publication,
            CanonicalRecoveryLimits::default(),
            |completed| {
                observed.push(completed);
                // Same process cannot reopen while publication still owns the lock.
                assert!(matches!(
                    GroupDirectory::open(&root, identity(), MetadataLimits::default()),
                    Err(DirectoryError::Locked)
                ));
                if completed == phase {
                    Err(io::Error::other("injected publication interruption"))
                } else {
                    Ok(())
                }
            },
        );
        assert!(
            matches!(result, Err(RecoveryPublicationError::Io(_))),
            "{phase:?}: {result:?}"
        );
        assert_eq!(observed, phases[..=cut]);
        let published = matches!(
            phase,
            PublicationPhase::ConfigurationRenamed | PublicationPhase::DirectorySynced
        );
        let directory = reopen_after_cut(&root, published);
        assert_eq!(directory.current(), publication.current);
        assert_eq!(directory.manifest().accepted, publication.accepted);
        assert_eq!(directory.manifest().committed, LogPosition::GENESIS);
        assert_eq!(directory.manifest().promised_view, 7);
        assert_eq!(directory.manifest().last_normal_view, 7);
        let journal = directory
            .recover(
                JournalGeneration(3),
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
        let candidate = journal
            .recover_canonical_candidate(CanonicalRecoveryLimits::default())
            .unwrap();
        assert_eq!(candidate.accepted_position(), publication.accepted);
        assert_eq!(candidate.committed_images().committed().revision(), 0);
        drop(candidate);
        if !published {
            // New external authority is required by the caller; only storage retry
            // behavior is tested here, preserving any previous temporary file.
            let (journal, _) = journal
                .publish_recovered_configuration(
                    CONFIGURATION,
                    RecoveryPublication {
                        generation: JournalGeneration(3),
                        ..publication
                    },
                    CanonicalRecoveryLimits::default(),
                )
                .unwrap();
            assert_eq!(journal.directory().configuration(), Some(CONFIGURATION));
        }
    }
}

fn reopen_after_cut(root: &Path, published: bool) -> GroupDirectory {
    if published {
        assert!(
            GroupDirectory::open_recovering(
                root,
                identity(),
                MetadataLimits::default(),
                CONFIGURATION
            )
            .is_err()
        );
        GroupDirectory::open_with_configuration(
            root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION,
        )
        .unwrap()
    } else {
        assert!(
            GroupDirectory::open_with_configuration(
                root,
                identity(),
                MetadataLimits::default(),
                CONFIGURATION
            )
            .is_err()
        );
        GroupDirectory::open_recovering(root, identity(), MetadataLimits::default(), CONFIGURATION)
            .unwrap()
    }
}

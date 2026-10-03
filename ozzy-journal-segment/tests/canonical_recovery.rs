use ozzy_core::state::{
    CanonicalImagesError, CanonicalState, IdentityIndex, IdentityIndexError, IdentityKey,
    MemoryIdentityIndex, StateError, StateLimits, StateSnapshotLimits,
};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Barrier, CreatePartition, OpenProducer, OperationBody,
    Progress, ProgressOwner, RetentionPolicy, encode_operation_body,
};
use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    CanonicalOperation, CanonicalRecoveryLimits, CanonicalStateRecoveryError, ChainPosition,
    CheckpointLimits, DecodeLimits, Digest, GroupDirectory, GroupIdentity, LogPosition,
    MetadataLimits, OpenGroupJournal, OperationLimits, RetentionFloors, SegmentHeader,
};
use ozzy_proto::{
    CheckpointId, GroupId, MessageId, NodeId, Offset, OperationId, OwnerEpoch, PartitionId,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, StoreId, SubscriptionId,
    VolumeId,
};
use tempfile::TempDir;

#[path = "canonical_recovery/shared.rs"]
mod shared;

fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([0x11; 16]),
        replica_node_id: NodeId::from_bytes([0x12; 16]),
        volume_id: VolumeId::from_bytes([0x13; 16]),
        store_id: StoreId::from_bytes([0x14; 16]),
        store_generation: 1,
    }
}

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([0x20; 16])
}

fn producer() -> ProducerId {
    ProducerId::from_bytes([0x30; 16])
}

fn create() -> OperationBody<'static> {
    OperationBody::CreatePartition(CreatePartition {
        partition: partition(),
        stream: "stream",
        topic: "topic",
        partition_id: PartitionId::ZERO,
        owner_epoch: OwnerEpoch::INITIAL,
        retention: RetentionPolicy::default(),
    })
}

fn open() -> OperationBody<'static> {
    OperationBody::OpenProducer(OpenProducer {
        partition: partition(),
        producer_id: producer(),
        expected_epoch: None,
        new_epoch: ProducerEpoch::INITIAL,
        operation_id: OperationId::from_bytes([0x40; 16]),
    })
}

fn append(message: u8, sequence: u64, offset: u64) -> OperationBody<'static> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id: producer(),
            producer_epoch: ProducerEpoch::INITIAL,
            first_sequence: ProducerSequence::new(sequence),
            first_offset: Offset::new(offset),
            append_timestamp_millis: 123,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([message; 16]),
                parts: vec![b"payload".as_slice()].into(),
            }]
            .into(),
        }],
    })
}

fn append_body(
    journal: &mut OpenGroupJournal,
    chain: ChainPosition,
    body: &OperationBody<'_>,
) -> ChainPosition {
    let bytes = encode_operation_body(body, OperationLimits::default()).unwrap();
    let written = journal
        .append(&[CanonicalOperation {
            group_id: identity().group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: chain.next_op_number(),
            previous_digest: chain.previous_digest(),
            kind: body.kind(),
            body: &bytes,
        }])
        .unwrap();
    journal.sync_through(written).unwrap();
    written.next_chain()
}

fn apply_body(
    state: &mut CanonicalState,
    identities: &mut MemoryIdentityIndex,
    body: &OperationBody<'_>,
) {
    let plan = state
        .prepare(state.revision() + 1, body, identities, state)
        .unwrap();
    state.apply(plan, identities).unwrap();
}

fn publish(mut journal: OpenGroupJournal, chain: ChainPosition) -> OpenGroupJournal {
    let mut manifest = journal.directory().manifest().clone();
    manifest.generation += 1;
    manifest.parent_generation = journal.directory().manifest().generation;
    manifest.accepted = LogPosition {
        op_number: chain.next_op_number() - 1,
        digest: chain.previous_digest(),
    };
    manifest.committed = manifest.accepted;
    journal = journal.install_metadata(manifest).unwrap();
    journal
}

fn format(temporary: &TempDir) -> OpenGroupJournal {
    let identity = identity();
    let header = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 16 * 1024).unwrap();
    GroupDirectory::format_new(temporary.path().join("group"), identity, 1, &header)
        .unwrap()
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap()
}

#[test]
fn selected_tail_recovery_does_not_consume_live_pending_capacity() {
    let temporary = tempfile::Builder::new()
        .prefix(".canonical-recovery-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let mut journal = format(&temporary);
    let mut chain = ChainPosition::GENESIS;
    for body in [create(), open()] {
        chain = append_body(&mut journal, chain, &body);
    }
    journal = publish(journal, chain);
    for sequence in 0..6 {
        if sequence % 2 == 0 {
            journal = journal.roll_active(16 * 1024).unwrap();
        }
        chain = append_body(&mut journal, chain, &append(0x61, sequence, sequence));
    }
    let limits = CanonicalRecoveryLimits {
        accepted_transitions: 1,
        retained_identities: 1,
        ..CanonicalRecoveryLimits::default()
    };
    assert!(matches!(
        journal.recover_canonical_images(limits),
        Err(CanonicalStateRecoveryError::Images(
            CanonicalImagesError::PendingCapacity
        ))
    ));
    let candidate = journal.recover_canonical_candidate(limits).unwrap();
    let known = candidate.committed_images();
    assert_eq!(known.committed().revision(), 2);
    assert_eq!(known.pending_len(), 0);
    assert_eq!(
        known
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::ZERO
    );
    assert_eq!(
        candidate.accepted_position(),
        journal.accepted_position().unwrap()
    );

    // Storage has no quorum authority. The adapter publishes this floor only
    // after consensus confirms the entire selected tail, once per activation.
    journal = publish(journal, chain);
    let mut images = candidate.activate(&journal).unwrap();
    assert_eq!(images.committed().revision(), 8);
    assert_eq!(images.pending_len(), 0);
    assert_eq!(
        images
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::new(6)
    );
    images.admit(9, &append(0x62, 6, 6)).unwrap();
    images.commit_through(9).unwrap();
    assert_eq!(
        images
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::new(7)
    );
}

#[test]
fn candidate_activation_requires_published_whole_tail_and_unchanged_view() {
    let temporary = tempfile::Builder::new()
        .prefix(".canonical-recovery-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let mut journal = format(&temporary);
    let chain = append_body(&mut journal, ChainPosition::GENESIS, &create());
    let limits = CanonicalRecoveryLimits::default();
    let candidate = journal.recover_canonical_candidate(limits).unwrap();
    assert!(matches!(
        candidate.activate(&journal),
        Err(CanonicalStateRecoveryError::CommitNotPublished)
    ));
    let candidate = journal.recover_canonical_candidate(limits).unwrap();
    let mut manifest = journal.directory().manifest().clone();
    manifest.parent_generation = manifest.generation;
    manifest.generation += 1;
    manifest.promised_view = 1;
    journal = journal.install_metadata(manifest).unwrap();
    journal = publish(journal, chain);
    assert!(matches!(
        candidate.activate(&journal),
        Err(CanonicalStateRecoveryError::SelectionChanged)
    ));
    let images = journal
        .recover_canonical_candidate(limits)
        .unwrap()
        .activate(&journal)
        .unwrap();
    assert_eq!(images.committed().revision(), 1);
}

#[test]
fn private_replay_handles_lagging_commit_marker_but_rejects_progress_beyond_prefix() {
    for progress_offset in [0, 1] {
        let temporary = tempfile::Builder::new()
            .prefix(".canonical-recovery-")
            .tempdir_in(env!("CARGO_MANIFEST_DIR"))
            .unwrap();
        let mut journal = format(&temporary);
        let mut chain = ChainPosition::GENESIS;
        for body in [create(), open(), append(0x63, 0, 0)] {
            chain = append_body(&mut journal, chain, &body);
        }
        journal = journal.roll_active(16 * 1024).unwrap();
        let owner = ProgressOwner::Subscription(SubscriptionId::from_bytes([0x64; 16]));
        let body = OperationBody::Progress(Progress {
            partition: partition(),
            owner,
            expected_progress: None,
            new_progress: Offset::new(progress_offset),
            assignment_epoch: None,
            operation_id: OperationId::from_bytes([0x65; 16]),
        });
        chain = append_body(&mut journal, chain, &body);
        let recovered = journal.recover_canonical_candidate(CanonicalRecoveryLimits {
            accepted_transitions: 1,
            retained_identities: 1,
            ..CanonicalRecoveryLimits::default()
        });
        if progress_offset == 1 {
            assert!(matches!(
                recovered,
                Err(CanonicalStateRecoveryError::Images(
                    CanonicalImagesError::State(StateError::ProgressBeyondCommit)
                ))
            ));
            assert_eq!(journal.committed_position().unwrap(), LogPosition::GENESIS);
        } else {
            let candidate = recovered.unwrap();
            assert!(
                candidate
                    .committed_images()
                    .committed()
                    .partition(partition())
                    .is_none()
            );
            journal = publish(journal, chain);
            let images = candidate.activate(&journal).unwrap();
            assert_eq!(
                images.committed().progress(owner, partition()),
                Some(Offset::ZERO)
            );
        }
    }
}

#[test]
fn extended_tail_cannot_activate_an_older_candidate() {
    let temporary = tempfile::Builder::new()
        .prefix(".canonical-recovery-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let mut journal = format(&temporary);
    let chain = append_body(&mut journal, ChainPosition::GENESIS, &create());
    let candidate = journal
        .recover_canonical_candidate(CanonicalRecoveryLimits::default())
        .unwrap();
    let chain = append_body(&mut journal, chain, &open());
    journal = publish(journal, chain);
    assert!(matches!(
        candidate.activate(&journal),
        Err(CanonicalStateRecoveryError::SelectionChanged)
    ));
}

#[test]
fn selected_control_history_uses_disk_indexes_not_the_live_identity_overlay() {
    let temporary = tempfile::Builder::new()
        .prefix(".canonical-recovery-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let mut journal = format(&temporary);
    let mut chain = ChainPosition::GENESIS;
    for number in 1..=10 {
        if number > 1 && number % 3 == 1 {
            journal = journal.roll_active(16 * 1024).unwrap();
        }
        chain = append_body(
            &mut journal,
            chain,
            &OperationBody::Barrier(Barrier {
                operation_id: OperationId::from_bytes([number; 16]),
            }),
        );
    }
    let candidate = journal
        .recover_canonical_candidate(CanonicalRecoveryLimits {
            accepted_transitions: 1,
            retained_identities: 1,
            ..CanonicalRecoveryLimits::default()
        })
        .unwrap();
    assert_eq!(candidate.committed_images().committed().revision(), 0);
    journal = publish(journal, chain);
    let images = candidate.activate(&journal).unwrap();
    assert!(images.committed_identities().overlay().is_empty());
    for number in 1..=10 {
        let claim = images
            .committed_identities()
            .lookup(IdentityKey::operation(OperationId::from_bytes(
                [number; 16],
            )))
            .unwrap()
            .unwrap();
        assert_eq!(claim.op_number, u64::from(number));
    }
}

#[test]
fn canonical_checkpoint_recovers_after_covered_prefix_is_deleted() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("group");
    let mut journal = format(&temporary);
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(16);
    let mut chain = ChainPosition::GENESIS;
    for body in [create(), open()] {
        chain = append_body(&mut journal, chain, &body);
        apply_body(&mut state, &mut identities, &body);
    }
    let journal = publish(journal, chain);
    let checkpoint_id = CheckpointId::from_bytes([0x50; 16]);
    journal
        .canonical_checkpoint_plan(checkpoint_id, 1024)
        .unwrap()
        .build_canonical(
            &state,
            StateSnapshotLimits::default(),
            CheckpointLimits::default(),
        )
        .unwrap();
    let journal = journal
        .install_canonical_checkpoint(
            checkpoint_id,
            StateLimits::default(),
            StateSnapshotLimits::default(),
            CheckpointLimits::default(),
        )
        .unwrap();
    let mut journal = journal.roll_active(16 * 1024).unwrap();
    let body = append(0x60, 0, 0);
    chain = append_body(&mut journal, chain, &body);
    apply_body(&mut state, &mut identities, &body);
    let journal = publish(journal, chain);
    let floors = RetentionFloors::from_canonical_state(&state).unwrap();
    let (journal, result) = journal.trim_sealed_prefix(&floors).unwrap();
    assert_eq!(result.removed_segment_ids, [1]);
    drop(journal);

    let reopened = GroupDirectory::open(&root, identity(), MetadataLimits::default())
        .unwrap()
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let recovered = reopened
        .recover_canonical_images(CanonicalRecoveryLimits {
            retained_identities: 16,
            accepted_transitions: 16,
            ..CanonicalRecoveryLimits::default()
        })
        .unwrap();
    assert_eq!(recovered.committed(), &state);
    assert_eq!(recovered.speculative(), &state);
    let recovered = reopened
        .recover_canonical_candidate(CanonicalRecoveryLimits::default())
        .unwrap()
        .activate(&reopened)
        .unwrap();
    assert_eq!(recovered.committed(), &state);
}

#[test]
fn checkpoint_recovery_rejects_hash_valid_duplicate_control_identity() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("group");
    let mut journal = format(&temporary);
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(16);
    let mut chain = ChainPosition::GENESIS;
    let barrier = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([0x71; 16]),
    });
    for body in [create(), open(), barrier.clone()] {
        chain = append_body(&mut journal, chain, &body);
        apply_body(&mut state, &mut identities, &body);
    }
    let journal = publish(journal, chain);
    let checkpoint_id = CheckpointId::from_bytes([0x51; 16]);
    journal
        .canonical_checkpoint_plan(checkpoint_id, 1024)
        .unwrap()
        .build_canonical(
            &state,
            StateSnapshotLimits::default(),
            CheckpointLimits::default(),
        )
        .unwrap();
    let journal = journal
        .install_canonical_checkpoint(
            checkpoint_id,
            StateLimits::default(),
            StateSnapshotLimits::default(),
            CheckpointLimits::default(),
        )
        .unwrap();
    let mut journal = journal.roll_active(16 * 1024).unwrap();
    chain = append_body(&mut journal, chain, &barrier);
    let journal = publish(journal, chain);
    drop(journal);

    let reopened = GroupDirectory::open(&root, identity(), MetadataLimits::default())
        .unwrap()
        .recover(
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert!(matches!(
        reopened.recover_canonical_images(CanonicalRecoveryLimits {
            retained_identities: 16,
            accepted_transitions: 16,
            ..CanonicalRecoveryLimits::default()
        }),
        Err(
            CanonicalStateRecoveryError::Images(CanonicalImagesError::State(
                StateError::IdentityConflict,
            )) | CanonicalStateRecoveryError::Identity(IdentityIndexError::Conflict)
        )
    ));
    assert!(matches!(
        reopened.recover_canonical_candidate(CanonicalRecoveryLimits::default()),
        Err(
            CanonicalStateRecoveryError::Images(CanonicalImagesError::State(
                StateError::IdentityConflict,
            )) | CanonicalStateRecoveryError::Identity(IdentityIndexError::Conflict)
        )
    ));
}

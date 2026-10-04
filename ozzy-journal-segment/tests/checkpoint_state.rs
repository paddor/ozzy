use ozzy_core::state::{
    CanonicalState, MemoryIdentityIndex, StateLimits, StateSnapshotLimits,
    canonical_state_schema_digest,
};
use ozzy_journal::operation::{Barrier, OperationBody, encode_operation_body};
use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    CanonicalOperation, CanonicalRecoveryLimits, CheckpointLimits, DecodeLimits, Digest,
    GroupDirectory, GroupIdentity, LogPosition, MetadataLimits, OperationKind, OperationLimits,
    RetentionFloors, SegmentHeader, checkpoint_name, open_checkpoint,
};
use ozzy_proto::{CheckpointId, GroupId, NodeId, OperationId, StoreId, VolumeId};
use tempfile::TempDir;

#[test]
#[expect(clippy::too_many_lines, reason = "linear typed checkpoint lifecycle")]
fn canonical_state_checkpoint_matches_exact_journal_position() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("group");
    let identity = GroupIdentity {
        group_id: GroupId::from_bytes([0x11; 16]),
        replica_node_id: NodeId::from_bytes([0x12; 16]),
        volume_id: VolumeId::from_bytes([0x13; 16]),
        store_id: StoreId::from_bytes([0x14; 16]),
        store_generation: 1,
    };
    let operation_id = OperationId::from_bytes([0x21; 16]);
    let body = OperationBody::Barrier(Barrier { operation_id });
    let body_bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
    let segment = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap();
    let directory = GroupDirectory::format_new(&root, identity, 1, &segment).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal
        .append(&[CanonicalOperation {
            group_id: identity.group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind: OperationKind::Barrier,
            body: &body_bytes,
        }])
        .unwrap();
    journal.sync_through(written).unwrap();
    let mut manifest = journal.directory().manifest().clone();
    manifest.generation += 1;
    manifest.parent_generation = journal.directory().manifest().generation;
    manifest.accepted = LogPosition {
        op_number: 1,
        digest: written.next_chain().previous_digest(),
    };
    manifest.committed = manifest.accepted;
    let journal = journal.install_metadata(manifest).unwrap();

    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(8);
    let plan = state.prepare(1, &body, &identities, &state).unwrap();
    state.apply(plan, &mut identities).unwrap();
    assert_eq!(
        state.revision(),
        journal.committed_position().unwrap().op_number
    );
    let checkpoint_id = CheckpointId::from_bytes([0x31; 16]);
    let prepared_checkpoint = journal
        .canonical_checkpoint_plan(checkpoint_id, 16 * 1024)
        .unwrap()
        .build_canonical(
            &state,
            StateSnapshotLimits::default(),
            CheckpointLimits::default(),
        )
        .unwrap();
    let cleanup = journal.reclaim_unreferenced_checkpoints().unwrap();
    assert_eq!(cleanup.removed_checkpoint_ids.len(), 0);
    assert_eq!(cleanup.pinned_checkpoint_ids, [checkpoint_id]);
    let journal = journal
        .install_canonical_checkpoint(
            checkpoint_id,
            StateLimits::default(),
            StateSnapshotLimits::default(),
            CheckpointLimits::default(),
        )
        .unwrap();
    drop(prepared_checkpoint);
    assert_eq!(
        journal
            .selected_canonical_checkpoint(
                StateLimits::default(),
                StateSnapshotLimits::default(),
                CheckpointLimits::default(),
            )
            .unwrap(),
        Some(state.clone())
    );
    let recovered = journal
        .recover_canonical_images(CanonicalRecoveryLimits {
            retained_identities: 8,
            accepted_transitions: 8,
            ..CanonicalRecoveryLimits::default()
        })
        .unwrap();
    assert_eq!(recovered.committed(), &state);
    assert_eq!(recovered.speculative(), &state);
    drop(recovered);
    drop(journal);

    let directory = GroupDirectory::open(&root, identity, MetadataLimits::default()).unwrap();
    let reference = directory.manifest().checkpoint.unwrap();
    let checkpoint = open_checkpoint(
        root.join("checkpoints")
            .join(checkpoint_name(checkpoint_id)),
        identity.group_id,
        identity.store_id,
        CheckpointLimits::default(),
    )
    .unwrap();
    assert_eq!(checkpoint.manifest().position, reference.position);
    assert_eq!(
        checkpoint.manifest().state_schema_digest,
        canonical_state_schema_digest()
    );
    let restored = CanonicalState::decode_snapshot(
        &checkpoint.read_state(CheckpointLimits::default()).unwrap(),
        StateLimits::default(),
        StateSnapshotLimits::default(),
    )
    .unwrap();
    assert_eq!(restored, state);
    assert_eq!(restored.revision(), reference.position.op_number);
    assert!(
        RetentionFloors::from_canonical_state(&restored)
            .unwrap()
            .is_empty()
    );
}

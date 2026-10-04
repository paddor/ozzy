use super::*;
mod cooperation;
use crate::{
    CanonicalCheckpointError, CanonicalRecoveryLimits, CanonicalStateRecoveryError as Error,
};
use ozzy_core::state::{
    CanonicalImagesError, CanonicalState, IdentityIndex, IdentityKey, MemoryIdentityIndex,
    StateError, StateLimits, StateSnapshotLimits,
};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Barrier, CreatePartition, OpenProducer, OperationBody,
    Progress, ProgressOwner, RetentionPolicy, encode_operation_body,
};
use ozzy_proto::{
    CheckpointId, MessageId, Offset, OperationId, OwnerEpoch, PartitionId, PartitionIncarnation,
    ProducerEpoch, ProducerId, ProducerSequence, SubscriptionId,
};

fn recovery_limits() -> CanonicalRecoveryLimits {
    CanonicalRecoveryLimits {
        index: super::indexes::build_limits(),
        checkpoint: limits().checkpoint,
        retained_identities: 2,
        accepted_transitions: 2,
        ..CanonicalRecoveryLimits::default()
    }
}
fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([0x20; 16])
}
fn producer(id: u8) -> ProducerId {
    ProducerId::from_bytes([id; 16])
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
fn open_producer(id: u8) -> OperationBody<'static> {
    OperationBody::OpenProducer(OpenProducer {
        partition: partition(),
        producer_id: producer(id),
        expected_epoch: None,
        new_epoch: ProducerEpoch::INITIAL,
        operation_id: OperationId::from_bytes([id; 16]),
    })
}
fn append(id: u8, sequence: u64, offset: u64) -> OperationBody<'static> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id: producer(id),
            producer_epoch: ProducerEpoch::INITIAL,
            first_sequence: ProducerSequence::new(sequence),
            first_offset: Offset::new(offset),
            append_timestamp_millis: 100,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([0x40; 16]),
                parts: vec![b"payload".as_slice()].into(),
            }]
            .into(),
        }],
    })
}
fn barrier(id: u8) -> OperationBody<'static> {
    OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([id; 16]),
    })
}
async fn write(journal: &mut Journal, body: &OperationBody<'_>) {
    let bytes = encode_operation_body(body, limits().operations).unwrap();
    let operation = CanonicalOperation {
        kind: body.kind(),
        body: &bytes,
        ..operation(journal)
    };
    let written = journal
        .append(&[operation], BodyEncoding::Raw)
        .await
        .unwrap();
    journal.sync_through(written).await.unwrap();
}
async fn confirm(journal: &mut Journal) {
    let mut next = journal.next_manifest().unwrap();
    next.accepted = journal.accepted_position().unwrap();
    next.committed = next.accepted;
    journal.install_metadata(next).await.unwrap();
}
fn apply(
    state: &mut CanonicalState,
    identities: &mut MemoryIdentityIndex,
    body: &OperationBody<'_>,
) {
    let plan = state
        .prepare(state.revision() + 1, body, identities, state)
        .unwrap();
    state.apply(plan, identities).unwrap();
}

#[test]
fn async_canonical_recovery_preserves_committed_and_speculative_identities() {
    let (mut controller, mut journal) = empty_journal();
    for body in [create(), open_producer(1)] {
        drive(&mut controller, write(&mut journal, &body));
    }
    drive(&mut controller, confirm(&mut journal));
    for body in [append(1, 0, 0), barrier(2)] {
        drive(&mut controller, write(&mut journal, &body));
    }
    let mut images = drive(
        &mut controller,
        journal.recover_canonical_images(recovery_limits()),
    )
    .unwrap();
    assert_eq!(images.committed().revision(), 2);
    assert_eq!(images.speculative().revision(), 4);
    assert_eq!(images.pending_len(), 2);
    assert!(
        drive(
            &mut controller,
            images
                .committed_identities()
                .snapshot()
                .read_message(partition(), MessageId::from_bytes([0x40; 16]),)
        )
        .unwrap()
        .is_none()
    );
    let key = IdentityKey::operation(OperationId::from_bytes([2; 16]));
    drive(
        &mut controller,
        images.committed_identities().resolve(&[key]),
    )
    .unwrap();
    drive(
        &mut controller,
        images.speculative_identities().resolve(&[key]),
    )
    .unwrap();
    assert_eq!(images.committed_identities().lookup(key), Ok(None));
    assert_eq!(
        images
            .speculative_identities()
            .lookup(key)
            .unwrap()
            .unwrap()
            .op_number,
        4
    );
    images.commit_through(4).unwrap();
    assert_eq!(
        images
            .committed_identities()
            .lookup(key)
            .unwrap()
            .unwrap()
            .op_number,
        4
    );
    assert_eq!(
        images
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::new(1)
    );
    let key = IdentityKey::operation(OperationId::from_bytes([3; 16]));
    drive(
        &mut controller,
        images.speculative_identities().resolve(&[key]),
    )
    .unwrap();
    images.admit(5, &barrier(3)).unwrap();
    images.commit_through(5).unwrap();
    assert_eq!(images.committed().revision(), 5);
    assert_eq!(controller.jobs().len(), 0);
}

#[test]
fn async_candidate_keeps_large_selected_tail_private_and_checks_activation() {
    let (mut controller, mut journal) = empty_journal();
    for body in [create(), open_producer(1)] {
        drive(&mut controller, write(&mut journal, &body));
    }
    drive(&mut controller, confirm(&mut journal));
    for sequence in 0..6 {
        if sequence % 2 == 0 {
            drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        }
        drive(
            &mut controller,
            write(&mut journal, &append(1, sequence, sequence)),
        );
    }
    let limits = CanonicalRecoveryLimits {
        accepted_transitions: 1,
        retained_identities: 1,
        ..recovery_limits()
    };
    assert!(matches!(
        drive(&mut controller, journal.recover_canonical_images(limits)),
        Err(Error::Images(CanonicalImagesError::PendingCapacity))
    ));
    let candidate = drive(&mut controller, journal.recover_canonical_candidate(limits)).unwrap();
    assert_eq!(candidate.committed_images().committed().revision(), 2);
    assert_eq!(candidate.committed_images().pending_len(), 0);
    assert_eq!(
        candidate
            .committed_images()
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::ZERO
    );
    assert!(matches!(
        candidate.activate(&journal),
        Err(Error::CommitNotPublished)
    ));
    let candidate = drive(&mut controller, journal.recover_canonical_candidate(limits)).unwrap();
    let mut next = journal.next_manifest().unwrap();
    next.promised_view = 1;
    drive(&mut controller, journal.install_metadata(next)).unwrap();
    drive(&mut controller, confirm(&mut journal));
    assert!(matches!(
        candidate.activate(&journal),
        Err(Error::SelectionChanged)
    ));
    let candidate = drive(&mut controller, journal.recover_canonical_candidate(limits)).unwrap();
    let mut images = candidate.activate(&journal).unwrap();
    assert_eq!(images.committed().revision(), 8);
    assert_eq!(images.pending_len(), 0);
    images.admit(9, &append(1, 6, 6)).unwrap();
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
fn async_private_replay_checks_progress_against_only_the_preceding_prefix() {
    for progress_offset in [0, 1] {
        let (mut controller, mut journal) = empty_journal();
        for body in [create(), open_producer(1), append(1, 0, 0)] {
            drive(&mut controller, write(&mut journal, &body));
        }
        drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        let owner = ProgressOwner::Subscription(SubscriptionId::from_bytes([0x64; 16]));
        let progress = OperationBody::Progress(Progress {
            partition: partition(),
            owner,
            expected_progress: None,
            new_progress: Offset::new(progress_offset),
            assignment_epoch: None,
            operation_id: OperationId::from_bytes([0x65; 16]),
        });
        drive(&mut controller, write(&mut journal, &progress));
        let result = drive(
            &mut controller,
            journal.recover_canonical_candidate(recovery_limits()),
        );
        if progress_offset == 1 {
            assert!(matches!(
                result,
                Err(Error::Images(CanonicalImagesError::State(
                    StateError::ProgressBeyondCommit
                )))
            ));
        } else {
            let candidate = result.unwrap();
            assert_eq!(candidate.committed_images().committed().revision(), 0);
            drive(&mut controller, confirm(&mut journal));
            assert_eq!(
                candidate
                    .activate(&journal)
                    .unwrap()
                    .committed()
                    .progress(owner, partition()),
                Some(Offset::ZERO)
            );
        }
    }
}

#[test]
fn async_recovery_checks_control_duplicates_before_and_after_checkpoint() {
    let (mut controller, mut journal) = empty_journal();
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(32);
    // Historical control identities exceed the final live-overlay capacity.
    for id in 1..=6 {
        drive(&mut controller, write(&mut journal, &barrier(id)));
        apply(&mut state, &mut identities, &barrier(id));
        if id % 2 == 0 {
            drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        }
    }
    drive(&mut controller, confirm(&mut journal));
    let images = drive(
        &mut controller,
        journal.recover_canonical_images(recovery_limits()),
    )
    .unwrap();
    assert_eq!(images.committed(), &state);
    let id = CheckpointId::from_bytes([10; 16]);
    let built = drive(
        &mut controller,
        journal.build_canonical_checkpoint(id, 4096, &state, StateSnapshotLimits::default()),
    )
    .unwrap();
    assert_eq!(
        drive(
            &mut controller,
            built.decode_canonical_state(StateLimits::default(), StateSnapshotLimits::default())
        )
        .unwrap(),
        state
    );
    drive(
        &mut controller,
        journal.install_canonical_checkpoint(
            id,
            StateLimits::default(),
            StateSnapshotLimits::default(),
            limits().checkpoint,
        ),
    )
    .unwrap();
    assert_eq!(
        drive(
            &mut controller,
            journal.selected_canonical_checkpoint(
                StateLimits::default(),
                StateSnapshotLimits::default(),
                limits().checkpoint
            )
        )
        .unwrap(),
        Some(state)
    );
    drive(&mut controller, write(&mut journal, &barrier(1)));
    let result = drive(
        &mut controller,
        journal.recover_canonical_candidate(recovery_limits()),
    );
    assert!(matches!(
        result,
        Err(Error::Images(CanonicalImagesError::State(
            StateError::IdentityConflict
        )))
    ));
}

#[tokio::test]
async fn async_checkpoint_plus_replay_preserves_interleaved_writer_results_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("group");
    let (pool, mut clients) = ozzy_io_pool::Pool::new(ozzy_io_pool::Config {
        threads: 1,
        max_inflight: 1,
        handles: 32,
        limits: io_limits(),
    })
    .unwrap();
    let io = Local::new(clients.remove(0));
    let mut journal = Journal::format(
        path.clone(),
        io.clone(),
        spec(CommitMode::External),
        JournalGeneration(1),
        limits(),
    )
    .await
    .unwrap();
    let mut state = CanonicalState::new(StateLimits::default());
    let mut identities = MemoryIdentityIndex::new(8);
    for body in [create(), open_producer(1), open_producer(2)] {
        write(&mut journal, &body).await;
        apply(&mut state, &mut identities, &body);
    }
    journal.roll_active(32768, 4).await.unwrap();
    for (offset, (id, sequence)) in [(1, 0), (2, 0), (1, 1), (2, 1)].into_iter().enumerate() {
        let body = append(id, sequence, offset as u64);
        write(&mut journal, &body).await;
        apply(&mut state, &mut identities, &body);
        if offset == 1 {
            confirm(&mut journal).await;
            let id = CheckpointId::from_bytes([0x60; 16]);
            journal
                .build_canonical_checkpoint(id, 4096, &state, StateSnapshotLimits::default())
                .await
                .unwrap();
            journal
                .install_canonical_checkpoint(
                    id,
                    StateLimits::default(),
                    StateSnapshotLimits::default(),
                    limits().checkpoint,
                )
                .await
                .unwrap();
            journal.roll_active(32768, 4).await.unwrap();
        }
    }
    confirm(&mut journal).await;
    drop(journal);
    let mut reopened = Journal::open(
        path,
        io,
        identity(),
        Some(b"test deployment"),
        JournalGeneration(2),
        limits(),
    )
    .await
    .unwrap();
    let images = reopened
        .recover_canonical_images(recovery_limits())
        .await
        .unwrap();
    assert_eq!(images.committed(), &state);
    for (id, sequence, offset) in [(1, 0, 0), (2, 0, 1), (1, 1, 2), (2, 1, 3)] {
        assert_eq!(
            images
                .committed()
                .partition(partition())
                .unwrap()
                .producer(producer(id))
                .unwrap()
                .result_offset(ProducerSequence::new(sequence))
                .unwrap(),
            Offset::new(offset)
        );
    }
    drop(images);
    drop(reopened);
    pool.shutdown().await;
}

#[test]
fn async_typed_checkpoint_rejects_wrong_revision_and_schema_before_selection() {
    let (mut controller, mut journal) = empty_journal();
    drive(&mut controller, write(&mut journal, &barrier(1)));
    drive(&mut controller, confirm(&mut journal));
    let initial = journal.current();
    let id = CheckpointId::from_bytes([10; 16]);
    let state = CanonicalState::new(StateLimits::default());
    assert!(matches!(
        drive(
            &mut controller,
            journal.build_canonical_checkpoint(id, 4096, &state, StateSnapshotLimits::default())
        ),
        Err(CanonicalCheckpointError::PositionMismatch)
    ));
    drive(
        &mut controller,
        journal.build_checkpoint(id, Digest::from_bytes([99; 32]), 4096, b"wrong schema"),
    )
    .unwrap();
    assert!(matches!(
        drive(
            &mut controller,
            journal.install_canonical_checkpoint(
                id,
                StateLimits::default(),
                StateSnapshotLimits::default(),
                limits().checkpoint
            )
        ),
        Err(CanonicalCheckpointError::SchemaMismatch)
    ));
    assert_eq!(journal.current(), initial);
    assert!(!journal.is_faulted());
}

#[test]
fn canceled_async_replay_leaves_no_partial_candidate_or_authority_change() {
    let (mut controller, mut journal) = empty_journal();
    drive(&mut controller, write(&mut journal, &barrier(1)));
    let before = journal.current();
    let mut future = Box::pin(journal.recover_canonical_candidate(recovery_limits()));
    // Stop on the first data read after catalog validation. Observer cancellation
    // must leave this read owned by the backend and expose no partially replayed state.
    loop {
        assert!(poll(future.as_mut()).is_pending());
        let (id, stage) = controller.jobs()[0];
        assert_eq!(stage, Stage::Queued);
        if matches!(
            controller.operation(id).unwrap().unprotected(),
            Operation::Read { .. }
        ) {
            drop(future);
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
            break;
        }
        controller.execute(id, Effect::Normal).unwrap();
        controller.deliver(id).unwrap();
    }
    assert_eq!(journal.current(), before);
    assert!(!journal.is_faulted());
    let candidate = drive(
        &mut controller,
        journal.recover_canonical_candidate(recovery_limits()),
    )
    .unwrap();
    assert_eq!(candidate.committed_images().committed().revision(), 0);
    drive(&mut controller, write(&mut journal, &barrier(2)));
    drive(&mut controller, confirm(&mut journal));
    assert!(matches!(
        candidate.activate(&journal),
        Err(Error::SelectionChanged)
    ));
}

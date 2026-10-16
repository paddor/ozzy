use super::*;
use ozzy_core::state::ProducerResultSource;
use ozzy_journal::operation::{AppendResults, ResolvedAppend, RetryFingerprint};
use ozzy_proto::append::{AppendDisposition, AppendOutcome};

fn resolved<'a>(
    sequence: u64,
    offset: u64,
    time: u64,
    outcomes: &'a [AppendOutcome],
    proofs: &'a [RetryFingerprint],
) -> OperationBody<'a> {
    let results = AppendResults::classified(time, outcomes, proofs, limits().operations).unwrap();
    let records = (0..results.stored())
        .map(|index| AppendRecord {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([0x50 + index as u8; 16]),
            parts: vec![b"payload".as_slice()].into(),
        })
        .collect();
    OperationBody::ResolvedAppend(ResolvedAppend {
        batch: AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id: producer(1),
            producer_epoch: ProducerEpoch::INITIAL,
            first_sequence: ProducerSequence::new(sequence),
            first_offset: Offset::new(offset),
            append_timestamp_millis: 100,
            records,
        },
        results,
    })
}

fn outcome(disposition: AppendDisposition, offset: u64) -> AppendOutcome {
    AppendOutcome {
        disposition,
        offset: Offset::new(offset),
    }
}

fn resolved_recovery_limits() -> CanonicalRecoveryLimits {
    let mut limits = recovery_limits();
    limits.index.max_entry_buffer_bytes = 1024;
    limits
}

#[test]
fn resolved_attempts_recover_commit_boundaries_and_index_only_stored_records() {
    use AppendDisposition::{Duplicate, Stored};
    let (mut controller, mut journal) = empty_journal();
    for body in [create(), open_producer(1), append(1, 0, 0)] {
        drive(&mut controller, write(&mut journal, &body));
    }
    let mixed = [
        outcome(Duplicate, 0),
        outcome(Stored, 1),
        outcome(Duplicate, 0),
        outcome(Stored, 2),
    ];
    let proofs = [RetryFingerprint::from_bytes([0xa1; 32]); 2];
    drive(
        &mut controller,
        write(&mut journal, &resolved(1, 1, 100, &mixed, &proofs)),
    );
    drive(&mut controller, confirm(&mut journal));
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    let duplicates = [outcome(Duplicate, 0), outcome(Duplicate, 1)];
    drive(
        &mut controller,
        write(&mut journal, &resolved(5, 3, 101, &duplicates, &proofs)),
    );
    let mut images = drive(
        &mut controller,
        journal.recover_canonical_images(resolved_recovery_limits()),
    )
    .unwrap();
    assert_eq!(images.committed().revision(), 4);
    assert_eq!(images.speculative().revision(), 5);
    assert_eq!(images.pending_len(), 1);
    let committed = images.committed().partition(partition()).unwrap();
    let accepted = images.speculative().partition(partition()).unwrap();
    assert_eq!(
        committed
            .producer(producer(1))
            .unwrap()
            .next_producer_sequence,
        ProducerSequence::new(5)
    );
    assert_eq!(
        accepted
            .producer(producer(1))
            .unwrap()
            .next_producer_sequence,
        ProducerSequence::new(7)
    );
    assert_eq!(
        (committed.next_offset, accepted.next_offset),
        (Offset::new(3), Offset::new(3))
    );
    for (index, sequence, offset) in [(0, 2, 1), (1, 4, 2)] {
        let record = drive(
            &mut controller,
            images
                .committed_identities()
                .snapshot()
                .read_message(partition(), MessageId::from_bytes([0x50 + index; 16])),
        )
        .unwrap()
        .unwrap();
        assert_eq!(record.producer_sequence, ProducerSequence::new(sequence));
        assert_eq!(record.offset, Offset::new(offset));
        assert_eq!(record.parts[0].as_ref(), b"payload");
    }
    images.discard_suffix();
    assert_eq!(images.committed(), images.speculative());
    drive(&mut controller, confirm(&mut journal));
    let restored = drive(
        &mut controller,
        journal.recover_canonical_images(resolved_recovery_limits()),
    )
    .unwrap();
    let state = restored.committed();
    let session = state
        .partition(partition())
        .unwrap()
        .producer(producer(1))
        .unwrap();
    assert_eq!(session.next_producer_sequence, ProducerSequence::new(7));
    assert_eq!(
        session
            .result_span(ProducerSequence::new(5))
            .unwrap()
            .source,
        ProducerResultSource::Duplicate { operation: 5 }
    );
    assert_eq!(state.partition(partition()).unwrap().dedup_time, 101);
    check_checkpoint(&mut controller, &mut journal, state);
    drive(&mut controller, journal.close()).unwrap();
}

#[test]
fn payload_free_results_read_by_number_across_cold_sources_and_snapshot_boundaries() {
    let (mut controller, mut journal) = empty_journal();
    for body in [create(), open_producer(1), append(1, 0, 0)] {
        drive(&mut controller, write(&mut journal, &body));
    }
    drive(&mut controller, confirm(&mut journal));
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    let outcomes = [outcome(AppendDisposition::Duplicate, 0)];
    for sequence in 1..=7 {
        let proofs = [RetryFingerprint::from_bytes([sequence as u8; 32])];
        drive(
            &mut controller,
            write(
                &mut journal,
                &resolved(sequence, 1, 100 + sequence, &outcomes, &proofs),
            ),
        );
        drive(&mut controller, confirm(&mut journal));
        if sequence < 7 {
            drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
        }
    }
    let snapshot = drive(
        &mut controller,
        journal.build_index_snapshot(
            crate::JournalIndexBoundary::Committed,
            resolved_recovery_limits().index,
        ),
    )
    .unwrap();
    assert_eq!(snapshot.through().op_number, 10);
    let indexed = drive(&mut controller, snapshot.find_operation_number(4))
        .unwrap()
        .unwrap();
    let mut read_bytes = 0;
    let operation = drive_with(&mut controller, snapshot.read_operation_number(4), |job| {
        if let Operation::Read { length, .. } = job {
            read_bytes += length;
        }
        Effect::Normal
    })
    .unwrap()
    .unwrap();
    assert_eq!(operation.op_number, 4);
    assert_eq!(
        read_bytes as u64,
        crate::SEGMENT_HEADER_BYTES as u64 + indexed.location.entry_bytes
    );
    // Visit more than the four cached sealed indexes, in both directions.
    for number in (4..=10).chain((4..=10).rev()) {
        let operation = drive(&mut controller, snapshot.read_operation_number(number))
            .unwrap()
            .unwrap();
        assert_eq!(operation.op_number, number);
        let decoded = ozzy_journal::operation::decode_resolved_append(
            operation.body.as_ref(),
            limits().operations,
        )
        .unwrap();
        assert_eq!(
            (decoded.results.stored(), decoded.results.attempts()),
            (0, 1)
        );
        assert_eq!(
            decoded.results.duplicate_fingerprints().collect::<Vec<_>>(),
            [RetryFingerprint::from_bytes([(number - 3) as u8; 32])]
        );
    }
    let proofs = [RetryFingerprint::from_bytes([8; 32])];
    drive(
        &mut controller,
        write(&mut journal, &resolved(8, 1, 108, &outcomes, &proofs)),
    );
    for outside in [0, 11, u64::MAX] {
        assert!(
            drive(&mut controller, snapshot.read_operation_number(outside))
                .unwrap()
                .is_none()
        );
    }
    let later = drive(
        &mut controller,
        journal.build_index_snapshot(
            crate::JournalIndexBoundary::Written,
            resolved_recovery_limits().index,
        ),
    )
    .unwrap();
    assert!(
        drive(&mut controller, later.read_operation_number(11))
            .unwrap()
            .is_some()
    );
    assert_eq!(controller.jobs().len(), 0);
}

#[test]
fn numeric_retry_reads_refuse_modified_authoritative_operation_bytes() {
    let (mut controller, mut journal) = empty_journal();
    for body in [create(), open_producer(1), append(1, 0, 0)] {
        drive(&mut controller, write(&mut journal, &body));
    }
    let outcomes = [outcome(AppendDisposition::Duplicate, 0)];
    let proofs = [RetryFingerprint::from_bytes([0x91; 32])];
    drive(
        &mut controller,
        write(&mut journal, &resolved(1, 1, 101, &outcomes, &proofs)),
    );
    drive(&mut controller, confirm(&mut journal));
    let snapshot = drive(
        &mut controller,
        journal.build_index_snapshot(
            crate::JournalIndexBoundary::Committed,
            resolved_recovery_limits().index,
        ),
    )
    .unwrap();
    let indexed = drive(&mut controller, snapshot.find_operation_number(4))
        .unwrap()
        .unwrap();
    assert!(
        drive(&mut controller, snapshot.read_operation_number(4))
            .unwrap()
            .is_some()
    );
    let reference = snapshot
        .segment_references()
        .iter()
        .find(|r| r.segment_id == indexed.source.segment_id)
        .unwrap();
    let path = journal.root().join("segments").join(reference.file_name());
    let file = drive(
        &mut controller,
        journal
            .access
            .open(path, ozzy_io::OpenMode::ReadWrite, false, false),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(
            &file,
            indexed.location.entry_offset + crate::ENTRY_HEADER_BYTES as u64,
            &[0xff],
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.done(Operation::Close { handle: file }),
    )
    .unwrap();
    assert!(matches!(
        drive(&mut controller, snapshot.read_operation_number(4)),
        Err(crate::JournalIndexError::Read(
            crate::IndexedReadError::Codec(_)
        ))
    ));
    assert!(!journal.is_faulted());
    assert_eq!(controller.jobs().len(), 0);
}

fn check_checkpoint(controller: &mut Controller, journal: &mut Journal, state: &CanonicalState) {
    let id = CheckpointId::from_bytes([0x77; 16]);
    let built = drive(
        controller,
        journal.build_canonical_checkpoint(id, 4096, state, StateSnapshotLimits::default()),
    )
    .unwrap();
    assert_eq!(
        drive(
            controller,
            built.decode_canonical_state(StateLimits::default(), StateSnapshotLimits::default())
        )
        .unwrap(),
        *state
    );
    drive(
        controller,
        journal.install_canonical_checkpoint(
            id,
            StateLimits::default(),
            StateSnapshotLimits::default(),
            limits().checkpoint,
        ),
    )
    .unwrap();
    let recovered = drive(
        controller,
        journal.recover_canonical_images(resolved_recovery_limits()),
    )
    .unwrap();
    assert_eq!(recovered.committed(), state);
    assert_eq!(recovered.pending_len(), 0);
}

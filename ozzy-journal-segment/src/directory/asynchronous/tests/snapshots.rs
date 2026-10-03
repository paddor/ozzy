use super::empty_journal as empty;
use super::indexes::{append_body, build_limits, partition, populate};
use super::*;
use crate::{
    AsyncJournalIdentityIndex, JournalIdentityHandoffError, JournalIndexBoundary as Boundary,
    JournalIndexError,
};
use ozzy_core::state::{IdentityClaim, IdentityIndex, IdentityIndexError, IdentityKey};
use ozzy_journal::ReadLimits;
use ozzy_proto::{MessageId, Offset, OperationId};

async fn append(journal: &mut Journal, offset: u8) -> LogPosition {
    let body = append_body(offset);
    let operation = CanonicalOperation {
        kind: OperationKind::Append,
        body: &body,
        ..operation(journal)
    };
    let written = journal
        .append(&[operation], BodyEncoding::Raw)
        .await
        .unwrap();
    journal.sync_through(written).await.unwrap();
    journal.written_position().unwrap()
}

async fn barrier(journal: &mut Journal, id: u8) -> IdentityClaim {
    let body = [id; 16];
    let op = CanonicalOperation {
        body: &body,
        ..operation(journal)
    };
    let claim = IdentityClaim {
        operation_id: OperationId::from_bytes(body),
        op_number: op.op_number,
    };
    let written = journal.append(&[op], BodyEncoding::Raw).await.unwrap();
    journal.sync_through(written).await.unwrap();
    claim
}

#[test]
fn async_control_retry_read_checks_identity_digest_and_captured_boundary() {
    let (mut controller, mut journal) = empty();
    let first = drive(&mut controller, barrier(&mut journal, 1));
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    let snapshot = drive(
        &mut controller,
        journal.build_index_snapshot(Boundary::Accepted, build_limits()),
    )
    .unwrap();
    let later = drive(&mut controller, barrier(&mut journal, 2));
    assert!(
        drive(&mut controller, snapshot.read_operation(later.operation_id))
            .unwrap()
            .is_none()
    );
    let operation = drive(&mut controller, snapshot.read_operation(first.operation_id))
        .unwrap()
        .unwrap();
    assert_eq!(operation.kind, OperationKind::Barrier);
    assert_eq!(operation.op_number, first.op_number);
    assert_eq!(operation.body.as_ref(), first.operation_id.as_bytes());
    let location = drive(&mut controller, snapshot.find_operation(first.operation_id))
        .unwrap()
        .unwrap();
    let reference = snapshot
        .segment_references()
        .iter()
        .find(|reference| reference.segment_id == location.source.segment_id)
        .unwrap();
    let file = drive(
        &mut controller,
        journal.access.open(
            journal.root().join("segments").join(reference.file_name()),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(
            &file,
            location.entry.location.entry_offset + crate::ENTRY_HEADER_BYTES as u64,
            &[0xff],
        ),
    )
    .unwrap();
    assert!(drive(&mut controller, snapshot.read_operation(first.operation_id)).is_err());
}

#[test]
fn async_snapshots_keep_selected_boundaries() {
    let (mut controller, mut journal) = empty();
    drive(&mut controller, populate(&mut journal));
    let accepted = drive(
        &mut controller,
        journal.build_index_snapshot(Boundary::Accepted, build_limits()),
    )
    .unwrap();
    let (_, committed_position) = drive(
        &mut controller,
        accepted.read_offset_with_position(partition(), Offset::new(2)),
    )
    .unwrap()
    .unwrap();
    let mut next = journal.next_manifest().unwrap();
    next.accepted = journal.accepted_position().unwrap();
    next.committed = committed_position;
    drive(&mut controller, journal.install_metadata(next)).unwrap();
    let committed = drive(
        &mut controller,
        journal.open_index_snapshot(Boundary::Committed, build_limits().file),
    )
    .unwrap();
    assert_eq!(committed.through(), committed_position);
    assert!(
        drive(
            &mut controller,
            committed.read_offset(partition(), Offset::new(3))
        )
        .unwrap()
        .is_none()
    );
    assert_eq!(
        drive(
            &mut controller,
            accepted.read_message(partition(), MessageId::from_bytes([0x41; 16]))
        )
        .unwrap()
        .unwrap()
        .offset,
        Offset::new(1)
    );
    drive(&mut controller, append(&mut journal, 6));
    assert!(
        drive(
            &mut controller,
            accepted.read_offset(partition(), Offset::new(6))
        )
        .unwrap()
        .is_none()
    );
}

#[test]
fn async_snapshots_ignore_later_active_tail_but_validate_captured_prefix() {
    let (mut controller, mut journal) = empty();
    let first = drive(&mut controller, append(&mut journal, 6));
    drive(&mut controller, append(&mut journal, 7));
    let active = drive(
        &mut controller,
        journal.open_index_snapshot(Boundary::Written, build_limits().file),
    )
    .unwrap();
    let prefix_end = journal.writer.written_position().end_offset();
    drive(&mut controller, append(&mut journal, 8));
    assert!(
        drive(
            &mut controller,
            active.read_offset(partition(), Offset::new(8))
        )
        .unwrap()
        .is_none()
    );
    assert!(drive(&mut controller, active.contains_position(first)).unwrap());
    let handle = drive(
        &mut controller,
        journal.access.open(
            journal
                .root()
                .join("segments")
                .join(journal.manifest.segments.last().unwrap().file_name()),
            ozzy_io::OpenMode::ReadWrite,
            false,
            false,
        ),
    )
    .unwrap();
    drive(
        &mut controller,
        journal.access.write_all(&handle, prefix_end, &[0xff; 8]),
    )
    .unwrap();
    assert!(drive(&mut controller, active.contains_position(first)).unwrap());
    assert_eq!(
        drive(
            &mut controller,
            active.read_offset(partition(), Offset::new(6))
        )
        .unwrap()
        .unwrap()
        .parts[0],
        b"payload"[..]
    );
    let wrong = LogPosition {
        digest: Digest::ZERO,
        ..first
    };
    assert!(!drive(&mut controller, active.contains_position(wrong)).unwrap());
    drive(
        &mut controller,
        journal
            .access
            .write_all(&handle, crate::SEGMENT_HEADER_BYTES as u64, &[0xff; 8]),
    )
    .unwrap();
    assert!(drive(&mut controller, active.contains_position(first)).is_err());
    assert!(
        !journal.is_faulted(),
        "read failures do not mutate journal authority"
    );
}

#[test]
fn async_snapshot_selected_offsets_keep_order_positions_and_limits() {
    let (mut controller, mut journal) = empty();
    let positions = (0..3)
        .map(|offset| drive(&mut controller, append(&mut journal, offset)))
        .collect::<Vec<_>>();
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    let snapshot = drive(
        &mut controller,
        journal.build_index_snapshot(Boundary::Accepted, build_limits()),
    )
    .unwrap();
    let offsets = [2, 0, 2, 1].map(Offset::new);
    for (max_records, max_bytes, expected) in [(3, 14, 2), (3, 100, 3), (2, 100, 2)] {
        let records = drive(
            &mut controller,
            snapshot.read_offsets_with_positions(
                partition(),
                &offsets,
                ReadLimits {
                    max_records,
                    max_bytes,
                },
            ),
        )
        .unwrap();
        assert_eq!(records.len(), expected);
        for ((record, position), offset) in records.iter().zip(&offsets) {
            assert_eq!(record.offset, *offset);
            assert_eq!(*position, positions[offset.get() as usize]);
            assert_eq!(record.parts[0], b"payload"[..]);
        }
    }
    drive(&mut controller, append(&mut journal, 3));
    assert!(matches!(
        drive(
            &mut controller,
            snapshot.read_offsets_with_positions(
                partition(),
                &[Offset::new(3)],
                ReadLimits {
                    max_records: 1,
                    max_bytes: 100,
                },
            ),
        ),
        Err(JournalIndexError::MissingOffset(offset)) if offset == Offset::new(3)
    ));
}

#[test]
fn async_snapshot_ranges_are_bounded_and_reject_missing_offsets() {
    let (mut controller, mut journal) = empty();
    drive(&mut controller, populate(&mut journal));
    let snapshot = drive(
        &mut controller,
        journal.build_index_snapshot(Boundary::Accepted, build_limits()),
    )
    .unwrap();
    let read = |max_records, max_bytes| ReadLimits {
        max_records,
        max_bytes,
    };
    let records = drive(
        &mut controller,
        snapshot.read_range(partition(), Offset::new(1), Offset::new(6), read(3, 100)),
    )
    .unwrap();
    assert_eq!(
        records.iter().map(|r| r.offset.get()).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        drive(
            &mut controller,
            snapshot.read_range(partition(), Offset::new(1), Offset::new(6), read(6, 14))
        )
        .unwrap()
        .len(),
        2
    );
    assert!(matches!(
        drive(
            &mut controller,
            snapshot.read_range(partition(), Offset::new(0), Offset::new(6), read(6, 6))
        ),
        Err(JournalIndexError::RecordExceedsReadLimit {
            actual: 7,
            limit: 6
        })
    ));
    assert!(
        matches!(drive(&mut controller, snapshot.read_range(partition(), Offset::new(5), Offset::new(7), read(6, 100))), Err(JournalIndexError::MissingOffset(offset)) if offset == Offset::new(6))
    );
    assert!(matches!(
        drive(
            &mut controller,
            snapshot.read_range(partition(), Offset::new(0), Offset::new(6), read(0, 100))
        ),
        Err(JournalIndexError::InvalidReadLimits)
    ));
    let mut too_small = build_limits().file;
    too_small.max_file_bytes = crate::INDEX_HEADER_BYTES;
    assert!(
        drive(
            &mut controller,
            journal.open_index_snapshot(Boundary::Accepted, too_small)
        )
        .is_err()
    );
    assert!(!journal.is_faulted());
}

#[test]
fn async_range_reads_each_enclosing_append_once() {
    use ozzy_journal::operation::{
        AppendRecord, OperationBody, decode_operation_body, encode_operation_body,
    };
    for encoding in [
        BodyEncoding::Raw,
        BodyEncoding::Lz4 {
            min_savings_bytes: 0,
        },
    ]
    .into_iter()
    .filter(|encoding| encoding.is_supported())
    {
        let (mut controller, mut journal) = empty();
        let original = append_body(0);
        let OperationBody::Append(mut append) =
            decode_operation_body(OperationKind::Append, &original, limits().operations).unwrap()
        else {
            unreachable!()
        };
        let payload = [0x42; 256];
        append.batches[0].records = (0..3)
            .map(|number| AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([number + 1; 16]),
                parts: vec![payload.as_slice()].into(),
            })
            .collect::<Vec<_>>()
            .into();
        let body =
            encode_operation_body(&OperationBody::Append(append), limits().operations).unwrap();
        let op = CanonicalOperation {
            kind: OperationKind::Append,
            body: &body,
            ..operation(&journal)
        };
        drive(&mut controller, journal.append(&[op], encoding)).unwrap();
        let snapshot = drive(
            &mut controller,
            journal.open_index_snapshot(Boundary::Written, build_limits().file),
        )
        .unwrap();
        let mut entry_reads = 0;
        let records = drive_with(
            &mut controller,
            snapshot.read_range(
                partition(),
                Offset::new(0),
                Offset::new(3),
                ReadLimits {
                    max_records: 3,
                    max_bytes: 1024,
                },
            ),
            |operation| {
                if let Operation::Read { offset, .. } = operation
                    && *offset >= crate::SEGMENT_HEADER_BYTES as u64
                {
                    entry_reads += 1;
                }
                Effect::Normal
            },
        )
        .unwrap();
        assert_eq!(entry_reads, 1);
        assert_eq!(records.len(), 3);
        for (number, record) in records.iter().enumerate() {
            assert_eq!(record.offset.get(), number as u64);
            assert_eq!(record.parts[0], payload[..]);
        }
    }
}

#[test]
fn async_identity_resolution_is_bounded_atomic_and_memory_only() {
    let (mut controller, mut journal) = empty();
    let old = drive(&mut controller, barrier(&mut journal, 1));
    drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    let snapshot = drive(
        &mut controller,
        journal.build_index_snapshot(Boundary::Accepted, build_limits()),
    )
    .unwrap();
    let mut identities = AsyncJournalIdentityIndex::new(snapshot, 2, 2);
    let first = IdentityClaim {
        operation_id: OperationId::from_bytes([2; 16]),
        op_number: 2,
    };
    let second = IdentityClaim {
        operation_id: OperationId::from_bytes([3; 16]),
        op_number: 3,
    };
    assert_eq!(
        identities.lookup(first.key()),
        Err(IdentityIndexError::LookupUnavailable)
    );
    drive(
        &mut controller,
        identities.resolve(&[old.key(), first.key()]),
    )
    .unwrap();
    assert_eq!(identities.lookup(old.key()), Ok(Some(old)));
    assert_eq!(identities.lookup(first.key()), Ok(None));
    assert_eq!(
        identities.reserve(&[first, old]),
        Err(IdentityIndexError::Conflict)
    );
    assert!(identities.overlay().is_empty());
    assert_eq!(
        identities.reserve(&[first, first]),
        Err(IdentityIndexError::Conflict)
    );
    let clone = identities.clone();
    identities.reserve(&[first]).unwrap();
    assert_eq!(identities.lookup(first.key()), Ok(Some(first)));
    assert_eq!(clone.lookup(first.key()), Ok(None));
    assert_eq!(
        identities.reserve(&[second]),
        Err(IdentityIndexError::LookupUnavailable)
    );
    assert_eq!(identities.overlay().len(), 1);
    drive(&mut controller, identities.resolve(&[second.key()])).unwrap();
    identities.reserve(&[second]).unwrap();
    assert_eq!(
        identities.lookup(old.key()),
        Err(IdentityIndexError::LookupUnavailable)
    );
    assert_eq!(
        identities.check_capacity(1),
        Err(IdentityIndexError::Capacity)
    );
    assert!(
        drive(
            &mut controller,
            clone.resolve(&[old.key(), first.key(), second.key()])
        )
        .is_err()
    );
    assert_eq!(
        clone.lookup(old.key()),
        Err(IdentityIndexError::LookupUnavailable)
    );
    assert!(
        controller.jobs().is_empty(),
        "synchronous identity operations submit no file jobs"
    );
}

#[test]
fn async_identity_handoff_checks_lineage_and_every_claim() {
    let (mut controller, mut journal) = empty();
    drive(&mut controller, barrier(&mut journal, 1));
    let initial = drive(
        &mut controller,
        journal.open_index_snapshot(Boundary::Accepted, build_limits().file),
    )
    .unwrap();
    let mut identities = AsyncJournalIdentityIndex::new(initial, 4, 4);
    let claim = drive(&mut controller, barrier(&mut journal, 2));
    drive(&mut controller, identities.resolve(&[claim.key()])).unwrap();
    identities.reserve(&[claim]).unwrap();
    let replacement = drive(
        &mut controller,
        journal.open_index_snapshot(Boundary::Accepted, build_limits().file),
    )
    .unwrap();
    let extended = drive(&mut controller, identities.clone().handoff(replacement)).unwrap();
    assert!(extended.overlay().is_empty());
    assert_eq!(
        extended.lookup(claim.key()),
        Err(IdentityIndexError::LookupUnavailable)
    );
    drive(&mut controller, extended.resolve(&[claim.key()])).unwrap();
    assert_eq!(extended.lookup(claim.key()), Ok(Some(claim)));
    let absent = IdentityClaim {
        operation_id: OperationId::from_bytes([3; 16]),
        op_number: 3,
    };
    drive(&mut controller, identities.resolve(&[absent.key()])).unwrap();
    identities.reserve(&[absent]).unwrap();
    let replacement = drive(
        &mut controller,
        journal.open_index_snapshot(Boundary::Accepted, build_limits().file),
    )
    .unwrap();
    assert!(matches!(
        drive(&mut controller, identities.handoff(replacement)),
        Err(JournalIdentityHandoffError::ClaimMissing)
    ));

    let (mut other_controller, mut other_journal) = empty();
    drive(&mut other_controller, barrier(&mut other_journal, 9));
    drive(&mut other_controller, barrier(&mut other_journal, 2));
    drive(&mut other_controller, barrier(&mut other_journal, 3));
    let conflicting = drive(
        &mut other_controller,
        other_journal.open_index_snapshot(Boundary::Accepted, build_limits().file),
    )
    .unwrap();
    assert!(matches!(
        drive(&mut other_controller, extended.handoff(conflicting)),
        Err(JournalIdentityHandoffError::SnapshotMismatch)
    ));
}

#[test]
fn async_catalog_cold_lookups_can_interleave_and_cancel_without_poisoning() {
    let (mut controller, mut journal) = empty();
    let mut claims = Vec::new();
    for number in 0..6 {
        drive(&mut controller, append(&mut journal, number));
        claims.push(drive(&mut controller, barrier(&mut journal, number + 1)));
        drive(&mut controller, journal.roll_active(32768, 4)).unwrap();
    }
    let snapshot = drive(
        &mut controller,
        journal.build_index_snapshot(Boundary::Accepted, build_limits()),
    )
    .unwrap();
    let index = AsyncJournalIdentityIndex::new(snapshot, 4, 4);
    drive(&mut controller, index.resolve(&[claims[0].key()])).unwrap();
    let cold = [claims[4].key()];
    let mut first = Box::pin(index.resolve(&cold));
    assert!(poll(first.as_mut()).is_pending());
    assert_eq!(
        index.lookup(claims[0].key()),
        Err(IdentityIndexError::LookupUnavailable)
    );
    drop(first);
    // Cancellation leaves owned physical work behind, not a false negative.
    for (id, stage) in controller.jobs() {
        assert_eq!(stage, Stage::Queued);
        controller.execute(id, Effect::Normal).unwrap();
        controller.deliver(id).unwrap();
    }
    assert_eq!(
        index.lookup(claims[4].key()),
        Err(IdentityIndexError::LookupUnavailable)
    );
    let (a, b) = drive(&mut controller, async {
        tokio::join!(
            index.snapshot().find_operation(claims[4].operation_id),
            index.snapshot().find_operation(claims[5].operation_id),
        )
    });
    assert_eq!(
        a.unwrap().unwrap().entry.location.op_number,
        claims[4].op_number
    );
    assert_eq!(
        b.unwrap().unwrap().entry.location.op_number,
        claims[5].op_number
    );
    for number in [5, 0, 4, 1, 3, 2] {
        let record = drive(
            &mut controller,
            index
                .snapshot()
                .read_offset(partition(), Offset::new(number)),
        )
        .unwrap()
        .unwrap();
        assert_eq!(record.offset.get(), number);
    }
    let unknown = IdentityKey::operation(OperationId::from_bytes([99; 16]));
    let mut failed = false;
    let result = drive_with(&mut controller, index.resolve(&[unknown]), |operation| {
        if !failed && matches!(operation, Operation::Read { .. }) {
            failed = true;
            Effect::FailBefore(io::ErrorKind::Other)
        } else {
            Effect::Normal
        }
    });
    assert!(failed && result.is_err());
    assert_eq!(
        index.lookup(unknown),
        Err(IdentityIndexError::LookupUnavailable)
    );
    drive(&mut controller, index.resolve(&[unknown])).unwrap();
    assert_eq!(index.lookup(unknown), Ok(None));
    assert!(!journal.is_faulted());
}

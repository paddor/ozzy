use super::*;

pub(super) fn limits() -> Limits {
    Limits {
        window_millis: 10,
        entries: 16,
        pending_records: 8,
        pending_operations: 8,
        batch_records: 8,
        bytes: 64 * 1024,
    }
}

pub(super) fn index() -> Index {
    Index::new(limits(), Offset::ZERO, 0).unwrap()
}

pub(super) fn id(value: u8) -> MessageId {
    MessageId::from_bytes([value; 16])
}

#[test]
fn first_wins_inside_a_batch_without_consuming_duplicate_offsets() {
    let mut index = index();
    let batch = index
        .admit(1, 0, Offset::ZERO, &[id(1), id(1), id(2), id(1)])
        .unwrap();
    assert_eq!(batch.next_offset, Offset::new(2));
    assert_eq!(
        batch
            .outcomes
            .iter()
            .map(|outcome| (outcome.disposition, outcome.offset.get()))
            .collect::<Vec<_>>(),
        [
            (AppendDisposition::Stored, 0),
            (AppendDisposition::Duplicate, 0),
            (AppendDisposition::Stored, 1),
            (AppendDisposition::Duplicate, 0),
        ]
    );
    assert_eq!(index.retained_entries(), 2);
    assert_eq!(
        index.lookup(id(1)),
        Some(Match::Pending {
            offset: Offset::ZERO,
            operation: 1
        })
    );
    index.commit_through(1).unwrap();
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
}

#[test]
fn pending_duplicates_reserve_nothing_until_the_original_commits() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    let prepared = index.prepare(2, 1, Offset::new(1), &[id(1)]).unwrap();
    assert_eq!(prepared.pending_original(), Some(1));
    assert_eq!(
        index.install(prepared).unwrap_err(),
        Error::PendingOriginal(1)
    );
    assert_eq!(index.commit_through(2), Err(Error::Operation));
    assert_eq!(index.retained_entries(), 1);
    index.commit_through(1).unwrap();
    let duplicate = index.admit(2, 1, Offset::new(1), &[id(1)]).unwrap();
    assert_eq!(
        duplicate.outcomes[0].disposition,
        AppendDisposition::Duplicate
    );
    assert_eq!(duplicate.next_offset, Offset::new(1));
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    assert_eq!(index.committed_time(), 0);
    index.commit_through(2).unwrap();
    assert_eq!(index.committed_time(), 1);
}

#[test]
fn duplicate_attempts_never_refresh_the_window() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    index.commit_through(1).unwrap();
    index.admit(2, 9, Offset::new(1), &[id(1)]).unwrap();
    index.commit_through(2).unwrap();
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    index.advance_time(3, 10).unwrap();
    assert_eq!(
        index.expire(8, 1024),
        0,
        "unconfirmed time cannot retire identities"
    );
    index.commit_through(3).unwrap();
    assert_eq!(index.lookup(id(1)), None);
    assert_eq!(index.expire(8, 1024), 1);
    let next = index.admit(4, 10, Offset::new(1), &[id(1)]).unwrap();
    assert_eq!(next.outcomes[0].disposition, AppendDisposition::Stored);
    assert_eq!(next.outcomes[0].offset, Offset::new(1));
}

#[test]
fn exhaustion_preserves_live_identities_but_allows_duplicates_and_idle_expiry() {
    let mut limits = limits();
    limits.entries = 2;
    limits.pending_records = 2;
    limits.batch_records = 2;
    let mut index = Index::new(limits, Offset::ZERO, 0).unwrap();
    index.admit(1, 0, Offset::ZERO, &[id(1), id(2)]).unwrap();
    index.commit_through(1).unwrap();
    assert_eq!(
        index
            .admit(2, 1, Offset::new(2), &[id(1), id(3)])
            .unwrap_err(),
        Error::Capacity
    );
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    assert_eq!(index.lookup(id(3)), None);
    index.admit(2, 1, Offset::new(2), &[id(1), id(2)]).unwrap();
    index.commit_through(2).unwrap();
    index.advance_time(3, 10).unwrap();
    index.commit_through(3).unwrap();
    assert_eq!(index.expire(2, size_of::<Entry>()), 1);
    assert_eq!(index.retained_entries(), 1);
    index.admit(4, 10, Offset::new(2), &[id(3)]).unwrap();
}

#[test]
fn discarded_pending_records_and_time_cannot_change_confirmed_identity_or_offsets() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    index.commit_through(1).unwrap();
    index.admit(2, 20, Offset::new(1), &[id(2)]).unwrap();
    assert_eq!(index.lookup(id(1)), None);
    index.discard_unconfirmed();
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    assert_eq!(index.lookup(id(2)), None);
    assert_eq!(index.committed_time(), 0);
    let replacement = index.admit(2, 1, Offset::new(1), &[id(2)]).unwrap();
    assert_eq!(replacement.outcomes[0].offset, Offset::new(1));
}

#[test]
fn malformed_clock_position_and_operation_errors_are_atomic() {
    let mut index = index();
    assert_eq!(
        index
            .admit(1, 0, Offset::ZERO, &[id(1), id(0)])
            .unwrap_err(),
        Error::Batch
    );
    assert_eq!(index.retained_entries(), 0);
    assert_eq!(index.lookup(id(1)), None);
    index.admit(1, 1, Offset::ZERO, &[id(1)]).unwrap();
    assert_eq!(
        index.admit(1, 1, Offset::new(1), &[id(2)]).unwrap_err(),
        Error::Operation
    );
    assert_eq!(
        index.admit(2, 0, Offset::new(1), &[id(2)]).unwrap_err(),
        Error::TimeRegression
    );
    assert_eq!(
        index.admit(2, 1, Offset::ZERO, &[id(2)]).unwrap_err(),
        Error::Offset
    );
    assert_eq!(index.commit_through(2), Err(Error::Operation));
    assert_eq!(index.retained_entries(), 1);
    index.commit_through(1).unwrap();
    assert_eq!(index.commit_through(0), Err(Error::Operation));
}

#[test]
fn admission_at_the_last_offset_rejects_overflow_without_reserving_identity() {
    let mut index = Index::new(limits(), Offset::new(u64::MAX), 0).unwrap();
    assert_eq!(
        index
            .admit(1, 0, Offset::new(u64::MAX), &[id(1)])
            .unwrap_err(),
        Error::PositionExhausted
    );
    assert_eq!(index.retained_entries(), 0);
    assert_eq!(index.lookup(id(1)), None);
}

#[test]
fn repeated_expiry_churn_keeps_physical_reservations_constant() {
    let mut index = index();
    let reserved = index.reserved_bytes();
    for turn in 0..1024u64 {
        let first = Offset::new(turn);
        index
            .admit(
                turn * 2 + 1,
                turn * 10,
                first,
                &[id((turn % 255 + 1) as u8)],
            )
            .unwrap();
        index.commit_through(turn * 2 + 1).unwrap();
        index.advance_time(turn * 2 + 2, (turn + 1) * 10).unwrap();
        index.commit_through(turn * 2 + 2).unwrap();
        assert_eq!(index.expire(16, 4096), 1);
        assert_eq!(memory::resident(&index), reserved);
    }
}

#[test]
fn accepted_rows_outgrow_the_small_table_and_return_without_losing_identities() {
    let mut index = index();
    let reserved = index.reserved_bytes();
    index
        .admit(1, 0, Offset::ZERO, &[id(1), id(2), id(3)])
        .unwrap();
    index.admit(2, 1, Offset::new(3), &[id(4), id(5)]).unwrap();
    assert!(index.pending.spilled());
    for (value, offset) in [(1, 0), (2, 1), (3, 2), (4, 3), (5, 4)] {
        assert!(matches!(
            index.lookup(id(value)),
            Some(Match::Pending { offset: found, .. }) if found == Offset::new(offset)
        ));
    }
    // Two rows remain accepted-only: more than a quarter of the small table.
    index.commit_through(1).unwrap();
    assert!(index.pending.spilled());
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    assert!(matches!(
        index.lookup(id(5)),
        Some(Match::Pending { operation: 2, .. })
    ));
    index.commit_through(2).unwrap();
    assert!(!index.pending.spilled());
    assert_eq!(index.lookup(id(5)), Some(Match::Committed(Offset::new(4))));
    // A batch larger than the small table classifies in the full one.
    let batch = index
        .admit(
            3,
            2,
            Offset::new(5),
            &[id(6), id(7), id(8), id(9), id(10), id(6), id(2)],
        )
        .unwrap();
    assert_eq!(batch.next_offset, Offset::new(10));
    assert_eq!(batch.outcomes[5].offset, Offset::new(5));
    assert_eq!(batch.outcomes[6].offset, Offset::new(1));
    assert!(index.pending.spilled());
    // Discarding the unconfirmed suffix empties both tables.
    index.discard_unconfirmed();
    assert!(!index.pending.spilled());
    assert_eq!(index.lookup(id(6)), None);
    assert_eq!(index.lookup(id(5)), Some(Match::Committed(Offset::new(4))));
    assert_eq!(index.reserved_bytes(), reserved);
    assert_eq!(memory::resident(&index), reserved);
}

#[test]
fn inconsistent_or_overflowing_limits_fail_before_allocating() {
    for bad in [
        Limits {
            window_millis: 0,
            ..limits()
        },
        Limits {
            bytes: 1,
            ..limits()
        },
        Limits {
            entries: usize::MAX,
            ..limits()
        },
    ] {
        assert_eq!(Index::new(bad, Offset::ZERO, 0).unwrap_err(), Error::Limits);
    }
}

#[test]
fn pending_capacity_and_operation_capacity_are_independent() {
    let mut limits = limits();
    limits.pending_records = 2;
    limits.batch_records = 2;
    limits.pending_operations = 2;
    let mut index = Index::new(limits, Offset::ZERO, 0).unwrap();
    index.admit(1, 0, Offset::ZERO, &[id(1), id(2)]).unwrap();
    assert_eq!(
        index.admit(2, 0, Offset::new(2), &[id(3)]).unwrap_err(),
        Error::PendingCapacity
    );
    index.advance_time(2, 0).unwrap();
    assert_eq!(index.advance_time(3, 10), Err(Error::PendingCapacity));
    assert_eq!(index.retained_entries(), 2);
    index.commit_through(1).unwrap();
    index.advance_time(3, 10).unwrap();
    assert_eq!(index.expire(8, 1024), 0);
    index.commit_through(3).unwrap();
    assert_eq!(index.expire(8, 1024), 2);
}

#[test]
fn pending_original_cannot_expire_before_its_commit() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    assert_eq!(
        index.admit(2, 100, Offset::new(1), &[id(1)]).unwrap_err(),
        Error::PendingOriginal(1)
    );
    assert_eq!(index.expire(8, 1024), 0);
    index.commit_through(1).unwrap();
    assert_eq!(index.retained_entries(), 1);
    let unique = index.admit(2, 100, Offset::new(1), &[id(1)]).unwrap();
    assert_eq!(unique.outcomes[0].disposition, AppendDisposition::Stored);
    assert_eq!(unique.outcomes[0].offset, Offset::new(1));
    index.commit_through(2).unwrap();
    assert_eq!(index.expire(8, 1024), 1);
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::new(1))));
}

#[test]
fn expiring_a_superseded_row_preserves_the_new_occurrence() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    index.commit_through(1).unwrap();
    let new = index.admit(2, 10, Offset::new(1), &[id(1)]).unwrap();
    assert_eq!(new.outcomes[0].disposition, AppendDisposition::Stored);
    index.commit_through(2).unwrap();
    assert_eq!(index.retained_entries(), 2);
    assert_eq!(index.expire(1, 1024), 1);
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::new(1))));
    index.advance_time(3, 20).unwrap();
    index.commit_through(3).unwrap();
    assert_eq!(index.expire(8, 1024), 1);
    assert_eq!(index.lookup(id(1)), None);
}

#[test]
fn discarding_a_partial_commit_preserves_only_the_confirmed_prefix() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    index.admit(2, 0, Offset::new(1), &[id(2)]).unwrap();
    index.commit_through(1).unwrap();
    index.admit(3, 1, Offset::new(2), &[id(1), id(3)]).unwrap();
    index.discard_unconfirmed();
    assert_eq!(index.retained_entries(), 1);
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    assert_eq!(index.lookup(id(2)), None);
    assert_eq!(index.lookup(id(3)), None);
    assert_eq!(index.committed_time(), 0);
    let replay = index.admit(2, 0, Offset::new(1), &[id(3)]).unwrap();
    assert_eq!(replay.next_offset, Offset::new(2));
    assert_eq!(replay.outcomes[0].offset, Offset::new(1));
}

#[test]
fn discarded_reacceptance_restores_the_old_identity_and_window() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    index.commit_through(1).unwrap();
    index.admit(2, 10, Offset::new(1), &[id(1)]).unwrap();
    assert_eq!(
        index.lookup(id(1)),
        Some(Match::Pending {
            offset: Offset::new(1),
            operation: 2
        })
    );
    index.discard_unconfirmed();
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    let retry = index.admit(2, 9, Offset::new(1), &[id(1)]).unwrap();
    assert_eq!(retry.outcomes[0].disposition, AppendDisposition::Duplicate);
    index.commit_through(2).unwrap();
    index.advance_time(3, 10).unwrap();
    index.commit_through(3).unwrap();
    assert_eq!(index.expire(8, 1024), 1);
}

#[test]
fn oversized_batches_and_zero_work_expiry_do_not_change_state() {
    let mut index = index();
    assert_eq!(
        index.admit(1, 0, Offset::ZERO, &[]).unwrap_err(),
        Error::Batch
    );
    assert_eq!(
        index.admit(1, 0, Offset::ZERO, &[id(1); 9]).unwrap_err(),
        Error::Batch
    );
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    index.commit_through(1).unwrap();
    index.advance_time(2, 10).unwrap();
    index.commit_through(2).unwrap();
    assert_eq!(index.expire(0, 1024), 0);
    assert_eq!(index.expire(8, size_of::<Entry>() - 1), 0);
    assert_eq!(index.retained_entries(), 1);
    assert_eq!(index.expire(1, size_of::<Entry>()), 1);
}

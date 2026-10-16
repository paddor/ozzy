use super::{tests::*, *};

fn stored(operation: u64, time: u64, offset: u64, ids: &[MessageId]) -> StoredOperation<'_> {
    StoredOperation {
        operation,
        logical_time: time,
        first_offset: Offset::new(offset),
        ids,
    }
}

#[test]
fn canonical_groups_reserve_nothing_before_install_and_apply_each_original_boundary() {
    let mut index = index();
    let first = [id(1), id(2)];
    let last = [id(3)];
    let group = [
        stored(1, 0, 0, &first),
        stored(2, 5, 2, &[]),
        stored(3, 6, 2, &last),
    ];
    let prepared = index.prepare_stored(&group, 0).unwrap();
    assert_eq!(prepared.operation(), 3);
    assert_eq!(prepared.next_offset(), Offset::new(3));
    assert_eq!(index.retained_entries(), 0);
    assert_eq!(index.lookup(id(1)), None);
    assert_eq!(index.commit_through(3), Err(Error::Operation));
    index.install(prepared).unwrap();
    for (operation, time, end) in [(1, 0, 2), (2, 5, 2), (3, 6, 3)] {
        index.commit_through(operation).unwrap();
        assert_eq!(index.committed_operation, operation);
        assert_eq!(index.committed_time(), time);
        assert_eq!(index.committed_end, Offset::new(end));
    }
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    index.advance_time(4, 10).unwrap();
    index.commit_through(4).unwrap();
    assert_eq!(
        index.lookup(id(1)),
        None,
        "control operations never refresh IDs"
    );
    assert_eq!(index.lookup(id(3)), Some(Match::Committed(Offset::new(2))));
}

#[test]
fn canonical_reuse_requires_expiry_and_verified_original_confirmation() {
    let mut index = index();
    let ids = [id(1)];
    index.admit(1, 0, Offset::ZERO, &ids).unwrap();
    assert_eq!(
        index
            .prepare_stored(&[stored(2, 9, 1, &ids)], 1)
            .unwrap_err(),
        Error::Batch
    );
    assert_eq!(
        index
            .prepare_stored(&[stored(2, 10, 1, &ids)], 0)
            .unwrap_err(),
        Error::PendingOriginal(1)
    );
    assert_eq!(index.retained_entries(), 1);
    let prepared = index.prepare_stored(&[stored(2, 10, 1, &ids)], 1).unwrap();
    index.install(prepared).unwrap();
    assert_eq!(
        index.lookup(id(1)),
        Some(Match::Pending {
            offset: Offset::new(1),
            operation: 2
        })
    );
    index.commit_through(1).unwrap();
    assert_eq!(
        index.lookup(id(1)),
        Some(Match::Pending {
            offset: Offset::new(1),
            operation: 2
        })
    );
    index.commit_through(2).unwrap();
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::new(1))));
    assert_eq!(index.expire(8, 1024), 1);
    assert_eq!(index.retained_entries(), 1);
}

#[test]
fn canonical_preparation_survives_confirmation_without_an_accepted_image_change() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    let ids = [id(2)];
    let prepared = index.prepare_stored(&[stored(2, 0, 1, &ids)], 1).unwrap();
    let primary = index.prepare(2, 0, Offset::new(1), &ids).unwrap();
    index.commit_through(1).unwrap();
    assert_eq!(index.install(primary).unwrap_err(), Error::Operation);
    index.install(prepared).unwrap();
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    assert_eq!(
        index.lookup(id(2)),
        Some(Match::Pending {
            offset: Offset::new(1),
            operation: 2
        })
    );
}

#[test]
fn grouped_expired_versions_remain_ordered_across_each_commit_and_suffix_discard() {
    let ids = [id(1)];
    let group = [
        stored(1, 0, 0, &ids),
        stored(2, 10, 1, &ids),
        stored(3, 20, 2, &ids),
    ];
    for cut in 1..=3 {
        let mut index = index();
        let prepared = index.prepare_stored(&group, 2).unwrap();
        index.install(prepared).unwrap();
        assert_eq!(index.pending.len(), 1);
        for boundary in 1..=cut {
            index.commit_through(boundary).unwrap();
            if boundary < 3 {
                assert_eq!(
                    index.lookup(id(1)),
                    Some(Match::Pending {
                        offset: Offset::new(2),
                        operation: 3
                    })
                );
            }
        }
        index.discard_unconfirmed();
        assert_eq!(
            index.lookup(id(1)),
            Some(Match::Committed(Offset::new(cut - 1)))
        );
        assert_eq!(index.expire(8, 1024), cut as usize - 1);
        assert_eq!(index.retained_entries(), 1);
        let mut snapshot = index.begin_snapshot().unwrap();
        assert_eq!(snapshot.header().first_offset(), Offset::new(cut - 1));
        assert_eq!(snapshot.header().next_offset(), Offset::new(cut));
        let mut restoring = RestoringIndex::new(limits(), *snapshot.header()).unwrap();
        let mut rows = [0_u8; SNAPSHOT_ROW_BYTES];
        assert_eq!(
            index
                .write_snapshot_rows(&mut snapshot, &mut rows, 1)
                .unwrap(),
            SNAPSHOT_ROW_BYTES
        );
        index.finish_snapshot(&snapshot).unwrap();
        restoring.push_rows(&rows).unwrap();
        let restored = restoring.finish().unwrap();
        assert_eq!(restored.lookup(id(1)), index.lookup(id(1)));
    }
}

#[test]
fn late_bad_canonical_operations_preserve_earlier_preparation_and_all_authoritative_state() {
    let mut index = index();
    let ids = [id(1)];
    let good = index.prepare_stored(&[stored(1, 0, 0, &ids)], 0).unwrap();
    let zero = [MessageId::from_bytes([0; 16])];
    for bad in [
        stored(1, 0, 1, &ids),
        stored(2, 0, 0, &ids),
        stored(2, 0, 1, &zero),
        stored(2, 1, 1, &ids),
    ] {
        assert!(
            index
                .prepare_stored(&[stored(1, 0, 0, &ids), bad], 0)
                .is_err()
        );
        assert_eq!(index.lookup(id(1)), None);
        assert_eq!(index.retained_entries(), 0);
        assert_eq!(index.committed_time(), 0);
    }
    index.install(good).unwrap();
    assert_eq!(index.retained_entries(), 1);
    index.commit_through(1).unwrap();
    assert_eq!(
        index
            .prepare_stored(&[stored(2, 0, 1, &[])], 0)
            .unwrap_err(),
        Error::Operation
    );
}

#[test]
fn canonical_group_record_operation_capacity_and_stale_preparation_are_strict() {
    let mut limits = limits();
    limits.entries = 4;
    limits.pending_records = 4;
    limits.pending_operations = 2;
    limits.batch_records = 2;
    let mut index = Index::new(limits, Offset::ZERO, 0).unwrap();
    let ids = [id(1), id(2)];
    let next = [id(3), id(4)];
    assert!(matches!(
        index.prepare_stored(&[stored(1, 0, 0, &[id(1), id(2), id(3)])], 0),
        Err(Error::Batch)
    ));
    assert!(matches!(
        index.prepare_stored(
            &[
                stored(1, 0, 0, &ids),
                stored(2, 0, 2, &next),
                stored(3, 0, 4, &[]),
            ],
            0
        ),
        Err(Error::PendingCapacity)
    ));
    let prepared = index
        .prepare_stored(&[stored(1, 0, 0, &ids), stored(2, 0, 2, &next)], 0)
        .unwrap();
    let stale = index.prepare_stored(&[stored(1, 0, 0, &ids)], 0).unwrap();
    index.install(prepared).unwrap();
    assert_eq!(index.install(stale).unwrap_err(), Error::Operation);
    index.commit_through(2).unwrap();
    assert_eq!(
        index
            .prepare_stored(&[stored(3, 10, 4, &[id(5)])], 2)
            .unwrap_err(),
        Error::Capacity
    );
    let time = index.prepare_stored(&[stored(3, 10, 4, &[])], 2).unwrap();
    index.install(time).unwrap();
    index.commit_through(3).unwrap();
    assert_eq!(index.expire(1, size_of::<Entry>()), 1);
    let prepared = index
        .prepare_stored(&[stored(4, 10, 4, &[id(5)])], 3)
        .unwrap();
    index.install(prepared).unwrap();
    assert_eq!(index.retained_entries(), 4);
}

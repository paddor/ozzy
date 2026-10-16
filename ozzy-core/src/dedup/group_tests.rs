use super::{tests::*, *};

#[test]
fn grouped_attempts_keep_each_sequence_range_and_assign_only_new_offsets() {
    let mut index = index();
    let a = [id(1), id(2)];
    let b = [id(2), id(3), id(1)];
    let prepared = index
        .prepare_group(
            &[
                AttemptOperation {
                    operation: 1,
                    logical_time: 0,
                    ids: &a,
                },
                AttemptOperation {
                    operation: 2,
                    logical_time: 1,
                    ids: &b,
                },
            ],
            Offset::ZERO,
        )
        .unwrap();
    assert_eq!(prepared.next_offset(), Offset::new(3));
    assert_eq!(prepared.pending_original(), None);
    assert_eq!(
        prepared.operation_results(1).unwrap().first_offset,
        Offset::ZERO
    );
    let second = prepared.operation_results(2).unwrap();
    assert_eq!(second.first_offset, Offset::new(2));
    assert_eq!(second.logical_time, 1);
    assert_eq!(
        second.outcomes,
        &[
            AppendOutcome {
                disposition: AppendDisposition::Duplicate,
                offset: Offset::new(1)
            },
            AppendOutcome {
                disposition: AppendDisposition::Stored,
                offset: Offset::new(2)
            },
            AppendOutcome {
                disposition: AppendDisposition::Duplicate,
                offset: Offset::ZERO
            },
        ]
    );
    assert_eq!(index.retained_entries(), 0);
    index.install(prepared).unwrap();
    index.commit_through(1).unwrap();
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    assert_eq!(
        index.lookup(id(3)),
        Some(Match::Pending {
            offset: Offset::new(2),
            operation: 2
        })
    );
    index.commit_through(2).unwrap();
    assert_eq!(index.lookup(id(3)), Some(Match::Committed(Offset::new(2))));
}

#[test]
fn grouped_attempts_never_publish_external_pending_matches_or_grow_past_scratch_bounds() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    let ids = [id(1), id(2)];
    let prepared = index
        .prepare_group(
            &[AttemptOperation {
                operation: 2,
                logical_time: 20,
                ids: &ids,
            }],
            Offset::new(1),
        )
        .unwrap();
    assert_eq!(prepared.pending_original(), Some(1));
    assert_eq!(
        index.install(prepared).unwrap_err(),
        Error::PendingOriginal(1)
    );
    index.commit_through(1).unwrap();
    let prepared = index
        .prepare_group(
            &[AttemptOperation {
                operation: 2,
                logical_time: 20,
                ids: &ids,
            }],
            Offset::new(1),
        )
        .unwrap();
    assert_eq!(
        prepared.outcomes()[0].disposition,
        AppendDisposition::Stored
    );

    let mut tight = limits();
    tight.entries = 4;
    tight.pending_records = 4;
    tight.batch_records = 2;
    let mut index = Index::new(tight, Offset::ZERO, 0).unwrap();
    let reserved = index.reserved_bytes();
    let a = [id(1), id(2)];
    let b = [id(3), id(4)];
    let c = [id(5)];
    assert!(matches!(
        index.prepare_group(
            &[
                AttemptOperation {
                    operation: 1,
                    logical_time: 0,
                    ids: &a
                },
                AttemptOperation {
                    operation: 2,
                    logical_time: 0,
                    ids: &b
                },
                AttemptOperation {
                    operation: 3,
                    logical_time: 0,
                    ids: &c
                },
            ],
            Offset::ZERO
        ),
        Err(Error::Capacity)
    ));
    assert_eq!(index.reserved_bytes(), reserved);
    assert_eq!(index.scratch_rows.len(), 4);
    assert_eq!(index.retained_entries(), 0);
    assert_eq!(index.lookup(id(1)), None);
}

use super::{tests::*, *};

#[test]
fn preparation_and_cancellation_leave_authoritative_identity_time_and_offsets_unchanged() {
    let mut index = index();
    let before = index.reserved_bytes();
    let prepared = index
        .prepare(1, 5, Offset::ZERO, &[id(1), id(1), id(2)])
        .unwrap();
    assert_eq!(prepared.operation(), 1);
    assert_eq!(prepared.logical_time(), 5);
    assert_eq!(prepared.next_offset(), Offset::new(2));
    assert_eq!(prepared.pending_original(), None);
    assert!(prepared.retained_bytes() >= 3 * size_of::<AppendOutcome>() + 2 * size_of::<Entry>());
    assert_eq!(index.retained_entries(), 0);
    assert_eq!(index.committed_time(), 0);
    assert_eq!(index.lookup(id(1)), None);
    assert_eq!(index.commit_through(1), Err(Error::Operation));
    drop(prepared);
    let accepted = index.admit(1, 0, Offset::ZERO, &[id(3)]).unwrap();
    assert_eq!(accepted.next_offset, Offset::new(1));
    assert_eq!(index.lookup(id(1)), None);
    assert_eq!(index.reserved_bytes(), before);
}

#[test]
fn preparation_is_fenced_by_acceptance_commit_branch_reuse_and_owner_identity() {
    let mut index = index();
    let prepared = index.prepare(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    index.admit(1, 0, Offset::ZERO, &[id(2)]).unwrap();
    assert_eq!(index.install(prepared).unwrap_err(), Error::Operation);
    let prepared = index.prepare(2, 1, Offset::new(1), &[id(3)]).unwrap();
    index.commit_through(1).unwrap();
    assert_eq!(index.install(prepared).unwrap_err(), Error::Operation);
    let prepared = index.prepare(2, 1, Offset::new(1), &[id(3)]).unwrap();
    index.discard_unconfirmed();
    assert_eq!(index.install(prepared).unwrap_err(), Error::Operation);
    let prepared = index.prepare(2, 1, Offset::new(1), &[id(3)]).unwrap();
    let mut other = Index::new(limits(), Offset::new(1), 0).unwrap();
    assert_eq!(other.install(prepared).unwrap_err(), Error::Operation);
    assert_eq!(index.retained_entries(), 1);
    assert_eq!(other.retained_entries(), 0);
}

#[test]
fn same_batch_duplicates_need_no_external_original_and_install_exactly_once() {
    let mut index = index();
    let prepared = index
        .prepare(1, 2, Offset::ZERO, &[id(1), id(2), id(1)])
        .unwrap();
    assert_eq!(prepared.pending_original(), None);
    assert_eq!(
        prepared
            .outcomes()
            .iter()
            .map(|outcome| outcome.disposition)
            .collect::<Vec<_>>(),
        [
            AppendDisposition::Stored,
            AppendDisposition::Stored,
            AppendDisposition::Duplicate
        ]
    );
    index.install(prepared).unwrap();
    index.commit_through(1).unwrap();
    assert_eq!(index.retained_entries(), 2);
    assert_eq!(index.lookup(id(1)), Some(Match::Committed(Offset::ZERO)));
    assert_eq!(index.lookup(id(2)), Some(Match::Committed(Offset::new(1))));
}

#[test]
fn the_latest_pending_original_is_exposed_without_accepting_the_attempt() {
    let mut index = index();
    index.admit(1, 0, Offset::ZERO, &[id(1)]).unwrap();
    index.admit(2, 0, Offset::new(1), &[id(2)]).unwrap();
    let prepared = index
        .prepare(3, 1, Offset::new(2), &[id(1), id(3), id(2)])
        .unwrap();
    assert_eq!(prepared.pending_original(), Some(2));
    assert_eq!(
        index.install(prepared).unwrap_err(),
        Error::PendingOriginal(2)
    );
    assert_eq!(index.lookup(id(3)), None);
    assert_eq!(index.retained_entries(), 2);
    index.commit_through(2).unwrap();
    let accepted = index
        .admit(3, 1, Offset::new(2), &[id(1), id(3), id(2)])
        .unwrap();
    assert_eq!(accepted.next_offset, Offset::new(3));
}

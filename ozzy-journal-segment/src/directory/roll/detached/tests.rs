use super::*;
use crate::directory::progress_tests::{append, journal_mode};
use crate::directory::segment_name;

fn pending() -> (tempfile::TempDir, PendingJournalRoll, PreparedJournalRoll) {
    let (root, mut journal) = journal_mode(true);
    journal
        .set_write_mode(crate::SegmentWriteMode::Buffered)
        .unwrap();
    append(&mut journal, 1);
    let capacity = journal.writer().header().capacity();
    let (pending, work) = journal.begin_owned_roll(capacity, 1).unwrap();
    (root, pending, work)
}

#[test]
fn publication_keeps_predecessor_readable_until_exact_installation() {
    let (_root, pending, work) = pending();
    let current = pending.journal().directory().current();
    let end = pending.journal().written_position().unwrap();
    let mut read = pending
        .journal()
        .freeze_written_history(1024 * 1024)
        .unwrap();
    assert_eq!(read.position(1).unwrap(), Some(end));
    assert_eq!(pending.journal().accepted_position().unwrap().op_number, 0);
    let completed = std::thread::spawn(|| work.publish()).join().unwrap();
    assert!(completed.succeeded());
    // Publication alone cannot replace the owner's selected reader image.
    assert_eq!(pending.journal().directory().current(), current);
    assert_eq!(pending.journal().writer().header().segment_id(), 1);
    assert_eq!(read.position(1).unwrap(), Some(end));
    let journal = pending.complete(completed).unwrap();
    assert_eq!(journal.writer().header().segment_id(), 2);
    assert!(!journal.writer().data_sync());
    assert_eq!(journal.accepted_position().unwrap(), end);
    assert_eq!(journal.directory().manifest().segments.len(), 2);
    let root = journal.directory.root.clone();
    let identity = journal.directory.identity;
    drop((read, journal));
    let journal = crate::GroupDirectory::open(root, identity, MetadataLimits::default())
        .unwrap()
        .recover(
            ozzy_journal::progress::JournalGeneration(2),
            crate::DecodeLimits::default(),
            crate::OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(journal.accepted_position().unwrap(), end);
}

#[test]
fn foreign_or_failed_publication_cannot_install_a_roll() {
    let (_left_root, left, left_work) = pending();
    let (_right_root, right, right_work) = pending();
    assert!(matches!(
        left.complete(right_work.publish()),
        Err(DirectoryError::RollPublicationMismatch)
    ));
    assert!(matches!(
        right.complete(left_work.publish()),
        Err(DirectoryError::RollPublicationMismatch)
    ));

    let (_root, pending, work) = pending();
    let current = pending.journal().directory().current();
    std::fs::create_dir(pending.journal().directory.root.join("segments/2.log")).unwrap();
    let failed = work.publish();
    assert!(!failed.succeeded());
    assert_eq!(pending.journal().directory().current(), current);
    assert!(pending.complete(failed).is_err());
}

#[test]
fn zeroed_successor_becomes_the_rolled_segment_and_a_stale_one_is_not_used() {
    let (_root, mut journal) = journal_mode(true);
    let first = append(&mut journal, 1);
    journal.sync_through(first).unwrap();
    let capacity = journal.writer().header().capacity();
    let prepared = journal
        .prepare_next_segment(capacity, 4)
        .unwrap()
        .prepare()
        .unwrap();
    let zeros = vec![0; 64 * 1024];
    prepared.zero_range(0, &zeros).unwrap();
    assert!(prepared.zero_range(capacity - 1, &zeros).is_err());
    let path = journal.directory.root.join(segment_name(2));
    assert_eq!(std::fs::metadata(&path).unwrap().len(), capacity);
    let (pending, work) = journal
        .begin_owned_roll_with(capacity, 1, Some(prepared))
        .unwrap();
    let mut journal = pending.complete(work.publish()).unwrap();
    // The prepared file is the successor; no probe skipped to another name.
    assert_eq!(journal.writer().header().segment_id(), 2);
    assert!(journal.writer().data_sync());
    let second = append(&mut journal, 2);
    journal.sync_through(second).unwrap();
    // A successor captured before this roll belongs to segment 1 and is dropped.
    let (_other_root, other) = journal_mode(true);
    let stale = other
        .prepare_next_segment(capacity, 4)
        .unwrap()
        .prepare()
        .unwrap();
    let (pending, work) = journal
        .begin_owned_roll_with(capacity, 4, Some(stale))
        .unwrap();
    let journal = pending.complete(work.publish()).unwrap();
    assert_eq!(journal.writer().header().segment_id(), 3);
}

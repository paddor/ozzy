use ozzy_journal::progress::{JournalGeneration, JournalProgress, OpNumber, ProgressError};

fn journal() -> JournalProgress {
    JournalProgress::recover(JournalGeneration(1), OpNumber(0))
}

#[test]
fn group_admission_does_not_claim_a_write_or_sync() {
    let mut journal = journal();
    let ticket = journal.admit(3).unwrap();
    assert_eq!(ticket.first(), OpNumber(1));
    assert_eq!(ticket.through(), OpNumber(3));
    assert_eq!(journal.snapshot().accepted, OpNumber(3));
    assert_eq!(journal.snapshot().written, OpNumber(0));
    assert_eq!(journal.snapshot().durable, OpNumber(0));
}

#[test]
fn failed_admission_is_atomic_at_zero_and_u64_boundary() {
    let mut journal = JournalProgress::recover(JournalGeneration(1), OpNumber(u64::MAX - 1));
    let before = journal.snapshot();
    assert_eq!(journal.admit(0), Err(ProgressError::EmptyGroup));
    assert_eq!(journal.admit(2), Err(ProgressError::OpExhausted));
    assert_eq!(journal.snapshot(), before);
    let last = journal.admit(1).unwrap();
    assert_eq!(last.through(), OpNumber(u64::MAX));
    journal.complete_write(last).unwrap();
    journal
        .complete_sync(journal.begin_sync().unwrap())
        .unwrap();
    assert_eq!(journal.snapshot().durable, OpNumber(u64::MAX));
    let before = journal.snapshot();
    assert_eq!(journal.admit(1), Err(ProgressError::OpExhausted));
    assert_eq!(journal.snapshot(), before);
}

#[test]
fn writes_cannot_skip_groups_and_duplicate_completions_do_not_regress() {
    let mut journal = journal();
    let first = journal.admit(2).unwrap();
    let second = journal.admit(3).unwrap();
    let before = journal.snapshot();
    assert_eq!(journal.complete_write(second), Err(ProgressError::WriteGap));
    assert_eq!(journal.snapshot(), before);
    journal.complete_write(first).unwrap();
    journal.complete_write(second).unwrap();
    let before = journal.snapshot();
    journal.complete_write(first).unwrap();
    assert_eq!(journal.snapshot(), before);
}

#[test]
fn sync_scope_is_captured_and_duplicate_completions_do_not_regress() {
    let mut journal = journal();
    let first = journal.admit(2).unwrap();
    journal.complete_write(first).unwrap();
    let first_sync = journal.begin_sync().unwrap();
    let second = journal.admit(3).unwrap();
    journal.complete_write(second).unwrap();
    journal.complete_sync(first_sync).unwrap();
    assert_eq!(journal.snapshot().durable, OpNumber(2));
    let second_sync = journal.begin_sync().unwrap();
    journal.complete_sync(second_sync).unwrap();
    journal.complete_sync(first_sync).unwrap();
    assert_eq!(journal.snapshot().durable, OpNumber(5));
}

#[test]
fn durable_write_completes_the_exact_group_without_a_separate_sync() {
    let mut journal = journal();
    let first = journal.admit(2).unwrap();
    let second = journal.admit(3).unwrap();

    assert_eq!(
        journal.complete_durable_write(second),
        Err(ProgressError::WriteGap)
    );
    journal.complete_durable_write(first).unwrap();
    assert_eq!(journal.snapshot().written, OpNumber(2));
    assert_eq!(journal.snapshot().durable, OpNumber(2));
    journal.complete_durable_write(second).unwrap();
    assert_eq!(journal.snapshot().written, OpNumber(5));
    assert_eq!(journal.snapshot().durable, OpNumber(5));
}

#[test]
fn reopen_rejects_stale_write_sync_and_failure_without_mutation() {
    let mut old = journal();
    let write = old.admit(1).unwrap();
    old.complete_write(write).unwrap();
    let sync = old.begin_sync().unwrap();
    let mut new = JournalProgress::recover(JournalGeneration(2), OpNumber(0));
    new.admit(1).unwrap();
    let before = new.snapshot();
    assert_eq!(
        new.complete_write(write),
        Err(ProgressError::StaleGeneration)
    );
    assert_eq!(new.complete_sync(sync), Err(ProgressError::StaleGeneration));
    assert_eq!(
        new.fail(JournalGeneration(1)),
        Err(ProgressError::StaleGeneration)
    );
    assert_eq!(new.snapshot(), before);
}

#[test]
fn uncertain_error_requires_recovery_and_retains_past_evidence() {
    let mut journal = journal();
    let first = journal.admit(1).unwrap();
    journal.complete_write(first).unwrap();
    journal
        .complete_sync(journal.begin_sync().unwrap())
        .unwrap();
    let second = journal.admit(1).unwrap();
    journal.complete_write(second).unwrap();
    let late = journal.begin_sync().unwrap();
    journal.fail(JournalGeneration(1)).unwrap();
    let before = journal.snapshot();
    assert_eq!(journal.admit(1), Err(ProgressError::Faulted));
    assert_eq!(journal.complete_write(second), Err(ProgressError::Faulted));
    assert_eq!(journal.complete_sync(late), Err(ProgressError::Faulted));
    assert_eq!(journal.begin_sync(), Err(ProgressError::Faulted));
    journal.fail(JournalGeneration(1)).unwrap();
    assert_eq!(journal.snapshot(), before);
    assert_eq!(journal.snapshot().durable, OpNumber(1));
}

//! Release and scheduling while the broker clocks disagree.

use super::*;

const PAUSED: crate::replica_journal::ClockReading = crate::replica_journal::ClockReading {
    wall_millis: NOW + 1_000_000,
    trusted: false,
    monotonic: std::time::Duration::ZERO,
};

/// Whether `result` is the refusal a producer retries after a backoff.
fn clock_refused(result: &ProposalValidation) -> bool {
    matches!(
        result,
        ProposalValidation::Rejected {
            reason: JournalError::ProducerAppend(
                crate::replica_journal::AppendAdmissionError::ClockPaused
            ),
            ..
        }
    )
}

#[test]
fn a_paused_clock_holds_due_records_until_it_is_trusted() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let due = [NOW + 5_000];
    let result = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 1),
        Some(&due),
        NOW,
    );
    accept(&mut controller, &mut journal, &mut driver, result);
    // Past due by the clock, which reads only as a retry interval.
    assert_eq!(journal.release_wait(|| PAUSED), Some(1000));
    let step = drive(
        &mut controller,
        journal.prepare_release(driver.begin_validation().unwrap(), None),
    )
    .unwrap();
    assert!(matches!(
        step,
        crate::replica_journal::owned::release::ReleaseStep::Idle
    ));
    assert_eq!(journal.release_wait(|| (NOW + 1_000).into()), Some(4_000));
    assert_eq!(journal.release_wait(|| (NOW + 6_000).into()), Some(0));
    let proposal = release(&mut controller, &mut journal, &driver, NOW + 6_000).unwrap();
    let result = validate(
        &mut controller,
        &mut journal,
        &driver,
        proposal,
        NOW + 6_000,
    );
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(journal.next_release(), None);
}

#[test]
fn a_paused_clock_refuses_schedules_so_no_stamp_keys_a_record_ahead() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let due = [NOW + 5_000];
    let ticket = driver.begin_validation().unwrap();
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_stream_append(
            ozzy_proto::append::Authority {
                group_id: ticket.scope().group_id,
                config_epoch: ticket.scope().configuration_epoch,
                view: ticket.scope().view,
            },
            request(12, 0, 1),
        )
        .unwrap();
    buffer.schedule(&due).unwrap();
    let refused = drive(
        &mut controller,
        journal.propose_append(ticket, buffer, PAUSED),
    );
    assert!(clock_refused(&refused), "{refused:?}");
    // The retry after the clock is fixed keys the record by its due time.
    let result = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 1),
        Some(&due),
        NOW,
    );
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(journal.next_release(), Some(NOW + 5_000));
}

#[test]
fn a_paused_clock_stamps_stored_records_and_leaves_the_delay_time_alone() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let due = [NOW + 5_000];
    let result = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 1),
        Some(&due),
        NOW,
    );
    accept(&mut controller, &mut journal, &mut driver, result);
    // A release built before the pause, validated during it.
    let proposal = release(&mut controller, &mut journal, &driver, NOW + 6_000).unwrap();
    let ticket = driver.begin_validation().unwrap();
    let result = drive(
        &mut controller,
        journal.propose_append(ticket, proposal, PAUSED),
    );
    let through = ReleaseKey {
        millis: NOW + 5_000,
        schedule: 0,
    };
    let (_, time, key, _) = released(&result);
    assert_eq!((time, key), (NOW + 5_000, through));
}

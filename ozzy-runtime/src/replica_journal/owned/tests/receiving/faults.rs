use super::*;

struct Attempt {
    primary: Replica,
    backup: Replica,
    receiver: RecoveringJournal,
    recovery: Recovery,
    ticket: RecoveryTicket,
    pin: PinnedRecovery,
}

fn attempt(controller: &mut Controller, io: Local) -> Attempt {
    let mut primary = replica(controller, io.clone(), 0, QuorumPolicy::Durable, 8192);
    let backup = replica(controller, io.clone(), 2, QuorumPolicy::Durable, 8192);
    persist(controller, &mut primary);
    let (mut receiver, startup) = drive(
        controller,
        RecoveringJournal::start(
            receiver_config(QuorumPolicy::Durable),
            io,
            generations(50),
            RecoveryOpen::FormatNew {
                segment_capacity: 32768,
            },
        ),
    )
    .unwrap();
    let (recovery, ticket, pin) = authorize(controller, &mut primary, &backup, startup, 64);
    assert_eq!(
        drive(controller, receiver.begin_recovery(ticket, physical())).unwrap(),
        RecoveryPlan::Full
    );
    Attempt {
        primary,
        backup,
        receiver,
        recovery,
        ticket,
        pin,
    }
}

#[test]
fn owned_recovery_publication_failure_never_supplies_restart_evidence() {
    for kind in 0..3 {
        let (mut controller, io) = setup();
        let mut attempt = attempt(&mut controller, io.clone());
        transfer(
            &mut controller,
            &mut attempt.primary,
            &mut attempt.receiver,
            &mut attempt.recovery,
            attempt.ticket,
            attempt.pin,
            RecoveryPlan::Full,
        );
        let mut failed = false;
        let result = drive_except(
            &mut controller,
            attempt.receiver.finish_recovery(attempt.ticket),
            None,
            |operation| {
                let matches = match kind {
                    0 => matches!(operation, Operation::Write { .. }),
                    1 => matches!(operation, Operation::Sync { .. }),
                    _ => matches!(operation, Operation::Rename { .. }),
                };
                if matches && !failed {
                    failed = true;
                    Effect::FailAfter(std::io::ErrorKind::Other)
                } else {
                    Effect::Normal
                }
            },
        );
        assert!(failed && result.is_err());
        assert!(attempt.receiver.is_faulted());
        assert!(
            drive(
                &mut controller,
                attempt.receiver.finish_recovery(attempt.ticket)
            )
            .is_err()
        );
        drop(attempt.receiver);
        // These failures precede publication of the real configuration. The
        // surviving store still requires fresh responses from both other members.
        assert!(
            drive(
                &mut controller,
                OwnedJournal::open(
                    receiver_config(QuorumPolicy::Durable),
                    io.clone(),
                    JournalGeneration(70)
                )
            )
            .is_err()
        );
        let (receiver, _) = drive(
            &mut controller,
            RecoveringJournal::start(
                receiver_config(QuorumPolicy::Durable),
                io,
                generations(80),
                RecoveryOpen::Resume,
            ),
        )
        .unwrap();
        drive(&mut controller, receiver.shutdown()).unwrap();
        attempt
            .primary
            .journal
            .release_recovery(attempt.pin)
            .unwrap();
        drive(&mut controller, attempt.primary.journal.shutdown()).unwrap();
        drive(&mut controller, attempt.backup.journal.shutdown()).unwrap();
    }
}

#[test]
fn owned_recovery_newer_view_rejects_even_a_completed_old_publication() {
    let (mut controller, io) = setup();
    let mut attempt = attempt(&mut controller, io.clone());
    transfer(
        &mut controller,
        &mut attempt.primary,
        &mut attempt.receiver,
        &mut attempt.recovery,
        attempt.ticket,
        attempt.pin,
        RecoveryPlan::Full,
    );
    let publication = drive(
        &mut controller,
        attempt.receiver.finish_recovery(attempt.ticket),
    )
    .unwrap();
    attempt
        .recovery
        .observe_view(
            attempt.backup.config.identity.replica_node_id,
            Scope {
                view: attempt.ticket.scope().view + 1,
                ..attempt.ticket.scope()
            },
        )
        .unwrap();
    assert!(matches!(
        drive(
            &mut controller,
            attempt.receiver.into_journal(
                &mut attempt.recovery,
                publication,
                JournalGeneration(70)
            )
        ),
        Err(JournalError::Recovery(RecoveryError::StaleView))
    ));
    // Disk publication exists, but reopening supplies only election authority.
    let (journal, startup) = drive(
        &mut controller,
        OwnedJournal::open(
            receiver_config(QuorumPolicy::Durable),
            io,
            JournalGeneration(80),
        ),
    )
    .unwrap();
    assert!(journal.images().is_err());
    assert!(
        startup
            .into_driver(Duration::ZERO, timing(), pipeline())
            .unwrap()
            .normal()
            .is_none()
    );
    drive(&mut controller, journal.shutdown()).unwrap();
    attempt
        .primary
        .journal
        .release_recovery(attempt.pin)
        .unwrap();
    drive(&mut controller, attempt.primary.journal.shutdown()).unwrap();
    drive(&mut controller, attempt.backup.journal.shutdown()).unwrap();
}

#[test]
fn owned_recovery_aborted_or_foreign_generation_work_cannot_publish() {
    for abort in [true, false] {
        let (mut controller, io) = setup();
        let mut attempt = attempt(&mut controller, io);
        if abort {
            drive(
                &mut controller,
                attempt.receiver.abort_recovery(attempt.ticket),
            )
            .unwrap();
            assert!(
                drive(
                    &mut controller,
                    attempt.receiver.finish_recovery(attempt.ticket)
                )
                .is_err()
            );
        } else {
            let mut foreign = attempt.primary.journal.lease_append_buffer().unwrap();
            let bytes = [1; 16];
            foreign
                .push(ozzy_journal::operation::CanonicalOperation {
                    group_id: attempt.ticket.scope().group_id,
                    configuration_epoch: 1,
                    original_view: 0,
                    op_number: 1,
                    previous_digest: Digest::ZERO,
                    kind: ozzy_journal::operation::OperationKind::Barrier,
                    body: &bytes,
                })
                .unwrap();
            assert!(
                drive(
                    &mut controller,
                    attempt.receiver.receive_chunk(attempt.ticket, foreign)
                )
                .is_err()
            );
        }
        assert!(attempt.receiver.is_faulted());
        drop(attempt.receiver);
        attempt
            .primary
            .journal
            .release_recovery(attempt.pin)
            .unwrap();
        drive(&mut controller, attempt.primary.journal.shutdown()).unwrap();
        drive(&mut controller, attempt.backup.journal.shutdown()).unwrap();
    }
}

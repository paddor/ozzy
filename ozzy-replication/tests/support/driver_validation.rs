use super::*;

#[test]
fn worker_validation_admits_once_against_the_exact_live_image() {
    let mut primary = driver(normal(0));
    let ticket = primary.begin_validation().unwrap();
    assert_eq!(ticket.scope(), configuration().scope());
    assert_eq!(ticket.generation(), JournalGeneration(1));
    assert_eq!(ticket.accepted(), Prefix::GENESIS);
    assert_eq!(ticket.committed(), Prefix::GENESIS);
    assert_eq!(ticket.applied(), Prefix::GENESIS);
    assert!(matches!(
        primary
            .prepare_validated(node(0), ticket, &[operation()], Duration::ZERO)
            .unwrap(),
        Admission::Write { .. }
    ));
    let admitted = primary.normal().unwrap().snapshot();
    assert_eq!(
        primary.prepare_validated(node(0), ticket, &[operation()], Duration::ZERO),
        Err(DriverError::StaleValidation)
    );
    assert_eq!(primary.normal().unwrap().snapshot(), admitted);
    assert_eq!(admitted.journal.written.0, 0);
}

#[test]
fn later_view_or_timeout_discards_worker_validation_without_admission() {
    for timeout in [false, true] {
        let mut backup = driver(normal(1));
        let ticket = backup.begin_validation().unwrap();
        let now = if timeout {
            timing().primary_timeout
        } else {
            Duration::ZERO
        };
        if timeout {
            peer_exit(&mut backup, 2, now);
            assert!(matches!(
                backup.poll(now).unwrap(),
                Some(Action::PersistPromise(_))
            ));
        } else {
            backup
                .receive(
                    node(2),
                    Control::StartViewChange(StartViewChange {
                        scope: Scope {
                            view: 1,
                            ..configuration().scope()
                        },
                    }),
                    now,
                )
                .unwrap();
            assert!(backup.begin_validation().is_err()); // Durable promise already fences validation.
            backup
                .receive(
                    node(0),
                    Control::StartViewChange(StartViewChange {
                        scope: Scope {
                            view: 1,
                            ..configuration().scope()
                        },
                    }),
                    now,
                )
                .unwrap();
        }
        assert_eq!(
            backup.prepare_validated(node(0), ticket, &[operation()], now),
            Err(DriverError::StaleValidation)
        );
        let ozzy_replication::driver::Role::ViewChanging(changing) = backup.into_role() else {
            panic!("fenced role");
        };
        assert_eq!(changing.normal_snapshot().accepted, Prefix::GENESIS);
        assert_eq!(changing.normal_snapshot().journal.written.0, 0);
    }
}

#[test]
fn journal_fault_and_another_writer_or_voter_invalidate_validation() {
    let mut primary = driver(normal(0));
    let ticket = primary.begin_validation().unwrap();
    primary.fail_io(ticket.generation()).unwrap();
    assert_eq!(
        primary.prepare_validated(node(0), ticket, &[operation()], Duration::ZERO),
        Err(DriverError::StaleValidation)
    );
    for (voter, generation) in [
        (node(1), JournalGeneration(1)),
        (node(0), JournalGeneration(99)),
    ] {
        let core = NormalReplica::bootstrap(
            configuration(),
            voter,
            generation,
            PipelineLimits {
                max_operations: 8,
                max_body_bytes: 1024,
            },
        )
        .unwrap();
        let mut other = driver(core);
        assert_eq!(
            other.prepare_validated(node(0), ticket, &[operation()], Duration::ZERO),
            Err(DriverError::StaleValidation)
        );
        assert_eq!(other.normal().unwrap().snapshot().accepted, Prefix::GENESIS);
    }
}

#[test]
fn commit_progress_preserves_validation_but_application_changes_its_image() {
    let mut primary = normal(0);
    let mut backup = normal(1);
    for replica in [&mut primary, &mut backup] {
        let Admission::Write { ticket, .. } = replica
            .prepare(node(0), configuration().scope(), &[operation()])
            .unwrap()
        else {
            panic!("fresh")
        };
        replica.complete_durable_write(ticket).unwrap();
    }
    let mut primary = driver(primary);
    let before_commit = primary.begin_validation().unwrap();
    primary
        .receive(
            node(1),
            Control::PrepareOk {
                ack: backup.acknowledgment().unwrap(),
                grant: Grant {
                    revision: 1,
                    record_limit: 8,
                    byte_limit: 1024,
                },
            },
            Duration::ZERO,
        )
        .unwrap();
    let second = PreparedOperation::from_verified(
        &CanonicalOperation {
            group_id: configuration().scope().group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: 2,
            previous_digest: operation().prefix().digest,
            kind: ozzy_journal::operation::OperationKind::Barrier,
            body: &[2; 16],
        },
        canonical_body_digest(&[2; 16]),
    );
    assert!(matches!(
        primary.prepare_validated(node(0), before_commit, &[second], Duration::ZERO),
        Ok(Admission::Write { .. })
    ));
    let before_application = primary.begin_validation().unwrap();
    primary.apply_through(operation().prefix()).unwrap();
    assert_eq!(
        primary.prepare_validated(node(0), before_application, &[operation()], Duration::ZERO),
        Err(DriverError::StaleValidation)
    );
    assert_eq!(
        primary.normal().unwrap().snapshot().accepted,
        second.prefix()
    );
}

#[test]
fn validation_behind_an_apply_names_the_prefix_the_worker_will_have_applied() {
    let mut primary = normal(0);
    let mut backup = normal(1);
    for replica in [&mut primary, &mut backup] {
        let Admission::Write { ticket, .. } = replica
            .prepare(node(0), configuration().scope(), &[operation()])
            .unwrap()
        else {
            panic!("fresh")
        };
        replica.complete_durable_write(ticket).unwrap();
    }
    let mut primary = driver(primary);
    primary
        .receive(
            node(1),
            Control::PrepareOk {
                ack: backup.acknowledgment().unwrap(),
                grant: Grant {
                    revision: 1,
                    record_limit: 8,
                    byte_limit: 1024,
                },
            },
            Duration::ZERO,
        )
        .unwrap();
    let second = PreparedOperation::from_verified(
        &CanonicalOperation {
            group_id: configuration().scope().group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: 2,
            previous_digest: operation().prefix().digest,
            kind: ozzy_journal::operation::OperationKind::Barrier,
            body: &[2; 16],
        },
        canonical_body_digest(&[2; 16]),
    );
    let applying = primary.begin_validation().unwrap();
    assert_eq!(applying.committed(), operation().prefix());
    assert_eq!(applying.applied(), Prefix::GENESIS);
    let behind = primary.begin_validation_after_apply(applying).unwrap();
    assert_eq!(behind.applied(), operation().prefix());
    assert_eq!(behind.accepted(), applying.accepted());
    assert_eq!(behind.committed(), applying.committed());
    // The core has not applied yet: admission must not treat it as current.
    assert_eq!(
        primary.prepare_validated(node(0), behind, &[second], Duration::ZERO),
        Err(DriverError::StaleValidation)
    );
    primary.apply_through(applying.committed()).unwrap();
    // Once applied, an old apply ticket names a stale image.
    assert_eq!(
        primary.begin_validation_after_apply(applying),
        Err(DriverError::StaleValidation)
    );
    assert!(matches!(
        primary.prepare_validated(node(0), behind, &[second], Duration::ZERO),
        Ok(Admission::Write { .. })
    ));
    // Admission moved the accepted prefix: the earlier ticket is stale too.
    let applied = primary.begin_validation().unwrap();
    assert_eq!(
        primary.begin_validation_after_apply(behind),
        Err(DriverError::StaleValidation)
    );
    // An apply through the applied prefix is a no-op and changes nothing.
    assert_eq!(
        primary.begin_validation_after_apply(applied).unwrap(),
        applied
    );
}

#[test]
fn heartbeats_and_valid_tickets_cannot_bypass_primary_membership() {
    let mut backup = driver(normal(1));
    let ticket = backup.begin_validation().unwrap();
    let now = Duration::from_millis(10);
    backup
        .receive(
            node(0),
            Control::Commit(normal(0).announcement().unwrap()),
            now,
        )
        .unwrap();
    assert_eq!(backup.begin_validation().unwrap(), ticket);
    assert_eq!(
        backup.prepare_validated(node(2), ticket, &[operation()], now),
        Err(DriverError::Replication(ReplicationError::WrongRole))
    );
    assert_eq!(
        backup.normal().unwrap().snapshot().accepted,
        Prefix::GENESIS
    );
    assert!(matches!(
        backup
            .prepare_validated(node(0), ticket, &[operation()], now)
            .unwrap(),
        Admission::Write { .. }
    ));
}

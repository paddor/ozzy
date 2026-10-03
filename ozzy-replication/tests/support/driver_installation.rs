use super::*;

fn selected_pair() -> (ReplicaDriver, ReplicaDriver, Duration) {
    selected_pair_with_commit(false)
}

fn selected_pair_with_commit(committed: bool) -> (ReplicaDriver, ReplicaDriver, Duration) {
    let mut normal = normal(1);
    let Admission::Write { ticket, .. } = normal
        .prepare(node(0), configuration().scope(), &[operation()])
        .unwrap()
    else {
        panic!("new operation")
    };
    normal.complete_durable_write(ticket).unwrap();
    if committed {
        normal
            .receive_commit(
                node(0),
                ozzy_replication::Commit {
                    scope: configuration().scope(),
                    committed: operation().prefix(),
                },
            )
            .unwrap();
    }
    let mut primary = driver(normal);
    let mut backup = driver(super::normal(2));
    let now = timing().primary_timeout;
    peer_exit(&mut primary, 2, now);
    peer_exit(&mut backup, 1, now);
    persist(&mut primary, now);
    persist(&mut backup, now);
    let primary_start = start(&mut primary, now);
    let backup_start = start(&mut backup, now);
    primary.receive(node(2), backup_start, now).unwrap();
    backup.receive(node(1), primary_start, now).unwrap();
    assert_eq!(primary.poll(now).unwrap(), None);
    let Some(Action::Send { to, message }) = backup.poll(now).unwrap() else {
        panic!("second phase report")
    };
    assert_eq!(to, node(1));
    primary.receive(node(2), message, now).unwrap();
    primary.select(|_, _| None).unwrap();
    (primary, backup, now)
}

#[test]
fn abandoned_installation_preserves_commit_floor_and_rejects_old_publication() {
    let (mut primary, _, now) = selected_pair_with_commit(true);
    let ticket = primary
        .begin_primary_install(JournalGeneration(10))
        .unwrap();
    primary
        .receive(
            node(2),
            Control::StartViewChange(StartViewChange {
                scope: Scope {
                    view: 4,
                    ..ticket.scope()
                },
            }),
            now,
        )
        .unwrap();
    primary.complete_abandon(ticket, now).unwrap();
    assert!(primary.normal().is_none());
    assert!(primary.installation_ticket().is_none());
    assert!(primary.begin_activation().is_err());
    assert!(
        primary
            .complete_installation(ticket, ticket.committed(), now)
            .is_err()
    );
    let Some(Action::PersistPromise(promise)) = primary.poll(now).unwrap() else {
        panic!("must persist a later promise, never reuse the abandoned view");
    };
    assert_eq!(promise.scope().view, 4);
    assert_eq!(promise.generation(), ticket.previous_generation());
    assert_eq!(promise.log().last_normal_view, 0);
    assert_eq!(promise.log().committed, ticket.protected_committed());
    assert_eq!(promise.log().committed, operation().prefix());
}

#[test]
fn previously_committed_tail_still_requires_new_view_backup_before_primary_activation() {
    let (mut primary, _, now) = selected_pair_with_commit(true);
    let ticket = primary
        .begin_primary_install(JournalGeneration(10))
        .unwrap();
    assert_eq!(ticket.committed(), ticket.accepted());
    primary
        .complete_installation(ticket, ticket.committed(), now)
        .unwrap();
    assert!(primary.begin_activation().is_err());
    primary
        .receive(
            node(2),
            Control::PrepareOk {
                ack: ozzy_replication::PrepareOk {
                    scope: primary.scope(),
                    durable: operation().prefix(),
                },
                grant: Grant {
                    revision: 1,
                    record_limit: 1,
                    byte_limit: 1024,
                },
            },
            now,
        )
        .unwrap();
    let activation = primary.begin_activation().unwrap();
    primary.complete_activation(activation).unwrap();
    assert!(primary.normal().unwrap().snapshot().ready_for_appends);
}

#[test]
fn activation_ticket_requires_new_view_quorum_and_rejects_late_completion() {
    let (mut primary, _, now) = selected_pair();
    assert!(primary.begin_activation().is_err());
    let installed = primary
        .begin_primary_install(JournalGeneration(10))
        .unwrap();
    primary.validate_install_suffix(&[operation()]).unwrap();
    primary
        .complete_installation(installed, Prefix::GENESIS, now)
        .unwrap();
    assert!(primary.begin_activation().is_err());
    primary
        .receive(
            node(2),
            Control::PrepareOk {
                ack: ozzy_replication::PrepareOk {
                    scope: primary.scope(),
                    durable: operation().prefix(),
                },
                grant: Grant {
                    revision: 1,
                    record_limit: 1,
                    byte_limit: 1024,
                },
            },
            now,
        )
        .unwrap();
    let activation = primary.begin_activation().unwrap();
    assert_eq!(activation.local(), node(1));
    assert_eq!(activation.generation(), installed.generation());
    assert_eq!(activation.through(), operation().prefix());
    assert_eq!(activation.applied(), Prefix::GENESIS);
    peer_exit(&mut primary, 2, now + timing().max_election_timeout);
    primary.poll(now + timing().max_election_timeout).unwrap();
    assert!(primary.complete_activation(activation).is_err());
    assert!(primary.normal().is_none());
}

#[test]
fn newer_view_during_installation_never_exposes_intermediate_normal_authority() {
    let (mut primary, _, now) = selected_pair();
    let ticket = primary
        .begin_primary_install(JournalGeneration(10))
        .unwrap();
    assert!(primary.normal().is_none());
    assert_eq!(primary.installation_ticket(), Some(ticket));
    primary.validate_install_suffix(&[operation()]).unwrap();
    primary
        .receive(
            node(2),
            Control::StartViewChange(StartViewChange {
                scope: Scope {
                    view: 4,
                    ..configuration().scope()
                },
            }),
            now,
        )
        .unwrap();
    assert_eq!(primary.scope().view, 4); // Durable first-phase request fences immediately.
    primary
        .receive(
            node(0),
            Control::StartViewChange(StartViewChange {
                scope: Scope {
                    view: 4,
                    ..configuration().scope()
                },
            }),
            now,
        )
        .unwrap();
    assert_eq!(primary.scope().view, 4);
    assert_eq!(primary.poll(now).unwrap(), None); // Publication must settle first.
    primary
        .complete_installation(ticket, Prefix::GENESIS, now)
        .unwrap();
    assert!(primary.normal().is_none());
    assert_eq!(primary.installation_ticket(), None);
    assert_eq!(primary.scope().view, 4);
    let Some(Action::PersistPromise(promise)) = primary.poll(now).unwrap() else {
        panic!("newer view needs a separate durable promise")
    };
    assert_eq!(promise.scope().view, 4);
    assert_eq!(promise.log().last_normal_view, 1);
    assert_eq!(promise.log().accepted, operation().prefix());
    assert_eq!(promise.log().committed, Prefix::GENESIS);
    assert!(
        primary
            .complete_installation(ticket, Prefix::GENESIS, now)
            .is_err()
    );
}

#[test]
fn lost_start_view_retries_then_installed_quorum_releases_fresh_admission() {
    let (mut primary, mut backup, now) = selected_pair();
    let ticket = primary
        .begin_primary_install(JournalGeneration(10))
        .unwrap();
    let outsider = Control::ExitView(Scope {
        view: 99,
        ..configuration().scope()
    });
    primary
        .receive(node(0), wire_control(0, outsider), now)
        .unwrap();
    backup
        .receive(node(0), wire_control(0, outsider), now)
        .unwrap();
    assert_eq!(primary.scope().view, 1);
    assert_eq!(backup.scope().view, 1);
    primary.validate_install_suffix(&[operation()]).unwrap();
    primary
        .complete_installation(ticket, Prefix::GENESIS, now)
        .unwrap();
    assert!(!primary.normal().unwrap().snapshot().ready_for_appends);
    let Some(Action::Broadcast(Control::StartView(start))) = primary.poll(now).unwrap() else {
        panic!("published primary must announce installed view")
    }; // Drop first START_VIEW.
    assert!(matches!(
        primary.poll(now).unwrap(),
        Some(Action::Broadcast(Control::Commit(_)))
    ));
    assert_eq!(primary.poll(now).unwrap(), None);
    let now = now + timing().heartbeat;
    assert_eq!(
        primary.poll(now).unwrap(),
        Some(Action::Broadcast(Control::StartView(start)))
    );
    assert_eq!(
        backup
            .receive(node(1), Control::StartView(start), now)
            .unwrap(),
        Some(start)
    );
    let installed = backup
        .begin_backup_install(node(1), start, JournalGeneration(20), |_, _| None)
        .unwrap();
    assert!(backup.normal().is_none());
    backup.validate_install_suffix(&[operation()]).unwrap();
    backup
        .complete_installation(installed, Prefix::GENESIS, now)
        .unwrap();
    let ack = backup.normal().unwrap().acknowledgment().unwrap();
    assert_eq!(ack.scope.view, 1);
    assert_eq!(ack.durable, operation().prefix());
    primary
        .receive(
            node(2),
            Control::PrepareOk {
                ack,
                grant: Grant {
                    revision: 1,
                    record_limit: 1,
                    byte_limit: 1024,
                },
            },
            now,
        )
        .unwrap();
    assert_eq!(
        primary.normal().unwrap().snapshot().committed,
        operation().prefix()
    );
    assert!(!primary.normal().unwrap().snapshot().ready_for_appends);
    assert_eq!(
        primary.begin_validation(),
        Err(DriverError::Replication(
            ReplicationError::ActivationPending
        ))
    );
    primary.apply_through(operation().prefix()).unwrap();
    assert!(primary.normal().unwrap().snapshot().ready_for_appends);
    let Some(Action::Broadcast(commit @ Control::Commit(_))) = primary.poll(now).unwrap() else {
        panic!("paired heartbeat announces current commit")
    };
    backup.receive(node(1), commit, now).unwrap();
    backup.apply_through(operation().prefix()).unwrap();
    assert!(backup.normal().unwrap().snapshot().ready_for_appends);
}

#[test]
fn installed_backup_rejects_changed_same_view_descriptor_without_reinstalling() {
    let (mut primary, mut backup, now) = selected_pair();
    let ticket = primary
        .begin_primary_install(JournalGeneration(10))
        .unwrap();
    primary.validate_install_suffix(&[operation()]).unwrap();
    primary
        .complete_installation(ticket, Prefix::GENESIS, now)
        .unwrap();
    let start = primary.normal().unwrap().start_view().unwrap().unwrap();
    let ticket = backup
        .begin_backup_install(node(1), start, JournalGeneration(20), |_, _| None)
        .unwrap();
    backup.validate_install_suffix(&[operation()]).unwrap();
    backup
        .complete_installation(ticket, Prefix::GENESIS, now)
        .unwrap();
    let before = backup.normal().unwrap().snapshot();
    assert_eq!(
        backup
            .receive(node(1), Control::StartView(start), now)
            .unwrap(),
        None
    );
    assert_eq!(backup.normal().unwrap().snapshot(), before);
    assert_eq!(
        backup.normal().unwrap().acknowledgment().unwrap().durable,
        operation().prefix()
    );
    assert_eq!(backup.installation_ticket(), None);
    let conflicting = ozzy_replication::StartView {
        generation: JournalGeneration(99),
        ..start
    };
    assert!(
        backup
            .receive(node(1), Control::StartView(conflicting), now)
            .is_err()
    );
    assert!(backup.normal().unwrap().acknowledgment().is_err());
}

#[test]
fn timed_out_installation_settles_before_new_promise_and_old_writer_cannot_fault_it() {
    let (mut primary, _, now) = selected_pair();
    let ticket = primary
        .begin_primary_install(JournalGeneration(10))
        .unwrap();
    primary.validate_install_suffix(&[operation()]).unwrap();
    assert!(primary.fail_io(ticket.previous_generation()).is_err());
    let later = now + timing().election_timeout;
    peer_exit(&mut primary, 2, later);
    assert_eq!(primary.poll(later).unwrap(), None);
    assert_eq!(primary.scope().view, 2);
    primary
        .complete_installation(ticket, Prefix::GENESIS, later)
        .unwrap();
    assert!(primary.normal().is_none());
    assert!(matches!(
        primary.poll(later).unwrap(),
        Some(Action::PersistPromise(_))
    ));
}

#[test]
fn failed_installation_never_reuses_old_role_or_emits_election_actions() {
    let (mut primary, _, now) = selected_pair();
    let ticket = primary
        .begin_primary_install(JournalGeneration(10))
        .unwrap();
    primary.validate_install_suffix(&[operation()]).unwrap();
    primary.fail_io(ticket.generation()).unwrap();
    assert!(primary.poll(now).is_err());
    assert!(primary.poll(now + timing().election_timeout).is_err());
    assert!(
        primary
            .complete_installation(ticket, Prefix::GENESIS, now + timing().election_timeout)
            .is_err()
    );
    assert!(primary.normal().is_none());
}

#[test]
fn selected_tail_activation_uses_recovery_deadline_not_steady_state_timeout() {
    let (mut primary, mut backup, now) = selected_pair();
    let ticket = primary
        .begin_primary_install(JournalGeneration(10))
        .unwrap();
    primary.validate_install_suffix(&[operation()]).unwrap();
    primary
        .complete_installation(ticket, Prefix::GENESIS, now)
        .unwrap();
    let start = primary.normal().unwrap().start_view().unwrap().unwrap();
    let later = now + timing().primary_timeout;
    assert!(matches!(
        primary.poll(later).unwrap(),
        Some(Action::Broadcast(Control::StartView(_)))
    ));
    let installed = backup
        .begin_backup_install(node(1), start, JournalGeneration(20), |_, _| None)
        .unwrap();
    backup.validate_install_suffix(&[operation()]).unwrap();
    backup
        .complete_installation(installed, Prefix::GENESIS, later)
        .unwrap();
    primary
        .receive(
            node(2),
            Control::PrepareOk {
                ack: backup.normal().unwrap().acknowledgment().unwrap(),
                grant: Grant {
                    revision: 1,
                    record_limit: 1,
                    byte_limit: 1024,
                },
            },
            later,
        )
        .unwrap();
    let after_application_io = later + timing().primary_timeout;
    assert!(matches!(
        primary.poll(after_application_io).unwrap(),
        Some(Action::Broadcast(Control::Commit(_)))
    ));
    assert!(!primary.normal().unwrap().snapshot().ready_for_appends);
    primary.apply_through(operation().prefix()).unwrap();
    assert!(primary.normal().unwrap().snapshot().ready_for_appends);
}

#[test]
fn stalled_selected_tail_still_fences_at_the_recovery_deadline() {
    let (mut primary, _, now) = selected_pair();
    let ticket = primary
        .begin_primary_install(JournalGeneration(10))
        .unwrap();
    primary.validate_install_suffix(&[operation()]).unwrap();
    primary
        .complete_installation(ticket, Prefix::GENESIS, now)
        .unwrap();
    peer_exit(&mut primary, 2, now + timing().election_timeout);
    assert!(matches!(
        primary.poll(now + timing().election_timeout).unwrap(),
        Some(Action::PersistPromise(_))
    ));
    assert!(primary.normal().is_none());
    assert_eq!(primary.scope().view, 2);
}

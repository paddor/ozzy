use super::{config, durable, installation::next, local_report, lookup, node, normal, operations};
use ozzy_journal::progress::ProgressError;
use ozzy_replication::{
    Admission, FrozenLog, InstallOutcome, JournalGeneration, PipelineLimits, Prefix, PrepareOk,
    RecoveredState, ReplicationError, Scope, Status, ViewChange, ViewChangeError as Error,
};

fn limits() -> PipelineLimits {
    PipelineLimits {
        max_operations: 4,
        max_body_bytes: 128,
    }
}

fn selected_pair(count: u8, bounds: PipelineLimits) -> (ViewChange, ViewChange) {
    let mut state = recovered(0, 0);
    state.log.accepted = operations(count)
        .last()
        .map_or(Prefix::GENESIS, |op| op.prefix());
    let restart = |index| {
        let mut replica = ViewChange::recover_intact(
            config(),
            node(index),
            JournalGeneration(100 + u128::from(index)),
            state,
            bounds,
        )
        .unwrap();
        let ticket = replica.begin_promise().unwrap();
        replica.complete_promise(ticket).unwrap();
        replica
    };
    let mut candidate = restart(1);
    let mut backup = restart(2);
    local_report(&mut candidate, 2);
    let report = local_report(&mut backup, 1);
    candidate.receive_report(node(2), report).unwrap();
    candidate.select(lookup).unwrap();
    (candidate, backup)
}

#[test]
fn streamed_installation_reconfirms_long_disk_tail_without_live_pipeline_growth() {
    let operations = operations(200);
    let (candidate, backup) = selected_pair(200, limits());
    let mut primary_install = candidate
        .begin_primary_install(JournalGeneration(201))
        .unwrap();
    let ticket = primary_install.ticket();
    assert_eq!(
        primary_install
            .complete(ticket, ticket.committed())
            .unwrap_err(),
        Error::HistoryMissing
    );
    for chunk in operations.chunks(limits().max_operations) {
        primary_install.validate_suffix(chunk).unwrap();
    }
    let InstallOutcome::Normal(mut primary) = primary_install
        .complete(ticket, ticket.committed())
        .unwrap()
    else {
        panic!("current installed view");
    };
    assert_eq!(primary.snapshot().pending_operations, 0);
    assert_eq!(primary.snapshot().pending_body_bytes, 0);
    assert_eq!(primary.snapshot().journal_tail, Some(ticket.accepted()));
    assert!(!primary.snapshot().ready_for_appends);
    let start = primary.start_view().unwrap().unwrap();
    let mut backup_install = backup
        .begin_backup_install(node(1), start, JournalGeneration(202), lookup)
        .unwrap();
    for chunk in operations.chunks(limits().max_operations) {
        backup_install.validate_suffix(chunk).unwrap();
    }
    let ticket = backup_install.ticket();
    let InstallOutcome::Normal(mut backup) =
        backup_install.complete(ticket, ticket.committed()).unwrap()
    else {
        panic!("current installed view");
    };
    assert!(!backup.snapshot().ready_for_appends);
    let before = primary.snapshot();
    assert_eq!(
        primary.receive_ack(
            node(2),
            PrepareOk {
                scope: start.scope,
                durable: operations[3].prefix()
            }
        ),
        Err(ReplicationError::HistoryUnavailable),
    );
    assert_eq!(primary.snapshot(), before);
    primary
        .receive_ack(node(2), backup.acknowledgment().unwrap())
        .unwrap();
    assert_eq!(primary.snapshot().committed, start.accepted);
    assert!(!primary.snapshot().ready_for_appends);
    primary.apply_through(start.accepted).unwrap();
    assert!(primary.snapshot().ready_for_appends);
    assert_eq!(primary.snapshot().journal_tail, None);
    backup
        .receive_commit(node(1), primary.announcement().unwrap())
        .unwrap();
    backup.apply_through(start.accepted).unwrap();
    assert!(backup.snapshot().ready_for_appends);
    // Return to the ordinary bounded pipeline at the recovered tail, not op zero.
    let operation = next(start.scope, start.accepted, 201);
    for replica in [&mut primary, &mut backup] {
        let Admission::Write { ticket, .. } =
            replica.prepare(node(1), start.scope, &[operation]).unwrap()
        else {
            panic!("fresh operation after selected tail");
        };
        replica.complete_durable_write(ticket).unwrap();
    }
    primary
        .receive_ack(node(2), backup.acknowledgment().unwrap())
        .unwrap();
    primary.apply_through(operation.prefix()).unwrap();
    backup
        .receive_commit(node(1), primary.announcement().unwrap())
        .unwrap();
    backup.apply_through(operation.prefix()).unwrap();
    assert_eq!(primary.snapshot().applied, operation.prefix());
    assert_eq!(backup.snapshot().applied, operation.prefix());
}

#[test]
fn streamed_validation_rejects_gaps_scope_conflicts_and_chunk_overruns_atomically() {
    let operations = operations(8);
    let (candidate, _) = selected_pair(8, limits());
    let mut pending = candidate
        .begin_primary_install(JournalGeneration(201))
        .unwrap();
    let ticket = pending.ticket();
    assert_eq!(
        pending.validate_suffix(&operations[..5]),
        Err(ReplicationError::Capacity.into())
    );
    assert_eq!(
        pending.validate_suffix(&operations[1..3]),
        Err(ReplicationError::HistoryGap.into())
    );
    // First entry is valid, second leaves a gap. Neither may advance the cursor.
    assert_eq!(
        pending.validate_suffix(&[operations[0], operations[2]]),
        Err(ReplicationError::HistoryGap.into())
    );
    let wrong_view = next(ticket.scope(), Prefix::GENESIS, 99);
    assert_eq!(
        pending.validate_suffix(&[wrong_view]),
        Err(ReplicationError::ScopeMismatch.into())
    );
    pending.validate_suffix(&operations[..4]).unwrap();
    let wrong_predecessor = next(
        config().scope(),
        Prefix {
            op: operations[3].prefix().op,
            digest: operations[2].prefix().digest,
        },
        99,
    );
    assert_eq!(
        pending.validate_suffix(&[wrong_predecessor]),
        Err(ReplicationError::ConflictingHistory.into())
    );
    assert_eq!(
        pending.complete(ticket, ticket.committed()).unwrap_err(),
        Error::HistoryMissing
    );
    pending.validate_suffix(&operations[4..]).unwrap();
    let extra = next(config().scope(), operations[7].prefix(), 9);
    assert_eq!(
        pending.validate_suffix(&[extra]),
        Err(ReplicationError::ConflictingHistory.into())
    );
    assert!(matches!(
        pending.complete(ticket, ticket.committed()),
        Ok(InstallOutcome::Normal(_))
    ));
    assert_eq!(
        pending.validate_suffix(&[]),
        Err(Error::InstallationCompleted)
    );

    let (candidate, _) = selected_pair(
        1,
        PipelineLimits {
            max_body_bytes: 1,
            ..limits()
        },
    );
    let mut pending = candidate
        .begin_primary_install(JournalGeneration(201))
        .unwrap();
    assert_eq!(
        pending.validate_suffix(&operations[..1]),
        Err(ReplicationError::Capacity.into())
    );
    let ticket = pending.ticket();
    assert_eq!(
        pending.complete(ticket, ticket.committed()).unwrap_err(),
        Error::HistoryMissing
    );
}

#[test]
fn newer_view_during_streaming_preserves_installed_tail_without_normal_authority() {
    let operations = operations(8);
    let (candidate, _) = selected_pair(8, limits());
    let mut pending = candidate
        .begin_primary_install(JournalGeneration(201))
        .unwrap();
    let ticket = pending.ticket();
    pending.validate_suffix(&operations[..4]).unwrap();
    pending
        .observe_view(
            node(2),
            Scope {
                view: 3,
                ..ticket.scope()
            },
        )
        .unwrap();
    assert_eq!(
        pending.complete(ticket, ticket.committed()).unwrap_err(),
        Error::HistoryMissing
    );
    pending.validate_suffix(&operations[4..]).unwrap();
    let InstallOutcome::ViewChanging(mut changing) =
        pending.complete(ticket, ticket.committed()).unwrap()
    else {
        panic!("higher view fenced installation");
    };
    assert_eq!(changing.normal_snapshot().status, Status::Fenced);
    assert!(!changing.normal_snapshot().ready_for_appends);
    assert_eq!(changing.normal_snapshot().pending_operations, 0);
    let promise = changing.begin_promise().unwrap();
    assert_eq!(promise.scope().view, 3);
    assert_eq!(promise.log().last_normal_view, 1);
    assert_eq!(promise.log().accepted, operations[7].prefix());
    assert_eq!(promise.generation(), JournalGeneration(201));
}

fn recovered(promised: u64, normal: u64) -> RecoveredState {
    RecoveredState {
        scope: Scope {
            view: promised,
            ..config().scope()
        },
        log: FrozenLog {
            last_normal_view: normal,
            accepted: Prefix::GENESIS,
            committed: Prefix::GENESIS,
        },
    }
}

#[test]
fn restart_retains_higher_promise_and_disk_tail_without_live_descriptors() {
    let operations = operations(200);
    let mut state = recovered(17, 12);
    state.log.accepted = operations[199].prefix();
    state.log.committed = operations[99].prefix();
    let mut restarting =
        ViewChange::recover_intact(config(), node(2), JournalGeneration(101), state, limits())
            .unwrap();
    assert_eq!(restarting.scope().view, 17);
    assert_eq!(restarting.promised_view(), 17);
    let snapshot = restarting.normal_snapshot();
    assert_eq!(snapshot.scope.view, 12);
    assert_eq!(snapshot.accepted, state.log.accepted);
    assert_eq!(snapshot.committed, state.log.committed);
    assert_eq!(snapshot.applied, Prefix::GENESIS);
    assert_eq!(snapshot.journal.durable, state.log.accepted.op);
    assert_eq!(snapshot.pending_operations, 0);
    assert_eq!(snapshot.pending_body_bytes, 0);
    assert_eq!(restarting.advance_view(17), Err(Error::ViewNotHigher));
    assert_eq!(restarting.begin_promise(), Err(Error::AlreadyPromised));
    assert_eq!(restarting.start_message().unwrap().scope.view, 17);
    assert_eq!(restarting.report(), Err(Error::StartQuorumMissing));
    assert_eq!(local_report(&mut restarting, 1).log, state.log);
    // One local report, not the old process's ACK set, survives startup.
    assert_eq!(restarting.select(lookup), Err(Error::ReportQuorumMissing));
}

#[test]
fn invalid_recovered_scope_history_and_view_exhaustion_cannot_vote() {
    let valid = recovered(5, 4);
    let restore = |state| {
        ViewChange::recover_intact(config(), node(0), JournalGeneration(100), state, limits())
            .unwrap_err()
    };
    let mut wrong_scope = valid;
    wrong_scope.scope.configuration_epoch += 1;
    assert_eq!(restore(wrong_scope), ReplicationError::ScopeMismatch.into());
    wrong_scope = valid;
    wrong_scope.scope.configuration_digest = operations(1)[0].prefix().digest;
    assert_eq!(restore(wrong_scope), ReplicationError::ScopeMismatch.into());
    wrong_scope = valid;
    wrong_scope.scope.group_id = ozzy_proto::GroupId::from_bytes([19; 16]);
    assert_eq!(restore(wrong_scope), ReplicationError::ScopeMismatch.into());
    for bad_log in [
        FrozenLog {
            last_normal_view: 6,
            ..valid.log
        },
        FrozenLog {
            committed: operations(1)[0].prefix(),
            ..valid.log
        },
        FrozenLog {
            accepted: Prefix {
                op: ozzy_replication::OpNumber(1),
                ..Prefix::GENESIS
            },
            ..valid.log
        },
        FrozenLog {
            accepted: operations(1)[0].prefix(),
            committed: Prefix {
                op: ozzy_replication::OpNumber(1),
                digest: operations(2)[1].prefix().digest,
            },
            ..valid.log
        },
    ] {
        assert_eq!(
            restore(RecoveredState {
                log: bad_log,
                ..valid
            }),
            Error::InvalidReport
        );
    }
    assert_eq!(restore(recovered(u64::MAX, u64::MAX)), Error::ViewExhausted);
    assert!(
        ViewChange::recover_intact(
            config(),
            node(0),
            JournalGeneration(100),
            recovered(u64::MAX, 4),
            limits()
        )
        .is_ok()
    ); // Can resume a promise, never wrap it.
    assert_eq!(
        ViewChange::recover_intact(config(), node(99), JournalGeneration(100), valid, limits())
            .unwrap_err(),
        ReplicationError::UnknownVoter.into(),
    );
}

#[test]
fn callbacks_from_crashed_writer_cannot_fault_or_change_recovered_evidence() {
    let mut old = normal(0);
    let Admission::Write { ticket: write, .. } = old
        .prepare(node(0), config().scope(), &operations(1))
        .unwrap()
    else {
        panic!("new operation");
    };
    old.complete_write(write).unwrap();
    let sync = old.begin_sync().unwrap();
    drop(old);
    let mut restarted = ViewChange::recover_intact(
        config(),
        node(0),
        JournalGeneration(100),
        recovered(0, 0),
        limits(),
    )
    .unwrap();
    let before = restarted.normal_snapshot();
    let stale = Error::Replication(ReplicationError::Journal(ProgressError::StaleGeneration));
    assert_eq!(restarted.complete_write(write), Err(stale));
    assert_eq!(restarted.complete_sync(sync), Err(stale));
    assert_eq!(restarted.fail_io(write.generation()), Err(stale));
    assert_eq!(restarted.normal_snapshot(), before);
    assert_eq!(restarted.start_message(), Err(Error::PromiseRequired));
    assert!(restarted.begin_promise().is_ok());
}

#[test]
fn restarted_primary_cannot_reuse_view_after_send_before_local_write() {
    let mut old_primary = normal(0);
    let operation = operations(1);
    assert!(matches!(
        old_primary.prepare(node(0), config().scope(), &operation),
        Ok(Admission::Write { .. })
    ));
    // Backup persisted the transmitted prepare. Primary never wrote it locally.
    let backup = durable(1, 1);
    drop(old_primary);
    let mut restarted = ViewChange::recover_intact(
        config(),
        node(0),
        JournalGeneration(100),
        RecoveredState {
            scope: config().scope(),
            log: FrozenLog {
                last_normal_view: 0,
                accepted: Prefix::GENESIS,
                committed: Prefix::GENESIS,
            },
        },
        limits(),
    )
    .unwrap();
    assert_eq!(restarted.scope().view, 1);
    assert_eq!(restarted.promised_view(), 0);
    assert_eq!(restarted.normal_snapshot().status, Status::Fenced);
    assert!(!restarted.normal_snapshot().ready_for_appends);
    assert_eq!(restarted.start_message(), Err(Error::PromiseRequired));
    let promise = restarted.begin_promise().unwrap();
    restarted.complete_promise(promise).unwrap();
    let report = local_report(&mut restarted, 1);

    let mut candidate = backup.into_view_change(1).unwrap();
    let promise = candidate.begin_promise().unwrap();
    candidate.complete_promise(promise).unwrap();
    local_report(&mut candidate, 0);
    candidate.receive_report(node(0), report).unwrap();
    let selected = candidate.select(lookup).unwrap();
    assert_eq!(selected.source().accepted, operation[0].prefix());
    assert_eq!(selected.committed(), Prefix::GENESIS);
    assert_eq!(
        restarted.select(lookup),
        Err(ReplicationError::WrongRole.into())
    );
}

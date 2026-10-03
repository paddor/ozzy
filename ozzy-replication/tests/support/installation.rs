use super::{
    config, durable, local_report, lookup, node, normal, operations, promised, remote_report,
};
use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
    encode_operation_body,
};
use ozzy_journal::progress::ProgressError;
use ozzy_proto::{GroupId, OperationId};
use ozzy_replication::{
    Admission, Commit, Digest, InstallOutcome, InstallingView, JournalGeneration, NormalReplica,
    OpNumber, PipelineLimits, Prefix, PreparedOperation, ReplicationError, Scope, StartView,
    Status, ViewChange, ViewChangeError as Error,
};

fn selected(count: u8) -> ViewChange {
    let mut candidate = promised(1, count, 1);
    local_report(&mut candidate, 2);
    candidate
        .receive_report(node(2), remote_report(2, count, 1))
        .unwrap();
    candidate.select(lookup).unwrap();
    candidate
}

fn installing(count: u8, generation: u128) -> InstallingView {
    selected(count)
        .install_primary(JournalGeneration(generation), &operations(count))
        .unwrap()
}

fn finish(installing: &mut InstallingView) -> NormalReplica {
    let ticket = installing.ticket();
    let InstallOutcome::Normal(normal) = installing.complete(ticket, ticket.committed()).unwrap()
    else {
        panic!("no newer view");
    };
    normal
}

#[test]
fn abandoned_stream_restores_only_original_durable_history_and_a_later_election() {
    let mut candidate = promised(1, 2, 1);
    local_report(&mut candidate, 2);
    candidate
        .receive_report(node(2), remote_report(2, 4, 1))
        .unwrap();
    candidate.select(lookup).unwrap();
    let mut pending = candidate
        .begin_primary_install(JournalGeneration(100))
        .unwrap();
    let ticket = pending.ticket();
    assert_eq!(ticket.accepted(), operations(4)[3].prefix());
    pending.validate_suffix(&operations(1)).unwrap();
    assert!(matches!(
        pending.complete_abandon(ticket),
        Err(Error::ViewNotHigher)
    ));
    pending.request_view(4).unwrap();
    let wrong = installing(4, 101).ticket();
    assert!(matches!(
        pending.complete_abandon(wrong),
        Err(Error::StaleInstallation)
    ));
    let mut changing = pending.complete_abandon(ticket).unwrap();
    let restored = changing.normal_snapshot();
    assert_eq!(restored.status, Status::Fenced);
    assert_eq!(restored.accepted, operations(2)[1].prefix());
    assert_eq!(restored.journal.generation, ticket.previous_generation());
    assert_eq!(restored.journal.durable, OpNumber(2));
    assert_eq!(restored.pending_operations, 0);
    assert_eq!(restored.scope.view, 0);
    assert!(!restored.ready_for_appends);
    assert_eq!(changing.promised_view(), 1);
    let promise = changing.begin_promise().unwrap();
    assert_eq!(promise.scope().view, 4);
    assert_eq!(promise.log().accepted, restored.accepted);
    assert_eq!(promise.log().last_normal_view, 0);
    assert!(matches!(
        pending.complete(ticket, ticket.committed()),
        Err(Error::InstallationCompleted)
    ));
}

#[test]
fn failed_installation_cannot_be_abandoned_to_reuse_uncertain_storage() {
    let mut pending = installing(4, 100);
    let ticket = pending.ticket();
    pending.request_view(4).unwrap();
    pending.fail(ticket).unwrap();
    assert!(matches!(
        pending.complete_abandon(ticket),
        Err(Error::Faulted)
    ));
}

fn backup(start: StartView, count: u8) -> InstallingView {
    promised(2, 0, start.scope.view)
        .install_backup(
            node(1),
            start,
            JournalGeneration(200),
            &operations(count),
            lookup,
        )
        .unwrap()
}

pub(super) fn next(scope: Scope, previous: Prefix, value: u8) -> PreparedOperation {
    let body = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([value; 16]),
    });
    let bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
    PreparedOperation::from_verified(
        &CanonicalOperation {
            group_id: scope.group_id,
            configuration_epoch: scope.configuration_epoch,
            original_view: scope.view,
            op_number: previous.op.0 + 1,
            previous_digest: previous.digest,
            kind: body.kind(),
            body: &bytes,
        },
        canonical_body_digest(&bytes),
    )
}

#[test]
fn selected_tail_requires_new_view_quorum_and_application_before_fresh_work() {
    let mut pending = installing(4, 100);
    let ticket = pending.ticket();
    assert_eq!(ticket.previous_generation(), JournalGeneration(2));
    assert_eq!(ticket.protected_committed(), Prefix::GENESIS);
    let mut primary = finish(&mut pending);
    assert_eq!(primary.snapshot().scope.view, 1);
    assert_eq!(primary.snapshot().journal.durable, OpNumber(4));
    assert!(!primary.snapshot().ready_for_appends);
    let start = primary.start_view().unwrap().unwrap();
    assert_eq!(start.generation, JournalGeneration(100));
    let fresh = next(start.scope, start.accepted, 5);
    assert_eq!(
        primary.prepare(node(1), start.scope, &[fresh]),
        Err(ReplicationError::ActivationPending)
    );
    assert_eq!(
        primary.prepare(node(1), start.scope, &operations(4)),
        Ok(Admission::Duplicate)
    );
    let mut backup = finish(&mut backup(start, 4));
    primary
        .receive_ack(node(2), backup.acknowledgment().unwrap())
        .unwrap();
    assert_eq!(primary.snapshot().committed, start.accepted);
    assert!(!primary.snapshot().ready_for_appends);
    assert_eq!(
        primary.prepare(node(1), start.scope, &[fresh]),
        Err(ReplicationError::ActivationPending)
    );
    primary.apply_through(start.accepted).unwrap();
    assert!(primary.snapshot().ready_for_appends);
    let Admission::Write { ticket: write, .. } =
        primary.prepare(node(1), start.scope, &[fresh]).unwrap()
    else {
        panic!("fresh new-view operation");
    };
    assert_eq!(write.generation(), JournalGeneration(100));
    assert_eq!(primary.start_view().unwrap(), Some(start));
    backup
        .receive_commit(node(1), primary.announcement().unwrap())
        .unwrap();
    backup.apply_through(start.accepted).unwrap();
    assert_eq!(backup.snapshot().applied, start.accepted);
}

#[test]
fn old_view_ack_and_old_writer_callbacks_cannot_activate_the_new_primary() {
    let mut old = normal(1);
    let Admission::Write { ticket: write, .. } = old
        .prepare(node(0), config().scope(), &operations(2))
        .unwrap()
    else {
        panic!("new write");
    };
    old.complete_write(write).unwrap();
    let sync = old.begin_sync().unwrap();
    old.complete_sync(sync).unwrap();
    let mut election = old.into_view_change(1).unwrap();
    let promise = election.begin_promise().unwrap();
    election.complete_promise(promise).unwrap();
    local_report(&mut election, 2);
    election
        .receive_report(node(2), remote_report(2, 2, 1))
        .unwrap();
    election.select(lookup).unwrap();
    let mut primary = finish(
        &mut election
            .install_primary(JournalGeneration(100), &operations(2))
            .unwrap(),
    );
    let before = primary.snapshot();
    let old_ack = durable(2, 2).acknowledgment().unwrap();
    assert_eq!(
        primary.receive_ack(node(2), old_ack),
        Err(ReplicationError::ScopeMismatch)
    );
    assert_eq!(
        primary.complete_write(write),
        Err(ProgressError::StaleGeneration.into())
    );
    assert_eq!(
        primary.complete_sync(sync),
        Err(ProgressError::StaleGeneration.into())
    );
    assert_eq!(
        primary.fail_io(write.generation()),
        Err(ProgressError::StaleGeneration.into())
    );
    assert_eq!(primary.snapshot(), before);
}

#[test]
fn installed_retries_preserve_original_views_but_new_operations_need_active_view() {
    let mut primary = finish(&mut installing(2, 100));
    let scope = primary.snapshot().scope;
    let mut backup = finish(&mut backup(primary.start_view().unwrap().unwrap(), 2));
    primary
        .receive_ack(node(2), backup.acknowledgment().unwrap())
        .unwrap();
    primary.apply_through(primary.snapshot().committed).unwrap();
    let before = primary.snapshot();
    assert_eq!(
        primary.prepare(node(1), scope, &operations(3)[2..]),
        Err(ReplicationError::ScopeMismatch)
    );
    assert_eq!(primary.snapshot(), before);
    assert_eq!(
        backup.prepare(node(1), scope, &operations(2)),
        Ok(Admission::Duplicate)
    );
}

#[test]
fn already_committed_selected_tail_still_needs_a_new_view_backup_vote() {
    let mut old = durable(1, 4);
    let committed = operations(4)[3].prefix();
    old.receive_commit(
        node(0),
        Commit {
            scope: config().scope(),
            committed,
        },
    )
    .unwrap();
    old.apply_through(committed).unwrap();
    let mut election = old.into_view_change(1).unwrap();
    let promise = election.begin_promise().unwrap();
    election.complete_promise(promise).unwrap();
    local_report(&mut election, 2);
    election
        .receive_report(node(2), remote_report(2, 4, 1))
        .unwrap();
    election.select(lookup).unwrap();
    let mut primary = finish(
        &mut election
            .install_primary(JournalGeneration(100), &[])
            .unwrap(),
    );
    assert_eq!(primary.snapshot().applied, committed);
    assert!(!primary.snapshot().ready_for_appends);
    let start = primary.start_view().unwrap().unwrap();
    let mut pending_backup = promised(2, 0, 1)
        .install_backup(node(1), start, JournalGeneration(200), &[], lookup)
        .unwrap();
    let backup = finish(&mut pending_backup);
    primary
        .receive_ack(node(2), backup.acknowledgment().unwrap())
        .unwrap();
    assert!(primary.snapshot().ready_for_appends);
}

#[test]
fn backup_keeps_a_higher_local_commit_than_start_view_advertises() {
    let primary = finish(&mut installing(4, 100));
    let start = primary.start_view().unwrap().unwrap();
    assert_eq!(start.committed, Prefix::GENESIS);
    let mut old = durable(0, 4);
    old.receive_ack(node(1), durable(1, 4).acknowledgment().unwrap())
        .unwrap();
    let committed = old.snapshot().committed;
    old.apply_through(committed).unwrap();
    let mut election = old.into_view_change(1).unwrap();
    let promise = election.begin_promise().unwrap();
    election.complete_promise(promise).unwrap();
    let mut pending = election
        .install_backup(node(1), start, JournalGeneration(300), &[], lookup)
        .unwrap();
    assert_eq!(pending.ticket().committed(), committed);
    assert_eq!(pending.ticket().protected_committed(), committed);
    let backup = finish(&mut pending);
    assert_eq!(backup.snapshot().committed, committed);
    assert_eq!(backup.acknowledgment().unwrap().durable, committed);
}

#[test]
fn exact_installation_and_application_completion_transfer_ownership_once() {
    let mut left = installing(2, 100);
    let right = installing(2, 101);
    let ticket = left.ticket();
    assert!(matches!(
        left.complete(right.ticket(), Prefix::GENESIS),
        Err(Error::StaleInstallation)
    ));
    assert_eq!(left.fail(right.ticket()), Err(Error::StaleInstallation));
    assert!(matches!(
        left.complete(ticket, operations(1)[0].prefix()),
        Err(Error::ApplicationPending)
    ));
    finish(&mut left);
    assert!(matches!(
        left.complete(ticket, Prefix::GENESIS),
        Err(Error::InstallationCompleted)
    ));
    assert_eq!(left.fail(ticket), Err(Error::InstallationCompleted));
}

#[test]
fn failed_publication_cannot_resume_either_old_or_new_role() {
    let mut pending = installing(2, 100);
    let ticket = pending.ticket();
    pending.fail(ticket).unwrap();
    assert!(matches!(
        pending.complete(ticket, Prefix::GENESIS),
        Err(Error::Faulted)
    ));
    assert_eq!(pending.request_view(2), Err(Error::Faulted));
}

#[test]
fn newer_view_during_disk_work_skips_normal_activation_and_keeps_installed_lineage() {
    let mut pending = installing(4, 100);
    let ticket = pending.ticket();
    pending
        .observe_view(
            node(0),
            Scope {
                view: 3,
                ..ticket.scope()
            },
        )
        .unwrap();
    pending
        .observe_view(
            node(2),
            Scope {
                view: 2,
                ..ticket.scope()
            },
        )
        .unwrap();
    let InstallOutcome::ViewChanging(mut election) =
        pending.complete(ticket, Prefix::GENESIS).unwrap()
    else {
        panic!("newer view must suppress activation");
    };
    assert_eq!(election.scope().view, 3);
    assert_eq!(election.normal_snapshot().scope.view, 1);
    assert_eq!(election.normal_snapshot().status, Status::Fenced);
    assert_eq!(election.normal_snapshot().accepted, ticket.accepted());
    assert_eq!(election.start_message(), Err(Error::PromiseRequired));
    let promise = election.begin_promise().unwrap();
    assert_eq!(promise.generation(), JournalGeneration(100));
    assert_eq!(promise.log().last_normal_view, 1);
    assert_eq!(promise.log().accepted, ticket.accepted());
    election.complete_promise(promise).unwrap();
    assert_eq!(election.start_message().unwrap().scope.view, 3);
}

#[test]
fn installation_progress_observations_validate_scope_voters_and_monotonicity() {
    let mut pending = installing(0, 100);
    let scope = pending.ticket().scope();
    let future = Scope { view: 3, ..scope };
    assert_eq!(
        pending.observe_view(node(3), future),
        Err(ReplicationError::UnknownVoter.into())
    );
    assert_eq!(
        pending.observe_view(
            node(0),
            Scope {
                group_id: GroupId::from_bytes([99; 16]),
                ..future
            }
        ),
        Err(ReplicationError::ScopeMismatch.into())
    );
    assert_eq!(pending.request_view(1), Err(Error::ViewNotHigher));
    pending.request_view(u64::MAX).unwrap();
    assert_eq!(pending.request_view(0), Err(Error::ViewNotHigher));
    let ticket = pending.ticket();
    let InstallOutcome::ViewChanging(election) = pending.complete(ticket, Prefix::GENESIS).unwrap()
    else {
        panic!("timer requested later view");
    };
    assert_eq!(election.scope().view, u64::MAX);
}

#[test]
fn installation_cannot_bypass_promises_selection_or_fresh_writer_generation() {
    assert!(matches!(
        normal(1)
            .into_view_change(1)
            .unwrap()
            .install_primary(JournalGeneration(100), &[])
            .map_err(|error| error.error()),
        Err(Error::PromiseRequired)
    ));
    assert!(matches!(
        promised(1, 0, 1)
            .install_primary(JournalGeneration(100), &[])
            .map_err(|error| error.error()),
        Err(Error::SelectionRequired)
    ));
    assert!(matches!(
        selected(2)
            .install_primary(JournalGeneration(2), &operations(2))
            .map_err(|error| error.error()),
        Err(Error::ReusedGeneration)
    ));
    assert!(matches!(
        promised(2, 0, 1)
            .install_primary(JournalGeneration(100), &[])
            .map_err(|error| error.error()),
        Err(Error::Replication(ReplicationError::WrongRole))
    ));
}

#[test]
fn installation_rejects_missing_wrong_or_new_view_tail_and_capacity_overruns() {
    assert!(matches!(
        selected(4)
            .install_primary(JournalGeneration(100), &operations(3))
            .map_err(|error| error.error()),
        Err(Error::Replication(ReplicationError::ConflictingHistory))
    ));
    assert!(matches!(
        selected(4)
            .install_primary(JournalGeneration(100), &operations(4)[1..])
            .map_err(|error| error.error()),
        Err(Error::Replication(ReplicationError::HistoryGap))
    ));
    let current_view_operation = next(
        Scope {
            view: 1,
            ..config().scope()
        },
        Prefix::GENESIS,
        1,
    );
    assert!(matches!(
        selected(1)
            .install_primary(JournalGeneration(100), &[current_view_operation])
            .map_err(|error| error.error()),
        Err(Error::Replication(ReplicationError::ScopeMismatch))
    ));
    let start = StartView {
        scope: Scope {
            view: 1,
            ..config().scope()
        },
        generation: JournalGeneration(100),
        accepted: operations(33)[32].prefix(),
        committed: Prefix::GENESIS,
    };
    assert!(matches!(
        promised(2, 0, 1)
            .install_backup(
                node(1),
                start,
                JournalGeneration(200),
                &operations(33),
                lookup
            )
            .map_err(|error| error.error()),
        Err(Error::Replication(ReplicationError::Capacity))
    ));
    let mut small = NormalReplica::bootstrap(
        config(),
        node(2),
        JournalGeneration(3),
        PipelineLimits {
            max_operations: 8,
            max_body_bytes: 16,
        },
    )
    .unwrap()
    .into_view_change(1)
    .unwrap();
    let ticket = small.begin_promise().unwrap();
    small.complete_promise(ticket).unwrap();
    assert!(matches!(
        small
            .install_backup(
                node(1),
                StartView {
                    accepted: operations(2)[1].prefix(),
                    ..start
                },
                JournalGeneration(200),
                &operations(2),
                lookup
            )
            .map_err(|error| error.error()),
        Err(Error::Replication(ReplicationError::Capacity))
    ));
}

#[test]
fn backup_rejects_wrong_primary_or_view_and_conflicting_protected_history() {
    let start = finish(&mut installing(4, 100))
        .start_view()
        .unwrap()
        .unwrap();
    assert!(matches!(
        promised(2, 0, 1)
            .install_backup(
                node(0),
                start,
                JournalGeneration(200),
                &operations(4),
                lookup
            )
            .map_err(|error| error.error()),
        Err(Error::Replication(ReplicationError::WrongRole))
    ));
    assert!(matches!(
        promised(2, 0, 2)
            .install_backup(
                node(1),
                start,
                JournalGeneration(200),
                &operations(4),
                lookup
            )
            .map_err(|error| error.error()),
        Err(Error::Replication(ReplicationError::ScopeMismatch))
    ));
    let mut old = durable(2, 2);
    old.receive_commit(
        node(0),
        Commit {
            scope: config().scope(),
            committed: operations(2)[1].prefix(),
        },
    )
    .unwrap();
    let mut election = old.into_view_change(1).unwrap();
    let promise = election.begin_promise().unwrap();
    election.complete_promise(promise).unwrap();
    assert!(matches!(
        election
            .install_backup(
                node(1),
                start,
                JournalGeneration(200),
                &operations(4)[2..],
                |_, _| Some(Digest::from_bytes([99; 32]))
            )
            .map_err(|error| error.error()),
        Err(Error::ConflictingHistory)
    ));
}

#[test]
fn empty_selected_view_can_accept_new_work_after_installation() {
    let mut primary = finish(&mut installing(0, 100));
    assert!(primary.snapshot().ready_for_appends);
    let scope = primary.snapshot().scope;
    assert!(matches!(
        primary.prepare(node(1), scope, &[next(scope, Prefix::GENESIS, 1)]),
        Ok(Admission::Write { .. })
    ));
    assert!(normal(0).start_view().unwrap().is_none());
}

#[test]
fn rejected_installation_returns_fenced_ownership_for_missing_data_and_stale_retry() {
    let election = selected(4);
    let before = election.normal_snapshot();
    let error = election
        .install_primary(JournalGeneration(100), &operations(3))
        .unwrap_err();
    assert_eq!(
        error.error(),
        Error::Replication(ReplicationError::ConflictingHistory)
    );
    let election = error.into_view_change();
    assert_eq!(election.normal_snapshot(), before);
    let primary = finish(
        &mut election
            .install_primary(JournalGeneration(100), &operations(4))
            .unwrap(),
    );
    let start = primary.start_view().unwrap().unwrap();
    let mut old = durable(2, 2);
    old.receive_commit(
        node(0),
        Commit {
            scope: config().scope(),
            committed: operations(2)[1].prefix(),
        },
    )
    .unwrap();
    let mut election = old.into_view_change(1).unwrap();
    let promise = election.begin_promise().unwrap();
    election.complete_promise(promise).unwrap();
    let before = election.normal_snapshot();
    let error = election
        .install_backup(
            node(1),
            start,
            JournalGeneration(200),
            &operations(4)[2..],
            |_, _| None,
        )
        .unwrap_err();
    assert_eq!(error.error(), Error::HistoryMissing);
    let election = error.into_view_change();
    assert_eq!(election.normal_snapshot(), before);
    let stale = StartView {
        scope: config().scope(),
        ..start
    };
    let error = election
        .install_backup(
            node(1),
            stale,
            JournalGeneration(200),
            &operations(4)[2..],
            lookup,
        )
        .unwrap_err();
    assert_eq!(
        error.error(),
        Error::Replication(ReplicationError::ScopeMismatch)
    );
    let election = error.into_view_change();
    assert_eq!(election.normal_snapshot(), before);
    let backup = finish(
        &mut election
            .install_backup(
                node(1),
                start,
                JournalGeneration(200),
                &operations(4)[2..],
                lookup,
            )
            .unwrap(),
    );
    assert_eq!(backup.snapshot().scope.view, 1);
    assert_eq!(backup.snapshot().committed.op, OpNumber(2));
}

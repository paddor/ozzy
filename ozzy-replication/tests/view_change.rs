use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
    encode_operation_body,
};
use ozzy_proto::{GroupId, NodeId, OperationId};
use ozzy_replication::{
    Admission, Configuration, Digest, DoViewChange, FrozenLog, JournalGeneration, NormalReplica,
    OpNumber, PipelineLimits, Prefix, PreparedOperation, ReplicationError, StartViewChange, Status,
    ViewChange, ViewChangeError as Error,
};

#[path = "support/installation.rs"]
mod installation;

#[path = "support/recovery.rs"]
mod recovery;

fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}

fn config() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [node(0), node(1), node(2)],
    )
    .unwrap()
}

fn operations(count: u8) -> Vec<PreparedOperation> {
    let mut previous = Prefix::GENESIS;
    (1..=count)
        .map(|number| {
            let body = OperationBody::Barrier(Barrier {
                operation_id: OperationId::from_bytes([number; 16]),
            });
            let bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
            let operation = CanonicalOperation {
                group_id: config().scope().group_id,
                configuration_epoch: 1,
                original_view: 0,
                op_number: u64::from(number),
                previous_digest: previous.digest,
                kind: body.kind(),
                body: &bytes,
            };
            let prepared =
                PreparedOperation::from_verified(&operation, canonical_body_digest(&bytes));
            previous = prepared.prefix();
            prepared
        })
        .collect()
}

fn normal(index: u8) -> NormalReplica {
    NormalReplica::bootstrap(
        config(),
        node(index),
        JournalGeneration(u128::from(index) + 1),
        PipelineLimits {
            max_operations: 32,
            max_body_bytes: 512,
        },
    )
    .unwrap()
}

fn durable(index: u8, count: u8) -> NormalReplica {
    let mut replica = normal(index);
    if count > 0 {
        let Admission::Write { ticket, .. } = replica
            .prepare(node(0), config().scope(), &operations(count))
            .unwrap()
        else {
            panic!("new operations");
        };
        replica.complete_durable_write(ticket).unwrap();
    }
    replica
}

fn promised(index: u8, count: u8, view: u64) -> ViewChange {
    let mut election = durable(index, count).into_view_change(view).unwrap();
    let ticket = election.begin_promise().unwrap();
    election.complete_promise(ticket).unwrap();
    election
}

fn local_report(election: &mut ViewChange, other: u8) -> DoViewChange {
    election
        .receive_start(
            node(other),
            StartViewChange {
                scope: election.scope(),
            },
        )
        .unwrap();
    election.report().unwrap()
}

fn remote_report(index: u8, count: u8, view: u64) -> DoViewChange {
    local_report(&mut promised(index, count, view), (index + 1) % 3)
}

fn lookup(_: ozzy_replication::LogSource, op: OpNumber) -> Option<Digest> {
    let count = u8::try_from(op.0).ok()?;
    operations(count)
        .last()
        .map(|operation| operation.prefix().digest)
}

#[test]
fn old_writes_and_sync_must_settle_before_any_promise_or_election_message() {
    let mut old = normal(0);
    let Admission::Write { ticket, .. } = old
        .prepare(node(0), config().scope(), &operations(3))
        .unwrap()
    else {
        panic!("new operations");
    };
    let mut election = old.into_view_change(1).unwrap();
    assert_eq!(election.normal_snapshot().status, Status::Fenced);
    assert_eq!(election.begin_promise(), Err(Error::StoragePending));
    assert_eq!(election.start_message(), Err(Error::PromiseRequired));
    assert_eq!(election.report(), Err(Error::PromiseRequired));
    election.complete_write(ticket).unwrap();
    assert_eq!(election.begin_promise(), Err(Error::StoragePending));
    let sync = election.begin_sync().unwrap();
    election.complete_sync(sync).unwrap();
    assert_eq!(election.normal_snapshot().committed, Prefix::GENESIS);
    let promise = election.begin_promise().unwrap();
    assert_eq!(promise.log().accepted, operations(3)[2].prefix());
    assert_eq!(promise.log().committed, Prefix::GENESIS);
    assert_eq!(promise.log().last_normal_view, 0);
    assert_eq!(promise.generation(), ticket.generation());
    assert_eq!(promise.scope().view, 1);
    assert_eq!(election.begin_promise(), Err(Error::PromisePending));
    assert_eq!(election.start_message(), Err(Error::PromiseRequired));
    election.complete_promise(promise).unwrap();
    assert_eq!(election.start_message().unwrap().scope.view, 1);
    assert_eq!(election.begin_promise(), Err(Error::AlreadyPromised));
}

#[test]
fn other_voters_cannot_replace_local_persistence_or_primary_own_report() {
    let mut election = normal(1).into_view_change(1).unwrap();
    for other in [0, 2] {
        election
            .receive_start(
                node(other),
                StartViewChange {
                    scope: election.scope(),
                },
            )
            .unwrap();
        election
            .receive_report(node(other), remote_report(other, 0, 1))
            .unwrap();
    }
    assert_eq!(election.report(), Err(Error::PromiseRequired));
    assert_eq!(election.select(lookup), Err(Error::PromiseRequired));
    let promise = election.begin_promise().unwrap();
    election.complete_promise(promise).unwrap();
    assert_eq!(election.select(lookup), Err(Error::ReportQuorumMissing));
    election.report().unwrap();
    assert_eq!(election.select(lookup).unwrap().voter_mask(), 0b111);
}

#[test]
fn duplicate_first_phase_messages_count_distinct_voters_only() {
    let mut election = promised(1, 0, 1);
    assert_eq!(election.report(), Err(Error::StartQuorumMissing));
    let start = election.start_message().unwrap();
    assert_eq!(
        election.receive_start(node(1), start),
        Err(ReplicationError::WrongRole.into())
    );
    assert_eq!(election.report(), Err(Error::StartQuorumMissing));
    election.receive_start(node(2), start).unwrap();
    election.receive_start(node(2), start).unwrap();
    let own = election.report().unwrap();
    assert_eq!(election.report().unwrap(), own);
    assert_eq!(election.select(lookup), Err(Error::ReportQuorumMissing));
    election
        .receive_report(node(2), remote_report(2, 0, 1))
        .unwrap();
    election
        .receive_report(node(2), remote_report(2, 0, 1))
        .unwrap();
    assert_eq!(election.select(lookup).unwrap().voter_mask(), 0b110);
}

#[test]
fn delayed_earlier_promise_cannot_authorize_an_unpersisted_later_view() {
    let mut election = normal(1).into_view_change(1).unwrap();
    let first = election.begin_promise().unwrap();
    election.advance_view(4).unwrap();
    assert_eq!(election.begin_promise(), Err(Error::PromisePending));
    election.complete_promise(first).unwrap();
    assert_eq!(election.promised_view(), 1);
    assert_eq!(election.start_message(), Err(Error::PromiseRequired));
    let later = election.begin_promise().unwrap();
    election.complete_promise(first).unwrap();
    assert_eq!(election.promised_view(), 1);
    election.complete_promise(later).unwrap();
    assert_eq!(election.start_message().unwrap().scope.view, 4);
    assert_eq!(election.complete_promise(first), Err(Error::StalePromise));
    assert_eq!(election.promised_view(), 4);
    assert_eq!(election.report(), Err(Error::StartQuorumMissing));
}

#[test]
fn promise_failures_and_foreign_tickets_are_generation_scoped() {
    let mut left = normal(1).into_view_change(1).unwrap();
    let mut right = normal(2).into_view_change(1).unwrap();
    let ticket = left.begin_promise().unwrap();
    let foreign = right.begin_promise().unwrap();
    assert_eq!(left.complete_promise(foreign), Err(Error::StalePromise));
    assert_eq!(left.fail_promise(foreign), Err(Error::StalePromise));
    assert_eq!(left.normal_snapshot().status, Status::Fenced);
    left.fail_promise(ticket).unwrap();
    assert_eq!(left.complete_promise(ticket), Err(Error::Faulted));
    assert_eq!(left.start_message(), Err(Error::Faulted));
    assert_eq!(left.advance_view(2), Err(Error::Faulted));
}

#[test]
fn uncertain_old_journal_error_prevents_view_promises() {
    let mut election = normal(1).into_view_change(1).unwrap();
    let generation = election.normal_snapshot().journal.generation;
    assert!(election.fail_io(JournalGeneration(999)).is_err());
    election.fail_io(generation).unwrap();
    assert_eq!(election.begin_promise(), Err(Error::Faulted));
    assert!(normal(0).into_view_change(0).is_err());
    let mut failed = normal(0);
    failed.fail_io(JournalGeneration(1)).unwrap();
    assert!(matches!(failed.into_view_change(1), Err(Error::Faulted)));
}

#[test]
fn view_changes_are_monotonic_and_do_not_wrap_at_maximum() {
    let mut election = promised(1, 0, 1);
    assert_eq!(election.advance_view(1), Err(Error::ViewNotHigher));
    assert_eq!(election.advance_view(0), Err(Error::ViewNotHigher));
    election.advance_view(u64::MAX).unwrap();
    let ticket = election.begin_promise().unwrap();
    election.complete_promise(ticket).unwrap();
    assert_eq!(election.scope().view, u64::MAX);
    assert_eq!(election.advance_view(0), Err(Error::ViewNotHigher));
}

#[test]
fn messages_require_exact_scope_membership_and_election_role() {
    let mut election = promised(1, 0, 1);
    let valid = election.start_message().unwrap();
    let mut scopes = [valid.scope; 4];
    scopes[0].group_id = GroupId::from_bytes([22; 16]);
    scopes[1].configuration_epoch += 1;
    scopes[2].configuration_digest = Digest::from_bytes([22; 32]);
    scopes[3].view += 1;
    for scope in scopes {
        assert_eq!(
            election.receive_start(node(2), StartViewChange { scope }),
            Err(ReplicationError::ScopeMismatch.into())
        );
        let mut report = remote_report(2, 0, 1);
        report.scope = scope;
        assert_eq!(
            election.receive_report(node(2), report),
            Err(ReplicationError::ScopeMismatch.into())
        );
    }
    assert_eq!(
        election.receive_start(node(3), valid),
        Err(ReplicationError::UnknownVoter.into())
    );
    assert_eq!(
        election.receive_report(node(3), remote_report(2, 0, 1)),
        Err(ReplicationError::UnknownVoter.into())
    );
    assert_eq!(
        election.receive_report(node(1), remote_report(2, 0, 1)),
        Err(ReplicationError::WrongRole.into())
    );
    let mut backup = promised(2, 0, 1);
    assert_eq!(
        backup.receive_report(node(0), remote_report(0, 0, 1)),
        Err(ReplicationError::WrongRole.into())
    );
    assert_eq!(
        backup.select(lookup),
        Err(ReplicationError::WrongRole.into())
    );
}

#[test]
fn selection_preserves_quorum_prepared_tail_without_any_survivor_commit_marker() {
    let mut old_primary = durable(0, 4);
    let backup = durable(1, 4);
    old_primary
        .receive_ack(node(1), backup.acknowledgment().unwrap())
        .unwrap();
    assert_eq!(old_primary.snapshot().committed.op, OpNumber(4));
    old_primary
        .apply_through(old_primary.snapshot().committed)
        .unwrap();
    drop(old_primary); // Producer replied; COMMIT never reached either backup.
    let mut candidate = backup.into_view_change(1).unwrap();
    let promise = candidate.begin_promise().unwrap();
    candidate.complete_promise(promise).unwrap();
    local_report(&mut candidate, 2);
    candidate
        .receive_report(node(2), remote_report(2, 0, 1))
        .unwrap();
    let selected = candidate.select(lookup).unwrap();
    assert_eq!(selected.source().accepted, operations(4)[3].prefix());
    assert_eq!(selected.committed(), Prefix::GENESIS);
    assert_eq!(selected.source().voter, node(1));
    assert_eq!(selected.scope().view, 1);
    assert_eq!(candidate.normal_snapshot().status, Status::Fenced);
}

#[test]
fn longest_same_normal_view_wins_and_preserves_every_reported_commit_floor() {
    let mut candidate = promised(1, 2, 1);
    local_report(&mut candidate, 0);
    let mut remote = remote_report(0, 5, 1);
    remote.log.committed = operations(3)[2].prefix();
    candidate.receive_report(node(0), remote).unwrap();
    let mut lookups = 0;
    let selected = candidate
        .select(|source, op| {
            assert_eq!(source.voter, node(0));
            assert_eq!(source.generation, remote.generation);
            assert_eq!(source.accepted, remote.log.accepted);
            lookups += 1;
            lookup(source, op)
        })
        .unwrap();
    assert!(lookups <= 6);
    assert_eq!(selected.source().accepted.op, OpNumber(5));
    assert_eq!(selected.committed().op, OpNumber(3));
}

#[test]
fn newer_installed_view_overrides_longer_older_uncertain_history() {
    let mut candidate = promised(1, 5, 7);
    local_report(&mut candidate, 2);
    let mut remote = remote_report(2, 2, 7);
    remote.log.last_normal_view = 4;
    // An older uncertain suffix may differ. It must not be merged into the
    // selected higher-normal-view history merely because it is longer.
    remote.log.accepted.digest = Digest::from_bytes([90; 32]);
    candidate.receive_report(node(2), remote).unwrap();
    let selected = candidate
        .select(|_, _| panic!("no nonzero protected floor"))
        .unwrap();
    assert_eq!(selected.source().accepted, remote.log.accepted);
    assert_eq!(selected.source().voter, node(2));
}

#[test]
fn selected_history_cannot_omit_an_older_committed_prefix() {
    let mut old = durable(1, 4);
    old.receive_commit(
        node(0),
        ozzy_replication::Commit {
            scope: config().scope(),
            committed: operations(4)[3].prefix(),
        },
    )
    .unwrap();
    let mut candidate = old.into_view_change(7).unwrap();
    let promise = candidate.begin_promise().unwrap();
    candidate.complete_promise(promise).unwrap();
    local_report(&mut candidate, 2);
    let mut remote = remote_report(2, 2, 7);
    remote.log.last_normal_view = 4;
    candidate.receive_report(node(2), remote).unwrap();
    assert_eq!(candidate.select(lookup), Err(Error::ConflictingHistory));
    assert_eq!(candidate.advance_view(8), Err(Error::Faulted));
}

#[test]
fn missing_history_blocks_selection_without_fault_or_fabricated_prefix() {
    let mut candidate = promised(1, 2, 1);
    local_report(&mut candidate, 2);
    candidate
        .receive_report(node(2), remote_report(2, 5, 1))
        .unwrap();
    assert_eq!(candidate.select(|_, _| None), Err(Error::HistoryMissing));
    assert_eq!(candidate.normal_snapshot().status, Status::Fenced);
    assert_eq!(
        candidate.select(lookup).unwrap().source().accepted.op,
        OpNumber(5)
    );
}

#[test]
fn equal_rank_conflicts_and_same_view_prefix_forks_fault() {
    for remote_count in [2, 4] {
        let mut candidate = promised(1, 2, 1);
        local_report(&mut candidate, 2);
        let mut remote = remote_report(2, remote_count, 1);
        remote.log.accepted.digest = Digest::from_bytes([99; 32]);
        candidate.receive_report(node(2), remote).unwrap();
        assert_eq!(
            candidate.select(|_, _| Some(Digest::from_bytes([98; 32]))),
            Err(Error::ConflictingHistory)
        );
        assert_eq!(candidate.select(lookup), Err(Error::Faulted));
    }
}

#[test]
fn protected_commit_digest_is_checked_even_when_its_offset_is_lower() {
    let mut candidate = promised(1, 2, 7);
    local_report(&mut candidate, 2);
    let mut remote = remote_report(2, 4, 7);
    remote.log.last_normal_view = 4;
    remote.log.committed = Prefix {
        op: OpNumber(1),
        digest: Digest::from_bytes([99; 32]),
    };
    candidate.receive_report(node(2), remote).unwrap();
    assert_eq!(candidate.select(lookup), Err(Error::ConflictingHistory));
}

#[test]
fn malformed_reports_never_enter_the_quorum() {
    let valid = remote_report(2, 2, 1);
    let mut cases = [valid; 6];
    cases[0].log.last_normal_view = 1;
    cases[1].log.accepted.digest = Digest::ZERO;
    cases[2].log.accepted.op = OpNumber(u64::MAX);
    cases[3].log.committed = operations(3)[2].prefix();
    cases[4].log.committed.digest = Digest::from_bytes([99; 32]);
    cases[5].log.committed = Prefix {
        digest: Digest::from_bytes([99; 32]),
        ..valid.log.accepted
    };
    for report in cases {
        let mut candidate = promised(1, 0, 1);
        local_report(&mut candidate, 2);
        assert_eq!(
            candidate.receive_report(node(2), report),
            Err(Error::InvalidReport)
        );
        assert_eq!(candidate.select(lookup), Err(Error::ReportQuorumMissing));
    }
}

#[test]
fn intact_restart_report_keeps_one_vote_and_the_original_transfer_source() {
    let original = remote_report(2, 3, 1);
    let mut restarted = ViewChange::recover_intact(
        config(),
        node(2),
        JournalGeneration(101),
        ozzy_replication::RecoveredState {
            scope: original.scope,
            log: original.log,
        },
        PipelineLimits {
            max_operations: 32,
            max_body_bytes: 512,
        },
    )
    .unwrap();
    let refreshed = local_report(&mut restarted, 1);
    assert_eq!(original.log, refreshed.log);
    assert_eq!(original.scope, refreshed.scope);
    assert_ne!(original.generation, refreshed.generation);
    // Both arrival orders, before and after selection. No generation ordering
    // is assumed; writer IDs are opaque and old packets may arrive late.
    for [first, second] in [[original, refreshed], [refreshed, original]] {
        for select_first in [false, true] {
            let mut candidate = promised(1, 0, 1);
            local_report(&mut candidate, 2);
            candidate.receive_report(node(2), first).unwrap();
            if select_first {
                candidate.select(lookup).unwrap();
            }
            assert_eq!(candidate.receive_report(node(2), second), Ok(()));
            let selected = candidate.select(lookup).unwrap();
            assert_eq!(selected.voter_mask(), 0b110);
            assert_eq!(selected.source().generation, first.generation);
            assert_eq!(selected.source().accepted, original.log.accepted);
            assert_eq!(candidate.receive_report(node(2), first), Ok(()));
            assert_eq!(candidate.select(lookup).unwrap(), selected);
        }
    }
}

#[test]
fn changed_frozen_history_faults_even_with_a_different_writer_generation() {
    for change_generation in [false, true] {
        let mut candidate = promised(1, 0, 1);
        let mut report = remote_report(2, 1, 1);
        candidate.receive_report(node(2), report).unwrap();
        if change_generation {
            report.generation = JournalGeneration(999);
        }
        report.log = FrozenLog {
            accepted: operations(2)[1].prefix(),
            ..report.log
        };
        assert_eq!(
            candidate.receive_report(node(2), report),
            Err(Error::ConflictingHistory)
        );
        assert_eq!(candidate.start_message(), Err(Error::Faulted));
    }
}

#[test]
fn selection_is_frozen_and_advancing_view_discards_old_quorum_evidence() {
    let mut candidate = promised(1, 2, 1);
    local_report(&mut candidate, 2);
    let report = remote_report(2, 2, 1);
    candidate.receive_report(node(2), report).unwrap();
    let selected = candidate.select(lookup).unwrap();
    candidate.receive_report(node(2), report).unwrap();
    assert_eq!(
        candidate.receive_report(node(0), remote_report(0, 3, 1)),
        Err(Error::SelectionFrozen)
    );
    assert_eq!(
        candidate
            .select(|_, _| panic!("selection already verified"))
            .unwrap(),
        selected
    );
    candidate.advance_view(4).unwrap();
    assert_eq!(candidate.select(lookup), Err(Error::PromiseRequired));
    let promise = candidate.begin_promise().unwrap();
    candidate.complete_promise(promise).unwrap();
    assert_eq!(candidate.report(), Err(Error::StartQuorumMissing));
    assert_eq!(
        candidate.receive_report(node(2), report),
        Err(ReplicationError::ScopeMismatch.into())
    );
    assert_eq!(candidate.select(lookup), Err(Error::ReportQuorumMissing));
}

#[test]
fn every_view_change_quorum_preserves_every_possible_old_normal_quorum() {
    for old_backup in [1, 2] {
        for new_primary in 0..3u8 {
            for other in 0..3u8 {
                if other == new_primary {
                    continue;
                }
                let count = |index| {
                    if index == 0 || index == old_backup {
                        4
                    } else {
                        0
                    }
                };
                let view = u64::from(new_primary) + 3;
                let mut candidate = promised(new_primary, count(new_primary), view);
                local_report(&mut candidate, other);
                candidate
                    .receive_report(node(other), remote_report(other, count(other), view))
                    .unwrap();
                let selected = candidate.select(lookup).unwrap();
                assert_eq!(
                    selected.source().accepted,
                    operations(4)[3].prefix(),
                    "old backup={old_backup}, new primary={new_primary}, other={other}"
                );
            }
        }
    }
}

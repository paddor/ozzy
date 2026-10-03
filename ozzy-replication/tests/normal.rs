use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
    encode_operation_body,
};
use ozzy_journal::progress::ProgressError;
use ozzy_proto::{GroupId, NodeId, OperationId};
use ozzy_replication::{
    Admission, Commit, Configuration, Digest, JournalGeneration, NormalReplica, OpNumber,
    PipelineLimits, Prefix, PrepareOk, PreparedOperation, ReplicationError, Scope, Status,
    WriteTicket,
};

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

fn replica(index: u8, operations: usize, bytes: usize) -> NormalReplica {
    NormalReplica::bootstrap(
        config(),
        node(index),
        JournalGeneration(u128::from(index) + 1),
        PipelineLimits {
            max_operations: operations,
            max_body_bytes: bytes,
        },
    )
    .unwrap()
}

fn operation(scope: Scope, previous: Prefix, value: u8) -> PreparedOperation {
    let body = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([value; 16]),
    });
    let bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
    let canonical = CanonicalOperation {
        group_id: scope.group_id,
        configuration_epoch: scope.configuration_epoch,
        original_view: scope.view,
        op_number: previous.op.0 + 1,
        previous_digest: previous.digest,
        kind: body.kind(),
        body: &bytes,
    };
    PreparedOperation::from_verified(&canonical, canonical_body_digest(&bytes))
}

fn prepare(replica: &mut NormalReplica, operations: &[PreparedOperation]) -> WriteTicket {
    match replica
        .prepare(node(0), config().scope(), operations)
        .unwrap()
    {
        Admission::Write { ticket, .. } => ticket,
        Admission::Duplicate => panic!("expected a new write"),
    }
}

fn ack(through: Prefix) -> PrepareOk {
    PrepareOk {
        scope: config().scope(),
        durable: through,
    }
}

#[test]
fn configuration_rejects_duplicate_voters_empty_digest_and_unknown_local() {
    for voters in [
        [node(0), node(0), node(2)],
        [node(0), node(1), node(0)],
        [node(0), node(1), node(1)],
    ] {
        assert_eq!(
            Configuration::new(
                config().scope().group_id,
                1,
                config().scope().configuration_digest,
                voters,
            ),
            Err(ReplicationError::InvalidConfiguration)
        );
    }
    assert_eq!(
        Configuration::new(
            config().scope().group_id,
            1,
            Digest::ZERO,
            *config().voters()
        ),
        Err(ReplicationError::InvalidConfiguration)
    );
    assert!(matches!(
        NormalReplica::bootstrap(
            config(),
            node(3),
            JournalGeneration(1),
            PipelineLimits {
                max_operations: 1,
                max_body_bytes: 16,
            },
        ),
        Err(ReplicationError::UnknownVoter)
    ));
    for view in 0..30 {
        assert_eq!(config().primary(view), node((view % 3) as u8));
    }
}

#[test]
fn zero_pipeline_limits_are_rejected() {
    for limits in [
        PipelineLimits {
            max_operations: 0,
            max_body_bytes: 16,
        },
        PipelineLimits {
            max_operations: 1,
            max_body_bytes: 0,
        },
    ] {
        assert!(matches!(
            NormalReplica::bootstrap(config(), node(0), JournalGeneration(1), limits),
            Err(ReplicationError::InvalidLimits)
        ));
    }
}

#[test]
fn bootstrap_rejects_zero_storage_identities_and_impossible_capacity() {
    for (group, voters) in [
        (GroupId::from_bytes([0; 16]), *config().voters()),
        (
            config().scope().group_id,
            [NodeId::from_bytes([0; 16]), node(1), node(2)],
        ),
    ] {
        assert_eq!(
            Configuration::new(group, 1, config().scope().configuration_digest, voters),
            Err(ReplicationError::InvalidConfiguration)
        );
    }
    assert!(matches!(
        NormalReplica::bootstrap(
            config(),
            node(0),
            JournalGeneration(1),
            PipelineLimits {
                max_operations: usize::MAX,
                max_body_bytes: usize::MAX,
            },
        ),
        Err(ReplicationError::Capacity)
    ));
}

#[test]
fn buffered_copies_are_not_durable_votes() {
    let mut primary = replica(0, 4, 64);
    let mut backup = replica(1, 4, 64);
    let op = operation(config().scope(), Prefix::GENESIS, 1);
    let local = prepare(&mut primary, &[op]);
    let remote = prepare(&mut backup, &[op]);
    primary.complete_write(local).unwrap();
    backup.complete_write(remote).unwrap();
    assert_eq!(backup.acknowledgment().unwrap().durable, Prefix::GENESIS);
    primary
        .receive_ack(node(1), backup.acknowledgment().unwrap())
        .unwrap();
    assert_eq!(primary.snapshot().committed, Prefix::GENESIS);

    let remote_sync = backup.begin_sync().unwrap();
    backup.complete_sync(remote_sync).unwrap();
    primary
        .receive_ack(node(1), backup.acknowledgment().unwrap())
        .unwrap();
    assert_eq!(primary.snapshot().committed, Prefix::GENESIS);
    let local_sync = primary.begin_sync().unwrap();
    primary.complete_sync(local_sync).unwrap();
    assert_eq!(primary.snapshot().committed, op.prefix());
    assert_eq!(primary.snapshot().applied, Prefix::GENESIS);
}

#[test]
fn local_disk_alone_cannot_commit_and_either_backup_can_complete_quorum() {
    for voter in [node(1), node(2)] {
        let mut primary = replica(0, 4, 64);
        let op = operation(config().scope(), Prefix::GENESIS, 1);
        let ticket = prepare(&mut primary, &[op]);
        primary.complete_durable_write(ticket).unwrap();
        assert_eq!(primary.snapshot().committed, Prefix::GENESIS);
        primary.receive_ack(voter, ack(op.prefix())).unwrap();
        assert_eq!(primary.snapshot().committed, op.prefix());
    }
}

#[test]
fn duplicate_backup_votes_cannot_replace_primary_disk() {
    let mut primary = replica(0, 4, 64);
    let op = operation(config().scope(), Prefix::GENESIS, 1);
    let ticket = prepare(&mut primary, &[op]);
    for _ in 0..100 {
        primary.receive_ack(node(1), ack(op.prefix())).unwrap();
        primary.receive_ack(node(2), ack(op.prefix())).unwrap();
    }
    assert_eq!(primary.snapshot().committed, Prefix::GENESIS);
    primary.complete_durable_write(ticket).unwrap();
    assert_eq!(primary.snapshot().committed, op.prefix());
}

#[test]
fn roles_and_authenticated_membership_are_checked() {
    let mut primary = replica(0, 4, 64);
    let mut backup = replica(1, 4, 64);
    assert_eq!(
        primary.receive_ack(node(0), ack(Prefix::GENESIS)),
        Err(ReplicationError::WrongRole)
    );
    assert_eq!(
        primary.receive_ack(node(3), ack(Prefix::GENESIS)),
        Err(ReplicationError::UnknownVoter)
    );
    assert_eq!(
        backup.receive_ack(node(2), ack(Prefix::GENESIS)),
        Err(ReplicationError::WrongRole)
    );
    assert_eq!(primary.acknowledgment(), Err(ReplicationError::WrongRole));
    assert_eq!(backup.announcement(), Err(ReplicationError::WrongRole));
    assert_eq!(
        backup.receive_commit(node(2), primary.announcement().unwrap()),
        Err(ReplicationError::WrongRole)
    );
    let op = operation(config().scope(), Prefix::GENESIS, 1);
    assert_eq!(
        backup.prepare(node(2), config().scope(), &[op]),
        Err(ReplicationError::WrongRole)
    );
    assert_eq!(backup.snapshot().accepted, Prefix::GENESIS);
}

#[test]
fn wrong_group_configuration_or_view_cannot_vote_or_admit() {
    let mut primary = replica(0, 4, 64);
    let op = operation(config().scope(), Prefix::GENESIS, 1);
    let ticket = prepare(&mut primary, &[op]);
    primary.complete_durable_write(ticket).unwrap();
    let before = primary.snapshot();
    let scope = config().scope();
    for wrong in [
        Scope {
            group_id: GroupId::from_bytes([9; 16]),
            ..scope
        },
        Scope {
            configuration_epoch: 2,
            ..scope
        },
        Scope {
            configuration_digest: Digest::from_bytes([9; 32]),
            ..scope
        },
        Scope { view: 1, ..scope },
    ] {
        assert_eq!(
            primary.receive_ack(
                node(1),
                PrepareOk {
                    scope: wrong,
                    durable: op.prefix(),
                },
            ),
            Err(ReplicationError::ScopeMismatch)
        );
        assert_eq!(
            primary.prepare(node(0), wrong, &[op]),
            Err(ReplicationError::ScopeMismatch)
        );
        assert_eq!(primary.snapshot(), before);
    }
}

#[test]
fn operation_envelopes_must_match_current_scope() {
    let mut primary = replica(0, 4, 64);
    let scope = config().scope();
    for wrong in [
        Scope {
            group_id: GroupId::from_bytes([9; 16]),
            ..scope
        },
        Scope {
            configuration_epoch: 2,
            ..scope
        },
        Scope { view: 1, ..scope },
    ] {
        let op = operation(wrong, Prefix::GENESIS, 1);
        assert_eq!(
            primary.prepare(node(0), scope, &[op]),
            Err(ReplicationError::ScopeMismatch)
        );
        assert_eq!(primary.snapshot().accepted, Prefix::GENESIS);
    }
}

#[test]
fn cumulative_ack_cannot_outrun_captured_sync() {
    let mut primary = replica(0, 4, 64);
    let first = operation(config().scope(), Prefix::GENESIS, 1);
    let second = operation(config().scope(), first.prefix(), 2);
    let a = prepare(&mut primary, &[first]);
    primary.complete_write(a).unwrap();
    let sync_a = primary.begin_sync().unwrap();
    let b = prepare(&mut primary, &[second]);
    primary.complete_write(b).unwrap();
    primary.receive_ack(node(1), ack(second.prefix())).unwrap();
    primary.complete_sync(sync_a).unwrap();
    assert_eq!(primary.snapshot().committed, first.prefix());
    let sync_b = primary.begin_sync().unwrap();
    primary.complete_sync(sync_b).unwrap();
    primary.receive_ack(node(1), ack(first.prefix())).unwrap();
    primary.complete_sync(sync_a).unwrap();
    assert_eq!(primary.snapshot().committed, second.prefix());
}

#[test]
fn follower_commit_waits_for_its_own_durable_prefix() {
    let mut backup = replica(1, 4, 64);
    let op = operation(config().scope(), Prefix::GENESIS, 1);
    let ticket = prepare(&mut backup, &[op]);
    backup
        .receive_commit(
            node(0),
            Commit {
                scope: config().scope(),
                committed: op.prefix(),
            },
        )
        .unwrap();
    assert_eq!(backup.snapshot().committed, Prefix::GENESIS);
    backup.complete_write(ticket).unwrap();
    assert_eq!(backup.snapshot().committed, Prefix::GENESIS);
    backup.complete_durable_write(ticket).unwrap();
    assert_eq!(backup.snapshot().committed, op.prefix());
    backup.apply_through(op.prefix()).unwrap();
    assert_eq!(backup.snapshot().applied, op.prefix());
}

#[test]
fn missing_prepare_or_commit_history_requests_catch_up_without_mutation() {
    let mut backup = replica(1, 4, 64);
    let first = operation(config().scope(), Prefix::GENESIS, 1);
    let second = operation(config().scope(), first.prefix(), 2);
    let before = backup.snapshot();
    assert_eq!(
        backup.prepare(node(0), config().scope(), &[second]),
        Err(ReplicationError::HistoryGap)
    );
    assert_eq!(
        backup.receive_commit(
            node(0),
            Commit {
                scope: config().scope(),
                committed: second.prefix(),
            },
        ),
        Err(ReplicationError::HistoryGap)
    );
    assert_eq!(backup.snapshot(), before);
    prepare(&mut backup, &[first, second]);
    assert_eq!(backup.snapshot().accepted, second.prefix());
}

#[test]
fn prepare_group_validation_is_atomic_and_retransmit_can_overlap() {
    let mut primary = replica(0, 4, 64);
    let first = operation(config().scope(), Prefix::GENESIS, 1);
    let second = operation(config().scope(), first.prefix(), 2);
    let third = operation(config().scope(), second.prefix(), 3);
    let before = primary.snapshot();
    assert_eq!(
        primary.prepare(node(0), config().scope(), &[first, third]),
        Err(ReplicationError::HistoryGap)
    );
    assert_eq!(primary.snapshot(), before);
    prepare(&mut primary, &[first]);
    let retained = primary.snapshot();
    assert_eq!(
        primary.prepare(node(0), config().scope(), &[first]),
        Ok(Admission::Duplicate)
    );
    assert_eq!(primary.snapshot(), retained);
    let Admission::Write { ticket, first_new } = primary
        .prepare(node(0), config().scope(), &[first, second, third])
        .unwrap()
    else {
        panic!("expected new suffix");
    };
    assert_eq!(first_new, 1);
    assert_eq!(ticket.first(), OpNumber(2));
    assert_eq!(ticket.through(), OpNumber(3));
    assert_eq!(primary.snapshot().pending_operations, 3);
    assert_eq!(primary.snapshot().pending_body_bytes, 48);
}

#[test]
fn operation_and_byte_bounds_hold_until_application_releases_capacity() {
    for (count, bytes) in [(1, 64), (4, 16)] {
        let mut primary = replica(0, count, bytes);
        let first = operation(config().scope(), Prefix::GENESIS, 1);
        let second = operation(config().scope(), first.prefix(), 2);
        let ticket = prepare(&mut primary, &[first]);
        primary.complete_durable_write(ticket).unwrap();
        primary.receive_ack(node(1), ack(first.prefix())).unwrap();
        let before = primary.snapshot();
        assert_eq!(
            primary.prepare(node(0), config().scope(), &[second]),
            Err(ReplicationError::Capacity)
        );
        assert_eq!(primary.snapshot(), before);
        primary.apply_through(first.prefix()).unwrap();
        assert_eq!(primary.snapshot().pending_body_bytes, 0);
        prepare(&mut primary, &[second]);
        assert_eq!(primary.snapshot().pending_operations, 1);
    }
}

#[test]
fn empty_and_oversized_admissions_leave_state_unchanged() {
    let mut primary = replica(0, 1, 16);
    let before = primary.snapshot();
    let first = operation(config().scope(), Prefix::GENESIS, 1);
    let second = operation(config().scope(), first.prefix(), 2);
    assert_eq!(
        primary.prepare(node(0), config().scope(), &[]),
        Err(ReplicationError::EmptyPrepare)
    );
    assert_eq!(
        primary.prepare(node(0), config().scope(), &[first, second]),
        Err(ReplicationError::Capacity)
    );
    assert_eq!(primary.snapshot(), before);
}

#[test]
fn application_cannot_advance_over_uncommitted_operations() {
    let mut primary = replica(0, 4, 64);
    let op = operation(config().scope(), Prefix::GENESIS, 1);
    prepare(&mut primary, &[op]);
    let before = primary.snapshot();
    assert_eq!(
        primary.apply_through(op.prefix()),
        Err(ReplicationError::ApplyBeyondCommit)
    );
    assert_eq!(primary.snapshot(), before);
}

#[test]
fn conflicting_duplicate_or_ack_faults_instead_of_replacing_history() {
    let first = operation(config().scope(), Prefix::GENESIS, 1);
    let other = operation(config().scope(), Prefix::GENESIS, 2);
    for via_ack in [false, true] {
        let mut primary = replica(0, 4, 64);
        let ticket = prepare(&mut primary, &[first]);
        let result = if via_ack {
            primary.receive_ack(node(1), ack(other.prefix()))
        } else {
            primary
                .prepare(node(0), config().scope(), &[other])
                .map(|_| ())
        };
        assert_eq!(result, Err(ReplicationError::ConflictingHistory));
        assert_eq!(primary.snapshot().status, Status::Faulted);
        assert_eq!(primary.snapshot().accepted, first.prefix());
        assert_eq!(primary.snapshot().committed, Prefix::GENESIS);
        assert_eq!(
            primary.complete_durable_write(ticket),
            Err(ReplicationError::Journal(ProgressError::Faulted))
        );
        assert_eq!(primary.announcement(), Err(ReplicationError::NotNormal));
    }
}

#[test]
fn wrong_predecessor_digest_faults_without_admitting_a_partial_group() {
    let mut primary = replica(0, 4, 64);
    let first = operation(config().scope(), Prefix::GENESIS, 1);
    let wrong = operation(config().scope(), Prefix::GENESIS, 2);
    let second = operation(config().scope(), wrong.prefix(), 3);
    assert_eq!(
        primary.prepare(node(0), config().scope(), &[first, second]),
        Err(ReplicationError::ConflictingHistory)
    );
    assert_eq!(primary.snapshot().status, Status::Faulted);
    assert_eq!(primary.snapshot().accepted, Prefix::GENESIS);
    assert_eq!(primary.snapshot().pending_operations, 0);
}

#[test]
fn foreign_writer_completions_and_errors_cannot_mutate_this_replica() {
    let mut primary = replica(0, 4, 64);
    let mut backup = replica(1, 4, 64);
    let op = operation(config().scope(), Prefix::GENESIS, 1);
    prepare(&mut primary, &[op]);
    let foreign = prepare(&mut backup, &[op]);
    backup.complete_write(foreign).unwrap();
    let foreign_sync = backup.begin_sync().unwrap();
    let before = primary.snapshot();
    for result in [
        primary.complete_write(foreign),
        primary.complete_durable_write(foreign),
        primary.complete_sync(foreign_sync),
        primary.fail_io(foreign.generation()),
    ] {
        assert_eq!(
            result,
            Err(ReplicationError::Journal(ProgressError::StaleGeneration))
        );
    }
    assert_eq!(primary.snapshot(), before);
}

#[test]
fn out_of_order_write_completion_cannot_skip_a_missing_prefix() {
    let mut primary = replica(0, 4, 64);
    let first = operation(config().scope(), Prefix::GENESIS, 1);
    let second = operation(config().scope(), first.prefix(), 2);
    let a = prepare(&mut primary, &[first]);
    let b = prepare(&mut primary, &[second]);
    let before = primary.snapshot();
    assert_eq!(
        primary.complete_durable_write(b),
        Err(ReplicationError::Journal(ProgressError::WriteGap))
    );
    assert_eq!(primary.snapshot(), before);
    primary.complete_durable_write(a).unwrap();
    primary.complete_durable_write(b).unwrap();
    assert_eq!(primary.snapshot().journal.durable, second.prefix().op);
}

#[test]
fn fencing_blocks_old_view_votes_commit_and_apply_while_disk_work_settles() {
    for index in [0, 1] {
        let mut replica = replica(index, 4, 64);
        let op = operation(config().scope(), Prefix::GENESIS, 1);
        let ticket = prepare(&mut replica, &[op]);
        if index == 0 {
            replica.receive_ack(node(1), ack(op.prefix())).unwrap();
        }
        replica.fence();
        replica.complete_write(ticket).unwrap();
        let sync = replica.begin_sync().unwrap();
        replica.complete_sync(sync).unwrap();
        assert_eq!(replica.snapshot().journal.durable, op.prefix().op);
        assert_eq!(replica.snapshot().committed, Prefix::GENESIS);
        assert_eq!(replica.snapshot().status, Status::Fenced);
        assert_eq!(replica.acknowledgment(), Err(ReplicationError::NotNormal));
        assert_eq!(replica.announcement(), Err(ReplicationError::NotNormal));
        assert_eq!(
            replica.prepare(node(0), config().scope(), &[op]),
            Err(ReplicationError::NotNormal)
        );
        assert_eq!(
            replica.apply_through(Prefix::GENESIS),
            Err(ReplicationError::NotNormal)
        );
    }
}

#[test]
fn io_failure_fences_success_even_if_remote_quorum_has_data() {
    let mut primary = replica(0, 4, 64);
    let op = operation(config().scope(), Prefix::GENESIS, 1);
    let ticket = prepare(&mut primary, &[op]);
    primary.receive_ack(node(1), ack(op.prefix())).unwrap();
    primary.receive_ack(node(2), ack(op.prefix())).unwrap();
    primary.fail_io(ticket.generation()).unwrap();
    primary.fence();
    assert_eq!(primary.snapshot().status, Status::Faulted);
    assert_eq!(primary.snapshot().committed, Prefix::GENESIS);
    assert_eq!(
        primary.complete_durable_write(ticket),
        Err(ReplicationError::Journal(ProgressError::Faulted))
    );
}

#[test]
fn repeated_pipeline_reuse_preserves_cumulative_votes_and_prefixes() {
    let mut primary = replica(0, 2, 32);
    let mut backup = replica(1, 2, 32);
    let mut previous = Prefix::GENESIS;
    for round in 0..1000 {
        let a = operation(config().scope(), previous, round as u8);
        let b = operation(config().scope(), a.prefix(), (round + 1) as u8);
        let local = prepare(&mut primary, &[a, b]);
        let remote = prepare(&mut backup, &[a, b]);
        backup.complete_durable_write(remote).unwrap();
        primary
            .receive_ack(node(1), backup.acknowledgment().unwrap())
            .unwrap();
        primary.complete_durable_write(local).unwrap();
        backup
            .receive_commit(node(0), primary.announcement().unwrap())
            .unwrap();
        primary.apply_through(a.prefix()).unwrap();
        primary.apply_through(b.prefix()).unwrap();
        backup.apply_through(b.prefix()).unwrap();
        assert_eq!(primary.snapshot().pending_operations, 0);
        assert_eq!(backup.snapshot().pending_body_bytes, 0);
        primary.receive_ack(node(2), ack(previous)).unwrap();
        assert_eq!(primary.snapshot().committed, b.prefix());
        previous = b.prefix();
    }
    assert_eq!(primary.snapshot().applied.op, OpNumber(2000));
}

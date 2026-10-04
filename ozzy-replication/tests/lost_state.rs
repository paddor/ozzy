//! Lost voters collect fresh evidence without contributing their forgotten vote.

use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
    encode_operation_body,
};
use ozzy_proto::{GroupId, NodeId, OperationId, RequestId};
use ozzy_replication::recovery::{
    Recovery, RecoveryError, RecoveryLog, RecoveryResponse, RecoveryTicket,
};
use ozzy_replication::{
    Admission, Configuration, Digest, JournalGeneration, NormalReplica, PipelineLimits, Prefix,
    PrepareOk, PreparedOperation, Scope, Status, ViewChange, ViewChangeError,
};

fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}

fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [node(0), node(1), node(2)],
    )
    .unwrap()
}

fn limits() -> PipelineLimits {
    PipelineLimits {
        max_operations: 4,
        max_body_bytes: 128,
    }
}

fn normal(index: u8) -> NormalReplica {
    NormalReplica::bootstrap(
        configuration(),
        node(index),
        JournalGeneration(u128::from(index) + 1),
        limits(),
    )
    .unwrap()
}

fn operation(scope: Scope, previous: Prefix, value: u8) -> PreparedOperation {
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
fn checkpoint_recovery_keeps_retained_chain_and_requires_private_state_validation() {
    use ozzy_replication::recovery::CheckpointAnchor;
    let scope = configuration().scope();
    let first = operation(scope, Prefix::GENESIS, 51);
    let second = operation(scope, first.prefix(), 52);
    let third = operation(scope, second.prefix(), 53);
    let anchor = CheckpointAnchor {
        predecessor: first.prefix(),
        position: second.prefix(),
        schema: Digest::from_bytes([61; 32]),
        state_digest: Digest::from_bytes([62; 32]),
        state_bytes: 4096,
        chunk_bytes: 1024,
    };
    let nonce = RequestId::from_bytes([63; 16]);
    let mut recovering = Recovery::new(
        configuration(),
        node(1),
        JournalGeneration(100),
        nonce,
        limits(),
    )
    .unwrap();
    recovering
        .receive(
            node(0),
            RecoveryResponse {
                scope,
                nonce,
                primary: Some(RecoveryLog {
                    generation: JournalGeneration(1),
                    accepted: third.prefix(),
                    committed: second.prefix(),
                    checkpoint: Some(anchor),
                }),
            },
        )
        .unwrap();
    recovering
        .receive(
            node(2),
            RecoveryResponse {
                scope,
                nonce,
                primary: None,
            },
        )
        .unwrap();
    let ticket = recovering.begin_transfer().unwrap();
    assert_eq!(ticket.checkpoint(), Some(anchor));
    assert_eq!(
        recovering.validate_chunk(ticket, &[second, third]),
        Err(RecoveryError::ApplicationPending)
    );
    assert_eq!(
        recovering.complete_checkpoint(ticket, anchor, 1),
        Err(RecoveryError::ApplicationPending)
    );
    recovering.complete_checkpoint(ticket, anchor, 2).unwrap();
    recovering.validate_chunk(ticket, &[second, third]).unwrap();
    let recovered = recovering
        .complete(ticket, third.prefix(), second.prefix())
        .unwrap();
    assert_eq!(recovered.log.accepted, third.prefix());
    assert_eq!(recovered.log.committed, second.prefix());
}

#[test]
fn recovery_selects_full_accepted_history_before_a_precrash_ack_can_commit_it() {
    let mut primary = normal(0);
    let mut lost = normal(1);
    let third = normal(2);
    let scope = configuration().scope();
    let operation = operation(scope, Prefix::GENESIS, 10);
    let Admission::Write {
        ticket: primary_write,
        ..
    } = primary.prepare(node(0), scope, &[operation]).unwrap()
    else {
        panic!("fresh primary write");
    };
    let Admission::Write { ticket, .. } = lost.prepare(node(0), scope, &[operation]).unwrap()
    else {
        panic!("fresh backup write");
    };
    lost.complete_durable_write(ticket).unwrap();
    let old_ack = lost.acknowledgment().unwrap();
    drop(lost); // Its disk/state is gone, but its earlier ACK remains in flight.
    let nonce = RequestId::from_bytes([90; 16]);
    let mut recovering = Recovery::new(
        configuration(),
        node(1),
        JournalGeneration(100),
        nonce,
        limits(),
    )
    .unwrap();
    let response = primary.recovery_response(nonce).unwrap();
    assert_eq!(primary.snapshot().journal.durable.0, 0);
    assert_eq!(response.primary.unwrap().accepted, operation.prefix());
    assert_eq!(response.primary.unwrap().committed, Prefix::GENESIS);
    assert_eq!(
        recovering.receive(node(1), response),
        Err(RecoveryError::SelfResponse)
    );
    recovering.receive(node(0), response).unwrap();
    recovering.receive(node(0), response).unwrap();
    assert_eq!(
        recovering.begin_transfer(),
        Err(RecoveryError::QuorumMissing)
    );
    recovering
        .receive(node(2), third.recovery_response(nonce).unwrap())
        .unwrap();
    let ticket = recovering.begin_transfer().unwrap();
    assert_eq!(ticket.source().voter, node(0));
    assert_eq!(ticket.source().accepted, operation.prefix());
    assert_eq!(ticket.committed(), Prefix::GENESIS);
    assert_eq!(ticket.voter_mask(), 0b101);
    primary.receive_ack(node(1), old_ack).unwrap();
    assert_eq!(primary.snapshot().committed, Prefix::GENESIS);
    primary.complete_durable_write(primary_write).unwrap();
    assert_eq!(primary.snapshot().committed, operation.prefix());
}

fn selected(nonce: u8) -> (Recovery, RecoveryTicket, [PreparedOperation; 2]) {
    selected_with_limits(nonce, limits())
}

fn selected_with_limits(
    nonce: u8,
    bounds: PipelineLimits,
) -> (Recovery, RecoveryTicket, [PreparedOperation; 2]) {
    let scope = configuration().scope();
    let first = operation(scope, Prefix::GENESIS, 10);
    let second = operation(scope, first.prefix(), 11);
    let operations = [first, second];
    let mut primary = normal(0);
    let Admission::Write { ticket, .. } = primary.prepare(node(0), scope, &operations).unwrap()
    else {
        panic!("fresh primary write");
    };
    primary.complete_durable_write(ticket).unwrap();
    primary
        .receive_ack(
            node(2),
            PrepareOk {
                scope,
                durable: first.prefix(),
            },
        )
        .unwrap();
    let nonce = RequestId::from_bytes([nonce; 16]);
    let mut recovery = Recovery::new(
        configuration(),
        node(1),
        JournalGeneration(100),
        nonce,
        bounds,
    )
    .unwrap();
    recovery
        .receive(node(0), primary.recovery_response(nonce).unwrap())
        .unwrap();
    recovery
        .receive(node(2), normal(2).recovery_response(nonce).unwrap())
        .unwrap();
    let ticket = recovery.begin_transfer().unwrap();
    (recovery, ticket, operations)
}

#[test]
fn full_transfer_disk_publication_and_application_precede_fenced_election_handoff() {
    let (mut recovery, ticket, operations) = selected(91);
    let accepted = operations[1].prefix();
    let committed = operations[0].prefix();
    assert_eq!(
        recovery.complete(ticket, accepted, committed),
        Err(RecoveryError::HistoryMissing)
    );
    recovery.validate_chunk(ticket, &operations[..1]).unwrap();
    assert_eq!(
        recovery.complete(ticket, accepted, committed),
        Err(RecoveryError::HistoryMissing)
    );
    recovery.validate_chunk(ticket, &operations[1..]).unwrap();
    assert_eq!(
        recovery.complete(ticket, committed, committed),
        Err(RecoveryError::StoragePending)
    );
    assert_eq!(
        recovery.complete(ticket, accepted, Prefix::GENESIS),
        Err(RecoveryError::ApplicationPending)
    );
    let restored = recovery.complete(ticket, accepted, committed).unwrap();
    assert_eq!(restored.scope, ticket.scope());
    assert_eq!(restored.log.accepted, accepted);
    assert_eq!(restored.log.committed, committed);
    assert_eq!(
        recovery.complete(ticket, accepted, committed),
        Err(RecoveryError::Completed)
    );
    let changing = ViewChange::recover_intact(
        configuration(),
        ticket.local(),
        ticket.generation(),
        restored,
        limits(),
    )
    .unwrap();
    assert_eq!(changing.normal_snapshot().status, Status::Fenced);
    assert_eq!(changing.scope().view, ticket.scope().view + 1);
    assert_eq!(
        changing.start_message(),
        Err(ViewChangeError::PromiseRequired)
    );
}

#[test]
fn physical_repair_keeps_local_promises_and_never_skips_fenced_restart() {
    let (mut recovery, ticket, operations) = selected(101);
    let repaired = ozzy_replication::RecoveredState {
        scope: Scope {
            view: 7,
            ..ticket.scope()
        },
        log: ozzy_replication::FrozenLog {
            last_normal_view: 0,
            accepted: operations[1].prefix(),
            committed: Prefix::GENESIS,
        },
    };
    let (_, foreign, _) = selected(102);
    assert_eq!(
        recovery.complete_repair(foreign, repaired),
        Err(RecoveryError::StaleTransfer)
    );
    let mut invalid = repaired;
    invalid.log.last_normal_view = 8;
    assert_eq!(
        recovery.complete_repair(ticket, invalid),
        Err(RecoveryError::StoragePending)
    );
    assert_eq!(
        recovery.complete_repair(ticket, repaired).unwrap(),
        repaired
    );
    let voter = ViewChange::recover_intact(
        configuration(),
        node(1),
        JournalGeneration(110),
        repaired,
        limits(),
    )
    .unwrap();
    assert_eq!(voter.scope().view, 7);
    assert_eq!(voter.normal_snapshot().status, Status::Fenced);

    let (mut recovery, ticket, _) = selected(103);
    recovery
        .observe_view(
            node(2),
            Scope {
                view: 3,
                ..ticket.scope()
            },
        )
        .unwrap();
    assert_eq!(
        recovery.complete_repair(ticket, repaired),
        Err(RecoveryError::StaleView)
    );
}

#[test]
fn newer_normal_view_during_transfer_blocks_old_recovery_completion() {
    let (mut recovery, ticket, operations) = selected(92);
    recovery.validate_chunk(ticket, &operations).unwrap();
    recovery
        .receive(
            node(2),
            RecoveryResponse {
                scope: Scope {
                    view: 1,
                    ..ticket.scope()
                },
                nonce: ticket.nonce(),
                primary: None,
            },
        )
        .unwrap();
    assert_eq!(
        recovery.complete(ticket, ticket.source().accepted, ticket.committed()),
        Err(RecoveryError::StaleView),
    );
    assert_eq!(
        recovery.begin_transfer(),
        Err(RecoveryError::TransferPending)
    );
}

#[test]
fn fresh_quorum_requires_the_highest_view_primary_not_an_older_primary() {
    let nonce = RequestId::from_bytes([93; 16]);
    let mut recovery = Recovery::new(
        configuration(),
        node(1),
        JournalGeneration(100),
        nonce,
        limits(),
    )
    .unwrap();
    let old = normal(0).recovery_response(nonce).unwrap();
    let stale = RecoveryResponse {
        nonce: RequestId::from_bytes([1; 16]),
        scope: Scope {
            view: 99,
            ..old.scope
        },
        ..old
    };
    assert_eq!(
        recovery.receive(node(0), stale),
        Err(RecoveryError::StaleNonce)
    );
    recovery.receive(node(0), old).unwrap();
    recovery
        .receive(node(2), normal(2).recovery_response(nonce).unwrap())
        .unwrap();
    recovery
        .receive(
            node(0),
            RecoveryResponse {
                scope: Scope {
                    view: 2,
                    ..old.scope
                },
                nonce,
                primary: None,
            },
        )
        .unwrap();
    recovery.receive(node(0), old).unwrap(); // Delayed old snapshot cannot lower view.
    assert_eq!(
        recovery.begin_transfer(),
        Err(RecoveryError::PrimaryMissing)
    );
    recovery
        .receive(
            node(2),
            RecoveryResponse {
                scope: Scope {
                    view: 2,
                    ..old.scope
                },
                nonce,
                primary: Some(RecoveryLog {
                    checkpoint: None,
                    generation: JournalGeneration(3),
                    accepted: Prefix::GENESIS,
                    committed: Prefix::GENESIS,
                }),
            },
        )
        .unwrap();
    let ticket = recovery.begin_transfer().unwrap();
    assert_eq!(ticket.scope().view, 2);
    assert_eq!(ticket.source().voter, node(2));
    assert_eq!(ticket.voter_mask(), 0b101);
    // Empty history still needs quorum. The explicit publication callback is
    // the evidence for the empty store; no fabricated operation is necessary.
    assert!(
        recovery
            .complete(ticket, Prefix::GENESIS, Prefix::GENESIS)
            .is_ok()
    );
}

#[test]
fn conflicting_frozen_response_cannot_leave_the_old_transfer_eligible() {
    let (mut recovery, ticket, operations) = selected(94);
    recovery.validate_chunk(ticket, &operations).unwrap();
    let changed = RecoveryResponse {
        scope: ticket.scope(),
        nonce: ticket.nonce(),
        primary: Some(RecoveryLog {
            checkpoint: None,
            generation: ticket.source().generation,
            accepted: ticket.source().accepted,
            committed: ticket.source().accepted,
        }),
    };
    assert_eq!(
        recovery.receive(node(0), changed),
        Err(RecoveryError::ConflictingResponse)
    );
    assert_eq!(
        recovery.complete(ticket, ticket.source().accepted, ticket.committed()),
        Err(RecoveryError::Faulted)
    );
}

#[test]
fn later_election_and_old_writer_callbacks_never_release_recovery() {
    let (mut recovery, ticket, operations) = selected(95);
    let (_, foreign, _) = selected(96);
    assert_eq!(
        recovery.validate_chunk(foreign, &operations),
        Err(RecoveryError::StaleTransfer)
    );
    assert_eq!(
        recovery.complete(foreign, ticket.source().accepted, ticket.committed()),
        Err(RecoveryError::StaleTransfer)
    );
    assert_eq!(recovery.fail(foreign), Err(RecoveryError::StaleTransfer));
    recovery.validate_chunk(ticket, &operations).unwrap();
    recovery
        .observe_view(
            node(2),
            Scope {
                view: 3,
                ..ticket.scope()
            },
        )
        .unwrap();
    assert_eq!(
        recovery.complete(ticket, ticket.source().accepted, ticket.committed()),
        Err(RecoveryError::StaleView)
    );
    // A disk error still belongs to its old pending action after view discovery.
    recovery.fail(ticket).unwrap();
    assert_eq!(
        recovery.complete(ticket, ticket.source().accepted, ticket.committed()),
        Err(RecoveryError::Faulted)
    );
}

#[test]
fn rejected_chunks_preserve_the_cursor_and_both_count_and_byte_limits() {
    use ozzy_replication::ReplicationError as Error;
    let (mut recovery, ticket, operations) = selected(97);
    let third = operation(ticket.scope(), operations[1].prefix(), 12);
    assert_eq!(
        recovery.validate_chunk(ticket, &[]),
        Err(Error::HistoryGap.into())
    );
    assert_eq!(
        recovery.validate_chunk(ticket, &[operations[0]; 5]),
        Err(Error::Capacity.into())
    );
    assert_eq!(
        recovery.validate_chunk(ticket, &[operations[0], third]),
        Err(Error::HistoryGap.into())
    );
    let future = operation(
        Scope {
            view: 1,
            ..ticket.scope()
        },
        Prefix::GENESIS,
        10,
    );
    assert_eq!(
        recovery.validate_chunk(ticket, &[future]),
        Err(Error::ScopeMismatch.into())
    );
    let conflict = operation(ticket.scope(), Prefix::GENESIS, 99);
    assert_eq!(
        recovery.validate_chunk(ticket, &[conflict]),
        Err(Error::ConflictingHistory.into())
    );
    recovery.validate_chunk(ticket, &operations).unwrap();
    assert_eq!(
        recovery.validate_chunk(ticket, &[third]),
        Err(Error::ConflictingHistory.into())
    );
    recovery
        .complete(ticket, ticket.source().accepted, ticket.committed())
        .unwrap();

    let (mut small, ticket, operations) = selected_with_limits(
        98,
        PipelineLimits {
            max_operations: 4,
            max_body_bytes: 16,
        },
    );
    assert_eq!(
        small.validate_chunk(ticket, &operations),
        Err(Error::Capacity.into())
    );
    for operation in operations {
        small.validate_chunk(ticket, &[operation]).unwrap();
    }
    small
        .complete(ticket, ticket.source().accepted, ticket.committed())
        .unwrap();
}

#[test]
fn recovered_history_can_exceed_the_live_pipeline_without_retaining_descriptors() {
    let scope = configuration().scope();
    let mut operations = Vec::new();
    let mut previous = Prefix::GENESIS;
    for value in 1..=40 {
        let next = operation(scope, previous, value);
        previous = next.prefix();
        operations.push(next);
    }
    let mut primary = NormalReplica::bootstrap(
        configuration(),
        node(0),
        JournalGeneration(1),
        PipelineLimits {
            max_operations: 40,
            max_body_bytes: 640,
        },
    )
    .unwrap();
    let Admission::Write { ticket, .. } = primary.prepare(node(0), scope, &operations).unwrap()
    else {
        panic!("fresh history");
    };
    primary.complete_durable_write(ticket).unwrap();
    let bounds = PipelineLimits {
        max_operations: 1,
        max_body_bytes: 16,
    };
    let nonce = RequestId::from_bytes([99; 16]);
    let mut recovery = Recovery::new(
        configuration(),
        node(1),
        JournalGeneration(100),
        nonce,
        bounds,
    )
    .unwrap();
    recovery
        .receive(node(0), primary.recovery_response(nonce).unwrap())
        .unwrap();
    recovery
        .receive(node(2), normal(2).recovery_response(nonce).unwrap())
        .unwrap();
    let ticket = recovery.begin_transfer().unwrap();
    for operation in operations {
        recovery.validate_chunk(ticket, &[operation]).unwrap();
    }
    let restored = recovery
        .complete(ticket, previous, Prefix::GENESIS)
        .unwrap();
    let changing = ViewChange::recover_intact(
        configuration(),
        node(1),
        ticket.generation(),
        restored,
        bounds,
    )
    .unwrap();
    let snapshot = changing.normal_snapshot();
    assert_eq!(snapshot.pending_operations, 0);
    assert_eq!(snapshot.pending_body_bytes, 0);
    assert_eq!(snapshot.journal_tail, Some(previous));
    assert_eq!(snapshot.status, Status::Fenced);
}

#[test]
fn replacement_writer_cannot_reuse_the_selected_source_incarnation() {
    let nonce = RequestId::from_bytes([100; 16]);
    let mut recovery = Recovery::new(
        configuration(),
        node(1),
        JournalGeneration(1),
        nonce,
        limits(),
    )
    .unwrap();
    recovery
        .receive(node(0), normal(0).recovery_response(nonce).unwrap())
        .unwrap();
    recovery
        .receive(node(2), normal(2).recovery_response(nonce).unwrap())
        .unwrap();
    assert_eq!(
        recovery.begin_transfer(),
        Err(RecoveryError::InvalidIdentity)
    );
}

#[test]
fn malformed_or_unready_responders_never_supply_recovery_evidence() {
    use ozzy_replication::ReplicationError;
    let nonce = RequestId::from_bytes([101; 16]);
    let mut primary = normal(0);
    let valid = primary.recovery_response(nonce).unwrap();
    assert_eq!(
        primary.recovery_response(RequestId::from_bytes([0; 16])),
        Err(RecoveryError::InvalidIdentity)
    );
    primary.fence();
    assert_eq!(
        primary.recovery_response(nonce),
        Err(RecoveryError::NotReady)
    );
    for bad in malformed_logs(&valid.primary.unwrap()) {
        let mut recovery = Recovery::new(
            configuration(),
            node(1),
            JournalGeneration(100),
            nonce,
            limits(),
        )
        .unwrap();
        assert_eq!(
            recovery.receive(
                node(0),
                RecoveryResponse {
                    primary: Some(bad),
                    ..valid
                }
            ),
            Err(RecoveryError::InvalidResponse)
        );
        assert_eq!(
            recovery.receive(
                node(0),
                RecoveryResponse {
                    primary: None,
                    ..valid
                }
            ),
            Err(RecoveryError::InvalidResponse)
        );
        assert_eq!(
            recovery.receive(node(2), valid),
            Err(RecoveryError::InvalidResponse)
        );
        assert_eq!(
            recovery.receive(node(3), valid),
            Err(ReplicationError::UnknownVoter.into())
        );
        let foreign = Scope {
            configuration_digest: Digest::from_bytes([9; 32]),
            view: 99,
            ..valid.scope
        };
        assert_eq!(
            recovery.receive(
                node(0),
                RecoveryResponse {
                    scope: foreign,
                    ..valid
                }
            ),
            Err(ReplicationError::ScopeMismatch.into())
        );
        assert_eq!(
            recovery.observe_view(node(0), foreign),
            Err(ReplicationError::ScopeMismatch.into())
        );
        recovery
            .receive(node(2), normal(2).recovery_response(nonce).unwrap())
            .unwrap();
        assert_eq!(recovery.begin_transfer(), Err(RecoveryError::QuorumMissing));
        recovery.receive(node(0), valid).unwrap();
        assert_eq!(recovery.begin_transfer().unwrap().scope().view, 0);
    }
}

fn malformed_logs(log: &RecoveryLog) -> [RecoveryLog; 5] {
    use ozzy_replication::OpNumber;
    let nonzero = Prefix {
        op: OpNumber(1),
        digest: Digest::from_bytes([1; 32]),
    };
    [
        RecoveryLog {
            checkpoint: None,
            generation: JournalGeneration(0),
            ..*log
        },
        RecoveryLog {
            checkpoint: None,
            accepted: Prefix {
                op: OpNumber(0),
                ..nonzero
            },
            ..*log
        },
        RecoveryLog {
            checkpoint: None,
            accepted: Prefix {
                op: OpNumber(u64::MAX),
                ..nonzero
            },
            ..*log
        },
        RecoveryLog {
            checkpoint: None,
            committed: nonzero,
            ..*log
        },
        RecoveryLog {
            checkpoint: None,
            accepted: nonzero,
            committed: Prefix {
                digest: Digest::from_bytes([2; 32]),
                ..nonzero
            },
            ..*log
        },
    ]
}

//! Retained votes, independent persistence, and the restart/election boundary.

use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
    encode_operation_body,
};
use ozzy_proto::{GroupId, NodeId, OperationId};
use ozzy_replication::{
    Admission, Commit, Configuration, ConfigurationRecord, ConfiguredVoter, Digest, FrozenLog,
    JournalGeneration, NormalReplica, OpNumber, PipelineLimits, Prefix, PreparedOperation,
    QuorumPolicy, RecoveredState, ReplicationError, Scope, ViewChange, ViewChangeError,
    WriteTicket,
};

#[path = "support/persisting_driver.rs"]
mod driver;

fn node(index: usize) -> NodeId {
    NodeId::from_bytes([index as u8 + 1; 16])
}

fn record(policy: QuorumPolicy) -> ConfigurationRecord {
    ConfigurationRecord::with_policy(
        GroupId::from_bytes([8; 16]),
        1,
        std::array::from_fn(|index| ConfiguredVoter {
            node_id: node(index),
            principal: Digest::from_bytes([index as u8 + 1; 32]),
        }),
        policy,
    )
    .unwrap()
}

fn limits() -> PipelineLimits {
    PipelineLimits {
        max_operations: 2,
        max_body_bytes: 32,
    }
}

fn replica(configuration: Configuration, index: usize) -> NormalReplica {
    NormalReplica::bootstrap(
        configuration,
        node(index),
        JournalGeneration(index as u128 + 1),
        limits(),
    )
    .unwrap()
}

fn operation(scope: Scope, previous: Prefix) -> PreparedOperation {
    let body = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes((u128::from(previous.op.0) + 1).to_be_bytes()),
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

fn prepare(replica: &mut NormalReplica, operation: PreparedOperation) -> WriteTicket {
    match replica
        .prepare(node(0), replica.snapshot().scope, &[operation])
        .unwrap()
    {
        Admission::Write { ticket, .. } => ticket,
        Admission::Duplicate => panic!("fresh operation"),
    }
}

#[test]
fn confirmation_policy_is_part_of_the_immutable_configuration() {
    let memory = record(QuorumPolicy::Replicated);
    let disk = record(QuorumPolicy::Durable);
    assert_eq!(
        ConfigurationRecord::decode(&memory.encode()).unwrap(),
        memory
    );
    assert_eq!(ConfigurationRecord::decode(&disk.encode()).unwrap(), disk);
    assert_ne!(memory.configuration().scope(), disk.configuration().scope());
    assert_eq!(memory.configuration().policy(), QuorumPolicy::Replicated);
    assert_eq!(disk.configuration().policy(), QuorumPolicy::Durable);
}

#[test]
fn retained_wire_evidence_is_distinct_and_cannot_be_retyped_as_disk_evidence() {
    use ozzy_proto::LinkSessionId;
    use ozzy_replication::wire::{
        Control, Grant, PeerBinding, ReplicaMessage, WireError, WireLimits, decode, encode_control,
    };

    let configuration = record(QuorumPolicy::Replicated).configuration();
    let session = LinkSessionId::from_bytes([9; 16]);
    let follower = replica(configuration, 1);
    let message = Control::PrepareRetained {
        ack: follower.retained_acknowledgment().unwrap(),
        grant: Grant {
            revision: 1,
            record_limit: 0,
            byte_limit: 0,
        },
    };
    let mut metadata = [0; 145];
    let encoded = encode_control(node(1), session, message, &mut metadata).unwrap();
    assert_eq!(metadata[120], 3);
    let binding = PeerBinding::new(configuration, node(1), session).unwrap();
    assert_eq!(
        decode(
            &[&encoded.header, &metadata, &[]],
            binding,
            WireLimits::default()
        )
        .unwrap(),
        ReplicaMessage::Control(message),
    );
    for evidence in [0, 1, 2, 4, 255] {
        metadata[120] = evidence;
        assert_eq!(
            decode(
                &[&encoded.header, &metadata, &[]],
                binding,
                WireLimits::default()
            ),
            Err(WireError::Evidence),
        );
    }

    let durable = record(QuorumPolicy::Durable).configuration();
    let forged = Control::PrepareRetained {
        ack: ozzy_replication::RetainedPrepareOk {
            scope: durable.scope(),
            retained: Prefix::GENESIS,
        },
        grant: Grant {
            revision: 1,
            record_limit: 0,
            byte_limit: 0,
        },
    };
    let encoded = encode_control(node(1), session, forged, &mut metadata).unwrap();
    assert_eq!(
        decode(
            &[&encoded.header, &metadata, &[]],
            PeerBinding::new(durable, node(1), session).unwrap(),
            WireLimits::default(),
        ),
        Err(WireError::Evidence),
    );
}

#[test]
fn two_retained_copies_confirm_before_either_disk_completes() {
    let configuration = record(QuorumPolicy::Replicated).configuration();
    let mut leader = replica(configuration, 0);
    let mut follower = replica(configuration, 1);
    let operation = operation(configuration.scope(), Prefix::GENESIS);
    prepare(&mut leader, operation);
    prepare(&mut follower, operation);
    assert_eq!(leader.snapshot().committed, Prefix::GENESIS);
    let ack = follower.retained_acknowledgment().unwrap();
    leader.receive_retained_ack(node(1), ack).unwrap();
    assert_eq!(leader.snapshot().committed, operation.prefix());
    for copy in [&leader, &follower] {
        assert_eq!(copy.snapshot().journal.written, OpNumber(0));
        assert_eq!(copy.snapshot().journal.durable, OpNumber(0));
    }
    follower
        .receive_commit(node(0), leader.announcement().unwrap())
        .unwrap();
    leader.apply_through(operation.prefix()).unwrap();
    follower.apply_through(operation.prefix()).unwrap();
    assert_eq!(leader.snapshot().pending_operations, 1);
    assert_eq!(follower.snapshot().pending_operations, 1);
}

#[test]
fn confirmed_memory_is_reclaimed_after_exact_buffered_write() {
    let configuration = record(QuorumPolicy::Replicated).configuration();
    let mut leader = replica(configuration, 0);
    let mut follower = replica(configuration, 1);
    let first = operation(configuration.scope(), Prefix::GENESIS);
    let second = operation(configuration.scope(), first.prefix());
    let old = prepare(&mut leader, first);
    prepare(&mut follower, first);
    leader
        .receive_retained_ack(node(1), follower.retained_acknowledgment().unwrap())
        .unwrap();
    leader.apply_through(first.prefix()).unwrap();
    let next = prepare(&mut leader, second);
    prepare(&mut follower, second);
    leader
        .receive_retained_ack(node(1), follower.retained_acknowledgment().unwrap())
        .unwrap();
    leader.apply_through(second.prefix()).unwrap();
    assert_eq!(
        leader.apply_through(first.prefix()),
        Err(ReplicationError::HistoryUnavailable)
    );
    let third = operation(configuration.scope(), second.prefix());
    assert_eq!(
        leader.prepare(node(0), configuration.scope(), &[third]),
        Err(ReplicationError::Capacity)
    );
    leader.complete_write(old).unwrap();
    assert_eq!(leader.snapshot().pending_operations, 1);
    assert_eq!(leader.snapshot().journal.durable, OpNumber(0));
    let barrier = leader.begin_sync().unwrap();
    leader.complete_write(next).unwrap();
    leader.complete_sync(barrier).unwrap();
    assert_eq!(leader.snapshot().journal.durable, first.prefix().op);
    assert_eq!(leader.snapshot().pending_operations, 0);
    assert_eq!(leader.snapshot().pending_body_bytes, 0);
    assert_eq!(leader.snapshot().applied, second.prefix());
    prepare(&mut leader, third);
    assert_eq!(leader.snapshot().pending_operations, 1);
}

#[test]
fn persistence_and_application_both_precede_reclamation() {
    let configuration = record(QuorumPolicy::Replicated).configuration();
    let mut leader = replica(configuration, 0);
    let mut follower = replica(configuration, 1);
    let operation = operation(configuration.scope(), Prefix::GENESIS);
    let write = prepare(&mut leader, operation);
    prepare(&mut follower, operation);
    leader.complete_durable_write(write).unwrap();
    assert_eq!(leader.snapshot().pending_operations, 1);
    assert_eq!(leader.snapshot().committed, Prefix::GENESIS);
    leader
        .receive_retained_ack(node(1), follower.retained_acknowledgment().unwrap())
        .unwrap();
    assert_eq!(leader.snapshot().pending_operations, 1);
    leader.apply_through(operation.prefix()).unwrap();
    assert_eq!(leader.snapshot().pending_operations, 0);
}

#[test]
fn retained_votes_cannot_satisfy_disk_quorum() {
    let durable = record(QuorumPolicy::Durable).configuration();
    let mut disk = replica(durable, 0);
    let memory = record(QuorumPolicy::Replicated).configuration();
    let follower = replica(memory, 1);
    let mut ack = follower.retained_acknowledgment().unwrap();
    // Even forged matching scope cannot turn memory evidence into disk evidence.
    ack.scope = durable.scope();
    assert_eq!(
        disk.receive_retained_ack(node(1), ack),
        Err(ReplicationError::PolicyMismatch)
    );
    assert!(matches!(
        replica(durable, 1).retained_acknowledgment(),
        Err(ReplicationError::PolicyMismatch)
    ));
    assert!(matches!(
        follower.acknowledgment(),
        Err(ReplicationError::PolicyMismatch)
    ));
}

#[test]
fn election_flushes_accepted_history_even_without_commit_announcement() {
    let configuration = record(QuorumPolicy::Replicated).configuration();
    let mut leader = replica(configuration, 0);
    let mut follower = replica(configuration, 1);
    let operation = operation(configuration.scope(), Prefix::GENESIS);
    prepare(&mut leader, operation);
    let write = prepare(&mut follower, operation);
    leader
        .receive_retained_ack(node(1), follower.retained_acknowledgment().unwrap())
        .unwrap();
    assert_eq!(leader.snapshot().committed, operation.prefix());
    assert_eq!(follower.snapshot().committed, Prefix::GENESIS);
    drop(leader); // Reply may have reached the writer before the leader died.
    let mut changing = follower.into_view_change(1).unwrap();
    assert_eq!(
        changing.begin_promise(),
        Err(ViewChangeError::StoragePending)
    );
    changing.complete_write(write).unwrap();
    assert_eq!(
        changing.begin_promise(),
        Err(ViewChangeError::StoragePending)
    );
    let sync = changing.begin_sync().unwrap();
    changing.complete_sync(sync).unwrap();
    let promise = changing.begin_promise().unwrap();
    assert_eq!(promise.log().accepted, operation.prefix());
    assert_eq!(promise.log().committed, Prefix::GENESIS);
}

#[test]
fn a_stale_disk_prefix_cannot_use_the_intact_restart_entry_point() {
    let configuration = record(QuorumPolicy::Replicated).configuration();
    let recovered = RecoveredState {
        scope: configuration.scope(),
        log: FrozenLog {
            last_normal_view: 0,
            accepted: Prefix::GENESIS,
            committed: Prefix::GENESIS,
        },
    };
    assert!(matches!(
        ViewChange::recover_intact(
            configuration,
            node(0),
            JournalGeneration(99),
            recovered,
            limits(),
        ),
        Err(ViewChangeError::VolatileRestart)
    ));
}

#[test]
fn drained_restart_still_fences_the_old_view_and_checks_configuration() {
    let configuration = record(QuorumPolicy::Replicated).configuration();
    let recovered = RecoveredState {
        scope: configuration.scope(),
        log: FrozenLog {
            last_normal_view: 0,
            accepted: Prefix::GENESIS,
            committed: Prefix::GENESIS,
        },
    };
    let changing = ViewChange::recover_drained(
        configuration,
        node(0),
        JournalGeneration(100),
        recovered,
        limits(),
    )
    .unwrap();
    assert_eq!(changing.scope().view, 1);
    assert_eq!(
        changing.normal_snapshot().status,
        ozzy_replication::Status::Fenced
    );
    assert!(matches!(
        ViewChange::recover_drained(
            record(QuorumPolicy::Durable).configuration(),
            node(0),
            JournalGeneration(101),
            recovered,
            limits(),
        ),
        Err(ViewChangeError::Replication(
            ReplicationError::PolicyMismatch
        ))
    ));
    let mut stale = recovered;
    stale.scope.configuration_digest = Digest::from_bytes([9; 32]);
    assert!(
        ViewChange::recover_drained(
            configuration,
            node(0),
            JournalGeneration(102),
            stale,
            limits(),
        )
        .is_err()
    );
}

#[test]
fn repeated_confirm_persist_cycles_keep_the_live_window_bounded() {
    let configuration = record(QuorumPolicy::Replicated).configuration();
    let mut leader = replica(configuration, 0);
    let mut follower = replica(configuration, 1);
    for _ in 0..128 {
        let operation = operation(configuration.scope(), leader.snapshot().accepted);
        let primary_write = prepare(&mut leader, operation);
        let backup_write = prepare(&mut follower, operation);
        leader
            .receive_retained_ack(node(1), follower.retained_acknowledgment().unwrap())
            .unwrap();
        follower
            .receive_commit(
                node(0),
                Commit {
                    scope: configuration.scope(),
                    committed: operation.prefix(),
                },
            )
            .unwrap();
        leader.apply_through(operation.prefix()).unwrap();
        follower.apply_through(operation.prefix()).unwrap();
        leader.complete_write(primary_write).unwrap();
        follower.complete_write(backup_write).unwrap();
        for copy in [&leader, &follower] {
            assert_eq!(copy.snapshot().pending_operations, 0);
            assert_eq!(copy.snapshot().pending_body_bytes, 0);
        }
    }
    assert_eq!(leader.snapshot().accepted.op, OpNumber(128));
}

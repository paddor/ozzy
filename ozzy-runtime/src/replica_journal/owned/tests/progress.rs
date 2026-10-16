use super::writeback::{
    Replica, admit_encoded, confirm, finish, initialize_writer, packed_append_body, prepare,
    replica, settle, synchronize,
};
use super::*;
use crate::replica_journal::{
    GroupAssignment, GroupProgressRequest, GroupProgressState, ProposalBuffer, ProposalValidation,
};
use ozzy_core::state::CanonicalState;
use ozzy_journal::operation::{
    Assign, OperationBody, OperationKind, ProgressOwner, encode_operation_body,
};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, Offset, OperationId, PartitionIncarnation,
    append::{Authority, Policy},
};
use ozzy_replication::Admission;

#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
fn assignment_boundary(
    controller: &mut Controller,
    primary: &mut Replica,
    backup: &mut Replica,
    policy: QuorumPolicy,
) {
    use crate::replica_journal::{GroupAssignRequest, GroupProgressError, JournalError};
    let before = primary.driver.begin_validation().unwrap();
    let assignment = assignment();
    let request = GroupAssignRequest {
        authority: Authority {
            group_id: before.scope().group_id,
            config_epoch: before.scope().configuration_epoch,
            view: before.scope().view,
        },
        group: assignment.group,
        partition: assignment.partition,
        plan_revision: 2,
        member: Some(ConsumerMemberId::from_bytes([81; 16])),
        initial: ozzy_proto::group::Start::Earliest,
        operation: OperationId::from_bytes([82; 16]),
    };
    let boundary = primary.config.configuration.configuration().append_policy();
    let mut buffer = primary.journal.lease_proposal_buffer().unwrap();
    buffer.prepare_group_assignment(request, boundary).unwrap();
    let ProposalValidation::Ready(validated) = drive(
        controller,
        primary.journal.propose_append(before, buffer, 777),
    ) else {
        panic!("source handoff")
    };
    let captured = validated
        .buffer
        .group_assignment
        .as_ref()
        .unwrap()
        .state
        .unwrap();
    assert_eq!(
        (captured.epoch, captured.cursor, captured.policy),
        (2, Offset::new(1), boundary)
    );
    let encoded = validated
        .buffer
        .operations()
        .map(|o| (o.kind, o.body.to_vec()))
        .collect::<Vec<_>>();
    let primary_receipt = super::proposal::accept(primary, validated);
    let pending = primary.driver.begin_validation().unwrap().accepted();
    let (_, _, backup_receipt) = admit_encoded(controller, backup, encoded);
    confirm(primary, backup, policy);
    follow_commit(primary, backup);
    assert_eq!(
        primary
            .journal
            .images()
            .unwrap()
            .committed()
            .assignment(assignment.group, assignment.partition)
            .unwrap()
            .epoch,
        if policy == QuorumPolicy::Durable {
            1
        } else {
            2
        }
    );
    assert!(primary.driver.normal().unwrap().snapshot().journal.written < pending.op);

    let mut buffer = primary.journal.lease_proposal_buffer().unwrap();
    buffer.prepare_group_assignment(request, boundary).unwrap();
    let ProposalValidation::Resolved {
        through, buffer, ..
    } = drive(
        controller,
        primary
            .journal
            .propose_append(primary.driver.begin_validation().unwrap(), buffer, 777),
    )
    else {
        panic!("covered handoff")
    };
    assert_eq!(through, pending);
    assert_eq!(buffer.group_assignment_state().unwrap(), captured);
    drop(buffer);
    for replica in [&mut *primary, &mut *backup] {
        let work = prepare(replica);
        finish(controller, replica, work);
    }
    settle(primary, primary_receipt);
    settle(backup, backup_receipt);
    for replica in [&mut *primary, &mut *backup] {
        synchronize(controller, replica);
    }
    confirm(primary, backup, policy);
    follow_commit(primary, backup);
    for replica in [&*primary, &*backup] {
        let state = replica
            .journal
            .images()
            .unwrap()
            .committed()
            .assignment(assignment.group, assignment.partition)
            .unwrap();
        assert_eq!(
            (state.epoch, state.member, state.resume_cursor),
            (2, request.member, Offset::new(1))
        );
    }
    let buffer = self::request(primary, Some(Offset::new(1)), 83);
    assert!(matches!(
        drive(
            controller,
            primary
                .journal
                .propose_append(primary.driver.begin_validation().unwrap(), buffer, 777)
        ),
        ProposalValidation::Rejected {
            reason: JournalError::GroupProgress(GroupProgressError::Fenced),
            ..
        }
    ));
}

fn follow_commit(primary: &Replica, backup: &mut Replica) {
    let ticket = primary.driver.begin_validation().unwrap();
    backup
        .driver
        .receive(
            primary.config.identity.replica_node_id,
            ozzy_replication::wire::Control::Commit(ozzy_replication::Commit {
                scope: ticket.scope(),
                committed: ticket.committed(),
            }),
            Duration::ZERO,
        )
        .unwrap();
    let ticket = backup.driver.begin_validation().unwrap();
    backup.journal.apply(ticket).unwrap();
    backup.driver.apply_through(ticket.committed()).unwrap();
}

fn assignment() -> GroupAssignment {
    GroupAssignment {
        group: ConsumerGroupId::from_bytes([61; 16]),
        partition: PartitionIncarnation::from_bytes([11; 16]),
        member: ConsumerMemberId::from_bytes([62; 16]),
        epoch: 1,
    }
}

fn request(replica: &Replica, cursor: Option<Offset>, id: u8) -> ProposalBuffer {
    let scope = replica.driver.scope();
    let mut buffer = replica.journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_group_progress(
            GroupProgressRequest {
                authority: Authority {
                    group_id: scope.group_id,
                    config_epoch: scope.configuration_epoch,
                    view: scope.view,
                },
                assignment: assignment(),
                cursor,
                operation: OperationId::from_bytes([id; 16]),
            },
            match replica.config.configuration.configuration().policy() {
                QuorumPolicy::Durable => Policy::QuorumDurable,
                QuorumPolicy::Replicated => Policy::QuorumReplicatedPersisting,
            },
        )
        .unwrap();
    buffer
}

fn resolved(
    controller: &mut Controller,
    replica: &mut Replica,
    buffer: ProposalBuffer,
) -> (Prefix, GroupProgressState) {
    let ticket = replica.driver.begin_validation().unwrap();
    let result = drive(
        controller,
        replica.journal.propose_append(ticket, buffer, 777),
    );
    let ProposalValidation::Resolved {
        through, buffer, ..
    } = result
    else {
        panic!("covered progress: {result:?}")
    };
    (through, buffer.group_progress_state().unwrap())
}

fn stored_cursor(state: &CanonicalState) -> Option<Offset> {
    state.progress(
        ProgressOwner::ConsumerGroup(assignment().group),
        assignment().partition,
    )
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
fn both_quorum_policies_preserve_group_progress_and_assignment_confirmation_boundaries() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut primary = replica(&mut controller, io.clone(), 0, policy, 8192);
        let mut backup = replica(&mut controller, io, 1, policy, 8192);
        for replica in [&mut primary, &mut backup] {
            let receipt = initialize_writer(&mut controller, replica);
            let work = prepare(replica);
            finish(&mut controller, replica, work);
            settle(replica, receipt);
            synchronize(&mut controller, replica);
        }
        confirm(&mut primary, &backup, policy);
        follow_commit(&primary, &mut backup);
        let data = packed_append_body(primary.config.limits.operations, &[42; 4096]);
        for replica in [&mut primary, &mut backup] {
            let (_, _, receipt) = admit_encoded(
                &mut controller,
                replica,
                [(OperationKind::Append, data.clone())],
            );
            let work = prepare(replica);
            finish(&mut controller, replica, work);
            settle(replica, receipt);
            synchronize(&mut controller, replica);
        }
        confirm(&mut primary, &backup, policy);
        follow_commit(&primary, &mut backup);
        let current = assignment();
        let body = OperationBody::Assign(Assign {
            consumer_group_id: current.group,
            partition: current.partition,
            expected_assignment_epoch: 0,
            new_assignment_epoch: current.epoch,
            plan_revision: 1,
            resume_cursor: Offset::ZERO,
            new_member: Some(current.member),
            operation_id: OperationId::from_bytes([63; 16]),
        });
        let encoded = encode_operation_body(&body, primary.config.limits.operations).unwrap();
        for replica in [&mut primary, &mut backup] {
            let (_, _, receipt) =
                admit_encoded(&mut controller, replica, [(body.kind(), encoded.clone())]);
            let work = prepare(replica);
            finish(&mut controller, replica, work);
            settle(replica, receipt);
            synchronize(&mut controller, replica);
        }
        confirm(&mut primary, &backup, policy);
        follow_commit(&primary, &mut backup);
        let before = primary.driver.begin_validation().unwrap();
        assert_eq!(before.accepted(), before.applied());
        assert_eq!(
            stored_cursor(primary.journal.images().unwrap().committed()),
            Some(Offset::ZERO)
        );
        let zero = request(&primary, Some(Offset::ZERO), 64);
        let (through, state) = resolved(&mut controller, &mut primary, zero);
        assert_eq!(through, before.applied());
        assert_eq!(state.cursor, Offset::ZERO);
        let buffer = request(&primary, Some(Offset::new(1)), 68);
        let result = drive(
            &mut controller,
            primary.journal.propose_append(before, buffer, 777),
        );
        let ProposalValidation::Ready(mut validated) = result else {
            panic!("new progress: {result:?}")
        };
        let encoded = validated
            .buffer()
            .operations()
            .map(|operation| (operation.kind, operation.body.to_vec()))
            .collect::<Vec<(OperationKind, Vec<u8>)>>();
        assert!(primary.journal.can_admit(&mut validated));
        let Admission::Write { ticket, .. } = primary
            .driver
            .prepare_validated(
                primary.config.identity.replica_node_id,
                before,
                validated.prepared(),
                Duration::ZERO,
            )
            .unwrap()
        else {
            panic!("progress write ticket")
        };
        let (_, buffer, primary_receipt) = primary
            .journal
            .admit_append(ticket, validated)
            .unwrap()
            .into_parts();
        assert_eq!(
            buffer
                .group_progress
                .as_ref()
                .unwrap()
                .state
                .unwrap()
                .cursor,
            Offset::new(1)
        );
        drop(buffer);
        let pending = primary.driver.begin_validation().unwrap().accepted();
        assert!(pending.op > before.applied().op);
        let retry = request(&primary, Some(Offset::new(1)), 65);
        let (through, state) = resolved(&mut controller, &mut primary, retry);
        assert_eq!(through, pending);
        assert_eq!(state.cursor, Offset::new(1));
        let query = request(&primary, None, 66);
        let (through, state) = resolved(&mut controller, &mut primary, query);
        assert_eq!(through, before.applied());
        assert_eq!((state.cursor, state.end), (Offset::ZERO, Offset::new(1)));
        assert_eq!(
            stored_cursor(primary.journal.images().unwrap().committed()),
            Some(Offset::ZERO)
        );
        let (_, _, backup_receipt) = admit_encoded(&mut controller, &mut backup, encoded);
        confirm(&mut primary, &backup, policy);
        follow_commit(&primary, &mut backup);
        let before_writes = match policy {
            QuorumPolicy::Durable => Some(Offset::ZERO),
            QuorumPolicy::Replicated => Some(Offset::new(1)),
        };
        assert_eq!(
            stored_cursor(primary.journal.images().unwrap().committed()),
            before_writes
        );
        let snapshot = primary.driver.normal().unwrap().snapshot();
        assert!(snapshot.journal.written < pending.op);
        assert_eq!(
            snapshot.applied,
            match policy {
                QuorumPolicy::Durable => before.applied(),
                QuorumPolicy::Replicated => pending,
            }
        );
        for replica in [&mut primary, &mut backup] {
            let work = prepare(replica);
            finish(&mut controller, replica, work);
        }
        settle(&mut primary, primary_receipt);
        settle(&mut backup, backup_receipt);
        for replica in [&mut primary, &mut backup] {
            synchronize(&mut controller, replica);
        }
        assert_eq!(
            stored_cursor(primary.journal.images().unwrap().committed()),
            before_writes
        );
        confirm(&mut primary, &backup, policy);
        follow_commit(&primary, &mut backup);
        assert_eq!(
            stored_cursor(primary.journal.images().unwrap().committed()),
            Some(Offset::new(1))
        );
        assert_eq!(
            stored_cursor(backup.journal.images().unwrap().committed()),
            Some(Offset::new(1))
        );
        for cursor in [None, Some(Offset::ZERO), Some(Offset::new(1))] {
            let buffer = request(&primary, cursor, 67);
            let (through, state) = resolved(&mut controller, &mut primary, buffer);
            assert_eq!(through, pending);
            assert_eq!(state.assignment, current);
            assert_eq!(state.cursor, Offset::new(1));
            assert_eq!(
                primary.driver.begin_validation().unwrap().accepted(),
                pending
            );
        }
        assignment_boundary(&mut controller, &mut primary, &mut backup, policy);
        drive(&mut controller, primary.journal.shutdown()).unwrap();
        drive(&mut controller, backup.journal.shutdown()).unwrap();
    }
}

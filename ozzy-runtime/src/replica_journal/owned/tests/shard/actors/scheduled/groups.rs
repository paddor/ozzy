use super::*;
use crate::replica_actor::{ProposalReply, ProposalSubmitter};
use crate::replica_journal::{GroupAssignment, GroupProgressRequest, ProposalBuffer};
use ozzy_core::state::{CoordinatorGroup, group_targets};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Assign, CreatePartition, GroupMember, GroupPlan,
    OpenProducer, OperationBody, OperationLimits, Progress, ProgressAssignment, ProgressOwner,
    RetentionPolicy, encode_operation_body,
};
use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, Offset, OperationId, OwnerEpoch, PartitionId,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, append::Authority,
    group::Start,
};

#[derive(Clone, Copy, Debug)]
enum Phase {
    Join,
    Plan,
    Assign,
}

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([11; 16])
}
fn group() -> ConsumerGroupId {
    ConsumerGroupId::from_bytes([61; 16])
}
fn member(id: u8) -> GroupMember {
    GroupMember {
        id: ConsumerMemberId::from_bytes([id; 16]),
        node: NodeId::from_bytes([70; 16]),
        join: OperationId::from_bytes([id; 16]),
    }
}
fn plan(prior: Option<&CoordinatorGroup>, id: u8) -> OperationBody<'static> {
    let revision = prior.map_or(1, |image| image.revision + 1);
    let members = vec![member(id)];
    OperationBody::GroupPlan(GroupPlan {
        coordinator: partition(),
        group: group(),
        name: "workers",
        expected_revision: revision - 1,
        revision,
        initial: Start::Earliest,
        targets: group_targets(prior, &members, &[partition()], revision).unwrap(),
        members,
        operation_id: OperationId::from_bytes([id + 90; 16]),
    })
}
fn push(buffer: &mut ProposalBuffer, body: &OperationBody<'_>) {
    buffer
        .push(
            body.kind(),
            &encode_operation_body(body, OperationLimits::default()).unwrap(),
        )
        .unwrap();
}
fn assignment(epoch: u64, revision: u64, id: u8, cursor: u64) -> OperationBody<'static> {
    OperationBody::Assign(Assign {
        consumer_group_id: group(),
        partition: partition(),
        expected_assignment_epoch: epoch - 1,
        new_assignment_epoch: epoch,
        plan_revision: revision,
        resume_cursor: Offset::new(cursor),
        new_member: Some(member(id).id),
        operation_id: OperationId::from_bytes([id + 100; 16]),
    })
}
fn seed(buffer: &mut ProposalBuffer, phase: Phase) {
    push(
        buffer,
        &OperationBody::CreatePartition(CreatePartition {
            partition: partition(),
            stream: "stream",
            topic: "orders",
            partition_id: PartitionId::ZERO,
            owner_epoch: OwnerEpoch::INITIAL,
            delay_max_millis: None,
            dedup_window_millis: None,
            retention: RetentionPolicy::default(),
        }),
    );
    push(
        buffer,
        &OperationBody::OpenProducer(OpenProducer {
            partition: partition(),
            producer_id: ProducerId::from_bytes([12; 16]),
            expected_epoch: None,
            new_epoch: ProducerEpoch::INITIAL,
            operation_id: OperationId::from_bytes([13; 16]),
        }),
    );
    push(
        buffer,
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: partition(),
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([12; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::ZERO,
                first_offset: Offset::ZERO,
                append_timestamp_millis: 123,
                records: (1..=3)
                    .map(|id| AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: ozzy_proto::MessageId::from_bytes([id; 16]),
                        parts: vec![b"opaque".as_slice()].into(),
                    })
                    .collect::<Vec<_>>()
                    .into(),
            }],
        }),
    );
    if !matches!(phase, Phase::Join) {
        push(buffer, &plan(None, 2));
        push(buffer, &assignment(1, 1, 2, 0));
        push(
            buffer,
            &OperationBody::Progress(Progress {
                owner: ProgressOwner::ConsumerGroup(group()),
                partition: partition(),
                assignment: Some(ProgressAssignment {
                    epoch: 1,
                    member: member(2).id,
                }),
                expected_progress: Some(Offset::ZERO),
                new_progress: Offset::new(2),
                operation_id: OperationId::from_bytes([95; 16]),
            }),
        );
    }
}

fn image(
    controller: &mut Controller,
    actor: &mut Scheduled,
    now: Duration,
) -> Option<Arc<CoordinatorGroup>> {
    for _ in 0..10000 {
        let (ticket, journal) = actor.read_access();
        match journal.coordinator_group(ticket.unwrap(), partition(), "workers") {
            Ok(image) => return image,
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            result => panic!("coordinator image failed: {result:?}"),
        }
        actor.advance(now).unwrap();
        {
            let mut changed = std::pin::pin!(actor.changed(now));
            if let Poll::Ready(result) = poll(changed.as_mut()) {
                result.unwrap();
            }
        }
        settle(controller, &[]);
    }
    panic!("coordinator image stalled");
}

fn round_survivors(actors: &mut [Scheduled], now: Duration) {
    let mut messages = Vec::new();
    for (from, actor) in actors.iter_mut().enumerate().skip(1) {
        actor.advance(now).unwrap();
        {
            let mut changed = std::pin::pin!(actor.changed(now));
            if let Poll::Ready(result) = poll(changed.as_mut()) {
                result.unwrap();
            }
        }
        actor
            .flush(|mut message| {
                let to = usize::from(message.pop_front().unwrap()[0] - 1);
                if to != 0 {
                    messages.push((
                        to,
                        Message::with_prefix(bytes::Bytes::from(vec![from as u8 + 1; 16]), message),
                    ));
                }
                Ok(())
            })
            .unwrap();
    }
    for (to, message) in messages {
        actors[to].receive(&message, now).unwrap();
    }
}

fn finish(
    controller: &mut Controller,
    actors: &mut [Scheduled],
    pending: PendingProposal,
    survivors: bool,
    now: &mut Duration,
) -> ProposalReply {
    let mut pending = Box::pin(pending);
    for step in 0..10000 {
        if step % 16 == 15 {
            *now += Duration::from_millis(1);
        }
        if survivors {
            round_survivors(actors, *now);
        } else {
            round_at(actors, *now, false);
        }
        settle(controller, &[]);
        if let Poll::Ready(reply) = poll(pending.as_mut()) {
            let reply = reply.unwrap();
            assert!(
                matches!(reply.outcome, ProposalOutcome::Committed { .. }),
                "{reply:?}"
            );
            return reply;
        }
    }
    panic!(
        "proposal failed to confirm (survivors={survivors}): {:?}; jobs {:?}",
        actors.iter().map(Scheduled::status).collect::<Vec<_>>(),
        controller.jobs()
    );
}

#[test]
fn coordinator_join_pending_plan_and_source_activation_survive_both_quorum_failovers() {
    for phase in [Phase::Join, Phase::Plan, Phase::Assign] {
        failover(phase);
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "keep the exact missing-receipt and election schedule together"
)]
fn failover(phase: Phase) {
    let (mut controller, io) = setup();
    let mut now = Duration::ZERO;
    let mut actors = Vec::new();
    let mut lanes = Vec::<ProposalSubmitter>::new();
    let mut buffers = Vec::new();
    let mut bootstrap = None;
    for broker in 0..3 {
        let mut actor = unstarted_actor(&mut controller, &io, 0, broker, 64);
        let mut lane = actor.take_submitter().unwrap();
        if broker == 0 {
            let mut buffer = actor.lease_proposal_buffer().unwrap();
            seed(&mut buffer, phase);
            bootstrap = Some(lane.try_submit(buffer).unwrap());
        }
        buffers.push(Some(actor.lease_proposal_buffer().unwrap()));
        lanes.push(lane);
        actors.push(Scheduled::new(actor).unwrap());
    }
    drop(finish(
        &mut controller,
        &mut actors,
        bootstrap.unwrap(),
        false,
        &mut now,
    ));
    let before = image(&mut controller, &mut actors[0], now);
    let mut buffer = buffers[0].take().unwrap();
    push(&mut buffer, &plan(before.as_deref(), 1));
    if matches!(phase, Phase::Assign) {
        buffer = finish(
            &mut controller,
            &mut actors,
            lanes[0].try_submit(buffer).unwrap(),
            false,
            &mut now,
        )
        .buffer;
        buffer.clear();
        push(&mut buffer, &assignment(2, 2, 1, 2));
    }
    let prior = image(&mut controller, &mut actors[0], now);
    let prior_prefix = actors[0].status().normal.unwrap().applied;
    let mut pending = Box::pin(lanes[0].try_submit(buffer).unwrap());
    let mut copied = false;
    for _ in 0..10000 {
        for (to, message) in collect(&mut actors, now, false) {
            let from = usize::from(message.part_slice(0).unwrap()[0] - 1);
            // One surviving copy gets the proposal. No follower receipt reaches
            // the leader.
            if to == 0 || (to == 2 && opcode(&message) == ozzy_proto::Opcode::PrepareFlow) {
                continue;
            }
            actors[to].receive(&message, now).unwrap();
            assert_ne!(from, to);
        }
        settle(&mut controller, &[]);
        assert!(
            poll(pending.as_mut()).is_pending(),
            "unacknowledged proposal confirmed"
        );
        assert_eq!(image(&mut controller, &mut actors[0], now), prior);
        if actors[1]
            .status()
            .normal
            .is_some_and(|normal| normal.accepted.op > prior_prefix.op)
        {
            copied = true;
            break;
        }
    }
    assert!(copied, "{phase:?}: proposal did not reach surviving copy");
    let mut leader = None;
    now += Duration::from_secs(3);
    for step in 0..20000 {
        if step % 16 == 15 {
            now += Duration::from_millis(1);
        }
        round_survivors(&mut actors, now);
        settle(&mut controller, &[]);
        leader = (1..3).find(|&index| {
            let status = actors[index].status();
            status.application_ready
                && status.scope.view > 0
                && status
                    .normal
                    .is_some_and(|normal| normal.applied.op > prior_prefix.op)
                && actors[index].authority_hint().primary
                    == NodeId::from_bytes([index as u8 + 1; 16])
        });
        if leader.is_some() {
            break;
        }
    }
    let leader = leader.expect("surviving quorum must select the available proposal suffix");
    let recovered = image(&mut controller, &mut actors[leader], now).unwrap();
    assert_eq!(recovered.members, [member(1)]);
    assert_eq!(
        recovered.revision,
        if matches!(phase, Phase::Join) { 1 } else { 2 }
    );
    let mut buffer = buffers[leader].take().unwrap();
    if !matches!(phase, Phase::Assign) {
        let epoch = if matches!(phase, Phase::Join) { 1 } else { 2 };
        push(
            &mut buffer,
            &assignment(
                epoch,
                recovered.revision,
                1,
                if matches!(phase, Phase::Join) { 0 } else { 2 },
            ),
        );
        buffer = finish(
            &mut controller,
            &mut actors,
            lanes[leader].try_submit(buffer).unwrap(),
            true,
            &mut now,
        )
        .buffer;
        buffer.clear();
    }
    let scope = actors[leader].status().scope;
    buffer
        .prepare_group_progress(
            GroupProgressRequest {
                authority: Authority {
                    group_id: scope.group_id,
                    config_epoch: scope.configuration_epoch,
                    view: scope.view,
                },
                assignment: GroupAssignment {
                    group: group(),
                    partition: partition(),
                    member: member(1).id,
                    epoch: if matches!(phase, Phase::Join) { 1 } else { 2 },
                },
                cursor: None,
                operation: OperationId::from_bytes([99; 16]),
            },
            ozzy_proto::append::Policy::QuorumDurable,
        )
        .unwrap();
    let reply = finish(
        &mut controller,
        &mut actors,
        lanes[leader].try_submit(buffer).unwrap(),
        true,
        &mut now,
    );
    assert_eq!(
        reply.buffer.group_progress_state().unwrap().cursor,
        Offset::new(if matches!(phase, Phase::Join) { 0 } else { 2 })
    );
    drop(pending);
    for actor in actors {
        close(&mut controller, actor);
    }
}

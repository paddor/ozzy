use super::*;
use crate::replica_actor::{LocalActor, ProposalOutcome, ProposalSubmitter};
use ozzy_journal::operation::{
    Assign, ProducerResultFloor, ProgressAssignment, ProgressOwner, Trim,
};
use ozzy_proto::{ConsumerGroupId, ConsumerMemberId, progress as wire};

mod activation;

#[derive(Clone, Copy)]
pub(super) enum History {
    Available,
    Expired { cursor: Offset },
}

fn assignment() -> wire::Assignment {
    wire::Assignment {
        group: ConsumerGroupId::from_bytes([61; 16]),
        partition: partition(),
        member: ConsumerMemberId::from_bytes([62; 16]),
        epoch: 1,
    }
}

fn consumer(peer: u8, session: u8) -> Link {
    Link {
        remote: handshake::Parameters::processing(limits(), handshake::CONSUMER).unwrap(),
        ..link(peer, session)
    }
}

fn submit(
    controller: &mut Controller,
    actor: &mut LocalActor,
    submitter: &mut ProposalSubmitter,
    buffer: ProposalBuffer,
) {
    let mut pending = Box::pin(submitter.try_submit(buffer).unwrap());
    let reply = drive(
        controller,
        std::future::poll_fn(|cx| {
            assert!(actor.poll_progress(cx).is_pending());
            pending.as_mut().poll(cx)
        }),
    )
    .unwrap();
    assert!(
        matches!(reply.outcome, ProposalOutcome::Committed { .. }),
        "{:?}",
        reply.outcome
    );
    // Durable evidence trails the commit. Let it finish, so steps that later
    // hold I/O start from a quiet partition.
    drive(
        controller,
        std::future::poll_fn(|cx| {
            assert!(actor.poll_progress(cx).is_pending());
            if actor.storage_idle() {
                std::task::Poll::Ready(())
            } else {
                std::task::Poll::Pending
            }
        }),
    );
}

pub(super) fn fixture(
    controller: &mut Controller,
    io: Local,
    history: History,
) -> (LocalActor, NativeIntake) {
    fixture_with(controller, io, history, &[])
}

/// Also assign the partition to each of `groups`.
#[expect(
    clippy::too_many_lines,
    reason = "one fixture history with optional groups and expiry"
)]
fn fixture_with(
    controller: &mut Controller,
    io: Local,
    history: History,
    groups: &[wire::Assignment],
) -> (LocalActor, NativeIntake) {
    let mut actor = actor(controller, io, 1);
    let mut submitter = actor.take_submitter().unwrap();
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    buffer.prepare_append(request(12, 0, 3)).unwrap();
    submit(controller, &mut actor, &mut submitter, buffer);
    let current = assignment();
    let mut values = vec![OperationBody::Assign(Assign {
        consumer_group_id: current.group,
        partition: current.partition,
        expected_assignment_epoch: 0,
        new_assignment_epoch: current.epoch,
        plan_revision: 1,
        resume_cursor: Offset::ZERO,
        new_member: Some(current.member),
        operation_id: OperationId::from_bytes([63; 16]),
    })];
    for (index, group) in groups.iter().enumerate() {
        values.push(OperationBody::Assign(Assign {
            consumer_group_id: group.group,
            partition: group.partition,
            expected_assignment_epoch: 0,
            new_assignment_epoch: group.epoch,
            plan_revision: 1,
            resume_cursor: Offset::ZERO,
            new_member: Some(group.member),
            operation_id: OperationId::from_bytes([100 + index as u8; 16]),
        }));
    }
    if let History::Expired { cursor } = history {
        if cursor != Offset::ZERO {
            values.push(OperationBody::Progress(ozzy_journal::operation::Progress {
                owner: ProgressOwner::ConsumerGroup(current.group),
                partition: current.partition,
                assignment: Some(ProgressAssignment {
                    epoch: current.epoch,
                    member: current.member,
                }),
                expected_progress: Some(Offset::ZERO),
                new_progress: cursor,
                operation_id: OperationId::from_bytes([64; 16]),
            }));
        }
        values.extend([
            OperationBody::ProducerResultFloor(ProducerResultFloor {
                partition: current.partition,
                producer_id: ProducerId::from_bytes([12; 16]),
                producer_epoch: ProducerEpoch::new(1),
                expected_floor: ozzy_proto::ProducerSequence::ZERO,
                new_floor: ozzy_proto::ProducerSequence::new(3),
                operation_id: OperationId::from_bytes([66; 16]),
            }),
            OperationBody::Trim(Trim {
                partition: current.partition,
                expected_floor: Offset::ZERO,
                new_floor: Offset::new(3),
                operation_id: OperationId::from_bytes([65; 16]),
            }),
        ]);
    }
    for value in values {
        let mut buffer = actor.lease_proposal_buffer().unwrap();
        buffer
            .push(
                value.kind(),
                &encode_operation_body(&value, ozzy_journal::operation::OperationLimits::default())
                    .unwrap(),
            )
            .unwrap();
        submit(controller, &mut actor, &mut submitter, buffer);
    }
    let config = NativeIntakeConfig {
        local: actor.authority_hint().primary,
        group: actor.group(),
        partition: partition(),
        policy: Policy::LocalDurable,
        access: NativeAccess::Writers(vec![
            ClientAccess {
                node: link(70, 80).binding.peer,
                producer: ProducerId::from_bytes([40; 16]),
            },
            ClientAccess {
                node: link(71, 81).binding.peer,
                producer: ProducerId::from_bytes([30; 16]),
            },
        ]),
        limits: limits(),
        reserve_dedup_results: false,
        requests_per_writer: 1,
        progress_requests: 3,
        progress_batch: 2,
        turn_slots: 2,
    };
    assert_eq!(config.required_buffers(), Some(7));
    let control = config.buffer_limits(4).unwrap();
    assert_eq!(
        (control.max_operations, control.max_body_bytes),
        (2, 2 * 98)
    );
    assert!(config.buffer_limits(7).is_none());
    let buffers = (0..config.required_buffers().unwrap())
        .map(|index| {
            actor
                .lease_proposal_buffer_with_limits(config.buffer_limits(index).unwrap())
                .unwrap()
        })
        .collect();
    let intake = NativeIntake::new(config, submitter, buffers).unwrap();
    (actor, intake)
}

fn message(authority: Authority, link: Link, cursor: Option<u64>, id: u8) -> Message {
    with_request(
        wire::Request {
            authority,
            assignment: assignment(),
            policy: Policy::LocalDurable,
            cursor: cursor.map(Offset::new),
        },
        link,
        id,
    )
}

fn with_request(request: wire::Request, link: Link, id: u8) -> Message {
    let mut metadata = Vec::with_capacity(wire::REQUEST_BYTES);
    let header = wire::encode_request(
        envelope(Opcode::ProgressCommit, link, id),
        request,
        &mut metadata,
        limits().envelope,
    )
    .unwrap();
    crate::native_frames::message(
        link.binding.peer.as_bytes(),
        header,
        &metadata,
        Bytes::new(),
    )
}

#[test]
fn native_progress_waits_for_durability_and_queries_bypass_held_updates() {
    let (mut controller, io) = setup();
    let (mut actor, mut intake) = fixture(&mut controller, io, History::Available);
    let hint = actor.authority_hint();
    let binding = consumer(70, 80);
    let before = actor.snapshot().accepted;
    let mut output = Vec::new();
    for (cursor, id) in [(Some(2), 90), (Some(1), 91), (None, 92)] {
        assert_eq!(
            intake
                .receive(&message(hint.authority, binding, cursor, id), binding, hint)
                .unwrap(),
            NativeReceive::Accepted
        );
        for _ in 0..100 {
            progress(&mut actor, &mut intake, None, &mut output, false);
        }
        if cursor.is_some() {
            assert!(output.is_empty(), "processing is not yet durable");
        }
    }
    assert_eq!(output.len(), 1, "query must continue past pending writes");
    let query = output.pop().unwrap();
    assert_eq!(
        packet(&query).envelope.request_id,
        Some(RequestId::from_bytes([92; 16]))
    );
    assert_eq!(
        wire::decode_confirmed(packet(&query), limits().envelope)
            .unwrap()
            .cursor,
        Offset::ZERO
    );
    assert_eq!(actor.snapshot().accepted.op.0, before.op.0 + 1);
    assert!(intake.has_work());
    assert_eq!(intake.client_work(binding.binding.peer), 2);
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    assert_eq!(output.len(), 2);
    for reply in &output {
        let confirmed = wire::decode_confirmed(packet(reply), limits().envelope).unwrap();
        assert_eq!(confirmed.assignment, assignment());
        assert_eq!(
            (confirmed.cursor, confirmed.end),
            (Offset::new(2), Offset::new(3))
        );
    }
    assert_eq!(intake.client_work(binding.binding.peer), 0);
    let confirmed_prefix = actor.snapshot().accepted;
    output.clear();
    intake
        .receive(
            &message(hint.authority, binding, Some(1), 93),
            binding,
            hint,
        )
        .unwrap();
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    assert_eq!(
        wire::decode_confirmed(packet(&output[0]), limits().envelope)
            .unwrap()
            .cursor,
        Offset::new(2)
    );
    assert_eq!(
        actor.snapshot().accepted,
        confirmed_prefix,
        "covered retries do not append"
    );
    drop(intake);
    close(&mut controller, actor);
}

fn second_group() -> wire::Assignment {
    wire::Assignment {
        group: ConsumerGroupId::from_bytes([67; 16]),
        member: ConsumerMemberId::from_bytes([68; 16]),
        ..assignment()
    }
}

/// Receive `updates` while the first one's proposal runs, then settle and
/// return each reply by request ID.
fn batch(
    controller: &mut Controller,
    actor: &mut LocalActor,
    intake: &mut NativeIntake,
    updates: &[(wire::Assignment, u64, Link, u8)],
    riders: usize,
) -> Vec<(u8, Message)> {
    let hint = actor.authority_hint();
    for &(assignment, cursor, link, id) in updates {
        let request = wire::Request {
            authority: hint.authority,
            assignment,
            policy: Policy::LocalDurable,
            cursor: Some(Offset::new(cursor)),
        };
        assert_eq!(
            intake
                .receive(&with_request(request, link, id), link, hint)
                .unwrap(),
            NativeReceive::Accepted
        );
    }
    let mut output = Vec::new();
    for _ in 0..10000 {
        progress(actor, intake, None, &mut output, false);
        if intake.progress_riders() == riders {
            break;
        }
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    }
    assert_eq!(
        intake.progress_riders(),
        riders,
        "updates held meanwhile share a proposal"
    );
    settle(controller, actor, intake, &mut output);
    let mut replies = output
        .into_iter()
        .map(|reply| {
            let id = packet(&reply).envelope.request_id.unwrap().as_bytes()[0];
            (id, reply)
        })
        .collect::<Vec<_>>();
    replies.sort_by_key(|(id, _)| *id);
    replies
}

#[test]
fn progress_received_during_a_proposal_shares_the_next_one() {
    let (mut controller, io) = setup();
    let second = second_group();
    let (mut actor, mut intake) = fixture_with(&mut controller, io, History::Available, &[second]);
    let before = actor.snapshot().accepted;
    let (one, two) = (consumer(70, 80), consumer(71, 81));
    let replies = batch(
        &mut controller,
        &mut actor,
        &mut intake,
        &[
            (assignment(), 1, one, 90),
            (second, 2, two, 91),
            (assignment(), 3, one, 92),
        ],
        1,
    );
    let confirmed = replies
        .iter()
        .map(|(id, reply)| {
            let confirmed = wire::decode_confirmed(packet(reply), limits().envelope).unwrap();
            (*id, confirmed.assignment, confirmed.cursor)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        confirmed,
        [
            (90, assignment(), Offset::new(1)),
            (91, second, Offset::new(2)),
            (92, assignment(), Offset::new(3)),
        ]
    );
    assert_eq!(actor.snapshot().accepted.op.0, before.op.0 + 3);
    drop(intake);
    close(&mut controller, actor);
}

#[test]
fn a_repeated_cursor_rides_without_an_operation() {
    let (mut controller, io) = setup();
    let second = second_group();
    let (mut actor, mut intake) = fixture_with(&mut controller, io, History::Available, &[second]);
    let before = actor.snapshot().accepted;
    let (one, two) = (consumer(70, 80), consumer(71, 81));
    let replies = batch(
        &mut controller,
        &mut actor,
        &mut intake,
        &[
            (assignment(), 2, one, 90),
            (second, 2, two, 91),
            (assignment(), 1, one, 92),
        ],
        1,
    );
    let cursors = replies
        .iter()
        .map(|(id, reply)| {
            let confirmed = wire::decode_confirmed(packet(reply), limits().envelope).unwrap();
            (*id, confirmed.cursor)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        cursors,
        [
            (90, Offset::new(2)),
            (91, Offset::new(2)),
            (92, Offset::new(2))
        ]
    );
    assert_eq!(
        intake.progress_proposals(),
        2,
        "the repeat rode without a split"
    );
    assert_eq!(actor.snapshot().accepted.op.0, before.op.0 + 2);
    drop(intake);
    close(&mut controller, actor);
}

#[test]
fn a_fenced_update_splits_its_batch_without_failing_the_others() {
    let (mut controller, io) = setup();
    let second = second_group();
    let (mut actor, mut intake) = fixture_with(&mut controller, io, History::Available, &[second]);
    let before = actor.snapshot().accepted;
    let (one, two) = (consumer(70, 80), consumer(71, 81));
    let fenced = wire::Assignment {
        member: ConsumerMemberId::from_bytes([99; 16]),
        ..assignment()
    };
    let replies = batch(
        &mut controller,
        &mut actor,
        &mut intake,
        &[
            (assignment(), 1, one, 90),
            (second, 2, two, 91),
            (fenced, 2, one, 92),
        ],
        1,
    );
    assert_eq!(replies.len(), 3);
    for (id, cursor) in [(90, 1), (91, 2)] {
        let reply = &replies.iter().find(|(reply, _)| *reply == id).unwrap().1;
        assert_eq!(
            wire::decode_confirmed(packet(reply), limits().envelope)
                .unwrap()
                .cursor,
            Offset::new(cursor)
        );
    }
    let rejected = ozzy_proto::nack::decode(packet(&replies[2].1), limits().envelope).unwrap();
    assert_eq!(rejected.code, 6);
    assert_eq!(actor.snapshot().accepted.op.0, before.op.0 + 2);
    drop(intake);
    close(&mut controller, actor);
}

#[test]
fn full_progress_slots_leave_producer_control_and_another_destination_running() {
    let (mut controller, io) = setup();
    let (mut actor, mut intake) = fixture(&mut controller, io, History::Available);
    let hint = actor.authority_hint();
    let binding = consumer(70, 80);
    for id in 90..93 {
        assert_eq!(
            intake
                .receive(&message(hint.authority, binding, None, id), binding, hint)
                .unwrap(),
            NativeReceive::Accepted
        );
    }
    let mut output = Vec::new();
    for _ in 0..100 {
        progress(&mut actor, &mut intake, None, &mut output, true);
    }
    assert_eq!(intake.client_work(binding.binding.peer), 3);
    assert_eq!(
        intake
            .receive(&message(hint.authority, binding, None, 93), binding, hint)
            .unwrap(),
        NativeReceive::Busy
    );
    let other = link(71, 81);
    let mut metadata = Vec::with_capacity(256);
    let header = producer::encode_open(
        envelope(Opcode::OpenProducer, other, 94),
        producer::Open {
            authority: hint.authority,
            partition: partition(),
            producer: ProducerId::from_bytes([30; 16]),
            mode: Mode::Resume,
            expected_epoch: None,
            operation: OperationId::from_bytes([94; 16]),
        },
        &mut metadata,
        limits().envelope,
    )
    .unwrap();
    let request = crate::native_frames::message(
        other.binding.peer.as_bytes(),
        header,
        &metadata,
        Bytes::new(),
    );
    assert_eq!(
        intake.receive(&request, other, hint).unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..100 {
        progress(&mut actor, &mut intake, None, &mut output, true);
    }
    assert_eq!(output.len(), 1);
    assert_eq!(packet(&output[0]).envelope.opcode, Opcode::ProducerOpened);
    assert_eq!(intake.client_work(binding.binding.peer), 3);
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    assert_eq!(output.len(), 4);
    drop(intake);
    close(&mut controller, actor);
}

#[test]
fn native_progress_rejects_fenced_members_overruns_roles_policy_and_authority() {
    let (mut controller, io) = setup();
    let (mut actor, mut intake) = fixture(&mut controller, io, History::Available);
    let hint = actor.authority_hint();
    let binding = consumer(70, 80);
    let request = wire::Request {
        authority: hint.authority,
        assignment: assignment(),
        policy: Policy::LocalDurable,
        cursor: Some(Offset::new(2)),
    };
    let mut foreign_member = request;
    foreign_member.assignment.member = ConsumerMemberId::from_bytes([99; 16]);
    let mut foreign_epoch = request;
    foreign_epoch.assignment.epoch += 1;
    let mut stale = request;
    stale.authority.view += 1;
    let before = actor.snapshot().accepted;
    for (request, source, code) in [
        (foreign_member, binding, 6),
        (foreign_epoch, binding, 6),
        (
            wire::Request {
                cursor: Some(Offset::new(4)),
                ..request
            },
            binding,
            7,
        ),
        (
            wire::Request {
                policy: Policy::QuorumDurable,
                ..request
            },
            binding,
            2,
        ),
        (request, link(70, 80), 3),
        (stale, binding, 5),
    ] {
        let mut output = Vec::new();
        assert_eq!(
            intake
                .receive(&with_request(request, source, 90), source, hint)
                .unwrap(),
            NativeReceive::Accepted
        );
        settle(&mut controller, &mut actor, &mut intake, &mut output);
        assert_eq!(output.len(), 1);
        let rejected = ozzy_proto::nack::decode(packet(&output[0]), limits().envelope).unwrap();
        assert_eq!(rejected.code, code);
        assert_eq!(actor.snapshot().accepted, before);
    }
    drop(intake);
    close(&mut controller, actor);
}

#[test]
fn native_progress_retention_error_preserves_cursor_and_floor() {
    let (mut controller, io) = setup();
    let (mut actor, mut intake) = fixture(
        &mut controller,
        io,
        History::Expired {
            cursor: Offset::new(1),
        },
    );
    let hint = actor.authority_hint();
    let binding = consumer(70, 80);
    let before = actor.snapshot().accepted;
    let mut output = Vec::new();
    assert_eq!(
        intake
            .receive(&message(hint.authority, binding, None, 90), binding, hint)
            .unwrap(),
        NativeReceive::Accepted
    );
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    let rejected = ozzy_proto::nack::decode(packet(&output[0]), limits().envelope).unwrap();
    assert_eq!(rejected.code, 9);
    assert_eq!(
        wire::HistoryExpired::decode(rejected.detail).unwrap(),
        wire::HistoryExpired {
            cursor: Offset::new(1),
            retained_from: Offset::new(3)
        }
    );
    assert_eq!(actor.snapshot().accepted, before);
    output.clear();
    assert_eq!(
        intake
            .receive(
                &message(hint.authority, binding, Some(1), 91),
                binding,
                hint
            )
            .unwrap(),
        NativeReceive::Accepted
    );
    settle(&mut controller, &mut actor, &mut intake, &mut output);
    let confirmed = wire::decode_confirmed(packet(&output[0]), limits().envelope).unwrap();
    assert_eq!(
        (confirmed.cursor, confirmed.retained_from),
        (Offset::new(1), Offset::new(3))
    );
    assert_eq!(actor.snapshot().accepted, before);
    drop(intake);
    close(&mut controller, actor);
}

#[test]
fn native_progress_drops_replaced_session_replies_without_losing_committed_progress() {
    let (mut controller, io) = setup();
    let (mut actor, mut intake) = fixture(&mut controller, io, History::Available);
    let hint = actor.authority_hint();
    let binding = consumer(70, 80);
    let mut output = Vec::new();
    assert_eq!(
        intake
            .receive(
                &message(hint.authority, binding, Some(2), 90),
                binding,
                hint
            )
            .unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..100 {
        progress(&mut actor, &mut intake, None, &mut output, true);
    }
    assert!(actor.snapshot().accepted.op > actor.snapshot().applied.op);
    for _ in 0..1000 {
        progress(
            &mut actor,
            &mut intake,
            Some(consumer(70, 90)),
            &mut output,
            false,
        );
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    }
    assert_eq!(output, [] as [Message; 0]);
    assert!(!intake.has_work());
    let new = consumer(70, 90);
    assert_eq!(
        intake
            .receive(&message(hint.authority, binding, None, 91), new, hint)
            .unwrap(),
        NativeReceive::Ignored
    );
    assert_eq!(
        intake
            .receive(&message(hint.authority, new, None, 92), new, hint)
            .unwrap(),
        NativeReceive::Accepted
    );
    for _ in 0..100 {
        progress(&mut actor, &mut intake, Some(new), &mut output, false);
    }
    assert_eq!(output.len(), 1);
    assert_eq!(
        packet(&output[0]).envelope.session,
        Some(new.binding.session)
    );
    assert_eq!(
        wire::decode_confirmed(packet(&output[0]), limits().envelope)
            .unwrap()
            .cursor,
        Offset::new(2)
    );
    drop(intake);
    close(&mut controller, actor);
}

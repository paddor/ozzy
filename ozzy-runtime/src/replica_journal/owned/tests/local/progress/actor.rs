use super::super::actor::{actor, close, pump};
use super::*;
use crate::replica_actor::{PendingProposal, ProposalOutcome, ProposalReply};

fn finish_reply(
    controller: &mut Controller,
    actor: &mut crate::replica_actor::LocalActor,
    mut pending: Pin<Box<PendingProposal>>,
) -> ProposalReply {
    drive(
        controller,
        std::future::poll_fn(|cx| {
            assert!(actor.poll_progress(cx).is_pending());
            pending.as_mut().poll(cx)
        }),
    )
    .unwrap()
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
fn local_actor_group_progress_waits_for_durability_while_queries_continue() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut submitter = actor.take_submitter().unwrap();
    let mut data = actor.lease_proposal_buffer().unwrap();
    data.prepare_append(request(12, 0, 1)).unwrap();
    let reply = finish_reply(
        &mut controller,
        &mut actor,
        Box::pin(submitter.try_submit(data).unwrap()),
    );
    assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }));
    drop(reply);
    let assignment = assignment();
    let value = OperationBody::Assign(Assign {
        consumer_group_id: assignment.group,
        partition: assignment.partition,
        expected_assignment_epoch: 0,
        new_assignment_epoch: assignment.epoch,
        plan_revision: 1,
        resume_cursor: Offset::ZERO,
        new_member: Some(assignment.member),
        operation_id: OperationId::from_bytes([90; 16]),
    });
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    buffer
        .push(
            value.kind(),
            &encode_operation_body(&value, ozzy_journal::operation::OperationLimits::default())
                .unwrap(),
        )
        .unwrap();
    let reply = finish_reply(
        &mut controller,
        &mut actor,
        Box::pin(submitter.try_submit(buffer).unwrap()),
    );
    assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }));
    drop(reply);
    let request = GroupProgressRequest {
        authority: actor.authority_hint().authority,
        assignment,
        cursor: Some(Offset::new(1)),
        operation: OperationId::from_bytes([91; 16]),
    };
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    buffer
        .prepare_group_progress(request, Policy::LocalDurable)
        .unwrap();
    let mut pending = Box::pin(submitter.try_submit(buffer).unwrap());
    for _ in 0..100 {
        pump(&mut actor);
        if actor.snapshot().accepted.op > actor.snapshot().applied.op {
            break;
        }
    }
    assert!(actor.snapshot().accepted.op > actor.snapshot().applied.op);
    assert!(poll(pending.as_mut()).is_pending());
    let held: Vec<_> = controller.jobs().into_iter().map(|(id, _)| id).collect();
    assert_ne!(held, [] as [JobId; 0]);
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    buffer
        .prepare_group_progress(
            GroupProgressRequest {
                cursor: None,
                ..request
            },
            Policy::LocalDurable,
        )
        .unwrap();
    let mut query = Box::pin(submitter.try_submit(buffer).unwrap());
    let reply = drive_except(
        &mut controller,
        std::future::poll_fn(|cx| {
            assert!(actor.poll_progress(cx).is_pending());
            query.as_mut().poll(cx)
        }),
        Some(held[0]),
        |_| Effect::Normal,
    )
    .unwrap();
    let ProposalOutcome::Committed { through, .. } = reply.outcome else {
        panic!("confirmed query: {:?}", reply.outcome)
    };
    assert_eq!(through, actor.snapshot().applied);
    assert_eq!(
        reply.buffer.group_progress_state().unwrap().cursor,
        Offset::ZERO
    );
    drop(reply);
    assert!(poll(pending.as_mut()).is_pending());
    let reply = finish_reply(&mut controller, &mut actor, pending);
    assert!(
        matches!(reply.outcome, ProposalOutcome::Committed { through, .. } if through == actor.snapshot().applied)
    );
    assert_eq!(
        reply.buffer.group_progress_state().unwrap().cursor,
        Offset::new(1)
    );
    drop(reply);
    drop(submitter);
    close(&mut controller, actor);
}

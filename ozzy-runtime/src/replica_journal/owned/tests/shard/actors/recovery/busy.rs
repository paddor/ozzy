use super::*;
use ozzy_io::Operation;
use ozzy_proto::RequestId;
use ozzy_replication::wire::{self, RecoveryRequest, WireLimits};

#[test]
fn recovery_donor_waits_for_outstanding_sync_and_preserves_confirmation() {
    donor_during_sync(QuorumPolicy::Durable);
}

fn finish_jobs(controller: &mut Controller, hold_sync: bool) {
    for (id, _) in controller.jobs() {
        if hold_sync
            && matches!(
                controller.operation(id).unwrap().unprotected(),
                Operation::Sync { .. }
            )
        {
            continue;
        }
        controller.execute(id, Effect::Normal).unwrap();
        controller.deliver(id).unwrap();
    }
}

fn donor_during_sync(policy: QuorumPolicy) {
    let (mut controller, io) = setup();
    let (mut actors, mut submitter, buffer) = donor_fixture(&mut controller, &io, policy);
    for _ in 0..10000 {
        round(&mut actors, Duration::ZERO);
        finish_jobs(&mut controller, false);
        if actors.iter().all(|actor| actor.status().application_ready) {
            break;
        }
    }
    assert!(actors.iter().all(|actor| actor.status().application_ready));
    let mut pending = Box::pin(submitter.try_submit(buffer).unwrap());
    for _ in 0..10000 {
        round(&mut actors, Duration::ZERO);
        finish_jobs(&mut controller, true);
        if actors[0].status().normal.unwrap().accepted.op.0 == 1 {
            break;
        }
    }
    let now = Duration::from_millis(100);
    let mut held_sync = false;
    for _ in 0..10000 {
        round(&mut actors, now);
        finish_jobs(&mut controller, true);
        held_sync = controller.jobs().iter().any(|(id, _)| {
            matches!(
                controller.operation(*id).unwrap().unprotected(),
                Operation::Sync { .. }
            )
        });
        if held_sync {
            break;
        }
    }
    assert!(
        held_sync,
        "the fixture must hold a real disk sync: {policy:?}, {:?}, jobs={:?}",
        actors.iter().map(Actor::status).collect::<Vec<_>>(),
        controller.jobs()
    );
    if policy == QuorumPolicy::Durable {
        assert!(poll(pending.as_mut()).is_pending());
    }
    let (request, message) = recovery_request(actors[0].status().scope);
    actors[0].receive(&message, now).unwrap();
    for _ in 0..20 {
        round(&mut actors, now);
        finish_jobs(&mut controller, true);
    }
    assert!(actors[0].status().application_ready);
    assert!(poll(pending.as_mut()).is_pending());
    let mut confirmed = false;
    let mut replied = false;
    for _ in 0..10000 {
        round_with_observer(&mut actors, now, None, |message| {
            let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
            let packet =
                ozzy_proto::decode_packet(&frames, ozzy_proto::EnvelopeLimits::default()).unwrap();
            replied |= packet.envelope.opcode == ozzy_proto::Opcode::RecoveryState
                && packet.envelope.request_id == Some(request.request_id);
        });
        finish_jobs(&mut controller, false);
        if !confirmed && let Poll::Ready(reply) = poll(pending.as_mut()) {
            assert!(
                matches!(reply.unwrap().outcome, ProposalOutcome::Committed { through, .. } if through.op.0 == 1)
            );
            confirmed = true;
        }
        if confirmed && replied {
            break;
        }
    }
    assert!(
        confirmed && replied,
        "confirmation={confirmed}, recovery reply={replied}"
    );
    for actor in actors {
        close(&mut controller, actor);
    }
}

fn recovery_request(scope: ozzy_replication::Scope) -> (RecoveryRequest, Message) {
    let request = RecoveryRequest {
        scope,
        request_id: RequestId::from_bytes([42; 16]),
        nonce: RequestId::from_bytes([91; 16]),
    };
    let mut metadata = [0; 1024];
    let encoded = wire::encode_recovery(
        NodeId::from_bytes([3; 16]),
        actor_config().sessions[2],
        request,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    let message = Message::multipart([
        bytes::Bytes::from_static(&[3; 16]),
        bytes::Bytes::copy_from_slice(&encoded.header),
        bytes::Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
        bytes::Bytes::new(),
    ]);
    (request, message)
}

fn donor_fixture(
    controller: &mut Controller,
    io: &Local,
    policy: QuorumPolicy,
) -> (
    Vec<Actor>,
    crate::replica_actor::ProposalSubmitter,
    crate::replica_journal::ProposalBuffer,
) {
    let mut actors = Vec::new();
    let mut leader = unstarted_actor(controller, io, 0, policy, 0, 64);
    let submitter = leader.take_submitter().unwrap();
    let mut buffer = leader.lease_proposal_buffer().unwrap();
    buffer
        .push(ozzy_journal::operation::OperationKind::Barrier, &[31; 16])
        .unwrap();
    actors.push(ControlledReplica::new(leader));
    for broker in 1..3 {
        actors.push(ControlledReplica::new(unstarted_actor(
            controller, io, 0, policy, broker, 64,
        )));
    }
    (actors, submitter, buffer)
}

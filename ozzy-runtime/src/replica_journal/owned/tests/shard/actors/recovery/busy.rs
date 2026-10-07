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
    (
        request,
        recovery_message(request, actor_config().sessions[2]),
    )
}

fn recovery_message(request: RecoveryRequest, session: LinkSessionId) -> Message {
    let mut metadata = [0; 1024];
    let encoded = wire::encode_recovery(
        NodeId::from_bytes([3; 16]),
        session,
        request,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    Message::multipart([
        bytes::Bytes::from_static(&[3; 16]),
        bytes::Bytes::copy_from_slice(&encoded.header),
        bytes::Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
        bytes::Bytes::new(),
    ])
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

#[test]
fn recovery_nonce_replacement_during_pin_disk_quorum() {
    nonce_replacement(QuorumPolicy::Durable, true);
}

#[test]
fn recovery_nonce_replacement_during_pin_replicated_persisting() {
    nonce_replacement(QuorumPolicy::Replicated, true);
}

#[test]
fn recovery_same_nonce_during_pin_preserves_its_snapshot() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        nonce_replacement(policy, false);
    }
}

fn pending_donor(policy: QuorumPolicy) -> (Controller, Vec<Actor>, RecoveryRequest) {
    let (mut controller, io) = setup();
    let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
    let mut pending = Box::pin(pending);
    let mut confirmed = false;
    for _ in 0..10000 {
        round(&mut actors, Duration::ZERO);
        finish_jobs(&mut controller, false);
        if let Poll::Ready(reply) = poll(pending.as_mut()) {
            assert!(matches!(
                reply.unwrap().outcome,
                ProposalOutcome::Committed { .. }
            ));
            confirmed = true;
            break;
        }
    }
    assert!(confirmed);
    let now = Duration::ZERO;
    for _ in 0..1000 {
        round(&mut actors, now);
        finish_jobs(&mut controller, false);
        if controller.jobs().is_empty() && actors.iter().all(|actor| !actor.status().disk_pending) {
            break;
        }
    }
    assert_eq!(controller.jobs().len(), 0);
    assert!(!actors[0].status().disk_pending);
    assert!(
        actors[0].status().application_ready,
        "{:?}",
        actors[0].status()
    );
    assert_eq!(actors[0].status().scope.view, 0);
    let (request, _) = recovery_request(actors[0].status().scope);
    let first = recovery_message(
        RecoveryRequest {
            request_id: RequestId::from_bytes([41; 16]),
            ..request
        },
        actor_config().sessions[2],
    );
    actors[0].receive(&first, now).unwrap();
    actors[0]
        .receive(&recovery_message(request, actor_config().sessions[2]), now)
        .unwrap();
    for _ in 0..1000 {
        actors[0].advance(now).unwrap();
        if actors[0].status().disk_pending {
            break;
        }
        let mut disk = std::pin::pin!(actors[0].complete_disk(now));
        if let Poll::Ready(result) = poll(disk.as_mut()) {
            result.unwrap();
        }
        finish_jobs(&mut controller, false);
    }
    assert!(actors[0].status().disk_pending);
    {
        let mut disk = std::pin::pin!(actors[0].complete_disk(now));
        assert!(poll(disk.as_mut()).is_pending());
    }
    assert!(!controller.jobs().is_empty(), "pin must own physical work");
    (controller, actors, request)
}

fn nonce_replacement(policy: QuorumPolicy, replace_nonce: bool) {
    let (mut controller, mut actors, mut request) = pending_donor(policy);
    let now = Duration::ZERO;
    if replace_nonce {
        request.nonce = RequestId::from_bytes([92; 16]);
    }
    request.request_id = RequestId::from_bytes([43; 16]);
    let second = recovery_message(request, actor_config().sessions[2]);
    actors[0].receive(&second, now).unwrap();
    complete_pin(&mut controller, &mut actors[0], now);
    assert!(actors[0].status().application_ready);
    assert_latest_response(&mut controller, &mut actors[0], policy, request);
    // Serving another nonce requires the worker to release the previous exact
    // source. Merely swallowing the obsolete completion cannot pass this.
    request.nonce = RequestId::from_bytes([93; 16]);
    request.request_id = RequestId::from_bytes([44; 16]);
    actors[0]
        .receive(&recovery_message(request, actor_config().sessions[2]), now)
        .unwrap();
    assert_latest_response(&mut controller, &mut actors[0], policy, request);
    for actor in actors {
        close(&mut controller, actor);
    }
}

fn complete_pin(controller: &mut Controller, actor: &mut Actor, now: Duration) {
    let mut completed = false;
    for _ in 0..1000 {
        finish_jobs(controller, false);
        let mut disk = std::pin::pin!(actor.complete_disk(now));
        if let Poll::Ready(result) = poll(disk.as_mut()) {
            result.expect("a superseded recovery pin must not stop a healthy donor");
            completed = true;
            break;
        }
    }
    assert!(completed, "pin completion must be observed");
}

#[test]
fn recovery_duplicates_before_pin_admission_use_the_latest_correlation() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, mut actors, request) = pending_donor(policy);
        complete_pin(&mut controller, &mut actors[0], Duration::ZERO);
        assert_latest_response(&mut controller, &mut actors[0], policy, request);
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

fn assert_latest_response(
    controller: &mut Controller,
    actor: &mut Actor,
    policy: QuorumPolicy,
    request: RecoveryRequest,
) {
    let binding = wire::PeerBinding::new(
        config("/unused", 4, policy).configuration.configuration(),
        NodeId::from_bytes([1; 16]),
        actor_config().sessions[2],
    )
    .unwrap();
    for _ in 0..1000 {
        actor.advance(Duration::ZERO).unwrap();
        {
            let mut disk = std::pin::pin!(actor.complete_disk(Duration::ZERO));
            if let Poll::Ready(result) = poll(disk.as_mut()) {
                result.unwrap();
            }
        }
        finish_jobs(controller, false);
        let mut replied = false;
        actor
            .flush(|message| {
                if message.len() == 4 {
                    let frames =
                        std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
                    if let wire::ReplicaMessage::Recovery(wire::RecoveryMessage::State(state)) =
                        wire::decode(&frames, binding, WireLimits::default()).unwrap()
                    {
                        assert_eq!(state.request_id, request.request_id);
                        assert_eq!(state.response.nonce, request.nonce);
                        assert_eq!(state.response.scope, request.scope);
                        assert_eq!(state.response.primary.unwrap().accepted.op.0, 1);
                        replied = true;
                    }
                }
                Ok(())
            })
            .unwrap();
        if replied {
            return;
        }
    }
    panic!("latest recovery request never received a pinned response");
}

#[test]
fn recovery_pin_completion_after_view_change_releases_the_old_source() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, mut actors, _) = pending_donor(policy);
        let now = Duration::from_secs(30);
        for _ in 0..1000 {
            let mut messages = Vec::new();
            for (from, actor) in actors.iter_mut().enumerate().skip(1) {
                actor.advance(now).unwrap();
                {
                    let mut disk = std::pin::pin!(actor.complete_disk(now));
                    if let Poll::Ready(result) = poll(disk.as_mut()) {
                        result.unwrap();
                    }
                }
                actor
                    .flush(|message| {
                        messages.push(replace_sender(message, from));
                        Ok(())
                    })
                    .unwrap();
            }
            for (to, message) in messages {
                actors[to].receive(&message, now).unwrap();
            }
            finish_jobs(&mut controller, false);
            if actors[0].status().scope.view > 0 {
                break;
            }
        }
        actors[0].advance(now).unwrap();
        assert!(actors[0].status().scope.view > 0);
        complete_pin(&mut controller, &mut actors[0], now);
        for step in 0..20000 {
            round(&mut actors, now + Duration::from_millis(step / 100));
            finish_jobs(&mut controller, false);
            if actors.iter().all(|actor| actor.status().application_ready) {
                break;
            }
        }
        assert!(
            actors.iter().all(|actor| actor.status().application_ready),
            "view installation stalled: {:?}",
            actors.iter().map(Actor::status).collect::<Vec<_>>()
        );
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

#[test]
fn recovery_shutdown_with_an_unobserved_pin_drains_physical_work() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, actors, _) = pending_donor(policy);
        for actor in actors {
            close(&mut controller, actor);
        }
        assert_eq!(controller.jobs().len(), 0);
    }
}

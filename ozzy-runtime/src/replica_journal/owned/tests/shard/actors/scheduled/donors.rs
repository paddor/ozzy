//! Physical donor work outlives transport sessions, but its request does not.

use super::*;
use ozzy_replication::wire::{self, RecoveryRequest, WireLimits};

#[test]
fn recovery_freezes_an_unsent_candidate_after_intervening_commit() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut actors = (0..3)
            .map(|broker| {
                Scheduled::new(unstarted_actor(&mut controller, &io, 0, policy, broker, 64))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        for _ in 0..100 {
            round(&mut actors, false);
            settle(&mut controller, &[]);
        }
        assert!(actors.iter().all(|actor| actor.status().application_ready));
        let mut buffer = actors[0]
            .lease_proposal_buffer_with_limits(actor_config().pipeline)
            .unwrap();
        buffer
            .push(ozzy_journal::operation::OperationKind::Barrier, &[31; 16])
            .unwrap();
        let mut pending = Box::pin(
            actors[0]
                .take_submitter()
                .unwrap()
                .try_submit(buffer)
                .unwrap(),
        );
        let mut held = Vec::new();
        for _ in 0..10000 {
            for (to, message) in collect(&mut actors, Duration::ZERO, false) {
                if to == 0 {
                    held.push(message);
                } else {
                    actors[to].receive(&message, Duration::ZERO).unwrap();
                }
            }
            settle(&mut controller, &[]);
            if actors
                .iter()
                .all(|actor| actor.status().normal.unwrap().journal.written.0 == 1)
                && !held.is_empty()
            {
                break;
            }
        }
        assert_eq!(
            actors[0].status().normal.unwrap().committed,
            Prefix::GENESIS
        );
        assert!(
            actors
                .iter()
                .all(|actor| actor.status().normal.unwrap().journal.written.0 == 1)
        );
        let now = Duration::from_millis(100);
        for _ in 0..32 {
            for (to, message) in collect(&mut actors, now, false) {
                if to == 0 {
                    held.push(message);
                } else {
                    actors[to].receive(&message, now).unwrap();
                }
            }
            settle(&mut controller, &[]);
        }
        let request = RecoveryRequest {
            scope: actors[0].status().scope,
            request_id: ozzy_proto::RequestId::from_bytes([95; 16]),
            nonce: ozzy_proto::RequestId::from_bytes([96; 16]),
        };
        actors[0]
            .receive(&request_message(request, actor_config().sessions[2]), now)
            .unwrap();
        for message in held {
            actors[0].receive(&message, now).unwrap();
        }
        assert_eq!(actors[0].status().normal.unwrap().committed.op.0, 1);
        let response = expect_response(
            &mut controller,
            &mut actors[0],
            policy,
            request,
            actor_config().sessions[2],
            now,
        );
        assert_eq!(
            response.primary.unwrap().committed.op.0,
            1,
            "unpublished donor candidate retained its obsolete commit floor"
        );
        assert!(matches!(poll(pending.as_mut()), Poll::Ready(Ok(_))));
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

#[test]
fn disconnected_and_replaced_recovery_requesters_release_pending_pins() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        for disconnect in [false, true] {
            replace_while_pinning(policy, disconnect);
        }
    }
}

fn replace_while_pinning(policy: QuorumPolicy, disconnect: bool) {
    let (mut controller, io) = setup();
    let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
    let mut pending = Box::pin(pending);
    let mut confirmed = false;
    for _ in 0..10000 {
        round(&mut actors, false);
        settle(&mut controller, &[]);
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
    for _ in 0..100 {
        round(&mut actors, false);
        settle(&mut controller, &[]);
    }
    assert_eq!(controller.jobs().len(), 0);
    let mut request = RecoveryRequest {
        scope: actors[0].status().scope,
        request_id: ozzy_proto::RequestId::from_bytes([42; 16]),
        nonce: ozzy_proto::RequestId::from_bytes([91; 16]),
    };
    let old = actor_config().sessions[2];
    let new = LinkSessionId::from_bytes([62; 16]);
    let stale = request_message(request, old);
    actors[0].receive(&stale, Duration::ZERO).unwrap();
    for _ in 0..100 {
        actors[0].advance(Duration::ZERO).unwrap();
        if actors[0].status().disk_pending {
            break;
        }
        observe(&mut actors[0]);
        settle(&mut controller, &[]);
    }
    assert!(actors[0].status().disk_pending);
    observe(&mut actors[0]);
    assert_ne!(controller.jobs().len(), 0);
    let peer = NodeId::from_bytes([3; 16]);
    let expected = if disconnect {
        assert!(
            actors[0]
                .disconnect_session(peer, old, Duration::ZERO)
                .unwrap()
        );
        LinkSessionId::from_bytes([0; 16])
    } else {
        old
    };
    assert!(
        actors[0]
            .replace_session(peer, expected, new, Duration::ZERO)
            .unwrap()
    );
    actors[0].receive(&stale, Duration::ZERO).unwrap();
    for _ in 0..100 {
        reject_response(&mut controller, &mut actors[0]);
    }
    assert!(actors[0].status().application_ready);
    // A valid new-session retry cannot give the released nonce a newer snapshot.
    actors[0]
        .receive(&request_message(request, new), Duration::ZERO)
        .unwrap();
    for _ in 0..32 {
        reject_response(&mut controller, &mut actors[0]);
    }
    request.nonce = ozzy_proto::RequestId::from_bytes([92; 16]);
    request.request_id = ozzy_proto::RequestId::from_bytes([43; 16]);
    let latest = request_message(request, new);
    // Retransmission retains the admitted snapshot and creates no second job.
    for _ in 0..2 {
        actors[0].receive(&latest, Duration::ZERO).unwrap();
    }
    expect_response(
        &mut controller,
        &mut actors[0],
        policy,
        request,
        new,
        Duration::ZERO,
    );
    for actor in actors {
        close(&mut controller, actor);
    }
    assert_eq!(controller.jobs().len(), 0);
}

fn reject_response(controller: &mut Controller, actor: &mut Scheduled) {
    actor.advance(Duration::ZERO).unwrap();
    observe(actor);
    settle(controller, &[]);
    actor
        .flush(|message| {
            assert_ne!(opcode(&message), ozzy_proto::Opcode::RecoveryState);
            Ok(())
        })
        .unwrap();
}

fn observe(actor: &mut Scheduled) {
    let mut event = std::pin::pin!(actor.changed(Duration::ZERO));
    if let Poll::Ready(result) = poll(event.as_mut()) {
        result.unwrap();
    }
}

fn request_message(request: RecoveryRequest, session: LinkSessionId) -> Message {
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

fn expect_response(
    controller: &mut Controller,
    actor: &mut Scheduled,
    policy: QuorumPolicy,
    request: RecoveryRequest,
    session: LinkSessionId,
    now: Duration,
) -> ozzy_replication::recovery::RecoveryResponse {
    let binding = wire::PeerBinding::new(
        config("/unused", 4, policy).configuration.configuration(),
        NodeId::from_bytes([1; 16]),
        session,
    )
    .unwrap();
    for _ in 0..1000 {
        actor.advance(now).unwrap();
        {
            let mut event = std::pin::pin!(actor.changed(now));
            if let Poll::Ready(result) = poll(event.as_mut()) {
                result.unwrap();
            }
        }
        settle(controller, &[]);
        let mut replied = None;
        actor
            .flush(|message| {
                if opcode(&message) == ozzy_proto::Opcode::RecoveryState {
                    let frames =
                        std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
                    let wire::ReplicaMessage::Recovery(wire::RecoveryMessage::State(state)) =
                        wire::decode(&frames, binding, WireLimits::default()).unwrap()
                    else {
                        panic!("expected recovery response")
                    };
                    assert_eq!(state.request_id, request.request_id);
                    assert_eq!(state.response.nonce, request.nonce);
                    replied = Some(state.response);
                }
                Ok(())
            })
            .unwrap();
        if let Some(response) = replied {
            return response;
        }
    }
    panic!("replacement requester never received a pinned response");
}

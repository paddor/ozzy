//! Physical donor work outlives transport sessions, but its request does not.

use super::*;
use ozzy_replication::wire::{self, RecoveryRequest, WireLimits};

#[test]
fn recovery_freezes_an_unsent_candidate_after_intervening_commit() {
    let (mut controller, io) = setup();
    let mut actors = (0..3)
        .map(|broker| Scheduled::new(unstarted_actor(&mut controller, &io, 0, broker, 64)).unwrap())
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
        nonvoting: true,
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

/// A replay to a lagging follower runs beside the next journal turn and holds
/// the leader's one fetch buffer. A recovering peer's fetch waits for it.
#[test]
fn a_recovery_fetch_waits_while_a_replay_holds_the_fetch_buffer() {
    let (mut controller, io) = setup();
    let mut actors = (0..3)
        .map(|broker| Scheduled::new(unstarted_actor(&mut controller, &io, 0, broker, 64)).unwrap())
        .collect::<Vec<_>>();
    for _ in 0..100 {
        round(&mut actors, false);
        settle(&mut controller, &[]);
    }
    assert!(actors.iter().all(|actor| actor.status().application_ready));

    // Follower 1 misses more operations than the replay cache holds.
    let mut lane = actors[0].take_submitter().unwrap();
    let mut buffer = actors[0]
        .lease_proposal_buffer_with_limits(actor_config().pipeline)
        .unwrap();
    for body in 0..32 {
        buffer.clear();
        buffer
            .push(ozzy_journal::operation::OperationKind::Barrier, &[body; 16])
            .unwrap();
        let mut pending = Box::pin(lane.try_submit(buffer).unwrap());
        let mut reply = None;
        for _ in 0..10000 {
            for (to, message) in collect(&mut actors, Duration::ZERO, false) {
                if to != 1 {
                    actors[to].receive(&message, Duration::ZERO).unwrap();
                }
            }
            settle(&mut controller, &[]);
            if let Poll::Ready(result) = poll(pending.as_mut()) {
                reply = Some(result.unwrap());
                break;
            }
        }
        buffer = reply.expect("the pair confirms").buffer;
    }

    // Its probe answer starts a replay from disk, which takes the buffer.
    // Hold that read.
    let idle = actors[0].arenas().0;
    let mut now = Duration::ZERO;
    let mut held = Vec::new();
    for _ in 0..10000 {
        now += Duration::from_millis(1);
        for (to, message) in collect(&mut actors, now, false) {
            actors[to].receive(&message, now).unwrap();
        }
        if actors[0].arenas().0 < idle {
            held = controller.jobs().iter().map(|(id, _)| *id).collect();
            break;
        }
        settle(&mut controller, &[]);
    }
    assert!(!held.is_empty(), "a replay reads the journal");

    let request = RecoveryRequest {
        scope: actors[0].status().scope,
        request_id: ozzy_proto::RequestId::from_bytes([97; 16]),
        nonce: ozzy_proto::RequestId::from_bytes([98; 16]),
        nonvoting: true,
    };
    actors[0]
        .receive(&request_message(request, actor_config().sessions[2]), now)
        .unwrap();
    let response = expect_response_holding(
        &mut controller,
        &mut actors[0],
        request,
        actor_config().sessions[2],
        now,
        &held,
    );
    let log = response.primary.unwrap();
    let fetch = wire::FetchOps {
        scope: request.scope,
        request_id: ozzy_proto::RequestId::from_bytes([99; 16]),
        source: ozzy_replication::LogSource {
            voter: NodeId::from_bytes([1; 16]),
            generation: log.generation,
            accepted: log.accepted,
        },
        predecessor: Prefix::GENESIS,
        max_operations: actor_config().transfer.max_operations as u32,
        max_body_bytes: actor_config().transfer.max_body_bytes as u32,
    };
    actors[0]
        .receive(&super::history::fetch_message(fetch), now)
        .unwrap();
    actors[0].advance(now).unwrap();

    // The replay finishes and returns the buffer, and the fetch gets its turn.
    let mut served = false;
    for _ in 0..1000 {
        settle(&mut controller, &[]);
        actors[0].advance(now).unwrap();
        observe_at(&mut actors[0], now);
        actors[0]
            .flush(|message| {
                served |= opcode(&message) == ozzy_proto::Opcode::Ops;
                Ok(())
            })
            .unwrap();
        if served {
            break;
        }
    }
    assert!(
        served,
        "the fetch must be served: {:#?}",
        actors[0].status()
    );
    for actor in actors {
        close(&mut controller, actor);
    }
}

fn observe_at(actor: &mut Scheduled, now: Duration) {
    let mut event = std::pin::pin!(actor.changed(now));
    if let Poll::Ready(result) = poll(event.as_mut()) {
        result.unwrap();
    }
}

#[test]
fn a_nonvoting_primary_ends_the_view_its_peers_elect_but_a_startup_probe_does_not() {
    let (mut controller, io) = setup();
    let (mut actors, pending) = cluster(&mut controller, &io, 0);
    let mut pending = Box::pin(pending);
    for _ in 0..10000 {
        round(&mut actors, false);
        settle(&mut controller, &[]);
        if poll(pending.as_mut()).is_ready() {
            break;
        }
    }
    // Cut broker 0 off. Once broker 2 elects view 1, which broker 1 leads,
    // drop broker 1's traffic to it so it keeps electing.
    let mut now = Duration::from_secs(3);
    for actor in &mut actors[1..] {
        actor
            .disconnect_session(NodeId::from_bytes([1; 16]), actor_config().sessions[0], now)
            .unwrap();
    }
    for step in 0..10000 {
        now = Duration::from_secs(3) + Duration::from_millis(step);
        for (to, message) in collect(&mut actors, now, false) {
            let from = message.part_slice(0).unwrap()[0];
            if to == 2 && from == 2 && actors[2].status().scope.view == 1 {
                continue;
            }
            actors[to].receive(&message, now).unwrap();
        }
        settle(&mut controller, &[]);
        if actors[2].status().scope.view == 1 {
            break;
        }
    }
    let status = actors[2].status();
    assert_eq!((status.scope.view, status.normal), (1, None));
    let exits = |actors: &mut [Scheduled]| {
        collect(&mut actors[2..], now, false)
            .iter()
            .filter(|(_, message)| opcode(message) == ozzy_proto::Opcode::ExitView)
            .count()
    };
    exits(&mut actors);
    let probe = RecoveryRequest {
        scope: status.scope,
        request_id: ozzy_proto::RequestId::from_bytes([97; 16]),
        nonce: ozzy_proto::RequestId::from_bytes([98; 16]),
        nonvoting: false,
    };
    let session = actor_config().sessions[1];
    actors[2]
        .receive(&request_from(1, probe, session), now)
        .unwrap();
    assert_eq!(exits(&mut actors), 0, "a startup probe still votes");
    let recovering = RecoveryRequest {
        request_id: ozzy_proto::RequestId::from_bytes([99; 16]),
        nonvoting: true,
        ..probe
    };
    actors[2]
        .receive(&request_from(1, recovering, session), now)
        .unwrap();
    assert_ne!(exits(&mut actors), 0, "a nonvoting primary cannot install");
    for actor in actors {
        close(&mut controller, actor);
    }
}

#[test]
fn disconnected_and_replaced_recovery_requesters_release_pending_pins() {
    for disconnect in [false, true] {
        replace_while_pinning(disconnect);
    }
}

fn replace_while_pinning(disconnect: bool) {
    let (mut controller, io) = setup();
    let (mut actors, pending) = cluster(&mut controller, &io, 0);
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
        nonvoting: true,
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
    request_from(2, request, session)
}

/// RECOVERY from broker `sender` over `session`.
fn request_from(sender: u8, request: RecoveryRequest, session: LinkSessionId) -> Message {
    let mut metadata = [0; 1024];
    let encoded = wire::encode_recovery(
        NodeId::from_bytes([sender + 1; 16]),
        session,
        request,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    Message::multipart([
        bytes::Bytes::copy_from_slice(&[sender + 1; 16]),
        bytes::Bytes::copy_from_slice(&encoded.header),
        bytes::Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
        bytes::Bytes::new(),
    ])
}

fn expect_response(
    controller: &mut Controller,
    actor: &mut Scheduled,
    request: RecoveryRequest,
    session: LinkSessionId,
    now: Duration,
) -> ozzy_replication::recovery::RecoveryResponse {
    expect_response_holding(controller, actor, request, session, now, &[])
}

fn expect_response_holding(
    controller: &mut Controller,
    actor: &mut Scheduled,
    request: RecoveryRequest,
    session: LinkSessionId,
    now: Duration,
    held: &[JobId],
) -> ozzy_replication::recovery::RecoveryResponse {
    let binding = wire::PeerBinding::new(
        config("/unused", 4).configuration.configuration(),
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
        settle(controller, held);
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

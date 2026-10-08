use super::*;
use ozzy_proto::RequestId;
use ozzy_replication::{LogSource, OpNumber, wire};

mod buffered;
mod retirement;

#[test]
fn history_request_retries_while_an_independent_lookup_owns_the_journal() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, now) = history_fixture(&mut controller, &io, policy);
        let status = actors[1].status();
        let snapshot = status.normal.unwrap();
        let request = wire::FetchOps {
            scope: status.scope,
            request_id: RequestId::from_bytes([93; 16]),
            source: LogSource {
                voter: NodeId::from_bytes([2; 16]),
                generation: snapshot.journal.generation,
                accepted: snapshot.accepted,
            },
            predecessor: Prefix::GENESIS,
            max_operations: actor_config().transfer.max_operations as u32,
            max_body_bytes: actor_config().transfer.max_body_bytes as u32,
        };
        let message = fetch_message(request);
        // Shared reader services use this same owner access independently of
        // the replica's pending action. Hold an actual history read there.
        let (ticket, journal) = actors[1].read_access();
        let mut lookup = Box::pin(
            journal
                .replication_positions(ticket.unwrap(), [OpNumber(1); 2])
                .unwrap(),
        );
        assert!(poll(lookup.as_mut()).is_pending());
        for _ in 0..1000 {
            if controller.jobs().iter().any(|(id, _)| {
                matches!(
                    controller.operation(*id).unwrap().unprotected(),
                    Operation::Read { .. }
                )
            }) {
                break;
            }
            settle(&mut controller, &[]);
            let mut event = std::pin::pin!(actors[1].changed(now));
            if let Poll::Ready(result) = poll(event.as_mut()) {
                result.unwrap();
            }
        }
        let held = controller.jobs();
        assert!(held.iter().any(|(id, _)| matches!(
            controller.operation(*id).unwrap().unprotected(),
            Operation::Read { .. }
        )));
        assert_eq!(actors[1].read_access().1.available_command_slots(), 0);
        for _ in 0..3 {
            actors[1].receive(&message, now).unwrap();
        }
        assert_eq!(controller.jobs(), held, "busy requests must submit no I/O");
        assert!(actors[1].status().application_ready);
        let mut looked_up = None;
        for _ in 0..10000 {
            round_at(&mut actors, now, false);
            settle(&mut controller, &[]);
            if let Poll::Ready(result) = poll(lookup.as_mut()) {
                let positions = result.unwrap().positions;
                assert_eq!(positions[0], positions[1]);
                assert_eq!(positions[0].unwrap().op, OpNumber(1));
                looked_up = positions[0];
                break;
            }
        }
        let looked_up = looked_up.expect("independent lookup must complete");
        actors[1].receive(&message, now).unwrap();
        let mut replied = false;
        for _ in 0..10000 {
            actors[1].advance(now).unwrap();
            {
                let mut event = std::pin::pin!(actors[1].changed(now));
                if let Poll::Ready(result) = poll(event.as_mut()) {
                    result.unwrap();
                }
            }
            actors[1]
                .flush(|message| {
                    if opcode(&message) == ozzy_proto::Opcode::Ops {
                        verify_response(&message, request, looked_up, policy);
                        replied = true;
                    }
                    Ok(())
                })
                .unwrap();
            settle(&mut controller, &[]);
            if replied {
                break;
            }
        }
        assert!(replied, "retry must receive the exact retained operation");
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

fn history_fixture(
    controller: &mut Controller,
    io: &Local,
    policy: QuorumPolicy,
) -> (Vec<Scheduled>, Duration) {
    let (mut actors, pending) = cluster_with_operation_count(controller, io, 0, policy, 2);
    let mut pending = Box::pin(pending);
    let mut confirmed = false;
    for _ in 0..10000 {
        round(&mut actors, false);
        settle(controller, &[]);
        if !confirmed && let Poll::Ready(result) = poll(pending.as_mut()) {
            assert!(matches!(
                result.unwrap().outcome,
                ProposalOutcome::Committed { .. }
            ));
            confirmed = true;
        }
        if confirmed && controller.jobs().is_empty() {
            break;
        }
    }
    assert!(confirmed);
    for _ in 0..100 {
        round(&mut actors, false);
        settle(controller, &[]);
    }
    let mut now = Duration::from_secs(3);
    for actor in &mut actors[1..] {
        actor
            .disconnect_session(NodeId::from_bytes([1; 16]), actor_config().sessions[0], now)
            .unwrap();
    }
    for step in 0..10000 {
        now = Duration::from_secs(3) + Duration::from_millis(step / 100);
        round_at(&mut actors, now, false);
        settle(controller, &[]);
        if actors[1].status().application_ready && actors[1].status().scope.view == 1 {
            break;
        }
    }
    assert_eq!(actors[1].status().scope.view, 1);
    assert!(
        actors[1].status().application_ready,
        "{:?}",
        actors.iter().map(Scheduled::status).collect::<Vec<_>>()
    );
    for _ in 0..100 {
        round_at(&mut actors, now, false);
        settle(controller, &[]);
    }
    (actors, now)
}

fn fetch_message(request: wire::FetchOps) -> Message {
    let mut metadata = [0; 1024];
    let encoded = wire::encode_fetch(
        NodeId::from_bytes([3; 16]),
        actor_config().sessions[2],
        request,
        &mut metadata,
        wire::WireLimits::default(),
    )
    .unwrap();
    Message::multipart([
        bytes::Bytes::from_static(&[3; 16]),
        bytes::Bytes::copy_from_slice(&encoded.header),
        bytes::Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
        bytes::Bytes::new(),
    ])
}

fn verify_response(
    message: &Message,
    request: wire::FetchOps,
    looked_up: Prefix,
    policy: QuorumPolicy,
) {
    let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
    let binding = wire::PeerBinding::new(
        config("/g0-b0", 4, policy).configuration.configuration(),
        NodeId::from_bytes([2; 16]),
        actor_config().sessions[2],
    )
    .unwrap();
    let wire::ReplicaMessage::Ops(batch) =
        wire::decode(&frames, binding, wire::WireLimits::default()).unwrap()
    else {
        panic!("expected canonical history reply");
    };
    batch.validate_response(request).unwrap();
    let operations: Vec<_> = batch.operations().collect();
    assert_eq!(operations.len(), 2);
    assert_eq!(operations[0].prefix(), looked_up);
    assert_eq!(operations[1].prefix(), request.source.accepted);
    assert_eq!(operations[0].canonical().body, &[1; 16]);
    assert_eq!(operations[1].canonical().body, &[2; 16]);
}

use super::*;

const UNBOUND: LinkSessionId = LinkSessionId::from_bytes([0; 16]);

fn connect(actors: &mut [Scheduled], other: usize, session: LinkSessionId) {
    for (local, remote) in [(0, other), (other, 0)] {
        assert!(
            actors[local]
                .replace_session(
                    NodeId::from_bytes([remote as u8 + 1; 16]),
                    UNBOUND,
                    session,
                    Duration::ZERO,
                )
                .unwrap()
        );
    }
}

fn unbound_cluster(
    controller: &mut Controller,
    io: &Local,
    policy: QuorumPolicy,
) -> (
    Vec<Scheduled>,
    PendingProposal,
    crate::replica_actor::ProposalSubmitter,
) {
    let mut actors = Vec::new();
    let mut pending = None;
    let mut proposal = None;
    for broker in 0..3 {
        let mut actor =
            unstarted_actor_with_sessions(controller, io, 0, policy, broker, 64, [UNBOUND; 3]);
        if broker == 0 {
            let mut buffer = actor.lease_proposal_buffer().unwrap();
            buffer
                .push(ozzy_journal::operation::OperationKind::Barrier, &[1; 16])
                .unwrap();
            let mut lane = actor.take_submitter().unwrap();
            pending = Some(lane.try_submit(buffer).unwrap());
            proposal = Some(lane);
        }
        actors.push(Scheduled::new(actor).unwrap());
    }
    (actors, pending.unwrap(), proposal.unwrap())
}

#[test]
fn unbound_startup_and_disconnected_intake_never_fabricate_sessions_or_confirmations() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, pending, proposal) = unbound_cluster(&mut controller, &io, policy);
        let mut pending = Box::pin(pending);
        for _ in 0..1000 {
            assert_eq!(collect(&mut actors, Duration::ZERO, false).len(), 0);
            settle(&mut controller, &[]);
        }
        assert_eq!(actors[0].status().normal.unwrap().accepted.op.0, 1);
        assert!(poll(pending.as_mut()).is_pending());
        assert!(
            actors
                .iter()
                .all(|actor| actor.status().normal.unwrap().committed == Prefix::GENESIS)
        );

        let first = LinkSessionId::from_bytes([71; 16]);
        connect(&mut actors, 1, first);
        let mut stale = None;
        for _ in 0..1000 {
            for (to, message) in collect(&mut actors, Duration::ZERO, false) {
                assert_ne!(to, 2, "unbound broker received traffic");
                if message.len() == 4 {
                    assert_eq!(envelope(&message).session, Some(first));
                }
                if opcode(&message) == ozzy_proto::Opcode::PrepareFlow {
                    stale = Some(message);
                } else {
                    actors[to].receive(&message, Duration::ZERO).unwrap();
                }
            }
            settle(&mut controller, &[]);
            if stale.is_some() {
                break;
            }
        }
        let stale = stale.expect("previously unbound template did not acquire a live header");
        for (local, remote) in [(0, 1), (1, 0)] {
            let remote = NodeId::from_bytes([remote + 1; 16]);
            assert!(
                actors[local]
                    .disconnect_session(remote, first, Duration::ZERO)
                    .unwrap()
            );
            assert!(
                !actors[local]
                    .disconnect_session(remote, first, Duration::ZERO)
                    .unwrap()
            );
        }
        for follower in [1, 2] {
            actors[follower].receive(&stale, Duration::ZERO).unwrap();
            drain_stale(&mut controller, &mut actors[follower]);
            assert_eq!(
                actors[follower].status().normal.unwrap().accepted,
                Prefix::GENESIS
            );
        }
        for _ in 0..32 {
            assert_eq!(collect(&mut actors, Duration::ZERO, false).len(), 0);
            settle(&mut controller, &[]);
        }
        assert!(poll(pending.as_mut()).is_pending());

        let replacement = LinkSessionId::from_bytes([72; 16]);
        connect(&mut actors, 1, replacement);
        assert!(matches!(
            actors[0].disconnect_session(NodeId::from_bytes([2; 16]), first, Duration::ZERO,),
            Err(ScheduleError::Binding)
        ));
        let mut confirmed = false;
        for _ in 0..10000 {
            for (to, message) in collect(&mut actors, Duration::ZERO, false) {
                assert_ne!(to, 2);
                if message.len() == 4 {
                    assert_eq!(envelope(&message).session, Some(replacement));
                }
                actors[to].receive(&message, Duration::ZERO).unwrap();
            }
            settle(&mut controller, &[]);
            if !confirmed && let Poll::Ready(result) = poll(pending.as_mut()) {
                assert!(matches!(
                    result.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                confirmed = true;
            }
            if confirmed && actors[1].status().normal.unwrap().applied.op.0 == 1 {
                break;
            }
        }
        assert!(confirmed);
        assert_eq!(actors[1].status().normal.unwrap().applied.op.0, 1);
        assert_eq!(actors[2].status().normal.unwrap().accepted, Prefix::GENESIS);

        catch_up_last(&mut controller, &mut actors);
        for actor in actors {
            close(&mut controller, actor);
        }
        drop(proposal);
    }
}

fn catch_up_last(controller: &mut Controller, actors: &mut [Scheduled]) {
    // The absent broker can catch up from the same cached canonical bytes.
    connect(actors, 2, LinkSessionId::from_bytes([73; 16]));
    for _ in 0..10000 {
        round(actors, false);
        settle(controller, &[]);
        if actors[2].status().normal.unwrap().applied.op.0 == 1 {
            break;
        }
    }
    assert_eq!(actors[2].status().normal.unwrap().applied.op.0, 1);
}

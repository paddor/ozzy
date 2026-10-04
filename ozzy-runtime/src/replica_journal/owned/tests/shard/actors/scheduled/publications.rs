use super::*;
use bytes::Bytes;
use ozzy_proto::Opcode;

/// The simulator supplies the same lossy PUB and reliable bounded PEER boundary
/// as the broker frontend. It keeps partition authority inside each actor.
pub(super) fn step(
    actors: &mut [Scheduled],
    now: Duration,
    drop_publication: bool,
    slow: &[usize],
    publication_count: &mut usize,
    repair_count: &mut usize,
) {
    network_step(
        actors,
        now,
        drop_publication,
        slow,
        publication_count,
        repair_count,
        None,
    );
}

enum PublicationDelay<'a> {
    Transport(&'a mut Vec<(usize, Message)>),
    Handoff(&'a mut Vec<(usize, Message)>),
}

fn network_step(
    actors: &mut [Scheduled],
    now: Duration,
    drop_publication: bool,
    slow: &[usize],
    publication_count: &mut usize,
    repair_count: &mut usize,
    mut delayed: Option<PublicationDelay<'_>>,
) {
    let mut deliveries = Vec::new();
    for (from, actor) in actors.iter_mut().enumerate() {
        actor.advance(now).unwrap();
        {
            let mut event = std::pin::pin!(actor.changed(now));
            if let Poll::Ready(result) = poll(event.as_mut()) {
                result.unwrap();
            }
        }
        actor
            .flush(|mut message| {
                let command = opcode(&message);
                if command == Opcode::PreparePub {
                    if let Some(PublicationDelay::Handoff(held)) = &mut delayed {
                        held.push((from, message.clone()));
                        return Err(omq_tokio::TrySendError::Full(message));
                    }
                    *publication_count += 1;
                    if !drop_publication {
                        message.pop_front();
                        for to in 0..3 {
                            if to != from && !slow.contains(&to) {
                                let message = Message::with_prefix(
                                    Bytes::from(vec![from as u8 + 1; 16]),
                                    message.clone(),
                                );
                                if let Some(PublicationDelay::Transport(delayed)) = &mut delayed {
                                    delayed.push((to, message));
                                } else {
                                    deliveries.push((to, message, true));
                                }
                            }
                        }
                    }
                    return Ok(());
                }
                let route = message.part_slice(0).unwrap();
                let to = usize::from(route[0] - 1);
                if command == Opcode::PrepareFlow {
                    if slow.contains(&to) {
                        return Err(omq_tokio::TrySendError::Full(message));
                    }
                    *repair_count += 1;
                }
                message.pop_front();
                deliveries.push((
                    to,
                    Message::with_prefix(Bytes::from(vec![from as u8 + 1; 16]), message),
                    command == Opcode::PrepareFlow,
                ));
                Ok(())
            })
            .unwrap();
    }
    for (to, message, data) in deliveries {
        if data {
            assert!(actors[to].receive_data(&message, now).unwrap());
        } else {
            actors[to].receive(&message, now).unwrap();
        }
    }
}

#[test]
fn control_overtaking_publication_does_not_start_duplicate_repair() {
    delayed_publication(false);
}

#[test]
fn local_publication_pressure_retries_the_same_frame_before_lossy_transport() {
    delayed_publication(true);
}

fn delayed_publication(handoff: bool) {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
        for actor in &mut actors {
            actor.enable_publication();
        }
        let mut pending = Box::pin(pending);
        let (mut publications, mut repairs) = (0, 0);
        let mut delayed = Vec::new();
        for turn in 0..20 {
            let now = Duration::from_micros(turn * 100);
            network_step(
                &mut actors,
                now,
                false,
                &[],
                &mut publications,
                &mut repairs,
                Some(if handoff {
                    PublicationDelay::Handoff(&mut delayed)
                } else {
                    PublicationDelay::Transport(&mut delayed)
                }),
            );
            settle(&mut controller, &[]);
            assert!(poll(pending.as_mut()).is_pending());
        }
        if handoff {
            assert_eq!(publications, 0);
            assert_ne!(delayed.len(), 0);
            let first = &delayed[0].1;
            for (from, message) in &delayed {
                assert_eq!(*from, 0);
                for part in 0..4 {
                    assert_eq!(message.part_slice(part), first.part_slice(part));
                }
            }
            delayed.clear();
        } else {
            assert_eq!(publications, 1);
            assert_eq!(delayed.len(), 2);
        }
        assert_eq!(repairs, 0, "moving PUB must not trigger speculative repair");
        for (to, message) in delayed {
            assert!(
                actors[to]
                    .receive_data(&message, Duration::from_millis(2))
                    .unwrap()
            );
        }
        let mut confirmed = false;
        for turn in 20..10000 {
            let now = Duration::from_micros(turn * 100);
            step(
                &mut actors,
                now,
                false,
                &[],
                &mut publications,
                &mut repairs,
            );
            settle(&mut controller, &[]);
            if let Poll::Ready(result) = poll(pending.as_mut()) {
                assert!(matches!(
                    result.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                confirmed = true;
                break;
            }
        }
        assert!(confirmed, "delayed PUB must confirm under {policy:?}");
        assert_eq!(publications, 1);
        assert_eq!(repairs, 0);
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

#[test]
fn lost_final_publication_repairs_and_confirms_without_another_append() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
        for actor in &mut actors {
            actor.enable_publication();
        }
        let mut pending = Box::pin(pending);
        let (mut publications, mut repairs) = (0, 0);
        let mut confirmed = false;
        let mut first_repair = None;
        for turn in 0..10000 {
            let now = Duration::from_micros(turn * 100);
            step(&mut actors, now, true, &[], &mut publications, &mut repairs);
            if repairs > 0 && first_repair.is_none() {
                first_repair = Some(now);
            }
            settle(&mut controller, &[]);
            if let Poll::Ready(result) = poll(pending.as_mut()) {
                assert!(matches!(
                    result.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                confirmed = true;
                break;
            }
        }
        assert!(confirmed, "lost idle tail must repair under {policy:?}");
        assert_eq!(publications, 1, "publish canonical group once");
        assert!(repairs > 0, "lost PUB requires correlated PEER repair");
        assert!(first_repair.unwrap() < Duration::from_millis(100));
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

#[test]
fn one_slow_follower_does_not_gate_the_healthy_pair() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
        for actor in &mut actors {
            actor.enable_publication();
        }
        let mut pending = Box::pin(pending);
        let (mut publications, mut repairs) = (0, 0);
        let mut confirmed = false;
        for turn in 0..10000 {
            let now = Duration::from_micros(turn * 100);
            step(
                &mut actors,
                now,
                false,
                &[1],
                &mut publications,
                &mut repairs,
            );
            settle(&mut controller, &[]);
            if let Poll::Ready(result) = poll(pending.as_mut()) {
                assert!(matches!(
                    result.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                confirmed = true;
                break;
            }
        }
        assert!(
            confirmed,
            "slow follower blocked healthy pair under {policy:?}"
        );
        assert_eq!(publications, 1);
        assert_eq!(actors[1].status().normal.unwrap().accepted, Prefix::GENESIS);
        assert_eq!(actors[2].status().normal.unwrap().accepted.op.0, 1);
        // With no more writes, removing pressure must repair the slow copy.
        for turn in 10000..20000 {
            let now = Duration::from_micros(turn * 100);
            step(
                &mut actors,
                now,
                false,
                &[],
                &mut publications,
                &mut repairs,
            );
            settle(&mut controller, &[]);
            if actors[1].status().normal.unwrap().applied.op.0 == 1 {
                break;
            }
        }
        assert_eq!(actors[1].status().normal.unwrap().applied.op.0, 1);
        assert!(repairs > 0);
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

#[test]
fn both_slow_followers_cannot_confirm_from_transport_or_receipts() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
        for actor in &mut actors {
            actor.enable_publication();
        }
        let mut pending = Box::pin(pending);
        let (mut publications, mut repairs) = (0, 0);
        for turn in 0..1000 {
            let now = Duration::from_micros(turn * 100);
            step(
                &mut actors,
                now,
                false,
                &[1, 2],
                &mut publications,
                &mut repairs,
            );
            settle(&mut controller, &[]);
            assert!(poll(pending.as_mut()).is_pending());
        }
        assert_eq!(publications, 1);
        assert_eq!(repairs, 0);
        let leader = actors[0].status().normal.unwrap();
        assert_eq!(leader.accepted.op.0, 1);
        assert_eq!(leader.committed, Prefix::GENESIS);
        assert_eq!(actors[1].status().normal.unwrap().accepted, Prefix::GENESIS);
        assert_eq!(actors[2].status().normal.unwrap().accepted, Prefix::GENESIS);
        let mut confirmed = false;
        for turn in 1000..10000 {
            let now = Duration::from_micros(turn * 100);
            step(
                &mut actors,
                now,
                false,
                &[1],
                &mut publications,
                &mut repairs,
            );
            settle(&mut controller, &[]);
            if let Poll::Ready(result) = poll(pending.as_mut()) {
                assert!(matches!(
                    result.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                confirmed = true;
                break;
            }
        }
        assert!(
            confirmed,
            "one resumed follower must restore progress under {policy:?}"
        );
        assert!(repairs > 0);
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

#[test]
fn blocked_repair_waits_for_progress_without_waking_itself() {
    blocked_data(false);
}

#[test]
fn contiguous_publication_waits_for_local_capacity_without_becoming_loss() {
    blocked_data(true);
}

fn blocked_data(publication: bool) {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
        let domain = crate::memory::Domain::new(None, 4096).unwrap();
        let owner = domain
            .owner(crate::memory::Limits {
                bytes: 4096,
                buffers: 1,
                cache_bytes: 4096,
            })
            .unwrap();
        let blocked = futures::executor::block_on(owner.lease(4096)).unwrap();
        actors[1].bind_receive_owner(&owner).unwrap();
        let mut held = None;
        for _ in 0..1000 {
            for (to, message) in collect(&mut actors, Duration::ZERO, false) {
                if to == 1 && opcode(&message) == Opcode::PrepareFlow {
                    held = Some(message);
                } else {
                    actors[to].receive(&message, Duration::ZERO).unwrap();
                }
            }
            settle(&mut controller, &[]);
            if held.is_some() {
                break;
            }
        }
        let mut held = held.expect("follower data reached transport");
        if publication {
            actors[1].enable_publication();
            let frames = std::array::from_fn::<_, 3, _>(|i| held.part_slice(i + 1).unwrap());
            let mut metadata = vec![0; 4096];
            let encoded = ozzy_replication::wire::encode_publication(
                NodeId::from_bytes([1; 16]),
                &frames,
                Prefix::GENESIS,
                &mut metadata,
                ozzy_replication::wire::WireLimits::default(),
            )
            .unwrap();
            held = Message::multipart([
                held.part_bytes(0).unwrap(),
                Bytes::copy_from_slice(&encoded.header),
                Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
                held.part_bytes(3).unwrap(),
            ]);
        }
        actors[1].advance(Duration::ZERO).unwrap();
        assert!(!actors[1].receive_data(&held, Duration::ZERO).unwrap());
        {
            let changed = std::pin::pin!(actors[1].changed(Duration::ZERO));
            assert!(
                poll(changed).is_pending(),
                "full follower data must not spin under {policy:?}"
            );
        }
        let mut now = Duration::ZERO;
        if publication {
            now = Duration::from_millis(200);
            let tail = actors[0].status().normal.unwrap().accepted;
            let response = probe_follower(
                &mut actors[1],
                tail,
                LinkSessionId::from_bytes([61; 16]),
                now,
            );
            assert_eq!(response.repair_limit, Some(response.report.received.op));
            assert_eq!(response.report.received, Prefix::GENESIS);
            let new_session = LinkSessionId::from_bytes([62; 16]);
            actors[1]
                .replace_session(
                    NodeId::from_bytes([1; 16]),
                    LinkSessionId::from_bytes([61; 16]),
                    new_session,
                    now,
                )
                .unwrap();
            let fenced = probe_follower(&mut actors[1], tail, new_session, now);
            assert_eq!(
                fenced.repair_limit, None,
                "fenced held frame cannot suppress repair"
            );
        }
        drop(blocked);
        assert!(actors[1].receive_data(&held, now).unwrap());
        drop(pending);
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

fn probe_follower(
    actor: &mut Scheduled,
    tail: Prefix,
    session: LinkSessionId,
    now: Duration,
) -> ozzy_replication::wire::FlowState {
    actor.advance(now).unwrap();
    let request_id = ozzy_proto::RequestId::from_bytes([71; 16]);
    let mut metadata = vec![0; 4096];
    let encoded = ozzy_replication::wire::encode_flow_probe(
        NodeId::from_bytes([1; 16]),
        session,
        ozzy_replication::wire::FlowProbe {
            scope: actor.status().scope,
            request_id,
            tail,
            available: tail.op,
        },
        &mut metadata,
        ozzy_replication::wire::WireLimits::default(),
    )
    .unwrap();
    let probe = Message::multipart([
        Bytes::from(vec![1; 16]),
        Bytes::copy_from_slice(&encoded.header),
        Bytes::copy_from_slice(&metadata[..encoded.metadata_bytes]),
        Bytes::new(),
    ]);
    actor.receive(&probe, now).unwrap();
    let mut response = None;
    actor
        .flush(|message| {
            if opcode(&message) == Opcode::ReplicaState {
                let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
                let limits = ozzy_proto::EnvelopeLimits::default();
                let packet = ozzy_proto::decode_packet(&frames, limits).unwrap();
                let state = ozzy_replication::wire::flow_state_route(packet, limits).unwrap();
                if state.request_id == Some(request_id) {
                    response = Some(state);
                }
            }
            Ok(())
        })
        .unwrap();
    response.expect("follower must answer control probes")
}

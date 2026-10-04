use super::*;
use crate::replica_actor::{ScheduleError, ScheduledReplica};

type Scheduled = ScheduledReplica;

mod disconnected;
mod history;
mod native;
mod publications;
mod routes;
mod set;

#[derive(Default)]
struct StepRoutes(
    std::collections::BTreeMap<(usize, usize, u32), ozzy_proto::GroupId>,
    Option<ozzy_proto::GroupId>,
);

impl StepRoutes {
    fn with_prior_group(group: ozzy_proto::GroupId) -> Self {
        Self(std::collections::BTreeMap::default(), Some(group))
    }

    fn group(
        &mut self,
        from: usize,
        message: &Message,
        stalled: Option<ozzy_proto::GroupId>,
    ) -> ozzy_proto::GroupId {
        let to = usize::from(message.part_slice(0).expect("routed message")[0] - 1);
        if message.len() == 2 {
            let state = ozzy_replication::wire::CompactState::decode(
                message.part_slice(1).expect("compact receipt"),
            )
            .unwrap();
            let key = (from, to, state.handle);
            // This test may start forwarding after the stalled group's full
            // channel opening was already delivered by the earlier fixture.
            let group = self
                .0
                .get(&key)
                .copied()
                .or(self.1)
                .or(stalled)
                .expect("bound compact handle");
            self.0.insert(key, group);
            return group;
        }
        let limits = ozzy_proto::EnvelopeLimits::default();
        let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
        let packet = ozzy_proto::decode_packet(&frames, limits).unwrap();
        let group = ozzy_replication::wire::route(packet, limits)
            .unwrap()
            .group_id;
        if packet.envelope.opcode == ozzy_proto::Opcode::ReplicaState {
            let state = ozzy_replication::wire::flow_state_route(packet, limits).unwrap();
            self.0.insert((from, to, state.handle), group);
        }
        group
    }
}

fn cluster(
    controller: &mut Controller,
    io: &Local,
    group: u8,
    policy: QuorumPolicy,
) -> (Vec<Scheduled>, PendingProposal) {
    cluster_with_operation_count(controller, io, group, policy, 1)
}

fn cluster_with_operation_count(
    controller: &mut Controller,
    io: &Local,
    group: u8,
    policy: QuorumPolicy,
    operations: u8,
) -> (Vec<Scheduled>, PendingProposal) {
    let mut actors = Vec::new();
    let mut pending = None;
    for broker in 0..3 {
        let mut actor = unstarted_actor(controller, io, group, policy, broker, 64);
        if broker == group % 3 {
            let mut buffer = actor.lease_proposal_buffer().unwrap();
            for operation in 0..operations {
                buffer
                    .push(
                        ozzy_journal::operation::OperationKind::Barrier,
                        &[group + operation + 1; 16],
                    )
                    .unwrap();
            }
            pending = Some(actor.take_submitter().unwrap().try_submit(buffer).unwrap());
        }
        actors.push(Scheduled::new(actor).unwrap());
    }
    (actors, pending.unwrap())
}

fn round(actors: &mut [Scheduled], blocked: bool) {
    round_at(actors, Duration::ZERO, blocked);
}

fn round_at(actors: &mut [Scheduled], now: Duration, blocked: bool) {
    for (to, message) in collect(actors, now, blocked) {
        actors[to].receive(&message, now).unwrap();
    }
}

fn collect(actors: &mut [Scheduled], now: Duration, blocked: bool) -> Vec<(usize, Message)> {
    let mut messages = Vec::new();
    for (from, actor) in actors.iter_mut().enumerate() {
        actor.advance(now).unwrap();
        {
            // Every poll cancels its observation, not the accepted file work.
            let mut event = std::pin::pin!(actor.changed(now));
            if let Poll::Ready(result) = poll(event.as_mut()) {
                result.unwrap();
            }
        }
        actor
            .flush(|mut message| {
                if blocked {
                    return Err(omq_tokio::TrySendError::Full(message));
                }
                let route = message.pop_front().unwrap();
                let to = usize::from(route[0] - 1);
                messages.push((
                    to,
                    Message::with_prefix(bytes::Bytes::from(vec![from as u8 + 1; 16]), message),
                ));
                Ok(())
            })
            .unwrap();
    }
    messages
}

#[test]
fn session_replacement_fences_stale_frames_and_repairs_outstanding_proposals() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
        let mut pending = Box::pin(pending);
        let mut stale = None;
        for _ in 0..1000 {
            for (to, message) in collect(&mut actors, Duration::ZERO, false) {
                if opcode(&message) == ozzy_proto::Opcode::PrepareFlow {
                    if to == 1 {
                        stale = Some(message);
                    }
                } else {
                    actors[to].receive(&message, Duration::ZERO).unwrap();
                }
            }
            settle(&mut controller, &[]);
            if stale.is_some() {
                break;
            }
        }
        let stale = stale.expect("proposal must reach transport before reconnect");
        assert!(poll(pending.as_mut()).is_pending());
        let old = LinkSessionId::from_bytes([61; 16]);
        let new = LinkSessionId::from_bytes([99; 16]);
        let before = actors[0].status();
        for (actor, peer) in [(0, 1), (1, 0)] {
            let peer = NodeId::from_bytes([peer + 1; 16]);
            assert!(
                actors[actor]
                    .replace_session(peer, old, new, Duration::ZERO)
                    .unwrap()
            );
            assert!(
                !actors[actor]
                    .replace_session(peer, old, new, Duration::ZERO)
                    .unwrap()
            );
            assert!(matches!(
                actors[actor].replace_session(peer, old, old, Duration::ZERO),
                Err(ScheduleError::Binding)
            ));
        }
        assert_eq!(actors[0].status().scope, before.scope);
        assert_eq!(actors[0].status().normal, before.normal);
        actors[1].receive(&stale, Duration::ZERO).unwrap();
        drain_stale(&mut controller, &mut actors[1]);
        assert_eq!(actors[1].status().normal.unwrap().accepted, Prefix::GENESIS);
        let mut repaired = false;
        let mut confirmed = false;
        for _ in 0..10000 {
            for (to, message) in collect(&mut actors, Duration::ZERO, false) {
                let from = usize::from(message.part_slice(0).unwrap()[0] - 1);
                let expected = if (from, to) == (0, 1) || (from, to) == (1, 0) {
                    new
                } else {
                    old
                };
                if message.len() == 4 {
                    assert_eq!(envelope(&message).session, Some(expected));
                }
                if opcode(&message) == ozzy_proto::Opcode::PrepareFlow {
                    if to == 2 {
                        continue;
                    } // No copy through the other follower.
                    assert_eq!(envelope(&message).session, Some(new));
                    repaired = true;
                }
                actors[to].receive(&message, Duration::ZERO).unwrap();
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
        assert!(
            repaired && confirmed,
            "new session failed to repair and confirm"
        );
        assert_eq!(actors[0].status().scope, before.scope);
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

fn drain_stale(controller: &mut Controller, actor: &mut Scheduled) {
    // Give invalid old traffic enough backend turns to reveal a queued mutation.
    // No newly stamped network traffic reaches this follower during this drain.
    for _ in 0..32 {
        actor.advance(Duration::ZERO).unwrap();
        let mut changed = std::pin::pin!(actor.changed(Duration::ZERO));
        if let Poll::Ready(result) = poll(changed.as_mut()) {
            result.unwrap();
        }
        settle(controller, &[]);
    }
}

fn envelope(message: &Message) -> ozzy_proto::Envelope {
    let frames = std::array::from_fn::<_, 3, _>(|i| message.part_slice(i + 1).unwrap());
    ozzy_proto::decode_packet(&frames, ozzy_proto::EnvelopeLimits::default())
        .unwrap()
        .envelope
}

fn opcode(message: &Message) -> ozzy_proto::Opcode {
    if message.len() == 2 {
        ozzy_proto::Opcode::ReplicaReceipt
    } else {
        envelope(message).opcode
    }
}

fn settle(controller: &mut Controller, held: &[JobId]) {
    for (id, _) in controller.jobs() {
        if !held.contains(&id) {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    }
}

fn close(controller: &mut Controller, actor: Scheduled) {
    let mut closing = std::pin::pin!(actor.shutdown());
    for _ in 0..10000 {
        if let Poll::Ready(result) = poll(closing.as_mut()) {
            result.unwrap();
            return;
        }
        settle(controller, &[]);
    }
    panic!("scheduled actor shutdown did not drain");
}

#[test]
fn shared_scheduler_preserves_other_groups_under_stalled_storage_and_full_transport() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut blocked, first) = cluster(&mut controller, &io, 0, policy);
        let (mut middle, second) = cluster(&mut controller, &io, 1, policy);
        let (mut last, third) = cluster(&mut controller, &io, 2, policy);
        let mut first = Box::pin(first);
        let mut second = Box::pin(second);
        let mut third = Box::pin(third);
        // Reach a real write for the first partition before holding its jobs.
        for _ in 0..1000 {
            round(&mut blocked, false);
            if controller.jobs().iter().any(|(id, _)| {
                matches!(
                    controller.operation(*id).unwrap().unprotected(),
                    Operation::Write { .. }
                )
            }) {
                break;
            }
            settle(&mut controller, &[]);
        }
        let held: Vec<_> = controller.jobs().into_iter().map(|(id, _)| id).collect();
        assert!(!held.is_empty(), "stalled partition must own file work");
        let mut confirmed = [false; 2];
        for _ in 0..10000 {
            round(&mut blocked, true);
            round(&mut middle, false);
            round(&mut last, false);
            settle(&mut controller, &held);
            for (done, reply) in confirmed.iter_mut().zip([&mut second, &mut third]) {
                if !*done && let Poll::Ready(result) = poll(reply.as_mut()) {
                    assert!(matches!(
                        result.unwrap().outcome,
                        ProposalOutcome::Committed { .. }
                    ));
                    *done = true;
                }
            }
            if confirmed == [true; 2] {
                break;
            }
        }
        assert_eq!(confirmed, [true; 2], "unrelated partition lost progress");
        assert!(held.iter().all(|id| controller.operation(*id).is_some()));
        if policy == QuorumPolicy::Durable {
            assert!(poll(first.as_mut()).is_pending());
        }
        // Release both forms of backpressure and finish the original request.
        let mut confirmed = false;
        for _ in 0..10000 {
            round(&mut blocked, false);
            settle(&mut controller, &[]);
            if let Poll::Ready(result) = poll(first.as_mut()) {
                assert!(matches!(
                    result.unwrap().outcome,
                    ProposalOutcome::Committed { .. }
                ));
                confirmed = true;
                break;
            }
        }
        assert!(confirmed);
        for actor in blocked.into_iter().chain(middle).chain(last) {
            close(&mut controller, actor);
        }
    }
}

#[test]
fn external_scheduler_rejects_reversed_time_without_changing_observed_authority() {
    let (mut controller, io) = setup();
    let actor = unstarted_actor(&mut controller, &io, 1, QuorumPolicy::Durable, 0, 64);
    let mut actor = Scheduled::new(actor).unwrap();
    let now = Duration::from_millis(5);
    actor.advance(now).unwrap();
    let before = actor.status();
    assert!(matches!(
        actor.advance(Duration::ZERO),
        Err(ScheduleError::TimeReversed)
    ));
    assert_eq!(actor.status().scope, before.scope);
    actor.advance(now).unwrap();
    close(&mut controller, actor);
}

#[test]
fn scheduled_idle_wait_wakes_for_intake_and_cancellation_cannot_lose_it() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct WakeCount(AtomicUsize);
    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    for cancel in [false, true] {
        let (mut controller, io) = setup();
        let mut actor = unstarted_actor(&mut controller, &io, 0, QuorumPolicy::Durable, 0, 64);
        let mut lane = actor.take_submitter().unwrap();
        let mut buffer = actor.lease_proposal_buffer().unwrap();
        buffer
            .push(ozzy_journal::operation::OperationKind::Barrier, &[77; 16])
            .unwrap();
        let mut actor = Scheduled::new(actor).unwrap();
        actor.advance(Duration::ZERO).unwrap();
        let wake = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(wake.clone());
        let mut cx = Context::from_waker(&waker);
        let mut waiting = Box::pin(actor.changed(Duration::ZERO));
        // Direct owner startup may already have completed an activation action.
        if matches!(waiting.as_mut().poll(&mut cx), Poll::Ready(Ok(()))) {
            drop(waiting);
            actor.advance(Duration::ZERO).unwrap();
            waiting = Box::pin(actor.changed(Duration::ZERO));
        }
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        if cancel {
            drop(waiting);
            let pending = lane.try_submit(buffer).unwrap();
            let mut waiting = Box::pin(actor.changed(Duration::ZERO));
            assert!(matches!(
                waiting.as_mut().poll(&mut cx),
                Poll::Ready(Ok(()))
            ));
            drop(waiting);
            drop(pending);
        } else {
            let pending = lane.try_submit(buffer).unwrap();
            assert!(wake.0.load(Ordering::Relaxed) > 0);
            assert!(matches!(
                waiting.as_mut().poll(&mut cx),
                Poll::Ready(Ok(()))
            ));
            drop(waiting);
            drop(pending);
        }
        close(&mut controller, actor);
    }
}

use super::*;
use crate::replica_actor::{ActorError, ScheduleError, ScheduledRecovery};

const ZERO_SESSION: LinkSessionId = LinkSessionId::from_bytes([0; 16]);

fn turn(actor: &mut ScheduledRecovery, now: Duration) -> Vec<Message> {
    let mut messages = Vec::new();
    actor
        .poll_progress(
            &mut Context::from_waker(Waker::noop()),
            now,
            |_, message| {
                messages.push(message);
                Ok(())
            },
        )
        .unwrap();
    messages
}

fn settle(controller: &mut Controller) {
    for (id, _) in controller.jobs() {
        controller.execute(id, Effect::Normal).unwrap();
        controller.deliver(id).unwrap();
    }
}

fn drain(controller: &mut Controller, future: impl Future<Output = Result<(), ActorError>>) {
    let mut closing = std::pin::pin!(future);
    for _ in 0..10000 {
        if let Poll::Ready(result) = poll(closing.as_mut()) {
            result.unwrap();
            return;
        }
        // Shared journal execution may yield a bounded CPU turn without
        // submitting another file operation. The yield wakes its observer.
        settle(controller);
    }
    panic!("scheduled recovery shutdown did not drain");
}

fn envelope(message: &Message) -> ozzy_proto::Envelope {
    let frames = std::array::from_fn::<_, 3, _>(|index| message.part_slice(index + 1).unwrap());
    ozzy_proto::decode_packet(&frames, ozzy_proto::EnvelopeLimits::default())
        .unwrap()
        .envelope
}

#[test]
fn recovery_retry_retains_latest_sessions_and_starts_new_relative_deadlines() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        retry(policy);
    }
}

fn retry(policy: QuorumPolicy) {
    let (mut controller, io) = setup();
    let (actor, memory) = recovery_actor(&mut controller, io, policy, [ZERO_SESSION; 3]);
    let mut actor = ScheduledRecovery::new(actor);
    actor.bind_receive_owner(&memory).unwrap();
    let origin = Duration::from_secs(3600);
    assert!(turn(&mut actor, origin).is_empty());
    let peer = NodeId::from_bytes([1; 16]);
    let first = LinkSessionId::from_bytes([61; 16]);
    actor
        .replace_session(peer, ZERO_SESSION, first, origin)
        .unwrap();
    let initial = turn(&mut actor, origin)
        .pop()
        .expect("bound recovery request");
    assert_eq!(envelope(&initial).session, Some(first));

    // Abandonment owns a close/reopen future. Its observer can disappear while
    // the backend job and the independently established link keep changing.
    let abandoned = origin + Duration::from_secs(11);
    turn(&mut actor, abandoned);
    turn(&mut actor, abandoned);
    let held = controller.jobs();
    assert!(
        !held.is_empty(),
        "retry must own asynchronous filesystem work"
    );
    let next = LinkSessionId::from_bytes([62; 16]);
    actor.replace_session(peer, first, next, abandoned).unwrap();
    let reopened = origin + Duration::from_secs(600);
    for _ in 0..16 {
        assert!(turn(&mut actor, reopened).is_empty());
    }
    assert_eq!(
        controller.jobs(),
        held,
        "canceling observation duplicated I/O"
    );
    let latest = LinkSessionId::from_bytes([63; 16]);
    actor.replace_session(peer, next, latest, reopened).unwrap();
    assert!(matches!(
        actor.disconnect_session(peer, first, reopened),
        Err(ScheduleError::Binding)
    ));
    let mut request = None;
    for _ in 0..10000 {
        request = turn(&mut actor, reopened).pop();
        if request.is_some() {
            break;
        }
        settle(&mut controller);
    }
    let request =
        request.expect("reopened attempt must send without an immediate second abandonment");
    assert_eq!(envelope(&request).session, Some(latest));
    assert_ne!(
        request.part_slice(2),
        initial.part_slice(2),
        "retry nonce/correlation must change"
    );
    assert_eq!(actor.status().scope.view, 0);
    assert!(!actor.status().application_ready);
    assert!(actor.status().normal.is_none());
    let before_stall = reopened + Duration::from_secs(9);
    assert!(!turn(&mut actor, before_stall).is_empty());
    assert!(
        controller.jobs().is_empty(),
        "fresh attempt abandoned before its deadline"
    );
    assert!(matches!(
        actor.receive(&initial, origin),
        Err(ScheduleError::TimeReversed)
    ));
    drain(&mut controller, actor.shutdown());
}

#[test]
fn scheduled_recovery_handoff_preserves_general_memory_and_requires_election() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        rebuild(policy);
    }
}

#[test]
fn recovery_shutdown_settles_a_transition_with_canceled_observations() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (actor, _) = recovery_actor(&mut controller, io, policy, [ZERO_SESSION; 3]);
        let mut actor = ScheduledRecovery::new(actor);
        turn(&mut actor, Duration::ZERO);
        let abandoned = Duration::from_secs(11);
        turn(&mut actor, abandoned);
        assert!(actor.transition_pending());
        turn(&mut actor, abandoned);
        let held = controller.jobs();
        assert!(!held.is_empty());
        let mut closing = Box::pin(actor.shutdown());
        for _ in 0..16 {
            assert!(poll(closing.as_mut()).is_pending());
        }
        assert_eq!(controller.jobs(), held);
        drain(&mut controller, closing);
        assert!(
            controller.jobs().is_empty(),
            "transition leaked admitted I/O"
        );
    }
}

fn rebuild(policy: QuorumPolicy) {
    let (mut controller, io) = setup();
    let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
    let mut pending = Box::pin(pending);
    let mut confirmed = false;
    for _ in 0..10000 {
        round(&mut actors, Duration::ZERO);
        settle(&mut controller);
        if let Poll::Ready(reply) = poll(pending.as_mut()) {
            assert!(
                matches!(reply.unwrap().outcome, ProposalOutcome::Committed { through, .. } if through.op.0 == 1)
            );
            confirmed = true;
            break;
        }
    }
    assert!(confirmed);
    let (actor, memory) = recovery_actor(&mut controller, io, policy, actor_config().sessions);
    let mut recovering = ScheduledRecovery::new(actor);
    recovering.bind_receive_owner(&memory).unwrap();
    finish_transfer(&mut controller, &mut actors, &mut recovering);
    assert!(!recovering.status().application_ready);
    turn(&mut recovering, Duration::ZERO);
    let held = controller.jobs();
    assert!(
        !held.is_empty(),
        "handoff must close and reopen its journal"
    );
    let peer = NodeId::from_bytes([1; 16]);
    let first = actor_config().sessions[0];
    let next = LinkSessionId::from_bytes([62; 16]);
    let latest = LinkSessionId::from_bytes([63; 16]);
    recovering
        .replace_session(peer, first, next, Duration::ZERO)
        .unwrap();
    for _ in 0..16 {
        assert!(turn(&mut recovering, Duration::ZERO).is_empty());
    }
    assert_eq!(controller.jobs(), held, "handoff duplicated I/O");
    recovering
        .replace_session(peer, next, latest, Duration::ZERO)
        .unwrap();
    let mut replacement = None;
    for _ in 0..10000 {
        turn(&mut recovering, Duration::ZERO);
        replacement = recovering.take_ready();
        if replacement.is_some() {
            break;
        }
        settle(&mut controller);
    }
    let mut replacement = replacement.expect("shared recovery must publish and reopen");
    assert!(!replacement.status().application_ready);
    assert!(replacement.receive_receipt().is_none());
    assert!(
        replacement
            .disconnect_session(peer, latest, Duration::ZERO)
            .unwrap()
    );
    replacement
        .replace_session(peer, ZERO_SESSION, first, Duration::ZERO)
        .unwrap();
    // Follower staging and normal proposal work share the bounded shard owner.
    let mut proposal = replacement
        .lease_proposal_buffer_with_limits(pipeline())
        .unwrap();
    proposal
        .push(ozzy_journal::operation::OperationKind::Barrier, &[77; 16])
        .unwrap();
    drop(proposal);
    activate(&mut controller, &mut actors, &mut replacement);
    drain(&mut controller, recovering.shutdown());
    drain(&mut controller, replacement.shutdown());
    for actor in actors {
        close(&mut controller, actor);
    }
    assert!(controller.jobs().is_empty(), "shutdown leaked admitted I/O");
}

fn finish_transfer(
    controller: &mut Controller,
    actors: &mut [Actor],
    recovering: &mut ScheduledRecovery,
) {
    for _ in 0..10000 {
        let mut messages = donor_turns(actors, Duration::ZERO);
        messages.extend(
            turn(recovering, Duration::ZERO)
                .into_iter()
                .map(|message| replace_sender(message, 2)),
        );
        for (to, message) in messages {
            if to == 2 {
                recovering.receive(&message, Duration::ZERO).unwrap();
            } else {
                actors[to].receive(&message, Duration::ZERO).unwrap();
            }
        }
        assert!(
            !recovering.status().application_ready,
            "recovery cannot vote or serve clients"
        );
        if recovering.transition_pending() {
            return;
        }
        settle(controller);
    }
    panic!("recovery did not complete history publication");
}

fn activate(
    controller: &mut Controller,
    actors: &mut [Actor],
    replacement: &mut crate::replica_actor::ScheduledReplica,
) {
    let mut activated = false;
    for step in 0..10000 {
        let now = Duration::from_millis(step / 100);
        let mut messages = donor_turns(actors, now);
        replacement.advance(now).unwrap();
        if let Poll::Ready(result) = poll(Box::pin(replacement.changed(now)).as_mut()) {
            result.unwrap();
        }
        replacement
            .flush(|message| {
                messages.push(replace_sender(message, 2));
                Ok(())
            })
            .unwrap();
        for (to, message) in messages {
            if to == 2 {
                replacement.receive(&message, now).unwrap();
            } else {
                actors[to].receive(&message, now).unwrap();
            }
        }
        settle(controller);
        if replacement.status().application_ready {
            assert!(replacement.status().scope.view > 0);
            assert_eq!(replacement.status().normal.unwrap().applied.op.0, 1);
            replacement.advance(now).unwrap();
            let (_, report) = replacement
                .receive_receipt()
                .expect("replacement is a follower");
            assert_eq!(
                report.received,
                replacement.status().normal.unwrap().accepted
            );
            activated = true;
            break;
        }
    }
    assert!(activated, "recovered actor requires and completes election");
}

fn donor_turns(actors: &mut [Actor], now: Duration) -> Vec<(usize, Message)> {
    let mut messages = Vec::new();
    for (from, actor) in actors.iter_mut().enumerate().take(2) {
        actor.advance(now).unwrap();
        if let Poll::Ready(result) = poll(Box::pin(actor.complete_disk(now)).as_mut()) {
            result.unwrap();
        }
        actor
            .flush(|message| {
                messages.push(replace_sender(message, from));
                Ok(())
            })
            .unwrap();
    }
    messages
}

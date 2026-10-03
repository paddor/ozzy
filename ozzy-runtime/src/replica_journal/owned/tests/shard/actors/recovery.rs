use super::*;
use crate::replica_actor::{
    ActorIds, RecoveryActor, RecoveryTiming,
    simulation::{ControlledRecovery, RecoveryTransition},
};
use crate::replica_journal::{
    OwnedRecoveringJournal, OwnedRecoveryGenerations, OwnedRecoveryOpen, ShardRecoveringJournal,
};

type Recovering = ControlledRecovery<ShardRecoveringJournal>;
mod scheduled;

fn recovery(controller: &mut Controller, io: Local, policy: QuorumPolicy) -> Recovering {
    ControlledRecovery::new(recovery_actor(controller, io, policy, actor_config().sessions).0)
}

fn recovery_actor(
    controller: &mut Controller,
    io: Local,
    policy: QuorumPolicy,
    sessions: [LinkSessionId; 3],
) -> (RecoveryActor<ShardRecoveringJournal>, crate::memory::Owner) {
    let mut config = config("/replacement", 4, policy);
    config.identity.replica_node_id = NodeId::from_bytes([3; 16]);
    config.append_buffers = 12;
    let (mut owner, startup) = drive(
        controller,
        OwnedRecoveringJournal::start(
            config,
            io,
            OwnedRecoveryGenerations {
                attempt: JournalGeneration(900),
                temporary: JournalGeneration(901),
            },
            OwnedRecoveryOpen::FormatNew {
                segment_capacity: 32768,
            },
        ),
    )
    .unwrap();
    let memory = payload_owner(1024 * 1024);
    owner.bind_append_memory(&memory).unwrap();
    let journal =
        ShardRecoveringJournal::from_owned(owner, ShardJournalConfig::default(), || 123).unwrap();
    let ids = ActorIds::deterministic(std::num::NonZeroU64::new(9).unwrap());
    (
        RecoveryActor::new_with_ids(
            journal,
            startup,
            ActorConfig {
                sessions,
                ..actor_config()
            },
            RecoveryTiming::default(),
            ids,
        )
        .unwrap(),
        memory,
    )
}

fn replace_sender(mut message: Message, from: usize) -> (usize, Message) {
    let route = message.pop_front().unwrap();
    let to = usize::from(route[0] - 1);
    (
        to,
        Message::with_prefix(bytes::Bytes::from(vec![from as u8 + 1; 16]), message),
    )
}

fn round_recovery(actors: &mut [Actor], recovering: &mut Recovering, now: Duration) {
    let mut messages = Vec::new();
    for (from, actor) in actors.iter_mut().enumerate().take(2) {
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
    recovering.advance(now).unwrap();
    if !recovering.transition_pending() {
        let mut disk = std::pin::pin!(recovering.complete_disk(now));
        if let Poll::Ready(result) = poll(disk.as_mut()) {
            result.unwrap();
        }
    }
    if !recovering.transition_pending() {
        recovering
            .flush(|message| {
                messages.push(replace_sender(message, 2));
                Ok(())
            })
            .unwrap();
    }
    for (to, message) in messages {
        if to == 2 {
            if !recovering.transition_pending() {
                recovering.receive(&message, now).unwrap();
            }
        } else {
            actors[to].receive(&message, now).unwrap();
        }
    }
}

#[test]
fn shard_owned_recovery_actor_publishes_and_rejoins_without_journal_workers() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        rebuild(policy);
    }
}

fn rebuild(policy: QuorumPolicy) {
    let (mut controller, io) = setup();
    let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
    let mut pending = Box::pin(pending);
    let mut confirmed = false;
    for _ in 0..10000 {
        round(&mut actors, Duration::ZERO);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        if let Poll::Ready(reply) = poll(pending.as_mut()) {
            assert!(
                matches!(reply.unwrap().outcome, ProposalOutcome::Committed { through, .. } if through.op.0 == 1)
            );
            confirmed = true;
            break;
        }
    }
    assert!(confirmed);
    let mut recovering = recovery(&mut controller, io, policy);
    let mut finished = false;
    for _ in 0..10000 {
        round_recovery(&mut actors, &mut recovering, Duration::ZERO);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        if recovering.transition_pending() {
            finished = true;
            break;
        }
    }
    assert!(
        finished,
        "recovery did not finish: {:?}",
        recovering.status()
    );
    assert!(!recovering.status().application_ready);
    assert!(recovering.status().normal.is_none());
    let mut transition = Box::pin(recovering.transition());
    let mut replacement = None;
    for _ in 0..10000 {
        if let Poll::Ready(result) = poll(transition.as_mut()) {
            let RecoveryTransition::Normal(actor) = result.unwrap() else {
                panic!("published recovery must reopen for election");
            };
            replacement = Some(ControlledReplica::new(*actor));
            break;
        }
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    }
    let replacement = replacement.expect("recovery adoption");
    assert!(!replacement.status().application_ready);
    let old = std::mem::replace(&mut actors[2], replacement);
    close(&mut controller, old);
    let mut activated = false;
    for step in 0..10000 {
        round(&mut actors, Duration::from_millis(step / 100));
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        if actors.iter().all(|actor| {
            let status = actor.status();
            status.application_ready
                && status.scope.view > 0
                && status.normal.is_some_and(|normal| normal.applied.op.0 == 1)
        }) {
            activated = true;
            break;
        }
    }
    assert!(activated, "recovered broker failed election/activation");
    for actor in actors {
        close(&mut controller, actor);
    }
}

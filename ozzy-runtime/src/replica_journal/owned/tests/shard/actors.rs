use super::*;
use crate::replica_actor::{
    ActorConfig, PendingProposal, ProposalOutcome, ReplicaActor, SyncBatchTarget,
    simulation::ControlledReplica,
};
use crate::replica_journal::InstallationConfig;
use crate::replica_transport::QueueLimits;
use omq_tokio::Message;
use ozzy_proto::LinkSessionId;
mod identities;
mod recovery;
mod scheduled;

type Actor = ControlledReplica<ShardJournal>;

fn actor_config() -> ActorConfig {
    ActorConfig {
        sessions: [LinkSessionId::from_bytes([61; 16]); 3],
        timing: timing(),
        flow_probe: ozzy_replication::flow::ProbeTiming {
            initial: Duration::from_millis(10),
            maximum: Duration::from_millis(100),
        },
        pipeline: pipeline(),
        replay_cache: pipeline(),
        transfer: pipeline(),
        sync_batch_target: SyncBatchTarget::half_window(pipeline()),
        sync_batch_max_age: Duration::from_millis(100),
        proposal_lanes: 1,
        proposal_capacity: 4,
        control: QueueLimits {
            messages: 16,
            bytes: 16384,
            message_bytes: 1024,
        },
        data: QueueLimits {
            messages: 8,
            bytes: 131_072,
            message_bytes: 16384,
        },
        installation: InstallationConfig {
            segment_capacity: 32768,
            body_encoding: ozzy_journal_segment::BodyEncoding::Raw,
            max_staged_bytes: 131_072,
            max_orphan_probes: 16,
        },
    }
}

fn cluster(
    controller: &mut Controller,
    io: &Local,
    group: u8,
    policy: QuorumPolicy,
) -> (Vec<Actor>, PendingProposal) {
    let (actors, mut pending, _) = cluster_with_proposals(controller, io, group, policy, 64, 1, 1);
    (actors, pending.pop().unwrap())
}

fn cluster_with_proposals(
    controller: &mut Controller,
    io: &Local,
    group: u8,
    policy: QuorumPolicy,
    identities: usize,
    proposals: usize,
    operations: u8,
) -> (
    Vec<Actor>,
    Vec<PendingProposal>,
    crate::replica_actor::ProposalSubmitter,
) {
    let mut actors = Vec::new();
    let mut pending = Vec::new();
    let mut submitter = None;
    let leader = group % 3;
    for broker in 0..3 {
        let mut actor = unstarted_actor(controller, io, group, policy, broker, identities);
        if broker == leader {
            let mut lane = actor.take_submitter().unwrap();
            for proposal in 0..proposals {
                let mut buffer = actor.lease_proposal_buffer().unwrap();
                for operation in 0..operations {
                    let id = u8::try_from(proposal).unwrap() * operations + operation + group + 1;
                    buffer
                        .push(ozzy_journal::operation::OperationKind::Barrier, &[id; 16])
                        .unwrap();
                }
                pending.push(lane.try_submit(buffer).unwrap());
            }
            submitter = Some(lane);
        }
        actors.push(ControlledReplica::new(actor));
    }
    (actors, pending, submitter.unwrap())
}

fn round(actors: &mut [Actor], now: Duration) {
    round_without(actors, now, None);
}

fn round_without(actors: &mut [Actor], now: Duration, missing: Option<usize>) {
    let mut messages = Vec::new();
    for (from, actor) in actors.iter_mut().enumerate() {
        if Some(from) == missing {
            continue;
        }
        actor.advance(now).unwrap();
        {
            let mut disk = std::pin::pin!(actor.complete_disk(now));
            if let Poll::Ready(result) = poll(disk.as_mut()) {
                result.unwrap();
            }
        }
        actor
            .flush(|mut message| {
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
    for (to, message) in messages {
        if Some(to) != missing {
            actors[to].receive(&message, now).unwrap();
        }
    }
}

#[test]
fn shard_owned_production_actors_confirm_independent_groups_with_different_leaders() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (first, first_result) = cluster(&mut controller, &io, 0, policy);
        let (second, second_result) = cluster(&mut controller, &io, 1, policy);
        let mut groups = [first, second];
        let mut results = [Some(Box::pin(first_result)), Some(Box::pin(second_result))];
        for step in 0..10000 {
            let now = Duration::from_millis(step / 100);
            for group in &mut groups {
                round(group, now);
            }
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
            for result in &mut results {
                if let Some(pending) = result
                    && let Poll::Ready(reply) = poll(pending.as_mut())
                {
                    assert!(
                        matches!(reply.unwrap().outcome, ProposalOutcome::Committed { through, .. } if through.op.0 == 1)
                    );
                    *result = None;
                }
            }
            if results.iter().all(Option::is_none) {
                break;
            }
        }
        assert!(
            results.iter().all(Option::is_none),
            "production actors failed to confirm"
        );
        for actor in groups.into_iter().flatten() {
            close(&mut controller, actor);
        }
    }
}

fn close(controller: &mut Controller, actor: Actor) {
    let mut close = Box::pin(actor.shutdown());
    for _ in 0..10000 {
        if let Poll::Ready(result) = poll(close.as_mut()) {
            result.unwrap();
            return;
        }
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
    }
    panic!("actor shutdown did not drain");
}

#[test]
fn shard_owned_actor_leader_change_preserves_confirmed_history() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut actors, pending) = cluster(&mut controller, &io, 0, policy);
        let mut pending = Box::pin(pending);
        let mut confirmed = false;
        for step in 0..10000 {
            round(&mut actors, Duration::from_millis(step / 100));
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
        let mut elected = false;
        for step in 0..20000 {
            let now = Duration::from_secs(3) + Duration::from_millis(step / 100);
            round_without(&mut actors, now, Some(0));
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
            if actors[1..].iter().all(|actor| {
                let status = actor.status();
                status.application_ready
                    && status.scope.view > 0
                    && status.normal.is_some_and(|normal| normal.applied.op.0 == 1)
            }) {
                elected = true;
                break;
            }
        }
        assert!(
            elected,
            "surviving pair failed to activate selected history: {:?}",
            actors.iter().map(Actor::status).collect::<Vec<_>>()
        );
        for actor in actors {
            close(&mut controller, actor);
        }
    }
}

fn unstarted_actor(
    controller: &mut Controller,
    io: &Local,
    group: u8,
    policy: QuorumPolicy,
    broker: u8,
    identities: usize,
) -> ReplicaActor<ShardJournal> {
    unstarted_actor_with_sessions(
        controller,
        io,
        group,
        policy,
        broker,
        identities,
        actor_config().sessions,
    )
}

fn unstarted_actor_with_sessions(
    controller: &mut Controller,
    io: &Local,
    group: u8,
    policy: QuorumPolicy,
    broker: u8,
    identities: usize,
    sessions: [LinkSessionId; 3],
) -> ReplicaActor<ShardJournal> {
    let leader = group % 3;
    let mut config = config(&format!("/g{group}-b{broker}"), group + 4, policy);
    let mut voters = std::array::from_fn(|n| ConfiguredVoter {
        node_id: NodeId::from_bytes([n as u8 + 1; 16]),
        principal: Digest::from_bytes([n as u8 + 20; 32]),
    });
    voters.rotate_left(usize::from(leader));
    config.configuration = ozzy_replication::ConfigurationRecord::with_policy(
        config.identity.group_id,
        1,
        voters,
        policy,
    )
    .unwrap();
    config.identity.replica_node_id = NodeId::from_bytes([broker + 1; 16]);
    config.append_buffers = 12;
    config.recovery.retained_identities = if broker == leader { identities } else { 8 };
    let (owner, startup) = drive(
        controller,
        OwnedJournal::format_new(
            config,
            io.clone(),
            JournalGeneration(u128::from(group) * 10 + u128::from(broker) + 1),
            32768,
        ),
    )
    .unwrap();
    let journal = owner
        .into_shard_journal(
            ShardJournalConfig {
                turn_steps: 4,
                ..Default::default()
            },
            || 123,
        )
        .unwrap();
    let ids = crate::replica_actor::ActorIds::deterministic(
        std::num::NonZeroU64::new(u64::from(group) * 10 + u64::from(broker) + 1).unwrap(),
    );
    let mut actor = ReplicaActor::new_with_ids(
        journal,
        startup,
        ActorConfig {
            sessions,
            ..actor_config()
        },
        ids,
    )
    .unwrap();
    actor.enable_recovery();
    actor
}

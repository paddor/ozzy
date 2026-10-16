use super::dedup::cluster_on;
use super::*;
use crate::replica_actor::ScheduledReplica;

#[test]
fn durable_commit_waits_for_the_leaders_own_barrier_after_follower_acks() {
    // Each broker owns its device, so only the leader's record I/O stalls.
    // Both followers persist and acknowledge, which cannot stand in for the
    // leader's own durable copy under disk quorum.
    let mut devices: Vec<_> = (0..3).map(|_| setup()).collect();
    let limits =
        crate::replica_journal::dedup_limits(300_000, 32, 256 * 1024, 8, actor_config().pipeline);
    let (mut actors, bootstrap, mut lane, [mut buffer, ..]) = {
        let mut borrowed: Vec<_> = devices
            .iter_mut()
            .map(|(controller, io)| (controller, &*io))
            .collect();
        cluster_on(&mut borrowed, limits)
    };
    let settle = |devices: &mut [(Controller, Local)], stalled: bool| {
        for (index, (controller, _)) in devices.iter_mut().enumerate() {
            let mut held = Vec::new();
            if stalled && index == 0 {
                for (id, _) in controller.jobs() {
                    if matches!(
                        controller
                            .operation(id)
                            .map(ozzy_io::Operation::unprotected),
                        Some(ozzy_io::Operation::Write { .. } | ozzy_io::Operation::Sync { .. })
                    ) {
                        held.push(id);
                    }
                }
            }
            scheduled::settle(controller, &held);
        }
    };
    let mut bootstrap = Box::pin(bootstrap);
    for _ in 0..10000 {
        scheduled::round(&mut actors, false);
        settle(&mut devices, false);
        if poll(bootstrap.as_mut()).is_ready() {
            break;
        }
    }
    let committed = actors[0].status().normal.unwrap().applied;
    buffer
        .prepare_append(super::super::super::proposal::request(12, 0, 1))
        .unwrap();
    let mut pending = Box::pin(lane.try_submit(buffer).unwrap());
    for _ in 0..2000 {
        scheduled::round(&mut actors, false);
        settle(&mut devices, true);
        assert!(poll(pending.as_mut()).is_pending());
    }
    for follower in &actors[1..] {
        assert!(follower_accepted(follower) > committed.op.0);
    }
    assert_eq!(actors[0].status().normal.unwrap().applied, committed);
    for _ in 0..10000 {
        scheduled::round(&mut actors, false);
        settle(&mut devices, false);
        if let Poll::Ready(reply) = poll(pending.as_mut()) {
            assert!(matches!(
                reply.unwrap().outcome,
                ProposalOutcome::Committed { .. }
            ));
            for (actor, (controller, _)) in actors.into_iter().zip(&mut devices) {
                scheduled::close(controller, actor);
            }
            return;
        }
    }
    panic!("released leader barrier did not confirm the proposal");
}

fn follower_accepted(actor: &ScheduledReplica) -> u64 {
    actor.status().normal.unwrap().accepted.op.0
}

use super::*;
use crate::replica_actor::{LocalActor, LocalActorConfig, ProposalOutcome};
use ozzy_journal::operation::OperationKind;

pub(super) fn actor(controller: &mut Controller, io: Local, id: u8) -> LocalActor {
    actor_with_writers(controller, io, id, &[12, 30])
}

pub(super) fn actor_with_writers(
    controller: &mut Controller,
    io: Local,
    id: u8,
    writers: &[u8],
) -> LocalActor {
    actor_with_partition(controller, io, id, writers, partition(), PartitionId::ZERO)
}

pub(super) fn actor_with_partition(
    controller: &mut Controller,
    io: Local,
    id: u8,
    writers: &[u8],
    incarnation: PartitionIncarnation,
    number: PartitionId,
) -> LocalActor {
    actor_with_partition_memory(controller, io, id, writers, incarnation, number, None)
}

pub(super) fn actor_with_partition_memory(
    controller: &mut Controller,
    io: Local,
    id: u8,
    writers: &[u8],
    incarnation: PartitionIncarnation,
    number: PartitionId,
    memory: Option<&crate::memory::Owner>,
) -> LocalActor {
    let mut config = local_config();
    config.root = format!("/local-{id}").into();
    config.identity.group_id = GroupId::from_bytes([id; 16]);
    config.configuration = Configuration::new(
        config.identity.group_id,
        1,
        config.identity.replica_node_id,
        Digest::from_bytes([20; 32]),
    )
    .unwrap();
    config.recovery.retained_identities = 8;
    config.append_buffers = 12;
    let (mut journal, mut driver) = drive(
        controller,
        OwnedJournal::format_local(config, io, JournalGeneration(u128::from(id)), 32768),
    )
    .unwrap();
    if let Some(memory) = memory {
        journal.bind_append_memory(memory).unwrap();
    }
    initialize_partition(
        controller,
        &mut journal,
        &mut driver,
        writers,
        incarnation,
        number,
    );
    drive(
        controller,
        journal.refresh_identities(driver.begin_validation().unwrap()),
    )
    .unwrap();
    LocalActor::new(
        journal,
        driver,
        LocalActorConfig {
            proposal_capacity: 4,
            turn_steps: 4,
            journal: crate::replica_journal::ShardJournalConfig {
                turn_steps: 4,
                ..Default::default()
            },
            ..Default::default()
        },
        || 777,
    )
    .unwrap()
}

pub(super) fn pump(actor: &mut LocalActor) {
    assert!(
        actor
            .poll_progress(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
}

pub(super) fn close(controller: &mut Controller, actor: LocalActor) {
    let mut closing = Box::pin(actor.shutdown());
    for _ in 0..10000 {
        if let Poll::Ready(result) = poll(closing.as_mut()) {
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
fn local_actor_keeps_other_partitions_running_while_one_write_is_stalled() {
    let (mut controller, io) = setup();
    let mut blocked = actor(&mut controller, io.clone(), 1);
    let mut healthy = actor(&mut controller, io, 2);
    let mut first = blocked.lease_proposal_buffer().unwrap();
    first.prepare_append(request(12, 0, 1)).unwrap();
    let mut abandoned = Box::pin(blocked.take_submitter().unwrap().try_submit(first).unwrap());
    for _ in 0..100 {
        pump(&mut blocked);
    }
    let held: Vec<_> = controller.jobs().into_iter().map(|(id, _)| id).collect();
    assert_ne!(held.len(), 0);
    assert!(poll(abandoned.as_mut()).is_pending());
    assert_eq!(blocked.snapshot().applied.op.0, 3);
    assert_eq!(blocked.snapshot().accepted.op.0, 4);
    drop(abandoned);
    let mut second = healthy.lease_proposal_buffer().unwrap();
    second.prepare_append(request(12, 0, 1)).unwrap();
    let mut pending = Box::pin(
        healthy
            .take_submitter()
            .unwrap()
            .try_submit(second)
            .unwrap(),
    );
    let mut confirmed = false;
    for _ in 0..10000 {
        pump(&mut blocked);
        pump(&mut healthy);
        for (id, _) in controller.jobs() {
            if !held.contains(&id) {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
        if let Poll::Ready(reply) = poll(pending.as_mut()) {
            assert!(
                matches!(reply.unwrap().outcome, ProposalOutcome::Committed { through, .. } if through.op.0 == 4)
            );
            confirmed = true;
            break;
        }
    }
    assert!(confirmed, "healthy partition stalled");
    assert!(held.iter().all(|id| controller.operation(*id).is_some()));
    assert_eq!(blocked.snapshot().applied.op.0, 3);
    // Drain also owns the accepted request whose observer was canceled.
    close(&mut controller, blocked);
    close(&mut controller, healthy);
}

#[test]
fn local_actor_refreshes_control_capacity_and_drains_queued_shutdown() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut lane = actor.take_submitter().unwrap();
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    for id in 40..64 {
        buffer.push(OperationKind::Barrier, &[id; 16]).unwrap();
        let mut pending = Box::pin(lane.try_submit(buffer).unwrap());
        let mut result = None;
        for _ in 0..10000 {
            pump(&mut actor);
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
            if let Poll::Ready(reply) = poll(pending.as_mut()) {
                result = Some(reply.unwrap());
                break;
            }
        }
        let reply = result.expect("control intake must survive bounded cache refresh");
        assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }));
        buffer = reply.buffer;
        buffer.clear();
    }
    assert_eq!(actor.snapshot().applied.op.0, 27);
    buffer.push(OperationKind::Barrier, &[90; 16]).unwrap();
    let mut pending = Box::pin(lane.try_submit(buffer).unwrap());
    close(&mut controller, actor);
    let Poll::Ready(reply) = poll(pending.as_mut()) else {
        panic!("shutdown must resolve queued intake");
    };
    assert!(matches!(
        reply.unwrap().outcome,
        ProposalOutcome::NotAdmitted
    ));
}

#[test]
fn local_actor_pipelines_independent_writers_and_retries_without_new_offsets() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut lane = actor.take_submitter().unwrap();
    let requests = [(12, 0), (30, 0), (12, 1), (30, 1)];
    let mut pending = Vec::new();
    for (writer, sequence) in requests {
        let mut buffer = actor.lease_proposal_buffer().unwrap();
        buffer.prepare_append(request(writer, sequence, 1)).unwrap();
        pending.push(Some(Box::pin(lane.try_submit(buffer).unwrap())));
    }
    let mut extra = actor.lease_proposal_buffer().unwrap();
    extra.prepare_append(request(12, 2, 1)).unwrap();
    assert!(matches!(
        lane.try_submit(extra).unwrap_err().reason,
        crate::replica_actor::ProposalSubmitError::Full
    ));
    let mut reusable = None;
    for _ in 0..10000 {
        pump(&mut actor);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        for (index, slot) in pending.iter_mut().enumerate() {
            let Some(work) = slot else { continue };
            if let Poll::Ready(result) = poll(work.as_mut()) {
                let reply = result.unwrap();
                assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }));
                assert_eq!(
                    coordinates(&reply.buffer.0),
                    [(requests[index].1, index as u64, 777, 1)]
                );
                reusable = Some(reply.buffer);
                *slot = None;
            }
        }
        if pending.iter().all(Option::is_none) {
            break;
        }
    }
    assert!(pending.iter().all(Option::is_none));
    let accepted = actor.snapshot().accepted;
    let mut buffer = reusable.unwrap();
    buffer.clear();
    buffer.prepare_append(request(12, 0, 1)).unwrap();
    let mut retry = Box::pin(lane.try_submit(buffer).unwrap());
    let mut resolved = false;
    for _ in 0..10000 {
        pump(&mut actor);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        if let Poll::Ready(result) = poll(retry.as_mut()) {
            let reply = result.unwrap();
            assert!(
                matches!(reply.outcome, ProposalOutcome::Committed { through, .. } if through.op.0 == 4)
            );
            assert_eq!(coordinates(&reply.buffer.0), [(0, 0, 777, 1)]);
            resolved = true;
            break;
        }
    }
    assert!(resolved);
    assert_eq!(actor.snapshot().accepted, accepted);
    close(&mut controller, actor);
}

#[test]
fn local_actor_shutdown_drains_after_io_failure_and_closes_callers() {
    let (mut controller, io) = setup();
    let mut actor = actor(&mut controller, io, 1);
    let mut lane = actor.take_submitter().unwrap();
    let mut buffer = actor.lease_proposal_buffer().unwrap();
    buffer.prepare_append(request(12, 0, 1)).unwrap();
    let mut pending = Box::pin(lane.try_submit(buffer).unwrap());
    for _ in 0..100 {
        pump(&mut actor);
    }
    assert_eq!(actor.snapshot().accepted.op.0, 4);
    assert_eq!(actor.snapshot().applied.op.0, 3);
    let mut shutdown = Box::pin(actor.shutdown());
    let mut injected = false;
    let mut finished = false;
    for _ in 0..10000 {
        if let Poll::Ready(result) = poll(shutdown.as_mut()) {
            assert!(result.is_err());
            finished = true;
            break;
        }
        for (id, _) in controller.jobs() {
            let fail = !injected
                && matches!(
                    controller.operation(id).unwrap().unprotected(),
                    Operation::Write { .. }
                );
            injected |= fail;
            controller
                .execute(
                    id,
                    if fail {
                        Effect::FailAfter(std::io::ErrorKind::Other)
                    } else {
                        Effect::Normal
                    },
                )
                .unwrap();
            controller.deliver(id).unwrap();
        }
    }
    assert!(injected && finished);
    assert_eq!(controller.jobs().len(), 0);
    let Poll::Ready(result) = poll(pending.as_mut()) else {
        panic!("failed actor left caller waiting");
    };
    assert!(result.is_err());
}

#[test]
fn local_actor_waits_for_retained_backing_before_installing_the_next_append() {
    let (mut controller, io) = setup();
    let memory = payload_owner(32768);
    let mut actor = actor_with_partition_memory(
        &mut controller,
        io,
        1,
        &[12],
        partition(),
        PartitionId::ZERO,
        Some(&memory),
    );
    memory.trim_cache();
    let cached = [
        memory.try_lease(8192).unwrap(),
        memory.try_lease(8192).unwrap(),
    ];
    drop(cached);
    let mut first = actor.lease_proposal_buffer().unwrap();
    first.prepare_append(request(12, 0, 1)).unwrap();
    let mut second = actor.lease_proposal_buffer().unwrap();
    second.prepare_append(request(12, 1, 1)).unwrap();
    let mut lane = actor.take_submitter().unwrap();
    let mut first = Box::pin(lane.try_submit(first).unwrap());
    let mut held = Vec::new();
    for _ in 0..10000 {
        pump(&mut actor);
        for (id, _) in controller.jobs() {
            if matches!(controller.operation(id).unwrap().unprotected(), Operation::Write { offset, .. } if *offset >= 4096)
            {
                held.push(id);
            } else {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
        if !held.is_empty() {
            break;
        }
    }
    assert_ne!(held.len(), 0);
    let mut second = Box::pin(lane.try_submit(second).unwrap());
    for _ in 0..32 {
        pump(&mut actor);
    }
    assert!(poll(first.as_mut()).is_pending());
    assert!(poll(second.as_mut()).is_pending());
    assert!(held.iter().all(|id| controller.operation(*id).is_some()));
    let mut done = [false; 2];
    for _ in 0..10000 {
        pump(&mut actor);
        for (id, _) in controller.jobs() {
            controller.execute(id, Effect::Normal).unwrap();
            controller.deliver(id).unwrap();
        }
        for (index, pending) in [&mut first, &mut second].into_iter().enumerate() {
            if !done[index]
                && let Poll::Ready(reply) = poll(pending.as_mut())
            {
                let reply = reply.unwrap();
                assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }));
                assert_eq!(
                    coordinates(&reply.buffer.0),
                    [(index as u64, index as u64, 777, 1)]
                );
                done[index] = true;
            }
        }
        if done == [true; 2] {
            break;
        }
    }
    assert_eq!(done, [true; 2]);
    close(&mut controller, actor);
}

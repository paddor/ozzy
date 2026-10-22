use super::*;
use crate::replica_actor::{ProposalReply, ProposalSubmitter, ScheduledReplica};
use crate::replica_journal::ProposalBuffer;
use ozzy_journal::operation::{
    CreatePartition, OpenProducer, OperationBody, OperationLimits, RetentionPolicy,
    decode_resolved_append, encode_operation_body,
};
use ozzy_proto::{
    OperationId, OwnerEpoch, PartitionId, PartitionIncarnation, ProducerEpoch, ProducerId,
};

fn cluster(
    controller: &mut Controller,
    io: &Local,
) -> (
    Vec<ScheduledReplica>,
    PendingProposal,
    ProposalSubmitter,
    [ProposalBuffer; 4],
) {
    let limits = ozzy_core::dedup::Limits {
        window_millis: 300_000,
        ..super::super::super::dedup::limits()
    };
    cluster_on(&mut [(controller, io)], limits)
}

/// Broker `n` formats and runs on `devices[n % devices.len()]`.
pub(super) fn cluster_on(
    devices: &mut [(&mut Controller, &Local)],
    limits: ozzy_core::dedup::Limits,
) -> (
    Vec<ScheduledReplica>,
    PendingProposal,
    ProposalSubmitter,
    [ProposalBuffer; 4],
) {
    let mut actors = Vec::new();
    let mut bootstrap = None;
    for broker in 0..3 {
        let count = devices.len();
        let (controller, io) = &mut devices[usize::from(broker) % count];
        let mut actor = configured_actor(
            controller,
            io,
            0,
            broker,
            ActorSettings {
                deduplication: Some(limits),
                ..Default::default()
            },
        );
        if broker == 0 {
            let mut buffer = actor.lease_proposal_buffer().unwrap();
            let create = OperationBody::CreatePartition(CreatePartition {
                partition: PartitionIncarnation::from_bytes([11; 16]),
                stream: "stream",
                topic: "orders",
                partition_id: PartitionId::ZERO,
                owner_epoch: OwnerEpoch::INITIAL,
                dedup_window_millis: std::num::NonZeroU64::new(limits.window_millis),
                retention: RetentionPolicy::default(),
            });
            buffer
                .push(
                    create.kind(),
                    &encode_operation_body(&create, OperationLimits::default()).unwrap(),
                )
                .unwrap();
            for writer in [12, 30] {
                let open = OperationBody::OpenProducer(OpenProducer {
                    partition: PartitionIncarnation::from_bytes([11; 16]),
                    producer_id: ProducerId::from_bytes([writer; 16]),
                    expected_epoch: None,
                    new_epoch: ProducerEpoch::INITIAL,
                    operation_id: OperationId::from_bytes([writer + 1; 16]),
                });
                buffer
                    .push(
                        open.kind(),
                        &encode_operation_body(&open, OperationLimits::default()).unwrap(),
                    )
                    .unwrap();
            }
            let mut lane = actor.take_submitter().unwrap();
            bootstrap = Some((
                lane.try_submit(buffer).unwrap(),
                lane,
                std::array::from_fn(|_| actor.lease_proposal_buffer().unwrap()),
            ));
        }
        actors.push(ScheduledReplica::new(actor).unwrap());
    }
    let (initial, lane, buffers) = bootstrap.unwrap();
    (actors, initial, lane, buffers)
}

fn finish(
    controller: &mut Controller,
    actors: &mut [ScheduledReplica],
    mut pending: Pin<Box<PendingProposal>>,
) -> ProposalReply {
    for _ in 0..10000 {
        scheduled::round(actors, false);
        scheduled::settle(controller, &[]);
        if let Poll::Ready(reply) = poll(pending.as_mut()) {
            let reply = reply.unwrap();
            assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }));
            return reply;
        }
    }
    panic!("confirmed proposal stopped progressing");
}

#[test]
fn both_quorum_actors_defer_redundant_producers_until_the_original_is_applied() {
    let (mut controller, io) = setup();
    let (mut actors, bootstrap, mut lane, buffers) = cluster(&mut controller, &io);
    drop(finish(&mut controller, &mut actors, Box::pin(bootstrap)));
    let mut pending = Vec::new();
    for (mut buffer, writer) in buffers.into_iter().take(2).zip([12, 30]) {
        buffer
            .prepare_append(super::super::super::proposal::request(writer, 0, 1))
            .unwrap();
        pending.push(Box::pin(lane.try_submit(buffer).unwrap()));
    }
    for _ in 0..1000 {
        scheduled::round(&mut actors, true);
        scheduled::settle(&mut controller, &[]);
    }
    let status = actors[0].status().normal.unwrap();
    assert_eq!(
        status.accepted.op.0, 4,
        "duplicate results must remain unadmitted"
    );
    assert_eq!(status.applied.op.0, 3);
    for work in &mut pending {
        assert!(poll(work.as_mut()).is_pending());
    }
    let first = finish(&mut controller, &mut actors, pending.remove(0));
    let second = finish(&mut controller, &mut actors, pending.remove(0));
    let first = decode_resolved_append(
        first.buffer.bodies().next().unwrap().1,
        OperationLimits::default(),
    )
    .unwrap();
    assert_eq!(first.results.stored(), 1);
    let second = decode_resolved_append(
        second.buffer.bodies().next().unwrap().1,
        OperationLimits::default(),
    )
    .unwrap();
    assert_eq!(second.results.stored(), 0);
    assert_eq!(second.results.attempts(), 1);
    assert_eq!(second.results.runs().next().unwrap().first_offset.get(), 0);
    for actor in actors {
        scheduled::close(&mut controller, actor);
    }
}

#[test]
fn other_producers_proceed_while_a_duplicate_waits_for_its_original() {
    let (mut controller, io) = setup();
    let (mut actors, bootstrap, mut lane, buffers) = cluster(&mut controller, &io);
    drop(finish(&mut controller, &mut actors, Box::pin(bootstrap)));
    // Writer 30 repeats writer 12's records; its second APPEND arrives
    // while its first still waits and must stay behind it.
    let mut pending = Vec::new();
    for (mut buffer, (writer, first)) in
        buffers
            .into_iter()
            .zip([(12, 0), (30, 0), (12, 1), (30, 1)])
    {
        buffer
            .prepare_append(super::super::super::proposal::request(writer, first, 1))
            .unwrap();
        pending.push(Box::pin(lane.try_submit(buffer).unwrap()));
    }
    for _ in 0..1000 {
        scheduled::round(&mut actors, true);
        scheduled::settle(&mut controller, &[]);
    }
    let status = actors[0].status().normal.unwrap();
    assert_eq!(
        status.accepted.op.0, 5,
        "writer 12 keeps admitting while writer 30 waits"
    );
    assert_eq!(status.applied.op.0, 3);
    for work in &mut pending {
        assert!(poll(work.as_mut()).is_pending());
    }
    for (work, expected) in pending.into_iter().zip([(1, 0), (0, 0), (1, 1), (0, 1)]) {
        let reply = finish(&mut controller, &mut actors, work);
        let resolved = decode_resolved_append(
            reply.buffer.bodies().next().unwrap().1,
            OperationLimits::default(),
        )
        .unwrap();
        let first = resolved.results.runs().next().unwrap().first_offset.get();
        assert_eq!((resolved.results.stored(), first), expected);
        assert_eq!(resolved.results.attempts(), 1);
    }
    for actor in actors {
        scheduled::close(&mut controller, actor);
    }
}

#[test]
fn fencing_a_parked_duplicate_releases_the_producers_queued_appends() {
    let (mut controller, io) = setup();
    let (mut actors, bootstrap, mut lane, buffers) = cluster(&mut controller, &io);
    drop(finish(&mut controller, &mut actors, Box::pin(bootstrap)));
    // Writer 12's original stays accepted-only under blocked transport.
    // Writer 30's duplicate parks behind it and its fresh record queues
    // behind that.
    let mut pending = Vec::new();
    for (mut buffer, (writer, first)) in
        buffers.into_iter().take(3).zip([(12, 0), (30, 0), (30, 1)])
    {
        buffer
            .prepare_append(super::super::super::proposal::request(writer, first, 1))
            .unwrap();
        pending.push(Box::pin(lane.try_submit(buffer).unwrap()));
    }
    for _ in 0..1000 {
        scheduled::round(&mut actors, true);
        scheduled::settle(&mut controller, &[]);
    }
    let status = actors[0].status().normal.unwrap();
    assert_eq!(status.accepted.op.0, 4);
    assert_eq!(status.applied.op.0, 3);
    for work in &mut pending {
        assert!(poll(work.as_mut()).is_pending());
    }
    pending[1].fence_unadmitted();
    for _ in 0..1000 {
        scheduled::round(&mut actors, true);
        scheduled::settle(&mut controller, &[]);
    }
    let Poll::Ready(fenced) = poll(pending[1].as_mut()) else {
        panic!("fenced parked proposal must finish");
    };
    assert!(matches!(
        fenced.unwrap().outcome,
        ProposalOutcome::NotAdmitted
    ));
    // The queued request validates next and fails its sequence check, so
    // the writer replays from its confirmed prefix instead of waiting.
    let Poll::Ready(follow) = poll(pending[2].as_mut()) else {
        panic!("the producer's queued request must validate after the fence");
    };
    assert!(matches!(
        follow.unwrap().outcome,
        ProposalOutcome::Invalid(crate::replica_journal::JournalError::ProducerAppend(
            crate::replica_journal::AppendAdmissionError::SequenceGap
        ))
    ));
    assert_eq!(actors[0].status().normal.unwrap().accepted.op.0, 4);
    let original = pending.remove(0);
    drop(finish(&mut controller, &mut actors, original));
    assert_eq!(actors[0].status().normal.unwrap().applied.op.0, 4);
    for actor in actors {
        scheduled::close(&mut controller, actor);
    }
}

#[test]
fn a_follower_behind_on_its_own_writes_validates_two_windows_of_identities() {
    // Each broker owns its device, so only the third broker's writes stall.
    // It keeps staging what the leader commits through the second broker,
    // then validates that suffix before it applies the stalled window.
    let mut devices: Vec<_> = (0..3).map(|_| setup()).collect();
    let pipeline = actor_config().pipeline;
    let limits = crate::replica_journal::dedup_limits(300_000, 32, 256 * 1024, 8, pipeline);
    let (mut actors, bootstrap, mut lane, buffers) = {
        let mut borrowed: Vec<_> = devices
            .iter_mut()
            .map(|(controller, io)| (controller, &*io))
            .collect();
        cluster_on(&mut borrowed, limits)
    };
    let settle = |devices: &mut [(Controller, Local)], stalled: bool| {
        for (index, (controller, _)) in devices.iter_mut().enumerate() {
            let mut held = Vec::new();
            if stalled && index == 2 {
                for (id, _) in controller.jobs() {
                    if matches!(
                        controller
                            .operation(id)
                            .map(ozzy_io::Operation::unprotected),
                        Some(ozzy_io::Operation::Write { .. })
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
    let released = actors[2].status().normal.unwrap().applied.op.0;
    let proposals = 2 * pipeline.max_operations as u64;
    let mut buffers = Vec::from(buffers);
    let mut pending = Vec::new();
    let mut next = 0;
    for _ in 0..20000 {
        while next < proposals
            && let Some(mut buffer) = buffers.pop()
        {
            buffer
                .prepare_append(super::super::super::proposal::request(12, next, 1))
                .unwrap();
            pending.push(Box::pin(lane.try_submit(buffer).unwrap()));
            next += 1;
        }
        scheduled::round(&mut actors, false);
        settle(&mut devices, true);
        pending.retain_mut(|work| {
            let Poll::Ready(reply) = poll(work.as_mut()) else {
                return true;
            };
            let mut reply = reply.unwrap();
            assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }));
            reply.buffer.clear();
            buffers.push(reply.buffer);
            false
        });
        if next == proposals && pending.is_empty() {
            break;
        }
    }
    assert!(
        pending.is_empty(),
        "the leader commits through the other follower"
    );
    let follower = actors[2].status().normal.unwrap();
    assert_eq!(follower.applied.op.0, released, "stalled writes hold apply");
    assert!(
        follower.accepted.op.0 > released,
        "the follower accepted part of the leader's history"
    );
    // Released writes let the follower validate, apply, and catch up. The
    // idle leader announces its commit on its heartbeat.
    let leader = actors[0].status().normal.unwrap().applied;
    for step in 0..20000 {
        scheduled::round_at(&mut actors, Duration::from_millis(step), false);
        settle(&mut devices, false);
        if actors[2].status().normal.unwrap().applied == leader {
            break;
        }
    }
    assert_eq!(actors[2].status().normal.unwrap().applied, leader);
    for (actor, (controller, _)) in actors.into_iter().zip(&mut devices) {
        scheduled::close(controller, actor);
    }
}

#[test]
fn a_follower_behind_on_its_writes_waits_to_reuse_identity_capacity() {
    // The leader frees identity capacity at a commit the third broker cannot
    // apply while its writes stall. That broker must wait to validate the
    // replacement identities instead of failing its partition.
    let Lagging {
        mut devices,
        mut actors,
        released,
        now,
        lane: _lane,
        ..
    } = deferred_follower(Lag::Writes);
    let follower = actors[2].status().normal.unwrap();
    assert_eq!(follower.applied, released, "stalled writes hold apply");
    // Released writes let the follower apply, expire, and validate.
    catch_up(&mut devices, &mut actors, now);
    for (actor, (controller, _)) in actors.into_iter().zip(&mut devices) {
        scheduled::close(controller, actor);
    }
}

#[test]
fn a_follower_receiving_expiry_with_the_reused_identities_waits_to_apply_it() {
    // The third broker misses the watermark and the appends that reuse the
    // capacity it frees, then stages them as one suffix with nothing earlier
    // left to apply. It must accept the watermark and validate the rest after
    // applying it, instead of failing its partition.
    let Lagging {
        mut devices,
        mut actors,
        released,
        now,
        lane: _lane,
        held,
    } = deferred_follower(Lag::Messages);
    assert_eq!(actors[2].status().normal.unwrap().applied, released);
    for message in &held {
        actors[2].receive(message, now).unwrap();
    }
    catch_up(&mut devices, &mut actors, now);
    for (actor, (controller, _)) in actors.into_iter().zip(&mut devices) {
        scheduled::close(controller, actor);
    }
}

/// Run until the third broker applies the leader's prefix. The idle leader
/// announces its commit on its heartbeat.
fn catch_up(devices: &mut [(Controller, Local)], actors: &mut [ScheduledReplica], now: Duration) {
    let leader = actors[0].status().normal.unwrap().applied;
    for step in 0..20000 {
        scheduled::round_at(actors, now + Duration::from_millis(step), false);
        settle_devices(devices, false);
        if actors[2].status().normal.unwrap().applied == leader {
            break;
        }
    }
    assert_eq!(actors[2].status().normal.unwrap().applied, leader);
}

#[test]
fn a_view_change_over_a_deferred_suffix_keeps_its_arena() {
    let Lagging {
        mut devices,
        mut actors,
        lane: _lane,
        ..
    } = deferred_follower(Lag::Writes);
    assert_eq!(actors[2].arenas(), (0, true), "the retry holds the arena");
    // Cut the leader off. Brokers 1 and 2 start a view change while broker
    // 2's writes still stall, so its deferred suffix is never retried.
    let session = actor_config().sessions[0];
    for (actor, peers) in [(0, &[2_u8, 3][..]), (1, &[1][..]), (2, &[1][..])] {
        for &peer in peers {
            actors[actor]
                .disconnect_session(NodeId::from_bytes([peer; 16]), session, Duration::ZERO)
                .unwrap();
        }
    }
    let mut now = Duration::from_secs(3);
    let mut stalled = true;
    let mut installed = false;
    for _ in 0..20000 {
        now += Duration::from_millis(1);
        scheduled::round_at(&mut actors, now, false);
        settle_devices(&mut devices, stalled);
        let status = actors[2].status();
        stalled &= status.scope.view == 0;
        if status.scope.view > 0 && status.normal.is_some() && status.application_ready {
            installed = true;
            break;
        }
    }
    assert!(
        installed,
        "{:#?}",
        actors
            .iter()
            .map(ScheduledReplica::status)
            .collect::<Vec<_>>()
    );
    for _ in 0..100 {
        now += Duration::from_millis(1);
        scheduled::round_at(&mut actors, now, false);
        settle_devices(&mut devices, false);
    }
    let (idle, retry) = actors[2].arenas();
    assert!(!retry, "the new view drops the old suffix");
    assert_eq!(idle, 1, "the new view keeps the old suffix's arena");
    for (actor, (controller, _)) in actors.into_iter().zip(&mut devices) {
        scheduled::close(controller, actor);
    }
}

/// Run the third broker's writes, holding them while `stalled`.
fn settle_devices(devices: &mut [(Controller, Local)], stalled: bool) {
    for (index, (controller, _)) in devices.iter_mut().enumerate() {
        let mut held = Vec::new();
        if stalled && index == 2 {
            for (id, _) in controller.jobs() {
                if matches!(
                    controller
                        .operation(id)
                        .map(ozzy_io::Operation::unprotected),
                    Some(ozzy_io::Operation::Write { .. })
                ) {
                    held.push(id);
                }
            }
        }
        scheduled::settle(controller, &held);
    }
}

/// How the third broker falls behind the leader.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Lag {
    /// Its writes stall; it keeps receiving.
    Writes,
    /// Messages to it are held and returned.
    Messages,
}

/// A cluster whose third broker lags behind the commit that frees the
/// identities the leader reused.
struct Lagging {
    devices: Vec<(Controller, Local)>,
    actors: Vec<ScheduledReplica>,
    /// The third broker's applied prefix before it fell behind.
    released: ozzy_replication::Prefix,
    /// The fixture's last time.
    now: Duration,
    lane: ProposalSubmitter,
    /// Messages to the third broker held back while it lagged.
    held: Vec<Message>,
}

/// With stalled writes, the third broker holds a deferred received suffix.
#[expect(
    clippy::too_many_lines,
    reason = "one controlled expiry schedule with a lagging follower"
)]
fn deferred_follower(lag: Lag) -> Lagging {
    let mut devices: Vec<_> = (0..3).map(|_| setup()).collect();
    let pipeline = actor_config().pipeline;
    let limits = crate::replica_journal::dedup_limits(1_000, 4, 256 * 1024, 4, pipeline);
    let (mut actors, bootstrap, mut lane, buffers) = {
        let mut borrowed: Vec<_> = devices
            .iter_mut()
            .map(|(controller, io)| (controller, &*io))
            .collect();
        cluster_on(&mut borrowed, limits)
    };
    let mut buffers = Vec::from(buffers);
    let mut held = Vec::new();
    let mut now = Duration::ZERO;
    let mut submit = |buffers: &mut Vec<ProposalBuffer>,
                      actors: &mut [ScheduledReplica],
                      devices: &mut [(Controller, Local)],
                      held: &mut Vec<Message>,
                      now: Duration,
                      prepare: &dyn Fn(&mut ProposalBuffer),
                      lagging: bool| {
        let mut buffer = buffers.pop().unwrap();
        prepare(&mut buffer);
        let mut pending = Box::pin(lane.try_submit(buffer).unwrap());
        for _ in 0..20000 {
            for (to, message) in scheduled::collect(actors, now, false) {
                if lagging && lag == Lag::Messages && to == 2 {
                    held.push(message);
                } else {
                    actors[to].receive(&message, now).unwrap();
                }
            }
            settle_devices(devices, lagging && lag == Lag::Writes);
            if let Poll::Ready(reply) = poll(pending.as_mut()) {
                let mut reply = reply.unwrap();
                assert!(matches!(reply.outcome, ProposalOutcome::Committed { .. }));
                reply.buffer.clear();
                buffers.push(reply.buffer);
                return;
            }
        }
        panic!("proposal stopped progressing");
    };
    let mut bootstrap = Box::pin(bootstrap);
    for _ in 0..10000 {
        scheduled::round(&mut actors, false);
        settle_devices(&mut devices, false);
        if poll(bootstrap.as_mut()).is_ready() {
            break;
        }
    }
    for sequence in 0..4 {
        submit(
            &mut buffers,
            &mut actors,
            &mut devices,
            &mut held,
            now,
            &|buffer| {
                buffer
                    .prepare_append(super::super::super::proposal::request(12, sequence, 1))
                    .unwrap();
            },
            false,
        );
    }
    if lag == Lag::Messages {
        // The third broker applies the full window before it falls behind,
        // so nothing earlier is left to apply when the held suffix arrives.
        let leader = actors[0].status().normal.unwrap().applied;
        while actors[2].status().normal.unwrap().applied != leader {
            assert!(now < Duration::from_secs(1), "the follower must catch up");
            now += Duration::from_millis(1);
            scheduled::round_at(&mut actors, now, false);
            settle_devices(&mut devices, false);
        }
    }
    let released = actors[2].status().normal.unwrap().applied;
    // Later logical time expires the full window once it commits.
    super::CLOCK.with(|clock| clock.set(5_123));
    submit(
        &mut buffers,
        &mut actors,
        &mut devices,
        &mut held,
        now,
        &|buffer| {
            let watermark =
                OperationBody::DedupWatermark(ozzy_journal::operation::DedupWatermark {
                    partition: PartitionIncarnation::from_bytes([11; 16]),
                    owner_epoch: OwnerEpoch::INITIAL,
                    logical_time: 5_123,
                });
            buffer
                .push(
                    watermark.kind(),
                    &encode_operation_body(&watermark, OperationLimits::default()).unwrap(),
                )
                .unwrap();
        },
        true,
    );
    for sequence in 4..8 {
        submit(
            &mut buffers,
            &mut actors,
            &mut devices,
            &mut held,
            now,
            &|buffer| {
                buffer
                    .prepare_append(super::super::super::proposal::request(12, sequence, 1))
                    .unwrap();
            },
            true,
        );
    }
    Lagging {
        devices,
        actors,
        released,
        now,
        lane,
        held,
    }
}

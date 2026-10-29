use super::*;
use crate::replica_actor::ScheduledReplica;
use crate::replica_journal::DelayLimits;
use ozzy_journal::operation::{
    CreatePartition, OpenProducer, OperationBody, OperationLimits, RetentionPolicy,
    encode_operation_body,
};
use ozzy_proto::{
    Offset, OperationId, OwnerEpoch, PartitionId, PartitionIncarnation, ProducerEpoch, ProducerId,
};

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([11; 16])
}

fn push(buffer: &mut crate::replica_journal::ProposalBuffer, body: &OperationBody<'_>) {
    buffer
        .push(
            body.kind(),
            &encode_operation_body(body, OperationLimits::default()).unwrap(),
        )
        .unwrap();
}

fn finish(
    controller: &mut Controller,
    actors: &mut [ScheduledReplica],
    mut pending: Pin<Box<PendingProposal>>,
) {
    for _ in 0..10000 {
        scheduled::round(actors, false);
        scheduled::settle(controller, &[]);
        if let Poll::Ready(reply) = poll(pending.as_mut()) {
            assert!(matches!(
                reply.unwrap().outcome,
                ProposalOutcome::Committed { .. }
            ));
            return;
        }
    }
    panic!("proposal stopped progressing");
}

/// The next offset of every copy, once each owner is idle.
fn next_offsets(actors: &mut [ScheduledReplica]) -> Option<Vec<Offset>> {
    actors
        .iter_mut()
        .map(|actor| {
            actor
                .read_access()
                .1
                .facts(partition())
                .map(|facts| facts.next_offset)
        })
        .collect()
}

/// Three copies of a delay partition with `retention`, its creation
/// confirmed through the leader's lane, and `spare` buffers of the leader.
fn delay_group(
    controller: &mut Controller,
    io: &Local,
    retention: RetentionPolicy,
    retention_interval: Option<Duration>,
    spare: usize,
) -> (
    Vec<ScheduledReplica>,
    crate::replica_actor::ProposalSubmitter,
    Vec<crate::replica_journal::ProposalBuffer>,
) {
    let mut actors = Vec::new();
    let mut started = None;
    for broker in 0..3 {
        let mut actor = configured_actor(
            controller,
            io,
            0,
            broker,
            ActorSettings {
                delay: Some(DelayLimits::new(16)),
                retention_interval,
                ..Default::default()
            },
        );
        if broker == 0 {
            let mut buffer = actor.lease_proposal_buffer().unwrap();
            push(
                &mut buffer,
                &OperationBody::CreatePartition(CreatePartition {
                    partition: partition(),
                    stream: "stream",
                    topic: "orders",
                    partition_id: PartitionId::ZERO,
                    owner_epoch: OwnerEpoch::INITIAL,
                    delay_max_millis: std::num::NonZeroU64::new(60_000),
                    dedup_window_millis: None,
                    retention,
                }),
            );
            push(
                &mut buffer,
                &OperationBody::OpenProducer(OpenProducer {
                    partition: partition(),
                    producer_id: ProducerId::from_bytes([12; 16]),
                    expected_epoch: None,
                    new_epoch: ProducerEpoch::INITIAL,
                    operation_id: OperationId::from_bytes([13; 16]),
                }),
            );
            let mut lane = actor.take_submitter().unwrap();
            let pending = lane.try_submit(buffer).unwrap();
            let buffers = (0..spare)
                .map(|_| actor.lease_proposal_buffer().unwrap())
                .collect();
            started = Some((pending, lane, buffers));
        }
        actors.push(ScheduledReplica::new(actor).unwrap());
    }
    let (bootstrap, lane, buffers) = started.unwrap();
    finish(controller, &mut actors, Box::pin(bootstrap));
    (actors, lane, buffers)
}

/// Confirm `buffer`, after `prepare` fills it, through `lane`.
fn submit(
    controller: &mut Controller,
    actors: &mut [ScheduledReplica],
    lane: &mut crate::replica_actor::ProposalSubmitter,
    mut buffer: crate::replica_journal::ProposalBuffer,
    prepare: impl FnOnce(&mut crate::replica_journal::ProposalBuffer),
) {
    prepare(&mut buffer);
    let pending = lane.try_submit(buffer).unwrap();
    finish(controller, actors, Box::pin(pending));
}

/// Run rounds at advancing actor time until `done`.
fn run_until(
    controller: &mut Controller,
    actors: &mut [ScheduledReplica],
    now: &mut Duration,
    mut done: impl FnMut(&mut Controller, &mut [ScheduledReplica]) -> bool,
) -> bool {
    for _ in 0..10000 {
        *now += Duration::from_millis(1);
        scheduled::round_at(actors, *now, false);
        scheduled::settle(controller, &[]);
        if done(controller, actors) {
            return true;
        }
    }
    false
}

#[test]
fn leader_releases_due_records_and_every_copy_applies_them() {
    let (mut controller, io) = setup();
    CLOCK.with(|clock| clock.set(1_000));
    let (mut actors, mut lane, mut buffers) =
        delay_group(&mut controller, &io, RetentionPolicy::default(), None, 1);
    submit(
        &mut controller,
        &mut actors,
        &mut lane,
        buffers.pop().unwrap(),
        |buffer| {
            buffer
                .prepare_append(super::super::super::proposal::request(12, 0, 2))
                .unwrap();
            buffer.schedule(&[1_010, 1_005]).unwrap();
        },
    );
    for _ in 0..100 {
        scheduled::round(&mut actors, false);
        scheduled::settle(&mut controller, &[]);
    }
    assert_eq!(next_offsets(&mut actors), Some(vec![Offset::ZERO; 3]));

    CLOCK.with(|clock| clock.set(1_010));
    let released = Some(vec![Offset::new(2); 3]);
    for _ in 0..10000 {
        scheduled::round(&mut actors, false);
        scheduled::settle(&mut controller, &[]);
        if next_offsets(&mut actors) == released {
            break;
        }
    }
    assert_eq!(next_offsets(&mut actors), released);
    CLOCK.with(|clock| clock.set(123));
    for actor in actors {
        scheduled::close(&mut controller, actor);
    }
}

/// Schedule one record due at `1_010`, then fill the leader's live window
/// with no message delivered: `fill` submits stored records through the
/// lane and returns them.
fn full_window(
    fill: impl FnOnce(
        &mut Controller,
        &mut [ScheduledReplica],
        &mut crate::replica_actor::ProposalSubmitter,
        Vec<crate::replica_journal::ProposalBuffer>,
    ) -> (Vec<Pin<Box<PendingProposal>>>, u64),
    spare: usize,
) {
    let (mut controller, io) = setup();
    CLOCK.with(|clock| clock.set(1_000));
    let (mut actors, mut lane, mut buffers) = delay_group(
        &mut controller,
        &io,
        RetentionPolicy::default(),
        None,
        spare + 1,
    );
    submit(
        &mut controller,
        &mut actors,
        &mut lane,
        buffers.pop().unwrap(),
        |buffer| {
            buffer
                .prepare_append(super::super::super::proposal::request(12, 0, 1))
                .unwrap();
            buffer.schedule(&[1_010]).unwrap();
        },
    );
    let (stored, records) = fill(&mut controller, &mut actors, &mut lane, buffers);
    // The due release waits for room instead of failing admission.
    CLOCK.with(|clock| clock.set(1_010));
    for _ in 0..100 {
        scheduled::round(&mut actors, true);
        scheduled::settle(&mut controller, &[]);
    }
    assert!(actors[0].holds_maintenance());
    let total = Some(vec![Offset::new(records + 1); 3]);
    for _ in 0..10000 {
        scheduled::round(&mut actors, false);
        scheduled::settle(&mut controller, &[]);
        if next_offsets(&mut actors) == total {
            break;
        }
    }
    assert_eq!(next_offsets(&mut actors), total);
    for mut pending in stored {
        let Poll::Ready(reply) = poll(pending.as_mut()) else {
            panic!("a stored record stayed unconfirmed");
        };
        assert!(matches!(
            reply.unwrap().outcome,
            ProposalOutcome::Committed { .. }
        ));
    }
    CLOCK.with(|clock| clock.set(123));
    for actor in actors {
        scheduled::close(&mut controller, actor);
    }
}

#[test]
fn a_due_release_waits_for_a_live_slot() {
    // One-record proposals take every live slot, then fill the lane.
    full_window(
        |controller, actors, lane, buffers| {
            let request = super::super::super::proposal::request;
            let mut waiting = (1..)
                .zip(buffers)
                .map(|(sequence, mut buffer)| {
                    buffer.prepare_append(request(12, sequence, 1)).unwrap();
                    buffer
                })
                .collect::<std::collections::VecDeque<_>>();
            let mut stored = Vec::new();
            for _ in 0..1000 {
                if let Some(buffer) = waiting.pop_front() {
                    match lane.try_submit(buffer) {
                        Ok(pending) => stored.push(Box::pin(pending)),
                        Err(rejected) => waiting.push_front(rejected.buffer),
                    }
                }
                scheduled::round(actors, true);
                scheduled::settle(controller, &[]);
            }
            assert!(!waiting.is_empty());
            let records = stored.len() as u64;
            (stored, records)
        },
        8,
    );
}

#[test]
fn a_due_release_waits_for_room_in_the_pipeline() {
    // One stored proposal leaves less body room than any one record.
    full_window(
        |controller, actors, lane, buffers| {
            let request = super::super::super::proposal::request;
            let [mut one, mut two, mut stored] = <[_; 3]>::try_from(buffers).unwrap();
            one.prepare_append(request(12, 1, 1)).unwrap();
            two.prepare_append(request(12, 1, 2)).unwrap();
            let record = two.body_bytes() - one.body_bytes();
            let room = crate::replica_journal::owned::tests::pipeline().max_body_bytes;
            let records = (room - (one.body_bytes() - record)) / record;
            drop((one, two));
            stored.prepare_append(request(12, 1, records)).unwrap();
            assert!(room - stored.admitted_body_bytes() < record);
            let stored = Box::pin(lane.try_submit(stored).unwrap());
            for _ in 0..100 {
                scheduled::round(actors, true);
                scheduled::settle(controller, &[]);
            }
            (vec![stored], records as u64)
        },
        3,
    );
}

/// The leader's oldest segment file.
fn oldest_segment(controller: &Controller) -> Option<u64> {
    (0..64).find(|id| {
        controller.image().exists(
            std::path::Path::new(&format!("/g0-b0/segments/{id}.log")),
            false,
        )
    })
}

#[test]
fn the_leader_carries_aged_held_records_and_every_copy_checks_them() {
    let (mut controller, io) = setup();
    CLOCK.with(|clock| clock.set(1_000));
    let retention = RetentionPolicy {
        max_age_millis: std::num::NonZeroU64::new(1_000),
        max_bytes: None,
    };
    let (mut actors, mut lane, mut buffers) = delay_group(
        &mut controller,
        &io,
        retention,
        Some(Duration::from_millis(1)),
        2,
    );
    let request = super::super::super::proposal::request;
    submit(
        &mut controller,
        &mut actors,
        &mut lane,
        buffers.pop().unwrap(),
        |buffer| {
            buffer.prepare_append(request(12, 0, 2)).unwrap();
            buffer.schedule(&[30_000, 20_000]).unwrap();
        },
    );
    // A stored record lets the active segment age and roll.
    submit(
        &mut controller,
        &mut actors,
        &mut lane,
        buffers.pop().unwrap(),
        |buffer| buffer.prepare_append(request(12, 2, 1)).unwrap(),
    );
    let first = oldest_segment(&controller).unwrap();

    // The leader rolls the aged segment, carries its held records forward,
    // trims the stored record, and retires the segment. Each copy checks
    // the carry.
    CLOCK.with(|clock| clock.set(5_000));
    let mut now = Duration::ZERO;
    assert!(run_until(
        &mut controller,
        &mut actors,
        &mut now,
        |controller, _| { oldest_segment(controller).is_some_and(|oldest| oldest > first) }
    ));
    assert_eq!(next_offsets(&mut actors), Some(vec![Offset::new(1); 3]));

    CLOCK.with(|clock| clock.set(30_000));
    let released = Some(vec![Offset::new(3); 3]);
    assert!(run_until(
        &mut controller,
        &mut actors,
        &mut now,
        |_, actors| { next_offsets(actors) == released }
    ));
    CLOCK.with(|clock| clock.set(123));
    for actor in actors {
        scheduled::close(&mut controller, actor);
    }
}

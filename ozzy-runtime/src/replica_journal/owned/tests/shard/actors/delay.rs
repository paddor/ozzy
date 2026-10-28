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

#[test]
fn primary_releases_due_records_and_every_copy_applies_them() {
    let (mut controller, io) = setup();
    CLOCK.with(|clock| clock.set(1_000));
    let mut actors = Vec::new();
    let mut started = None;
    for broker in 0..3 {
        let mut actor = configured_actor(
            &mut controller,
            &io,
            0,
            broker,
            ActorSettings {
                delay: Some(DelayLimits::new(16)),
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
                    retention: RetentionPolicy::default(),
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
            let mut scheduled = actor.lease_proposal_buffer().unwrap();
            scheduled
                .prepare_append(super::super::super::proposal::request(12, 0, 2))
                .unwrap();
            scheduled.schedule(&[1_010, 1_005]).unwrap();
            started = Some((pending, lane, scheduled));
        }
        actors.push(ScheduledReplica::new(actor).unwrap());
    }
    let (bootstrap, mut lane, scheduled) = started.unwrap();
    finish(&mut controller, &mut actors, Box::pin(bootstrap));
    let pending = lane.try_submit(scheduled).unwrap();
    finish(&mut controller, &mut actors, Box::pin(pending));
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

//! A long-lived flow channel's base falls out of retained history. A follower
//! that then reports a receipt older than the flow history must not stop the
//! leader, which verifies that receipt from the journal.

use super::*;
use ozzy_journal::operation::{
    CreatePartition, OpenProducer, OperationBody, OperationLimits, RetentionPolicy,
    encode_operation_body,
};
use ozzy_proto::{
    OperationId, OwnerEpoch, PartitionId, PartitionIncarnation, ProducerEpoch, ProducerId,
};

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([11; 16])
}

fn push(buffer: &mut ProposalBuffer, body: &OperationBody<'_>) {
    buffer
        .push(
            body.kind(),
            &encode_operation_body(body, OperationLimits::default()).unwrap(),
        )
        .unwrap();
}

fn create(buffer: &mut ProposalBuffer) {
    push(
        buffer,
        &OperationBody::CreatePartition(CreatePartition {
            partition: partition(),
            stream: "stream",
            topic: "orders",
            partition_id: PartitionId::ZERO,
            owner_epoch: OwnerEpoch::INITIAL,
            delay_max_millis: None,
            dedup_window_millis: None,
            retention: RetentionPolicy {
                max_age_millis: std::num::NonZeroU64::new(1_000),
                max_bytes: None,
            },
        }),
    );
    push(
        buffer,
        &OperationBody::OpenProducer(OpenProducer {
            partition: partition(),
            producer_id: ProducerId::from_bytes([12; 16]),
            expected_epoch: None,
            new_epoch: ProducerEpoch::INITIAL,
            operation_id: OperationId::from_bytes([13; 16]),
        }),
    );
}

fn oldest_segment(cluster: &Cluster) -> Option<u64> {
    (0..64).find(|id| {
        cluster.controller.image().exists(
            std::path::Path::new(&format!("/g0-b0/segments/{id}.log")),
            false,
        )
    })
}

#[test]
fn a_receipt_older_than_flow_history_does_not_stop_the_leader_after_retirement() {
    let request = super::super::super::super::proposal::request;
    CLOCK.with(|clock| clock.set(1_000));
    let mut cluster = Cluster::new();
    cluster.submit(0, "create", create);
    cluster.submit(0, "record", |buffer| {
        buffer.prepare_append(request(12, 0, 1)).unwrap();
    });
    let first = oldest_segment(&cluster).unwrap();

    // Retention rolls the aged segment and retires it, and the channel base
    // with it.
    CLOCK.with(|clock| clock.set(5_000));
    let mut retired = false;
    for _ in 0..100_000 {
        cluster.steps(1);
        if oldest_segment(&cluster).is_some_and(|oldest| oldest > first) {
            retired = true;
            break;
        }
    }
    assert!(
        retired,
        "the first segment stayed: {:#?}",
        cluster.statuses()
    );

    // Publications to follower 1 wait until the leader has moved on far past
    // its flow history. The follower then reads a few and loses the rest. Its
    // receipt is older than that history but newer than the leader last
    // counted as sent, and only repair can move it.
    cluster.transport.held = vec![1];
    for sequence in 1..=64 {
        cluster.submit(0, "behind", |buffer| {
            buffer.prepare_append(request(12, sequence, 1)).unwrap();
        });
    }
    let counted = cluster.status(1).normal.unwrap().accepted.op.0 + 8;
    cluster.transport.held.clear();
    assert!(cluster.run_until(|nodes| {
        nodes[1]
            .status()
            .normal
            .is_some_and(|normal| normal.accepted.op.0 > counted)
    }));
    cluster.transport.lanes[1].clear();
    cluster.submit(0, "after", |buffer| {
        buffer.prepare_append(request(12, 65, 1)).unwrap();
    });
    cluster.wait_caught_up(1, "follower 1");
    CLOCK.with(|clock| clock.set(123));
    cluster.close();
}

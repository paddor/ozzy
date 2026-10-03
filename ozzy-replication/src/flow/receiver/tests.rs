use super::*;
use crate::flow::ReceiveEpoch;
use crate::{Digest, OpNumber, Scope};
use ozzy_proto::GroupId;

fn receiver() -> Receiver {
    Receiver::new(
        Channel {
            scope: Scope {
                group_id: GroupId::from_bytes([1; 16]),
                configuration_epoch: 1,
                configuration_digest: Digest::from_bytes([2; 32]),
                view: 0,
            },
            epoch: ReceiveEpoch::new(1).unwrap(),
        },
        Prefix::GENESIS,
        PipelineLimits {
            max_operations: 4,
            max_body_bytes: 100,
        },
    )
    .unwrap()
}

fn next(previous: Prefix) -> Operation {
    Operation {
        prefix: Prefix {
            op: OpNumber(previous.op.0 + 1),
            digest: Digest::from_bytes([1; 32]),
        },
        previous_digest: previous.digest,
        body_bytes: 1,
    }
}

#[test]
fn startup_metadata_reservations_survive_repeated_window_turnover() {
    let mut receiver = receiver();
    let receiver_capacity = receiver.retained.capacity();
    for _ in 0..10_000 {
        let op = next(receiver.report().received);
        receiver.retain(receiver.report().channel, &[op]).unwrap();
        receiver.release(op.prefix).unwrap();
        assert_eq!(receiver.retained.capacity(), receiver_capacity);
    }
}

#[test]
fn revision_exhaustion_cannot_publish_partial_retention_or_release() {
    let mut receiver = receiver();
    let op = next(Prefix::GENESIS);
    receiver.report.revision = u64::MAX;
    let before = receiver.report();
    assert_eq!(
        receiver.retain(before.channel, &[op]),
        Err(FlowError::Exhausted)
    );
    assert_eq!(receiver.report(), before);
    assert!(receiver.retained.is_empty());
    receiver.report.revision -= 1;
    receiver.retain(before.channel, &[op]).unwrap();
    let before = receiver.report();
    receiver.release(op.prefix).unwrap();
    assert_eq!(receiver.report(), before);
    assert!(receiver.retained.is_empty());
}

#[test]
fn retraction_reuses_capacity_and_recovers_exhausted_revision() {
    let mut receiver = receiver();
    let capacity = receiver.retained.capacity();
    for epoch in 2..10_002 {
        let op = next(receiver.report().received);
        receiver.retain(receiver.report().channel, &[op]).unwrap();
        let successor = next(op.prefix);
        receiver
            .retain(receiver.report().channel, &[successor])
            .unwrap();
        receiver.report.revision = u64::MAX;
        receiver
            .retract(ReceiveEpoch::new(epoch).unwrap(), op.prefix)
            .unwrap();
        assert_eq!(receiver.report().revision, 1);
        assert_eq!(receiver.report().received_bytes, 1);
        assert_eq!(receiver.retained.len(), 1);
        assert_eq!(receiver.retained.capacity(), capacity);
        receiver.release(op.prefix).unwrap();
    }
}

#[test]
fn local_capacity_release_does_not_change_the_wire_receipt() {
    let mut receiver = receiver();
    let channel = receiver.report().channel;
    let mut operation = next(Prefix::GENESIS);
    operation.body_bytes = 100;
    receiver.retain(channel, &[operation]).unwrap();
    let report = receiver.report();
    let next = next(operation.prefix);
    assert_eq!(receiver.retain(channel, &[next]), Err(FlowError::Capacity));
    assert_eq!(receiver.report(), report);
    receiver.release(operation.prefix).unwrap();
    assert_eq!(receiver.report(), report);
    assert_eq!(receiver.available().max_body_bytes, 100);
    receiver.retain(channel, &[next]).unwrap();
}

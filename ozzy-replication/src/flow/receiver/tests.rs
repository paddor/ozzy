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
    assert_eq!(receiver.release(op.prefix), Err(FlowError::Exhausted));
    assert_eq!(receiver.report(), before);
    assert_eq!(receiver.retained.len(), 1);
}

#[test]
fn cumulative_credit_exhaustion_preserves_unreleased_accounting() {
    for byte_counter in [false, true] {
        let mut receiver = receiver();
        let op = next(Prefix::GENESIS);
        receiver.retain(receiver.report().channel, &[op]).unwrap();
        // Reach otherwise impractical counter exhaustion without billions of transitions.
        if byte_counter {
            receiver.report.byte_limit = u64::MAX;
        } else {
            receiver.report.operation_limit = u64::MAX;
        }
        let before = receiver.report();
        assert_eq!(receiver.release(op.prefix), Err(FlowError::Exhausted));
        assert_eq!(receiver.report(), before);
        assert_eq!(receiver.retained.len(), 1);
    }
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

fn reserved() -> Receiver {
    let source = receiver();
    Receiver::new_reserved(source.report.channel, source.report.base, source.limits).unwrap()
}

#[test]
fn reserved_credit_waits_for_grants_and_release_does_not_refill_it() {
    let mut receiver = reserved();
    let channel = receiver.report().channel;
    let op = next(Prefix::GENESIS);
    assert_eq!(
        (receiver.report.operation_limit, receiver.report.byte_limit),
        (0, 0)
    );
    assert_eq!(receiver.retain(channel, &[op]), Err(FlowError::Capacity));
    receiver.grant(channel, 1, 0).unwrap();
    assert_eq!(receiver.retain(channel, &[op]), Err(FlowError::Capacity));
    receiver.grant(channel, 0, 1).unwrap();
    receiver.retain(channel, &[op]).unwrap();
    receiver.release(op.prefix).unwrap();
    let report = receiver.report();
    assert_eq!((report.operation_limit, report.byte_limit), (1, 1));
    assert_eq!(
        receiver.retain(channel, &[next(op.prefix)]),
        Err(FlowError::Capacity)
    );
    receiver.grant(channel, 1, 1).unwrap();
    receiver.retain(channel, &[next(op.prefix)]).unwrap();
}

#[test]
fn reserved_credit_counts_unused_grants_and_retained_work_together() {
    let mut receiver = reserved();
    let channel = receiver.report().channel;
    receiver.grant(channel, 4, 100).unwrap();
    let mut op = next(Prefix::GENESIS);
    op.body_bytes = 30;
    receiver.retain(channel, &[op]).unwrap();
    let before = receiver.report();
    for (count, bytes) in [(1, 0), (0, 1)] {
        assert_eq!(
            receiver.grant(channel, count, bytes),
            Err(FlowError::Capacity)
        );
        assert_eq!(receiver.report(), before);
    }
    receiver.release(op.prefix).unwrap();
    receiver.grant(channel, 1, 30).unwrap();
    assert_eq!(
        (receiver.report.operation_limit, receiver.report.byte_limit),
        (5, 130)
    );
    let before = receiver.report();
    assert_eq!(receiver.grant(channel, 0, 1), Err(FlowError::Capacity));
    assert_eq!(receiver.report(), before);
}

#[test]
fn reserved_credit_epoch_changes_keep_retained_history_but_drop_unused_grants() {
    let mut receiver = reserved();
    let channel = receiver.report().channel;
    receiver.grant(channel, 4, 100).unwrap();
    let first = next(Prefix::GENESIS);
    let second = next(first.prefix);
    let third = next(second.prefix);
    receiver.retain(channel, &[first, second, third]).unwrap();
    receiver.release(first.prefix).unwrap();
    receiver
        .retract(ReceiveEpoch::new(2).unwrap(), second.prefix)
        .unwrap();
    let after = receiver.report();
    assert_eq!((after.base, after.received), (first.prefix, second.prefix));
    assert_eq!((after.operation_limit, after.byte_limit), (1, 1));
    assert_eq!(receiver.grant(channel, 1, 1), Err(FlowError::Channel));
    assert_eq!(
        receiver.retain(after.channel, &[third]),
        Err(FlowError::Capacity)
    );
    assert_eq!(receiver.report(), after);
    receiver.grant(after.channel, 3, 99).unwrap();
    receiver.retain(after.channel, &[third]).unwrap();
    receiver
        .reinitialize(
            Channel {
                epoch: ReceiveEpoch::new(3).unwrap(),
                ..channel
            },
            third.prefix,
        )
        .unwrap();
    assert_eq!(
        (receiver.report.operation_limit, receiver.report.byte_limit),
        (0, 0)
    );
    assert_eq!(receiver.released_bytes, 0);
    assert!(receiver.retained.is_empty());
    receiver.grant(receiver.report.channel, 4, 100).unwrap();
}

#[test]
fn reserved_credit_rejects_invalid_and_exhausted_grants_atomically() {
    let mut receiver = reserved();
    let channel = receiver.report.channel;
    assert_eq!(receiver.grant(channel, 0, 0), Err(FlowError::Invalid));
    receiver.report.revision = u64::MAX;
    let before = receiver.report();
    assert_eq!(receiver.grant(channel, 1, 1), Err(FlowError::Exhausted));
    assert_eq!(receiver.report(), before);
    receiver.report.revision = 1;
    receiver.report.operation_limit = u64::MAX;
    let before = receiver.report();
    assert_eq!(receiver.grant(channel, 1, 1), Err(FlowError::Exhausted));
    assert_eq!(receiver.report(), before);
    let mut automatic = super::tests::receiver();
    let before = automatic.report();
    assert_eq!(
        automatic.grant(before.channel, 1, 1),
        Err(FlowError::Invalid)
    );
    assert_eq!(automatic.report(), before);
}

#[test]
fn revoking_unused_credit_preserves_retained_suffix_and_fences_old_senders() {
    let mut receiver = reserved();
    let old = receiver.report().channel;
    let first = next(Prefix::GENESIS);
    let second = next(first.prefix);
    receiver.grant(old, 4, 100).unwrap();
    receiver.retain(old, &[first, second]).unwrap();
    receiver.release(first.prefix).unwrap();
    let capacity = receiver.retained.capacity();
    let report = receiver
        .revoke_unused(ReceiveEpoch::new(2).unwrap())
        .unwrap();
    assert_eq!(report.base, first.prefix);
    assert_eq!(report.received, second.prefix);
    assert_eq!(
        (
            report.received_bytes,
            report.operation_limit,
            report.byte_limit
        ),
        (1, 1, 1)
    );
    assert_eq!(receiver.retained.capacity(), capacity);
    let third = next(second.prefix);
    assert_eq!(receiver.retain(old, &[third]), Err(FlowError::Channel));
    assert_eq!(
        receiver.retain(report.channel, &[third]),
        Err(FlowError::Capacity)
    );
    receiver.grant(report.channel, 1, 1).unwrap();
    receiver.retain(report.channel, &[third]).unwrap();
    receiver.release(third.prefix).unwrap();
    let empty = receiver
        .revoke_unused(ReceiveEpoch::new(3).unwrap())
        .unwrap();
    assert_eq!((empty.base, empty.received), (third.prefix, third.prefix));
    assert_eq!((empty.operation_limit, empty.byte_limit), (0, 0));
}

#[test]
fn invalid_revocation_changes_neither_mode_nor_credit() {
    let mut reserved = reserved();
    let before = reserved.report();
    assert_eq!(
        reserved.revoke_unused(before.channel.epoch),
        Err(FlowError::Channel)
    );
    assert_eq!(reserved.report(), before);
    let mut automatic = receiver();
    let before = automatic.report();
    assert_eq!(
        automatic.revoke_unused(ReceiveEpoch::new(99).unwrap()),
        Err(FlowError::Invalid)
    );
    assert_eq!(automatic.report(), before);
    assert!(!automatic.is_reserved());
}

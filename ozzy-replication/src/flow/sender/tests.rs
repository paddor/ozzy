use super::*;
use crate::flow::{ReceiveEpoch, Receiver};
use crate::{Digest, OpNumber, Scope};
use ozzy_proto::GroupId;

#[test]
fn sender_metadata_capacity_survives_partial_receipts_and_ring_wraparound() {
    let limits = PipelineLimits {
        max_operations: 4,
        max_body_bytes: 100,
    };
    let channel = Channel {
        scope: Scope {
            group_id: GroupId::from_bytes([1; 16]),
            configuration_epoch: 1,
            configuration_digest: Digest::from_bytes([2; 32]),
            view: 0,
        },
        epoch: ReceiveEpoch::new(1).unwrap(),
    };
    let remote = PipelineLimits {
        max_operations: 64,
        max_body_bytes: 8192,
    };
    let mut receiver = Receiver::new(channel, Prefix::GENESIS, remote).unwrap();
    let mut sender = Sender::open(receiver.report(), limits, Prefix::GENESIS).unwrap();
    let capacity = sender.pending.capacity();
    for _ in 0..2_500 {
        let mut previous = sender.sent();
        let operations: [_; 4] = std::array::from_fn(|_| {
            let operation = Operation {
                prefix: Prefix {
                    op: OpNumber(previous.op.0 + 1),
                    digest: Digest::from_bytes([1; 32]),
                },
                previous_digest: previous.digest,
                body_bytes: 1,
            };
            previous = operation.prefix;
            operation
        });
        sender.record_send(channel, &operations).unwrap();
        assert_eq!(sender.available().max_operations, 0);
        receiver.retain(channel, &operations[..1]).unwrap();
        receiver.release(operations[0].prefix).unwrap();
        sender.observe(receiver.report()).unwrap();
        assert_eq!(sender.pending.len(), 3);
        assert_eq!(sender.pending.capacity(), capacity);
        receiver.retain(channel, &operations[1..]).unwrap();
        receiver.release(operations[3].prefix).unwrap();
        sender.observe(receiver.report()).unwrap();
        assert!(sender.pending.is_empty());
        assert_eq!(sender.pending.capacity(), capacity);
    }
}

#[test]
fn reopening_after_lost_data_reuses_the_send_ledger() {
    let limits = PipelineLimits {
        max_operations: 4,
        max_body_bytes: 100,
    };
    let channel = Channel {
        scope: Scope {
            group_id: GroupId::from_bytes([1; 16]),
            configuration_epoch: 1,
            configuration_digest: Digest::from_bytes([2; 32]),
            view: 0,
        },
        epoch: ReceiveEpoch::new(1).unwrap(),
    };
    let mut receiver = Receiver::new(channel, Prefix::GENESIS, limits).unwrap();
    let mut sender = Sender::open(receiver.report(), limits, Prefix::GENESIS).unwrap();
    let capacity = sender.pending.capacity();
    for epoch in 2..10_002 {
        let previous = sender.sent();
        let op = Operation {
            prefix: Prefix {
                op: OpNumber(previous.op.0 + 1),
                digest: Digest::from_bytes([1; 32]),
            },
            previous_digest: previous.digest,
            body_bytes: 1,
        };
        sender.record_send(sender.channel(), &[op]).unwrap(); // Lost packet.
        receiver
            .retract(ReceiveEpoch::new(epoch).unwrap(), previous)
            .unwrap();
        sender.reopen(receiver.report(), previous).unwrap();
        assert!(sender.pending.is_empty());
        assert_eq!(sender.sent(), previous);
        sender.record_send(sender.channel(), &[op]).unwrap();
        receiver.retain(sender.channel(), &[op]).unwrap();
        receiver.release(op.prefix).unwrap();
        sender.observe(receiver.report()).unwrap();
        assert_eq!(sender.pending.capacity(), capacity);
    }
}

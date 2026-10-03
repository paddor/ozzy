use super::routing::*;
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::{Envelope, MessageId, RequestId, SubscriptionId, data::Authority};
use ozzy_proto::{
    EnvelopeLimits, GroupId, LinkSessionId, NodeId, Opcode, PartitionIncarnation, ProducerId,
    append, reader,
};
use ozzy_replication::wire;

pub(super) fn binding(kind: Kind) -> Binding {
    Binding {
        peer: NodeId::from_bytes([1; 16]),
        session: LinkSessionId::from_bytes([2; 16]),
        kind,
    }
}

pub(super) fn placement(index: u8, shard: u32) -> Placement {
    Placement {
        group: GroupId::from_bytes([index + 10; 16]),
        partition: PartitionIncarnation::from_bytes([index + 20; 16]),
        shard,
    }
}

pub(super) fn envelope(opcode: Opcode, binding: Binding) -> Envelope {
    Envelope {
        opcode,
        response: false,
        request_id: Some(RequestId::from_bytes([3; 16])),
        sender: binding.peer,
        session: Some(binding.session),
    }
}

pub(super) fn authority(target: Placement) -> Authority {
    Authority {
        group_id: target.group,
        config_epoch: 1,
        view: 2,
    }
}

pub(super) fn append(target: Placement, binding: Binding) -> Message {
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let header = append::encode_append(
        envelope(Opcode::Append, binding),
        append::Append {
            authority: authority(target),
            partition: target.partition,
            owner_epoch: 1,
            key: append::AppendKey {
                producer_id: ProducerId::from_bytes([4; 16]),
                producer_epoch: 1,
                first_sequence: 0,
            },
            policy: append::Policy::QuorumDurable,
            records: &[append::Record {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([5; 16]),
                parts: &[b"opaque payload"],
            }],
        },
        &mut metadata,
        &mut payload,
        append::DataLimits::default(),
    )
    .unwrap();
    Message::multipart([
        Bytes::copy_from_slice(binding.peer.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::from(payload),
    ])
}

pub(super) fn reader(target: Placement, binding: Binding, opcode: Opcode) -> Message {
    let mut metadata = Vec::with_capacity(1024);
    let subscription = reader::Subscription {
        id: SubscriptionId::from_bytes([5; 16]),
        generation: 1,
    };
    let source = reader::Source::Group {
        authority: authority(target),
        partition: target.partition,
        owner_epoch: 1,
    };
    let mut envelope = envelope(opcode, binding);
    if opcode == Opcode::Ack {
        envelope.request_id = None;
    }
    let limits = EnvelopeLimits::default();
    let header = match opcode {
        Opcode::Subscribe => reader::encode_subscribe(
            envelope,
            &reader::Subscribe {
                subscription,
                target: reader::Target::Group {
                    authority: authority(target),
                    partition: target.partition,
                    owner_epoch: 1,
                },
                start: 0,
            },
            &mut metadata,
            limits,
        ),
        Opcode::Credit => reader::encode_credit(
            envelope,
            reader::Credit {
                subscription,
                source,
                records: 10,
                bytes: 1000,
            },
            &mut metadata,
            limits,
        ),
        Opcode::Ack => reader::encode_ack(
            envelope,
            reader::Ack {
                subscription,
                source,
                received: Some(1),
                processed: None,
            },
            &mut metadata,
            limits,
        ),
        Opcode::Unsubscribe => reader::encode_unsubscribe(
            envelope,
            reader::Subscribed {
                subscription,
                source,
            },
            &mut metadata,
            limits,
        ),
        _ => unreachable!(),
    }
    .unwrap();
    Message::multipart([
        Bytes::copy_from_slice(binding.peer.as_bytes()),
        Bytes::copy_from_slice(&header),
        Bytes::from(metadata),
        Bytes::new(),
    ])
}

pub(super) fn control(target: Placement, binding: Binding) -> Message {
    let configuration = ozzy_replication::Configuration::new(
        target.group,
        1,
        ozzy_replication::Digest::from_bytes([6; 32]),
        [
            binding.peer,
            NodeId::from_bytes([8; 16]),
            NodeId::from_bytes([9; 16]),
        ],
    )
    .unwrap();
    let mut metadata = [0; 120];
    let encoded = wire::encode_control(
        binding.peer,
        binding.session,
        wire::Control::Commit(ozzy_replication::Commit {
            scope: configuration.scope(),
            committed: ozzy_replication::Prefix::GENESIS,
        }),
        &mut metadata,
    )
    .unwrap();
    Message::multipart([
        Bytes::copy_from_slice(binding.peer.as_bytes()),
        Bytes::copy_from_slice(&encoded.header),
        Bytes::copy_from_slice(&metadata),
        Bytes::new(),
    ])
}

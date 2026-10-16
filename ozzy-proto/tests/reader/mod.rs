//! Reader protocol codec: subscriptions, deliveries, shared payloads and
//! publications, checked against fixed bytes and the generic encoder.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "test code"
)]

use bytes::Bytes;
use ozzy_proto::data::{CodecError, DataLimits, Record};
use ozzy_proto::reader::{
    self, Ack, PreparedBlock, PublicationHeader, RecordHeader, Source, Subscribe, Subscribed,
    Subscription,
};
use ozzy_proto::{
    Envelope, Frames, GroupId, LinkSessionId, MessageId, NodeId, Opcode, Packet, PartitionId,
    PartitionIncarnation, ProducerId, RequestId, SubscriptionId, Topic, decode_packet,
};

fn envelope(opcode: Opcode, response: bool) -> Envelope {
    Envelope {
        opcode,
        response,
        request_id: Some(RequestId::from_bytes([1; 16])),
        sender: NodeId::from_bytes([2; 16]),
        session: Some(LinkSessionId::from_bytes([3; 16])),
    }
}
fn subscription() -> Subscription {
    Subscription {
        id: SubscriptionId::from_bytes([4; 16]),
        generation: 5,
    }
}
fn local() -> Source {
    Source::Local {
        producer: ProducerId::from_bytes([6; 16]),
        partition: PartitionId::new(7),
    }
}
fn sources() -> [Source; 2] {
    [
        local(),
        Source::Group {
            authority: ozzy_proto::data::Authority {
                group_id: GroupId::from_bytes([8; 16]),
                config_epoch: 9,
                view: 10,
            },
            partition: PartitionIncarnation::from_bytes([11; 16]),
            owner_epoch: 12,
        },
    ]
}

fn packed_records(records: &[Record<'_>]) -> (Vec<u8>, Vec<u8>) {
    let mut metadata = Vec::with_capacity(4096);
    let mut payload = Vec::with_capacity(4096);
    reader::encode_records(
        Envelope {
            request_id: None,
            ..envelope(Opcode::Records, false)
        },
        RecordHeader {
            subscription: subscription(),
            source: local(),
            first_offset: 0,
        },
        records,
        Frames {
            metadata: &mut metadata,
            payload: &mut payload,
        },
        DataLimits::default(),
    )
    .unwrap();
    let descriptor_bytes: usize = records
        .iter()
        .map(|r| 20 + r.encoding.metadata_bytes() + 4 * r.parts.len())
        .sum();
    (
        metadata.split_off(metadata.len() - descriptor_bytes),
        payload,
    )
}

mod control;
mod delivery;
mod packed;
mod publication;
mod shared;

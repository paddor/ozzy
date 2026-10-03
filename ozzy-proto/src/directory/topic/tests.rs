use super::*;
use crate::{LinkSessionId, RequestId, decode_packet};

fn node(value: u8) -> NodeId {
    NodeId::from_bytes([value; 16])
}

fn envelope(opcode: Opcode) -> Envelope {
    Envelope {
        opcode,
        response: opcode == Opcode::StateSnapshot,
        request_id: Some(RequestId::from_bytes([8; 16])),
        sender: node(9),
        session: Some(LinkSessionId::from_bytes([7; 16])),
    }
}

fn packet<'a>(header: &'a [u8; ENVELOPE_BYTES], metadata: &'a [u8]) -> Packet<'a> {
    decode_packet(&[header, metadata, &[]], EnvelopeLimits::default()).unwrap()
}

fn page() -> TopicPage {
    let members = vec![node(1), node(2), node(3)];
    TopicPage {
        id: TopicId::from_bytes([4; 16]),
        name: "orders".to_owned(),
        partitioner_seed: 10,
        total: 16,
        policy: Policy::QuorumDurable,
        brokers: members
            .iter()
            .map(|&node| BrokerEndpoint {
                node,
                peer: format!("tcp://127.0.0.1:10{}0", node.as_bytes()[0]),
                reader_pub: format!("tcp://127.0.0.1:20{}0", node.as_bytes()[0]),
                follower_pub: Some(format!("tcp://127.0.0.1:30{}0", node.as_bytes()[0])),
            })
            .collect(),
        first: 4,
        partitions: (4..6)
            .map(|number| TopicPartition {
                number,
                group: GroupId::from_bytes([number as u8; 16]),
                config_epoch: 6,
                incarnation: PartitionIncarnation::from_bytes([number as u8 + 20; 16]),
                members: members.clone(),
            })
            .collect(),
    }
}

#[test]
fn request_and_page_round_trip() {
    let frame = EnvelopeLimits::default();
    let limits = Limits::default();
    let mut metadata = Vec::with_capacity(4096);
    let request = TopicRequest {
        name: "orders".to_owned(),
        first: 4,
        maximum: 2,
    };
    let header = encode_topic_request(
        envelope(Opcode::StateSnapshotRequest),
        &request,
        &mut metadata,
        frame,
        limits,
    )
    .unwrap();
    assert_eq!(
        decode_topic_request(packet(&header, &metadata), frame, limits),
        Ok(request)
    );
    assert!(super::super::decode_request(packet(&header, &metadata), frame, limits).is_err());

    let page = page();
    let header = encode_topic_page(
        envelope(Opcode::StateSnapshot),
        &page,
        &mut metadata,
        frame,
        limits,
    )
    .unwrap();
    assert_eq!(page.metadata_bytes(), Ok(metadata.len()));
    assert_eq!(
        decode_topic_page(packet(&header, &metadata), frame, limits),
        Ok(page)
    );
    assert!(super::super::decode_snapshot(packet(&header, &metadata), frame, limits).is_err());
}

#[test]
fn page_rejects_changed_identity_and_unbounded_fields() {
    let mut current = page();
    let limits = Limits::default();
    assert!(current.valid(limits));
    current.partitions[1].number = 7;
    assert!(!current.valid(limits));
    current = page();
    current.partitions[0].members[0] = node(8);
    assert!(!current.valid(limits));
    current = page();
    current.partitions[0].config_epoch = 0;
    assert!(!current.valid(limits));
    current = page();
    current.brokers[0].peer = format!("tcp://{}", "a".repeat(1024));
    assert!(!current.valid(limits));
    current = page();
    current.brokers[0].peer = "tcp://127.0.0.1:0".to_owned();
    assert!(!current.valid(limits));
    current = page();
    current.policy = Policy::LocalDurable;
    assert!(!current.valid(limits));
}

#[test]
fn truncated_page_and_excessive_request_are_rejected() {
    let frame = EnvelopeLimits::default();
    let limits = Limits::default();
    let mut metadata = Vec::with_capacity(4096);
    let header = encode_topic_page(
        envelope(Opcode::StateSnapshot),
        &page(),
        &mut metadata,
        frame,
        limits,
    )
    .unwrap();
    let shortened = &metadata[..metadata.len() - 1];
    assert!(
        decode_topic_page(
            Packet {
                envelope: envelope(Opcode::StateSnapshot),
                metadata: shortened,
                payload: &[]
            },
            frame,
            limits
        )
        .is_err()
    );
    assert_eq!(header.len(), ENVELOPE_BYTES);
    assert_eq!(
        encode_topic_request(
            envelope(Opcode::StateSnapshotRequest),
            &TopicRequest {
                name: "orders".to_owned(),
                first: 0,
                maximum: 257
            },
            &mut metadata,
            frame,
            limits,
        ),
        Err(CodecError::Profile)
    );
}

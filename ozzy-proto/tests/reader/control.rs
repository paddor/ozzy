//! Reader control commands: routing, `SUBSCRIBE` and `SUBSCRIBED`, credit and
//! cancellation, `ACK` positions, and capability.

use super::*;

#[test]
fn reader_routing_preserves_shared_partition_source_without_decoding_records() {
    let source = sources()[1];
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let envelope = envelope(Opcode::Records, false);
    reader::encode_records(
        envelope,
        RecordHeader {
            subscription: subscription(),
            source,
            first_offset: 0,
        },
        &[Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([1; 16]),
            parts: &[b"record"],
        }],
        Frames {
            metadata: &mut metadata,
            payload: &mut payload,
        },
        limits,
    )
    .unwrap();
    let packet = Packet {
        envelope,
        metadata: &metadata,
        payload: &payload,
    };
    assert_eq!(reader::route(packet, limits.envelope).unwrap(), source);
    for length in 0..89 {
        assert!(
            reader::route(
                Packet {
                    metadata: &metadata[..length],
                    ..packet
                },
                limits.envelope
            )
            .is_err()
        );
    }
    // Leave the subscription/source valid while removing every record descriptor.
    let prefix_only = Packet {
        metadata: &metadata[..89],
        ..packet
    };
    assert_eq!(reader::route(prefix_only, limits.envelope).unwrap(), source);
    assert!(reader::decode_records(prefix_only, limits).is_err());
    let mut invalid = metadata.clone();
    invalid[32] = 0;
    assert!(
        reader::route(
            Packet {
                metadata: &invalid,
                ..packet
            },
            limits.envelope
        )
        .is_err()
    );
}

fn packet(e: Envelope, metadata: &[u8]) -> Packet<'_> {
    Packet {
        envelope: e,
        metadata,
        payload: &[],
    }
}

#[test]
fn subscribe_fixed_bytes_and_all_truncations() {
    let limits = DataLimits::default().envelope;
    let topic = Topic::new("s", "t").unwrap();
    let e = envelope(Opcode::Subscribe, false);
    let mut bytes = Vec::with_capacity(1024);
    let header = reader::encode_subscribe(
        e,
        &Subscribe {
            subscription: subscription(),
            target: reader::Target::Local {
                topic: topic.clone(),
                partition: PartitionId::ZERO,
            },
            start: ozzy_proto::reader::Start::Offset(9),
        },
        &mut bytes,
        limits,
    )
    .unwrap();
    let mut golden = vec![4; 16];
    golden.extend_from_slice(&5_u128.to_be_bytes());
    golden.push(0);
    golden.extend_from_slice(b"\0\0\0\x01s\0\0\0\x01t\0\0\0\0\x02\0\0\0\0\0\0\0\x09");
    assert_eq!(bytes, golden);
    assert_eq!(header[4], 1);
    let decoded = reader::decode_subscribe(
        decode_packet(&[&header, &bytes, &[]], limits).unwrap(),
        limits,
    )
    .unwrap();
    assert_eq!(decoded.subscription, subscription());
    assert_eq!(
        decoded.target,
        reader::Target::Local {
            topic,
            partition: PartitionId::ZERO
        }
    );
    assert_eq!(decoded.start, ozzy_proto::reader::Start::Offset(9));
    for n in 0..bytes.len() {
        assert!(reader::decode_subscribe(packet(e, &bytes[..n]), limits).is_err());
    }
    bytes.push(0);
    assert!(reader::decode_subscribe(packet(e, &bytes), limits).is_err());
    bytes.truncate(55);
    bytes[33..37].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(
        reader::decode_subscribe(packet(e, &bytes), limits),
        Err(CodecError::Profile)
    );
}

#[test]
fn group_subscription_credit_and_cancellation_are_fenced_and_exact() {
    let limits = DataLimits::default().envelope;
    let source = sources()[1];
    let Source::Group {
        authority,
        partition,
        owner_epoch,
    } = source
    else {
        unreachable!()
    };
    let request = Subscribe {
        subscription: subscription(),
        target: reader::Target::Group {
            authority,
            partition,
            owner_epoch,
        },
        start: ozzy_proto::reader::Start::Offset(42),
    };
    let mut bytes = Vec::with_capacity(1024);
    let e = envelope(Opcode::Subscribe, false);
    reader::encode_subscribe(e, &request, &mut bytes, limits).unwrap();
    assert_eq!(bytes.len(), 98);
    let decoded = reader::decode_subscribe(packet(e, &bytes), limits).unwrap();
    assert_eq!(decoded.target, request.target);
    assert_eq!(decoded.start, ozzy_proto::reader::Start::Offset(42));
    for n in 0..bytes.len() {
        assert!(reader::decode_subscribe(packet(e, &bytes[..n]), limits).is_err());
    }
    let selected = Subscribed {
        resolved_offset: 0,
        subscription: subscription(),
        source,
    };
    let e = envelope(Opcode::Unsubscribe, false);
    reader::encode_unsubscribe(e, selected, &mut bytes, limits).unwrap();
    assert_eq!(
        reader::decode_unsubscribe(packet(e, &bytes), limits).unwrap(),
        selected
    );
    let e = envelope(Opcode::Unsubscribed, true);
    reader::encode_unsubscribed(e, selected, &mut bytes, limits).unwrap();
    assert_eq!(
        reader::decode_unsubscribed(packet(e, &bytes), limits).unwrap(),
        selected
    );
}

#[test]
fn both_sources_and_independent_receipt_processing_positions_roundtrip() {
    let limits = DataLimits::default().envelope;
    for source in sources() {
        let mut bytes = Vec::with_capacity(1024);
        let e = envelope(Opcode::Subscribed, true);
        let v = Subscribed {
            resolved_offset: 0,
            subscription: subscription(),
            source,
        };
        reader::encode_subscribed(e, v, &mut bytes, limits).unwrap();
        assert_eq!(
            reader::decode_subscribed(packet(e, &bytes), limits).unwrap(),
            v
        );
        for n in 0..bytes.len() {
            assert!(reader::decode_subscribed(packet(e, &bytes[..n]), limits).is_err());
        }
        for (received, processed) in [
            (None, None),
            (Some(0), None),
            (Some(9), Some(7)),
            (Some(u64::MAX), Some(u64::MAX)),
        ] {
            let v = Ack {
                subscription: subscription(),
                source,
                received,
                processed,
            };
            for e in [
                envelope(Opcode::Ack, true),
                Envelope {
                    request_id: None,
                    ..envelope(Opcode::Ack, false)
                },
            ] {
                reader::encode_ack(e, v, &mut bytes, limits).unwrap();
                assert_eq!(reader::decode_ack(packet(e, &bytes), limits).unwrap(), v);
                for n in 0..bytes.len() {
                    assert!(reader::decode_ack(packet(e, &bytes[..n]), limits).is_err());
                }
            }
        }
        let v = Ack {
            subscription: subscription(),
            source,
            received: None,
            processed: Some(0),
        };
        let old = bytes.clone();
        assert_eq!(
            reader::encode_ack(envelope(Opcode::Ack, true), v, &mut bytes, limits),
            Err(CodecError::Position)
        );
        assert_eq!(bytes, old);
    }
}

#[test]
fn reader_capability_is_explicit_and_does_not_negotiate_with_writer_only() {
    use ozzy_proto::handshake::{self, Parameters};
    let reader = Parameters::reader(DataLimits::default(), handshake::CONSUMER).unwrap();
    let writer = Parameters::append(DataLimits::default(), handshake::OWNER).unwrap();
    assert!(reader.select(writer).is_err());
    assert_eq!(reader.capabilities, 1 << 10);
}

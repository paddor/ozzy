//! `RECORDS_PUB` publications: records without a session, request,
//! subscription, or local log.

use super::*;

fn publication_records() -> [Record<'static>; 2] {
    [
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([21; 16]),
            parts: &[b"first", b""],
        },
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([22; 16]),
            parts: &[b"second"],
        },
    ]
}

fn publication_envelope() -> Envelope {
    Envelope {
        opcode: Opcode::RecordsPub,
        response: false,
        request_id: None,
        sender: NodeId::from_bytes([2; 16]),
        session: None,
    }
}

#[test]
fn publications_are_records_without_a_session_request_or_subscription() {
    let limits = DataLimits::default();
    let records = publication_records();
    let [local, group] = sources();
    let header = reader::PublicationHeader {
        source: group,
        first_offset: 40,
    };
    let (mut metadata, mut payload) = (Vec::with_capacity(4096), Vec::with_capacity(4096));
    let mut output = reader::RecordsEncoder::publication(
        publication_envelope(),
        header,
        &mut metadata,
        &mut payload,
        limits,
    )
    .unwrap();
    output
        .extend(
            records
                .iter()
                .map(|r| (r.message_id, r.encoding, r.parts.iter().copied())),
        )
        .unwrap();
    let envelope_bytes = output.finish().unwrap();

    // The record table equals a subscription's RECORDS without its 32 B identity.
    let (mut expected, mut expected_payload) = (Vec::with_capacity(4096), Vec::with_capacity(4096));
    reader::encode_records(
        Envelope {
            request_id: None,
            ..envelope(Opcode::Records, false)
        },
        RecordHeader {
            subscription: subscription(),
            source: group,
            first_offset: 40,
        },
        &records,
        Frames {
            metadata: &mut expected,
            payload: &mut expected_payload,
        },
        limits,
    )
    .unwrap();
    assert_eq!(metadata, expected[32..]);
    assert_eq!(payload, expected_payload);

    let packet = decode_packet(&[&envelope_bytes, &metadata, &payload], limits.envelope).unwrap();
    assert_eq!(packet.envelope, publication_envelope());
    let publication = reader::decode_publication(packet, limits).unwrap();
    assert_eq!(publication.header, header);
    assert_eq!(publication.records.len(), 2);
    let decoded: Vec<_> = publication.records.iter().collect();
    assert_eq!(decoded[1].message_id, records[1].message_id);

    // A reader that already has the first record takes only the rest.
    let rest = publication.records.skip(1);
    assert_eq!(rest.len(), 1);
    let second = rest.iter().next().unwrap();
    assert_eq!(second.message_id, records[1].message_id);
    assert_eq!(second.parts.collect::<Vec<_>>(), records[1].parts);
    assert_eq!(rest.payload_bytes(), b"second".len());
    assert!(publication.records.skip(2).is_empty());
    assert!(publication.records.skip(9).is_empty());

    // The topic is the group, then the partition: a group prefix matches all.
    let topic = reader::publication_topic(group).unwrap();
    assert_eq!(topic[..16], [8; 16]);
    assert_eq!(topic[16..], [11; 16]);
    assert_eq!(reader::publication_topic(local), Err(CodecError::Profile));

    let mut truncated = metadata.clone();
    truncated.pop();
    let mut bytes = envelope_bytes;
    bytes[56..60].copy_from_slice(&(truncated.len() as u32).to_be_bytes());
    let packet = decode_packet(&[&bytes, &truncated, &payload], limits.envelope).unwrap();
    assert!(reader::decode_publication(packet, limits).is_err());
}

#[test]
fn publications_name_no_link_request_subscription_or_local_log() {
    let limits = DataLimits::default();
    let [local, group] = sources();
    let shared = publication_envelope();
    let header = reader::PublicationHeader {
        source: group,
        first_offset: 40,
    };
    let (mut metadata, mut payload) = (Vec::with_capacity(4096), Vec::with_capacity(4096));
    for bad in [
        Envelope {
            session: Some(LinkSessionId::from_bytes([3; 16])),
            ..shared
        },
        Envelope {
            request_id: Some(RequestId::from_bytes([1; 16])),
            ..shared
        },
        Envelope {
            opcode: Opcode::Records,
            ..shared
        },
    ] {
        assert!(
            reader::RecordsEncoder::publication(bad, header, &mut metadata, &mut payload, limits)
                .is_err()
        );
    }
    let local_header = reader::PublicationHeader {
        source: local,
        ..header
    };
    assert!(
        reader::RecordsEncoder::publication(
            shared,
            local_header,
            &mut metadata,
            &mut payload,
            limits
        )
        .is_err()
    );
    reader::RecordsEncoder::publication(shared, header, &mut metadata, &mut payload, limits)
        .unwrap();
}

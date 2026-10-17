//! Shared payload spans and prepared blocks forwarded without copying, and
//! rejection that keeps credit and the encoded prefix.

use super::*;

#[test]
fn shared_descriptor_spans_match_record_encoding_and_reject_without_progress() {
    use ozzy_proto::data::Encoding;
    let compressed = [0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 4, 0xff];
    let records = [
        Record {
            message_id: MessageId::new(),
            encoding: Encoding::Raw,
            parts: &[b"", b"abc", b""],
        },
        Record {
            message_id: MessageId::new(),
            encoding: Encoding::Lz4 { decoded_bytes: 7 },
            parts: &[&compressed],
        },
        Record {
            message_id: MessageId::new(),
            encoding: Encoding::Raw,
            parts: &[b""],
        },
    ];
    let (descriptors, payload) = packed_records(&records);
    let body = Bytes::from(payload);
    let header = RecordHeader {
        subscription: subscription(),
        source: local(),
        first_offset: 10,
    };
    let envelope = envelope(Opcode::Records, false);
    let mut expected_metadata = Vec::with_capacity(4096);
    let mut expected_payload = Vec::with_capacity(4096);
    let expected = reader::encode_records(
        envelope,
        header,
        &records,
        Frames {
            metadata: &mut expected_metadata,
            payload: &mut expected_payload,
        },
        DataLimits::default(),
    )
    .unwrap();
    for maximum_parts in [2, 5] {
        let mut metadata = Vec::with_capacity(4096);
        let mut fallback = Vec::with_capacity(4096);
        let limits = DataLimits {
            max_parts: maximum_parts,
            ..DataLimits::default()
        };
        let mut output =
            reader::RecordsEncoder::new(envelope, header, &mut metadata, &mut fallback, limits)
                .unwrap();
        output.allow_shared_payload();
        let before = output.remaining();
        assert!(
            output
                .extend_shared_packed(
                    &descriptors[..descriptors.len() - 1],
                    &body,
                    0..body.len(),
                    3
                )
                .is_err()
        );
        assert_eq!(output.len(), 0);
        assert_eq!(output.remaining(), before);
        let result = output.extend_shared_packed(&descriptors, &body, 0..body.len(), 3);
        if maximum_parts == 2 {
            assert!(result.is_err());
            assert_eq!(output.len(), 0);
            assert_eq!(output.remaining(), before);
            continue;
        }
        assert_eq!(result, Ok(true));
        let credit = output.remaining();
        assert!(
            output
                .extend_shared_packed(&[], &body, body.len()..body.len() + 1, 1)
                .is_err()
        );
        assert_eq!(output.remaining(), credit);
        let (actual, shared) = output.finish_with_payload().unwrap();
        assert_eq!(actual, expected);
        assert_eq!(metadata, expected_metadata);
        assert_eq!(fallback.len(), 0);
        let shared = shared.unwrap();
        assert_eq!(shared.as_ptr(), body.as_ptr());
        assert_eq!(shared.as_ref(), expected_payload);
    }
}

#[test]
fn publication_forwards_one_exact_prepared_lz4_group() {
    let raw = vec![b'x'; 4096];
    let records = [Record {
        message_id: MessageId::from_bytes([7; 16]),
        encoding: ozzy_proto::data::Encoding::Raw,
        parts: &[&raw],
    }];
    let (descriptors, decoded) = packed_records(&records);
    let encoded = lz4rip::block::compress(&decoded);
    let mut backing = vec![0xaa; 17];
    let start = backing.len();
    backing.extend_from_slice(&encoded);
    let end = backing.len();
    backing.extend_from_slice(&[0xbb; 13]);
    let backing = Bytes::from(backing);
    let source = sources()[1];
    let e = Envelope {
        opcode: Opcode::RecordsPub,
        request_id: None,
        session: None,
        ..envelope(Opcode::RecordsPub, false)
    };
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(8192);
    let mut fallback = Vec::with_capacity(8192);
    let mut output = reader::RecordsEncoder::publication(
        e,
        PublicationHeader {
            source,
            first_offset: 19,
        },
        &mut metadata,
        &mut fallback,
        limits,
    )
    .unwrap();
    output.allow_shared_payload();
    assert_eq!(
        output.extend_shared_prepared_lz4(
            PreparedBlock {
                descriptors: &descriptors,
                decoded_bytes: decoded.len(),
                body: &backing,
                encoded: start..end
            },
            records.len()
        ),
        Ok(true)
    );
    let (header, payload) = output.finish_with_payload().unwrap();
    let payload = payload.unwrap();
    assert_eq!(payload.as_ptr(), backing[start..].as_ptr());
    assert_eq!(payload.as_ref(), encoded);
    assert_eq!(fallback.len(), 0);
    let packet = decode_packet(&[&header, &metadata, &payload], limits.envelope).unwrap();
    assert!(reader::decode_publication(packet, limits).is_err());
    let metadata = Bytes::from(metadata);
    let packet = decode_packet(&[&header, &metadata, &payload], limits.envelope).unwrap();
    let publication =
        reader::decode_owned_publication(packet, &metadata, &payload, limits).unwrap();
    assert_eq!(
        publication.payload_encoding,
        ozzy_proto::append::PayloadEncoding::Lz4
    );
    assert_eq!(
        publication
            .records
            .as_records()
            .iter()
            .next()
            .unwrap()
            .parts
            .next(),
        Some(raw.as_slice())
    );
}

#[test]
fn shared_payload_preserves_wire_bytes_and_mixed_backings_fall_back_to_copying() {
    use ozzy_proto::data::Encoding;
    use ozzy_proto::reader::RecordsEncoder;
    let body = Bytes::from_static(b"abc\0\0\0\x01\0\0\x04\0\xffdef");
    let shapes: &[&[&[u8]]] = &[
        &[b"", &body[..3], b""],
        &[&body[3..12]],
        &[b""],
        &[&body[12..]],
    ];
    let ranges = [
        vec![0..0, 0..3, 3..3],
        std::iter::once(3..12).collect(),
        std::iter::once(12..12).collect(),
        std::iter::once(12..15).collect(),
    ];
    let records: Vec<_> = shapes
        .iter()
        .enumerate()
        .map(|(i, parts)| Record {
            message_id: MessageId::from_bytes([i as u8 + 1; 16]),
            encoding: if i == 1 {
                Encoding::Lz4 {
                    decoded_bytes: 1024,
                }
            } else {
                Encoding::Raw
            },
            parts,
        })
        .collect();
    let limits = DataLimits::default();
    for source in sources() {
        for mixed in [false, true] {
            let envelope = Envelope {
                request_id: None,
                ..envelope(Opcode::Records, false)
            };
            let header = RecordHeader {
                subscription: subscription(),
                source,
                first_offset: 11,
            };
            let (expected_header, expected_metadata, expected_payload) =
                reader_frames(envelope, header, &records);
            let mut metadata = Vec::with_capacity(4096);
            let mut payload = Vec::with_capacity(4096);
            let mut output =
                RecordsEncoder::new(envelope, header, &mut metadata, &mut payload, limits).unwrap();
            output.allow_shared_payload();
            for (index, record) in records.iter().enumerate() {
                let backing = if mixed && index == 3 {
                    Bytes::copy_from_slice(&body)
                } else {
                    body.clone()
                };
                let shared = output
                    .push_shared(
                        record.message_id,
                        record.encoding,
                        &backing,
                        ranges[index].clone().into_iter(),
                    )
                    .unwrap();
                assert_eq!(shared, !(mixed && index == 3));
                if !shared {
                    output
                        .extend(std::iter::once((
                            record.message_id,
                            record.encoding,
                            record.parts.iter().copied(),
                        )))
                        .unwrap();
                }
            }
            assert_eq!(output.payload_bytes(), body.len());
            assert_eq!(
                output.remaining().max_records,
                limits.max_records - records.len()
            );
            let (actual_header, shared) = output.finish_with_payload().unwrap();
            assert_eq!(actual_header, expected_header);
            assert_eq!(metadata, expected_metadata);
            if mixed {
                assert!(shared.is_none());
                assert_eq!(payload, expected_payload);
            } else {
                let shared = shared.unwrap();
                assert_eq!(payload.len(), 0);
                assert_eq!(shared.as_ptr(), body.as_ptr());
                assert_eq!(shared.as_ref(), expected_payload);
            }
        }
    }
}

fn reader_frames(
    envelope: Envelope,
    header: RecordHeader,
    records: &[Record<'_>],
) -> ([u8; ozzy_proto::ENVELOPE_BYTES], Vec<u8>, Vec<u8>) {
    let mut metadata = Vec::with_capacity(4096);
    let mut payload = Vec::with_capacity(4096);
    let header = reader::encode_records(
        envelope,
        header,
        records,
        Frames {
            metadata: &mut metadata,
            payload: &mut payload,
        },
        DataLimits::default(),
    )
    .unwrap();
    (header, metadata, payload)
}

#[test]
fn shared_payload_rejection_preserves_credit_and_prefix() {
    use ozzy_proto::data::Encoding;
    use ozzy_proto::reader::RecordsEncoder;
    let body = Bytes::from_static(b"abcdef");
    let limits = DataLimits {
        max_records: 2,
        max_parts: 3,
        max_record_bytes: 3,
        envelope: ozzy_proto::EnvelopeLimits {
            max_payload_bytes: 4,
            max_metadata_bytes: 4096,
        },
    };
    let mut metadata = Vec::with_capacity(4096);
    let mut payload = Vec::with_capacity(4);
    let mut output = RecordsEncoder::new(
        Envelope {
            request_id: None,
            ..envelope(Opcode::Records, false)
        },
        RecordHeader {
            subscription: subscription(),
            source: local(),
            first_offset: 0,
        },
        &mut metadata,
        &mut payload,
        limits,
    )
    .unwrap();
    output.allow_shared_payload();
    let id = MessageId::from_bytes([9; 16]);
    assert!(
        !output
            .push_shared(id, Encoding::Raw, &body, [0..1, 2..3].into_iter())
            .unwrap()
    );
    assert!(
        output
            .push_shared(id, Encoding::Raw, &body, std::iter::once(0..7))
            .is_err()
    );
    assert!(
        output
            .push_shared(
                id,
                Encoding::Raw,
                &body,
                [std::ops::Range { start: 2, end: 1 }].into_iter()
            )
            .is_err()
    );
    assert!(
        output
            .push_shared(id, Encoding::Raw, &body, std::iter::once(1..3))
            .unwrap()
    );
    let remaining = output.remaining();
    assert!(
        output
            .push_shared(id, Encoding::Raw, &body, std::iter::once(3..6))
            .is_err()
    );
    assert_eq!(output.remaining(), remaining);
    assert_eq!(output.len(), 1);
    assert_eq!(output.payload_bytes(), 2);
    let (_, shared) = output.finish_with_payload().unwrap();
    assert_eq!(shared.unwrap().as_ref(), b"bc");
    assert_eq!(payload.len(), 0);
}

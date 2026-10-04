use bytes::Bytes;
use ozzy_proto::data::{CodecError, DataLimits, Record};
use ozzy_proto::reader::{
    self, Ack, PublicationHeader, RecordHeader, Source, Subscribe, Subscribed, Subscription,
};
use ozzy_proto::{
    Envelope, GroupId, LinkSessionId, MessageId, NodeId, Opcode, Packet, PartitionId,
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
        &mut metadata,
        &mut payload,
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
        &mut expected_metadata,
        &mut expected_payload,
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
        &mut metadata,
        &mut payload,
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
            &descriptors,
            decoded.len(),
            &backing,
            start..end,
            records.len(),
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
    let publication = reader::decode_owned_publication(
        packet,
        &metadata,
        &payload,
        limits,
        &mut bytes::BytesMut::new(),
    )
    .unwrap();
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
        &mut metadata,
        &mut payload,
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

#[test]
fn packed_reader_chunks_match_generic_encoding_for_both_sources() {
    let records = [
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([1; 16]),
            parts: &[b"first", b""],
        },
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([2; 16]),
            parts: &[b"", b"\0\xff", b"last"],
        },
    ];
    let limits = DataLimits {
        max_parts: 5,
        max_records: 2,
        ..DataLimits::default()
    };
    for source in sources() {
        let e = Envelope {
            request_id: None,
            ..envelope(Opcode::Records, false)
        };
        let header = RecordHeader {
            subscription: subscription(),
            source,
            first_offset: 40,
        };
        let mut metadata = Vec::with_capacity(4096);
        let mut payload = Vec::with_capacity(4096);
        let mut output =
            reader::RecordsEncoder::new(e, header, &mut metadata, &mut payload, limits).unwrap();
        for record in records {
            let (table, bytes) = packed_records(&[record]);
            output.extend_packed(&table, &bytes, 1).unwrap();
        }
        assert_eq!(output.remaining().max_parts, 0);
        assert_eq!(output.remaining().max_records, 0);
        let encoded = output.finish().unwrap();
        let mut expected_metadata = Vec::with_capacity(4096);
        let mut expected_payload = Vec::with_capacity(4096);
        let expected = reader::encode_records(
            e,
            header,
            &records,
            &mut expected_metadata,
            &mut expected_payload,
            limits,
        )
        .unwrap();
        assert_eq!(
            (encoded, metadata, payload),
            (expected, expected_metadata, expected_payload)
        );
    }
}

#[test]
fn packed_reader_rejects_bad_tables_and_keeps_previous_chunk() {
    let e = Envelope {
        request_id: None,
        ..envelope(Opcode::Records, false)
    };
    let header = RecordHeader {
        subscription: subscription(),
        source: local(),
        first_offset: 10,
    };
    let record = Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([9; 16]),
        parts: &[b"abc"],
    };
    let (table, bytes) = packed_records(&[record]);
    let limits = DataLimits {
        max_parts: 2,
        max_records: 2,
        max_record_bytes: 3,
        ..DataLimits::default()
    };
    let mut metadata = Vec::with_capacity(4096);
    let mut payload = Vec::with_capacity(4096);
    let mut output =
        reader::RecordsEncoder::new(e, header, &mut metadata, &mut payload, limits).unwrap();
    output.extend_packed(&table, &bytes, 1).unwrap();
    let remaining = output.remaining();
    for end in 0..table.len() {
        assert!(output.extend_packed(&table[..end], &bytes, 1).is_err());
    }
    for end in 0..bytes.len() {
        assert!(output.extend_packed(&table, &bytes[..end], 1).is_err());
    }
    assert!(output.extend_packed(&table, &bytes, 0).is_err());
    assert!(output.extend_packed(&table, &bytes, 2).is_err());
    let mut invalid = table.clone();
    for count in [0_u32, u32::MAX] {
        invalid[16..20].copy_from_slice(&count.to_be_bytes());
        assert!(output.extend_packed(&invalid, &bytes, 1).is_err());
    }
    invalid.copy_from_slice(&table);
    invalid[20..24].copy_from_slice(&4_u32.to_be_bytes());
    assert!(output.extend_packed(&invalid, b"abcd", 1).is_err());
    invalid.copy_from_slice(&table);
    invalid.push(0);
    assert!(output.extend_packed(&invalid, &bytes, 1).is_err());
    assert!(output.extend_packed(&table, b"abcd", 1).is_err());
    assert_eq!(output.len(), 1);
    assert_eq!(output.remaining(), remaining);
    let encoded = output.finish().unwrap();
    let mut expected_metadata = Vec::with_capacity(4096);
    let mut expected_payload = Vec::with_capacity(4096);
    let expected = reader::encode_records(
        e,
        header,
        &[record],
        &mut expected_metadata,
        &mut expected_payload,
        limits,
    )
    .unwrap();
    assert_eq!(
        (encoded, metadata, payload),
        (expected, expected_metadata, expected_payload)
    );
}

#[test]
fn incremental_part_budget_survives_failed_chunk_without_extra_record_pass() {
    use std::cell::Cell;

    let limits = DataLimits {
        max_parts: 3,
        max_records: 4,
        max_record_bytes: 1024,
        envelope: ozzy_proto::EnvelopeLimits {
            max_metadata_bytes: 4096,
            max_payload_bytes: 4096,
        },
    };
    let records = [
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([1; 16]),
            parts: &[b"a", b""],
        },
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([2; 16]),
            parts: &[b"bc"],
        },
    ];
    let envelope = Envelope {
        request_id: None,
        ..envelope(Opcode::Records, false)
    };
    let header = RecordHeader {
        subscription: subscription(),
        source: local(),
        first_offset: 0,
    };
    let mut metadata = Vec::with_capacity(4096);
    let mut payload = Vec::with_capacity(4096);
    let mut encoder =
        reader::RecordsEncoder::new(envelope, header, &mut metadata, &mut payload, limits).unwrap();
    let visits = Cell::new(0);
    let first = || {
        records[..1]
            .iter()
            .inspect(|_| visits.set(visits.get() + 1))
            .map(|record| {
                (
                    record.message_id,
                    record.encoding,
                    record.parts.iter().copied(),
                )
            })
    };
    encoder.extend(first()).unwrap();
    assert_eq!(visits.get(), 1, "validate and encode in the same pass");
    assert!(encoder.extend(first()).is_err());
    assert_eq!(encoder.len(), 1);
    encoder
        .extend(records[1..].iter().map(|record| {
            (
                record.message_id,
                record.encoding,
                record.parts.iter().copied(),
            )
        }))
        .unwrap();
    let wire_header = encoder.finish().unwrap();
    let mut expected_metadata = Vec::with_capacity(4096);
    let mut expected_payload = Vec::with_capacity(4096);
    let expected = reader::encode_records(
        envelope,
        header,
        &records,
        &mut expected_metadata,
        &mut expected_payload,
        limits,
    )
    .unwrap();
    assert_eq!(
        (wire_header, metadata, payload),
        (expected, expected_metadata, expected_payload)
    );
}

#[test]
fn late_reader_chunk_errors_preserve_bytes_capacity_and_credit() {
    let envelope = Envelope {
        request_id: None,
        ..envelope(Opcode::Records, false)
    };
    let header = RecordHeader {
        subscription: subscription(),
        source: local(),
        first_offset: 7,
    };
    let kept = Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([1; 16]),
        parts: &[b"a"],
    };
    let limits = DataLimits {
        max_records: 4,
        max_parts: 8,
        max_record_bytes: 16,
        envelope: ozzy_proto::EnvelopeLimits {
            max_metadata_bytes: 4096,
            max_payload_bytes: 64,
        },
    };
    let mut expected_metadata = Vec::with_capacity(4096);
    let mut expected_payload = Vec::with_capacity(64);
    let expected = reader::encode_records(
        envelope,
        header,
        &[kept],
        &mut expected_metadata,
        &mut expected_payload,
        limits,
    )
    .unwrap();
    for failure in 0..7 {
        let mut limits = limits;
        let mut metadata_capacity = 4096;
        let mut payload_capacity = 64;
        let mut last_parts: &[&[u8]] = &[b"xyz"];
        match failure {
            0 => last_parts = &[],
            1 => limits.max_parts = 2,
            2 => limits.max_record_bytes = 2,
            3 => limits.envelope.max_metadata_bytes = expected_metadata.len() + 24,
            4 => limits.envelope.max_payload_bytes = 2,
            5 => metadata_capacity = expected_metadata.len() + 24,
            6 => payload_capacity = 2,
            _ => unreachable!(),
        }
        let mut metadata = Vec::with_capacity(metadata_capacity);
        let mut payload = Vec::with_capacity(payload_capacity);
        let allocations = (
            metadata.as_ptr(),
            metadata.capacity(),
            payload.as_ptr(),
            payload.capacity(),
        );
        let mut encoder =
            reader::RecordsEncoder::new(envelope, header, &mut metadata, &mut payload, limits)
                .unwrap();
        encoder
            .extend(std::iter::once((
                kept.message_id,
                kept.encoding,
                kept.parts.iter().copied(),
            )))
            .unwrap();
        let credit = encoder.remaining();
        // The first record fits. Rejection must undo it and any partially written
        // descriptor or payload of the second record.
        let chunk = [
            kept,
            Record {
                parts: last_parts,
                ..kept
            },
        ];
        assert!(
            encoder
                .extend(chunk.iter().map(|record| (
                    record.message_id,
                    record.encoding,
                    record.parts.iter().copied()
                )))
                .is_err(),
            "case {failure}"
        );
        assert_eq!(encoder.len(), 1);
        assert_eq!(encoder.remaining(), credit);
        assert_eq!(encoder.finish().unwrap(), expected);
        assert_eq!(metadata, expected_metadata, "case {failure}");
        assert_eq!(payload, expected_payload, "case {failure}");
        assert_eq!(
            (
                metadata.as_ptr(),
                metadata.capacity(),
                payload.as_ptr(),
                payload.capacity()
            ),
            allocations
        );
    }
}

#[test]
fn packed_buffer_adoption_reuses_payload_and_matches_wire_bytes() {
    let limits = DataLimits {
        max_record_bytes: 1024 * 1024,
        max_records: 3,
        max_parts: 6,
        envelope: ozzy_proto::EnvelopeLimits {
            max_metadata_bytes: 4096,
            max_payload_bytes: 4096,
        },
    };
    for source in sources() {
        let records = [
            Record {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::new(),
                parts: &[b"first", b""],
            },
            Record {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::new(),
                parts: &[b"", b"\0\xff", b""],
            },
        ];
        let mut batch = ozzy_proto::data::RecordBuffer::new(limits);
        let mut metadata = Vec::with_capacity(4096);
        let mut payload = Vec::with_capacity(4096);
        for _ in 0..3 {
            for record in records {
                batch
                    .push(
                        record.message_id,
                        record.encoding,
                        record.parts.iter().copied(),
                        limits,
                    )
                    .unwrap();
            }
            let pointer = batch
                .records()
                .iter()
                .next()
                .unwrap()
                .parts
                .next()
                .unwrap()
                .as_ptr();
            let e = Envelope {
                request_id: None,
                ..envelope(Opcode::Records, false)
            };
            let header = RecordHeader {
                subscription: subscription(),
                source,
                first_offset: 40,
            };
            let mut too_small = limits;
            too_small.envelope.max_payload_bytes = 1;
            let mut output =
                reader::RecordsEncoder::new(e, header, &mut metadata, &mut payload, too_small)
                    .unwrap();
            assert!(output.take_buffer(&mut batch).is_err());
            assert!(output.is_empty());
            assert_eq!(batch.len(), 2);
            let mut output =
                reader::RecordsEncoder::new(e, header, &mut metadata, &mut payload, limits)
                    .unwrap();
            output.take_buffer(&mut batch).unwrap();
            let encoded = output.finish().unwrap();
            assert!(batch.is_empty());
            assert_eq!(
                payload.as_ptr(),
                pointer,
                "payload was copied rather than adopted"
            );
            let mut expected_metadata = Vec::with_capacity(4096);
            let mut expected_payload = Vec::with_capacity(4096);
            let expected = reader::encode_records(
                e,
                header,
                &records,
                &mut expected_metadata,
                &mut expected_payload,
                limits,
            )
            .unwrap();
            assert_eq!(encoded, expected);
            assert_eq!(metadata, expected_metadata);
            assert_eq!(payload, expected_payload);
        }
    }
}

#[test]
fn incremental_records_match_single_encoding_and_failed_chunks_preserve_output() {
    let limits = DataLimits {
        max_records: 3,
        max_parts: 6,
        ..DataLimits::default()
    };
    let records = [
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::new(),
            parts: &[b"first", b""],
        },
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::new(),
            parts: &[b"second"],
        },
        Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::new(),
            parts: &[b"", b"\0\xff"],
        },
    ];
    for source in sources() {
        let e = Envelope {
            request_id: None,
            ..envelope(Opcode::Records, false)
        };
        let header = RecordHeader {
            subscription: subscription(),
            source,
            first_offset: 40,
        };
        let mut metadata = Vec::with_capacity(4096);
        let mut payload = Vec::with_capacity(4096);
        let mut output =
            reader::RecordsEncoder::new(e, header, &mut metadata, &mut payload, limits).unwrap();
        output
            .extend(
                records[..1]
                    .iter()
                    .map(|r| (r.message_id, r.encoding, r.parts.iter().copied())),
            )
            .unwrap();
        assert_eq!(output.len(), 1);
        assert!(
            output
                .extend(
                    records
                        .iter()
                        .map(|r| (r.message_id, r.encoding, r.parts.iter().copied()))
                )
                .is_err()
        );
        assert_eq!(output.len(), 1);
        output
            .extend(
                records[1..]
                    .iter()
                    .map(|r| (r.message_id, r.encoding, r.parts.iter().copied())),
            )
            .unwrap();
        let encoded = output.finish().unwrap();
        let mut expected_metadata = Vec::with_capacity(4096);
        let mut expected_payload = Vec::with_capacity(4096);
        let expected = reader::encode_records(
            e,
            header,
            &records,
            &mut expected_metadata,
            &mut expected_payload,
            limits,
        )
        .unwrap();
        assert_eq!(
            (encoded, metadata, payload),
            (expected, expected_metadata, expected_payload)
        );
    }
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
fn local_records_fixed_metadata_shared_bytes_and_owned_cursor() {
    let limits = DataLimits::default();
    let e = envelope(Opcode::Records, false);
    let record = Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([8; 16]),
        parts: &[b"ab", b"", b"c"],
    };
    let second = Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([9; 16]),
        parts: &[b"", b"d"],
    };
    let v = RecordHeader {
        subscription: subscription(),
        source: local(),
        first_offset: 10,
    };
    let mut metadata = Vec::with_capacity(1024);
    let mut payload = Vec::with_capacity(1024);
    let header =
        reader::encode_records(e, v, &[record, second], &mut metadata, &mut payload, limits)
            .unwrap();
    let mut golden = vec![4; 16];
    golden.extend_from_slice(&5_u128.to_be_bytes());
    golden.push(0);
    golden.extend_from_slice(&[6; 16]);
    golden.extend_from_slice(&7_u32.to_be_bytes());
    golden.extend_from_slice(&10_u64.to_be_bytes());
    golden.push(ozzy_proto::append::PayloadEncoding::Raw as u8);
    golden.extend_from_slice(&4_u32.to_be_bytes());
    golden.extend_from_slice(&2_u32.to_be_bytes());
    golden.extend_from_slice(&[8; 16]);
    for n in [3_u32, 2, 0, 1] {
        golden.extend_from_slice(&n.to_be_bytes());
    }
    golden.extend_from_slice(&[9; 16]);
    for n in [2_u32, 0, 1] {
        golden.extend_from_slice(&n.to_be_bytes());
    }
    assert_eq!(metadata, golden);
    assert_eq!(payload, b"abcd");
    let metadata = Bytes::from(metadata);
    let payload = Bytes::from(payload);
    let delivery = reader::decode_records(
        decode_packet(&[&header, &metadata, &payload], limits.envelope).unwrap(),
        limits,
    )
    .unwrap();
    assert_eq!(delivery.header, v);
    let owned = delivery.records.to_owned(&metadata, &payload);
    assert_eq!(owned.len(), 2);
    assert_eq!(owned.payload_bytes_after(0), 4);
    assert_eq!(owned.payload_bytes_after(1), 1);
    assert_eq!(owned.payload_bytes_after(99), 0);
    let pointer = payload.as_ptr();
    drop(metadata);
    drop(payload);
    let mut records = owned.into_records();
    let first = records.next().unwrap();
    assert_eq!(first.payload[0].as_ptr(), pointer);
    assert_eq!(
        first.payload.as_slice(),
        &[
            Bytes::from_static(b"ab"),
            Bytes::new(),
            Bytes::from_static(b"c")
        ]
    );
    assert_eq!(records.len(), 1);
    let second = records.next().unwrap();
    assert_eq!(
        second.payload.as_slice(),
        &[Bytes::new(), Bytes::from_static(b"d")]
    );
    assert_eq!(records.len(), 0);
    assert!(records.next().is_none());
}

#[test]
fn reader_records_reject_malformed_limits_sources_correlation_and_overflow() {
    let limits = DataLimits::default();
    let parts: &[&[u8]] = &[b"", b"x"];
    let records = [Record {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([8; 16]),
        parts,
    }];
    for source in sources() {
        let mut metadata = Vec::with_capacity(1024);
        let mut payload = Vec::with_capacity(1024);
        let v = RecordHeader {
            subscription: subscription(),
            source,
            first_offset: 0,
        };
        let e = envelope(Opcode::Records, false);
        reader::encode_records(e, v, &records, &mut metadata, &mut payload, limits).unwrap();
        for n in 0..metadata.len() {
            assert!(
                reader::decode_records(
                    Packet {
                        envelope: e,
                        metadata: &metadata[..n],
                        payload: &payload
                    },
                    limits
                )
                .is_err()
            );
        }
        let valid = metadata.clone();
        metadata[32] = 2;
        assert!(
            reader::decode_records(
                Packet {
                    envelope: e,
                    metadata: &metadata,
                    payload: &payload
                },
                limits
            )
            .is_err()
        );
        metadata.copy_from_slice(&valid);
        let p = Packet {
            envelope: e,
            metadata: &metadata,
            payload: &payload,
        };
        assert!(
            reader::decode_records(
                p,
                DataLimits {
                    max_parts: 1,
                    ..limits
                }
            )
            .is_err()
        );
        assert!(reader::decode_records(Packet { payload: &[], ..p }, limits).is_err());
        let old = (metadata.clone(), payload.clone());
        assert!(
            reader::encode_records(
                e,
                RecordHeader {
                    first_offset: u64::MAX,
                    ..v
                },
                &records,
                &mut metadata,
                &mut payload,
                limits
            )
            .is_err()
        );
        assert_eq!((metadata.clone(), payload.clone()), old);
        let mut tiny = vec![99];
        assert_eq!(
            reader::encode_records(e, v, &records, &mut tiny, &mut payload, limits),
            Err(CodecError::Capacity)
        );
        assert_eq!(tiny, [99]);
        assert_eq!(payload, old.1);
        let mut wrong = e;
        wrong.response = true;
        assert_eq!(
            reader::encode_records(wrong, v, &records, &mut metadata, &mut payload, limits),
            Err(CodecError::Command)
        );
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

#[test]
fn raw_single_reader_records_match_generic_encoding() {
    for source in sources() {
        for size in [0, 1, 16, 255, 256, 4096] {
            let bytes = vec![0x7e; size];
            let records = [
                Record {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([1; 16]),
                    parts: &[b"prefix"],
                },
                Record {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([2; 16]),
                    parts: &[&bytes],
                },
            ];
            let e = Envelope {
                request_id: None,
                ..envelope(Opcode::Records, false)
            };
            let header = RecordHeader {
                subscription: subscription(),
                source,
                first_offset: 7,
            };
            let limits = DataLimits::default();
            let mut metadata = Vec::with_capacity(8192);
            let mut payload = Vec::with_capacity(8192);
            let mut output =
                reader::RecordsEncoder::new(e, header, &mut metadata, &mut payload, limits)
                    .unwrap();
            for record in records {
                output.push_raw(record.message_id, record.parts[0]).unwrap();
            }
            let encoded = output.finish().unwrap();
            let mut expected_metadata = Vec::with_capacity(8192);
            let mut expected_payload = Vec::with_capacity(8192);
            let expected = reader::encode_records(
                e,
                header,
                &records,
                &mut expected_metadata,
                &mut expected_payload,
                limits,
            )
            .unwrap();
            assert_eq!(encoded, expected);
            assert_eq!(metadata, expected_metadata);
            assert_eq!(payload, expected_payload);
        }
    }
}

#[test]
fn rejected_raw_single_reader_record_preserves_prefix_and_credit() {
    let e = Envelope {
        request_id: None,
        ..envelope(Opcode::Records, false)
    };
    for failure in 0..8 {
        let mut limits = DataLimits::default();
        let mut header = RecordHeader {
            subscription: subscription(),
            source: local(),
            first_offset: 7,
        };
        let kept = Record {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([1; 16]),
            parts: &[b"a"],
        };
        if failure == 7 {
            header.first_offset = u64::MAX - 1;
        }
        let mut expected_metadata = Vec::with_capacity(4096);
        let mut expected_payload = Vec::with_capacity(4096);
        let expected = reader::encode_records(
            e,
            header,
            &[kept],
            &mut expected_metadata,
            &mut expected_payload,
            limits,
        )
        .unwrap();
        let mut metadata_capacity = 4096;
        let mut payload_capacity = 4096;
        match failure {
            0 => limits.max_records = 1,
            1 => limits.max_parts = 1,
            2 => limits.max_record_bytes = 2,
            3 => limits.envelope.max_metadata_bytes = expected_metadata.len() + 23,
            4 => limits.envelope.max_payload_bytes = 3,
            5 => metadata_capacity = expected_metadata.len() + 23,
            6 => payload_capacity = 3,
            7 => (),
            _ => unreachable!(),
        }
        let mut metadata = Vec::with_capacity(metadata_capacity);
        let mut payload = Vec::with_capacity(payload_capacity);
        let allocations = (metadata.as_ptr(), payload.as_ptr());
        let mut output =
            reader::RecordsEncoder::new(e, header, &mut metadata, &mut payload, limits).unwrap();
        output.push_raw(kept.message_id, b"a").unwrap();
        let credit = output.remaining();
        assert!(
            output.push_raw(kept.message_id, b"xyz").is_err(),
            "{failure}"
        );
        assert_eq!(output.remaining(), credit);
        assert_eq!(output.len(), 1);
        assert_eq!(output.finish().unwrap(), expected);
        assert_eq!(metadata, expected_metadata);
        assert_eq!(payload, expected_payload);
        assert_eq!((metadata.as_ptr(), payload.as_ptr()), allocations);
    }
}

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
        &mut expected,
        &mut expected_payload,
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

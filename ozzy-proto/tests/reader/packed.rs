//! Packed, incremental and single-record encoding, matched against the
//! generic encoder, with failed chunks leaving output unchanged.

use super::*;

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
            Frames {
                metadata: &mut expected_metadata,
                payload: &mut expected_payload,
            },
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
        Frames {
            metadata: &mut expected_metadata,
            payload: &mut expected_payload,
        },
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
        Frames {
            metadata: &mut expected_metadata,
            payload: &mut expected_payload,
        },
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
        Frames {
            metadata: &mut expected_metadata,
            payload: &mut expected_payload,
        },
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
                Frames {
                    metadata: &mut expected_metadata,
                    payload: &mut expected_payload,
                },
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
            Frames {
                metadata: &mut expected_metadata,
                payload: &mut expected_payload,
            },
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
                Frames {
                    metadata: &mut expected_metadata,
                    payload: &mut expected_payload,
                },
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
            Frames {
                metadata: &mut expected_metadata,
                payload: &mut expected_payload,
            },
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

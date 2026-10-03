use ozzy_journal::operation::validate_operation_body;

use super::*;

fn compare(kind: OperationKind, input: &[u8], limits: OperationLimits) {
    let decoded = decode_operation_body(kind, input, limits);
    assert_eq!(
        validate_operation_body(kind, input, limits),
        decoded.as_ref().map(|_| ()).map_err(Clone::clone),
        "{kind:?}: {input:?}, {limits:?}",
    );
    if kind == OperationKind::Append {
        let batches = ozzy_journal::operation::decode_append_batches(input, limits);
        assert_eq!(
            batches.as_ref().map(|_| ()).map_err(Clone::clone),
            decoded.as_ref().map(|_| ()).map_err(Clone::clone)
        );
        if let Ok(batches) = batches {
            let append = ozzy_journal::operation::decode_append_view(input, limits).unwrap();
            assert_eq!(batches.len(), append.batches().len());
            for (batch, expected) in batches.iter().zip(append.batches()) {
                assert_eq!(batch.summary, expected.summary);
                assert_eq!(
                    batch.append_timestamp_millis,
                    expected.append_timestamp_millis
                );
                for (record, expected) in batch.records().zip(expected.records()) {
                    assert_eq!(record.message_id, expected.message_id);
                    assert!(record.parts.eq(expected.parts));
                }
            }
        }
        let view = ozzy_journal::operation::decode_append_view(input, limits);
        assert_eq!(
            view.as_ref().map(|_| ()).map_err(Clone::clone),
            decoded.as_ref().map(|_| ()).map_err(Clone::clone)
        );
        if let (Ok(view), Ok(OperationBody::Append(append))) = (view, &decoded) {
            assert_eq!(view.batches().len(), append.batches.len());
            for (view, batch) in view.batches().zip(&append.batches) {
                assert_eq!(view.summary.partition, batch.partition);
                assert_eq!(view.summary.owner_epoch, batch.owner_epoch);
                assert_eq!(view.summary.producer_id, batch.producer_id);
                assert_eq!(view.summary.producer_epoch, batch.producer_epoch);
                assert_eq!(view.summary.first_sequence, batch.first_sequence);
                assert_eq!(view.summary.first_offset, batch.first_offset);
                assert_eq!(view.append_timestamp_millis, batch.append_timestamp_millis);
                assert_eq!(view.records().len(), batch.records.len());
                for (view, record) in view.records().zip(&batch.records) {
                    assert_eq!(view.message_id, record.message_id);
                    assert_eq!(view.parts.len(), record.parts.len());
                    assert_eq!(
                        view.parts.payload_bytes(),
                        record.parts.iter().map(<[u8]>::len).sum::<usize>()
                    );
                    for (part, expected) in view.parts.zip(record.parts.iter()) {
                        assert_eq!(part, expected);
                        assert_eq!(part.as_ptr(), expected.as_ptr());
                    }
                }
            }
        }
        let summary = ozzy_journal::operation::decode_append_summary(input, limits);
        assert_eq!(
            summary.as_ref().map(|_| ()).map_err(Clone::clone),
            decoded.as_ref().map(|_| ()).map_err(Clone::clone)
        );
        if let (Ok(summary), Ok(OperationBody::Append(append))) = (&summary, &decoded) {
            assert_eq!(summary.batches().len(), append.batches.len());
            for (summary, batch) in summary.batches().iter().zip(&append.batches) {
                assert_eq!(summary.partition, batch.partition);
                assert_eq!(summary.owner_epoch, batch.owner_epoch);
                assert_eq!(summary.producer_id, batch.producer_id);
                assert_eq!(summary.producer_epoch, batch.producer_epoch);
                assert_eq!(summary.first_sequence, batch.first_sequence);
                assert_eq!(summary.first_offset, batch.first_offset);
                assert_eq!(summary.record_count, batch.records.len());
                assert_eq!(
                    summary.nonzero_message_ids,
                    batch
                        .records
                        .iter()
                        .all(|record| record.message_id.as_bytes() != &[0; 16])
                );
            }
        }
    }
    if let Ok(body) = decoded {
        assert_eq!(encode_operation_body(&body, limits).unwrap(), input);
    }
}

#[test]
fn schema_validation_and_materialization_agree_on_all_frozen_bodies_and_mutations() {
    for (body, fixture) in bodies().iter().zip(FIXTURES) {
        let bytes = unhex(fixture);
        assert!(validate_operation_body(body.kind(), &bytes, OperationLimits::default()).is_ok());
        for length in 0..=bytes.len() {
            compare(body.kind(), &bytes[..length], OperationLimits::default());
        }
        let mut trailing = bytes.clone();
        trailing.extend_from_slice(&[0, 0xff]);
        compare(body.kind(), &trailing, OperationLimits::default());
        for index in 0..bytes.len() {
            for value in [0, 1, 0x7f, 0xff] {
                let mut mutated = bytes.clone();
                mutated[index] = value;
                compare(body.kind(), &mutated, OperationLimits::default());
            }
        }
        for limit in [0, 1, bytes.len().saturating_sub(1), bytes.len()] {
            let default = OperationLimits::default();
            for limits in [
                OperationLimits {
                    max_body_bytes: limit,
                    ..default
                },
                OperationLimits {
                    max_name_bytes: limit,
                    ..default
                },
                OperationLimits {
                    max_append_batches: limit,
                    ..default
                },
                OperationLimits {
                    max_records: limit,
                    ..default
                },
                OperationLimits {
                    max_parts: limit,
                    ..default
                },
                OperationLimits {
                    max_payload_bytes: limit,
                    ..default
                },
            ] {
                compare(body.kind(), &bytes, limits);
            }
        }
    }
}

fn reject(input: &[u8], limits: OperationLimits, error: OperationCodecError) {
    assert_eq!(
        validate_operation_body(OperationKind::Append, input, limits),
        Err(error.clone())
    );
    assert_eq!(
        decode_operation_body(OperationKind::Append, input, limits),
        Err(error)
    );
}

#[test]
fn append_empty_counts_and_sequence_or_offset_overflow_stay_invalid() {
    for (offset, value, error) in [
        (0, 0_u32, OperationCodecError::EmptyAppend),
        (76, 0, OperationCodecError::EmptyAppend),
        (96, 0, OperationCodecError::EmptyRecordParts),
    ] {
        let mut bytes = unhex(FIXTURES[2]);
        bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
        reject(&bytes, OperationLimits::default(), error);
    }
    for offset in [52, 60] {
        let mut bytes = unhex(FIXTURES[2]);
        bytes[offset..offset + 8].copy_from_slice(&u64::MAX.to_be_bytes());
        reject(
            &bytes,
            OperationLimits::default(),
            OperationCodecError::AppendPositionOverflow,
        );
    }
}

#[test]
fn append_limits_accumulate_across_batches_records_and_parts() {
    let bytes = unhex(FIXTURES[2]);
    let mut two = 2_u32.to_be_bytes().to_vec();
    two.extend_from_slice(&bytes[4..]);
    two.extend_from_slice(&bytes[4..]);
    let limits = OperationLimits::default();
    compare(OperationKind::Append, &two, limits);
    for (limits, kind, actual, limit) in [
        (
            OperationLimits {
                max_append_batches: 1,
                ..limits
            },
            "append batch count",
            2,
            1,
        ),
        (
            OperationLimits {
                max_records: 3,
                ..limits
            },
            "append record count",
            4,
            3,
        ),
        (
            OperationLimits {
                max_parts: 7,
                ..limits
            },
            "append part count",
            8,
            7,
        ),
        (
            OperationLimits {
                max_payload_bytes: 11,
                ..limits
            },
            "append payload bytes",
            12,
            11,
        ),
    ] {
        reject(
            &two,
            limits,
            OperationCodecError::LimitExceeded {
                kind,
                actual,
                limit,
            },
        );
    }
    let mut empty_payloads = bytes.clone();
    for offset in [100, 104, 108, 132] {
        empty_payloads[offset..offset + 4].fill(0);
    }
    empty_payloads.truncate(136);
    assert!(validate_operation_body(OperationKind::Append, &empty_payloads, limits).is_ok());
    compare(OperationKind::Append, &empty_payloads, limits);
}

#[test]
fn generated_multipart_bodies_preserve_ids_parts_and_payload_borrows() {
    let OperationBody::Append(mut append) = bodies().remove(2) else {
        unreachable!()
    };
    for record_count in [1, 2, 31] {
        for part_count in [1, 2, 3, 17] {
            let batch = &mut append.batches[0];
            batch.records = (0..record_count)
                .map(|record| AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([record; 16]),
                    parts: (0..part_count)
                        .map(|part| {
                            if part % 2 == 0 {
                                b"".as_slice()
                            } else {
                                b"structured data".as_slice()
                            }
                        })
                        .collect(),
                })
                .collect();
            let body = OperationBody::Append(append.clone());
            let bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
            let decoded =
                decode_operation_body(body.kind(), &bytes, OperationLimits::default()).unwrap();
            assert_eq!(
                encode_operation_body(&decoded, OperationLimits::default()).unwrap(),
                bytes
            );
            let OperationBody::Append(decoded) = decoded else {
                unreachable!()
            };
            for (actual, expected) in decoded.batches[0]
                .records
                .iter()
                .zip(&append.batches[0].records)
            {
                assert_eq!(actual.message_id, expected.message_id);
                assert_eq!(actual.parts.len(), expected.parts.len());
                for (actual, expected) in actual.parts.iter().zip(expected.parts.iter()) {
                    assert_eq!(*actual, *expected);
                }
            }
            for part in decoded
                .batches
                .iter()
                .flat_map(|batch| &batch.records)
                .flat_map(|record| record.parts.iter())
            {
                if !part.is_empty() {
                    assert!(bytes.as_ptr_range().contains(&part.as_ptr()));
                }
            }
            compare(body.kind(), &bytes, OperationLimits::default());
        }
    }
}

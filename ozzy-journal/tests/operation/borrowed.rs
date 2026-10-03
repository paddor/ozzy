use bytes::Bytes;
use ozzy_journal::operation::{
    AppendHeader, AppendRecordList, append_prepared_record_batch, canonical_body_digest,
    decode_append_batches,
};
use ozzy_proto::data::OwnedRecord;

use super::*;

fn body(records: AppendRecordList<'_>) -> OperationBody<'_> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: PartitionIncarnation::from_bytes([0x10; 16]),
            owner_epoch: OwnerEpoch::new(9),
            producer_id: ProducerId::from_bytes([0x20; 16]),
            producer_epoch: ProducerEpoch::new(9),
            first_sequence: ProducerSequence::new(4),
            first_offset: Offset::new(40),
            append_timestamp_millis: 1_700_000_000_123,
            records,
        }],
    })
}

fn described(records: &[OwnedRecord]) -> AppendRecordList<'_> {
    records
        .iter()
        .map(|record| AppendRecord {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: record.message_id,
            parts: record.payload.iter().map(Bytes::as_ref).collect(),
        })
        .collect()
}

#[test]
fn prepared_lz4_is_exact_and_replaces_raw_canonical_payload() {
    let raw = vec![b'x'; 4096];
    let records = [OwnedRecord {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([0x41; 16]),
        payload: smallvec::smallvec![Bytes::copy_from_slice(&raw)],
    }];
    let encoded = lz4rip::block::compress(&raw);
    let mut body = Vec::new();
    append_prepared_record_batch(
        &mut body,
        AppendHeader {
            partition: PartitionIncarnation::from_bytes([0x10; 16]),
            owner_epoch: OwnerEpoch::new(9),
            producer_id: ProducerId::from_bytes([0x20; 16]),
            producer_epoch: ProducerEpoch::new(9),
            first_sequence: ProducerSequence::new(4),
            first_offset: Offset::new(40),
            append_timestamp_millis: 1_700_000_000_123,
        },
        records.iter().map(|record| {
            (
                record.message_id,
                record.encoding,
                record.payload.iter().map(Bytes::as_ref),
            )
        }),
        &encoded,
        OperationLimits::default(),
    )
    .unwrap();
    {
        let batches = decode_append_batches(&body, OperationLimits::default()).unwrap();
        let prepared = batches[0].prepared_payload.unwrap();
        assert_eq!(prepared.encoding, ozzy_proto::append::PayloadEncoding::Lz4);
        assert_eq!(prepared.decoded_bytes, raw.len());
        assert_eq!(prepared.encoded, encoded);
        assert!(batches[0].raw_records().is_none());
        let descriptor = batches[0].descriptors().next().unwrap();
        assert_eq!(descriptor.message_id, records[0].message_id);
        assert_eq!(descriptor.part_lengths.collect::<Vec<_>>(), [raw.len()]);
    }

    let digest = canonical_body_digest(&body);
    *body.last_mut().unwrap() ^= 1;
    assert_ne!(canonical_body_digest(&body), digest);
    body.pop();
    assert!(decode_append_batches(&body, OperationLimits::default()).is_err());
}

#[test]
fn borrowed_batches_match_frozen_bytes_and_logical_content() {
    let records = [
        OwnedRecord {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([0x41; 16]),
            payload: smallvec::smallvec![
                Bytes::from_static(b"ab"),
                Bytes::new(),
                Bytes::from_static(b"c"),
            ],
        },
        OwnedRecord {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: MessageId::from_bytes([0x42; 16]),
            payload: smallvec::smallvec![Bytes::from_static(b"xyz")],
        },
    ];
    let bytes = unhex(FIXTURES[2]);
    let mut list = AppendRecordList::borrowed(&records[..1]);
    list.extend_borrowed(&[]).unwrap();
    list.extend_borrowed(&records[1..]).unwrap();
    assert_eq!(list, described(&records));
    assert_eq!(list.get(1).unwrap().message_id, records[1].message_id);
    assert!(list.get(2).is_none());
    let borrowed = body(list);
    assert_eq!(
        encode_operation_body(&borrowed, OperationLimits::default()).unwrap(),
        bytes
    );
    assert_eq!(
        decode_operation_body(OperationKind::Append, &bytes, OperationLimits::default()).unwrap(),
        borrowed
    );
}

#[test]
fn borrowed_slices_preserve_order_parts_limits_and_failure_rollback() {
    for parts in [0, 1, 2, 3, 8] {
        for bytes in [0, 128, 70_000] {
            let records = (0..5_u128)
                .map(|index| OwnedRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes((index + 1).to_be_bytes()),
                    payload: (0..parts)
                        .map(|part| Bytes::from(vec![(index as u8) ^ part as u8; bytes]))
                        .collect(),
                })
                .collect::<Vec<_>>();
            let mut list = AppendRecordList::borrowed(&[]);
            for record in &records {
                list.extend_borrowed(std::slice::from_ref(record)).unwrap();
            }
            assert_eq!(list, described(&records));
            let mut iter = list.iter();
            for (index, expected) in records.iter().enumerate() {
                assert_eq!(iter.len(), records.len() - index);
                assert_eq!(iter.next().unwrap().message_id, expected.message_id);
                assert_eq!(list.get(index).unwrap().message_id, expected.message_id);
            }
            assert!(iter.next().is_none());
            assert!(iter.next().is_none());
            let borrowed = body(list);
            let described = body(described(&records));
            let defaults = OperationLimits::default();
            for limits in [
                defaults,
                OperationLimits {
                    max_records: 4,
                    ..defaults
                },
                OperationLimits {
                    max_parts: 4,
                    ..defaults
                },
                OperationLimits {
                    max_payload_bytes: 127,
                    ..defaults
                },
                OperationLimits {
                    max_body_bytes: 128,
                    ..defaults
                },
            ] {
                let mut actual = b"previous operation".to_vec();
                let mut expected = actual.clone();
                let a = append_operation_body(&mut actual, &borrowed, limits);
                let b = append_operation_body(&mut expected, &described, limits);
                assert_eq!(a, b);
                assert_eq!(actual, expected);
                assert_eq!(
                    canonical_body_digest(&actual),
                    canonical_body_digest(&expected)
                );
                if a.is_err() {
                    assert_eq!(actual, b"previous operation");
                }
            }
        }
    }
}

#[test]
fn borrowed_ranges_reject_counter_overflow_and_empty_records() {
    let record = OwnedRecord {
        encoding: ozzy_proto::data::Encoding::Raw,
        message_id: MessageId::from_bytes([1; 16]),
        payload: smallvec::smallvec![Bytes::new()],
    };
    let records = [record.clone(), record];
    for sequence in [false, true] {
        let OperationBody::Append(mut append) = body(AppendRecordList::borrowed(&records)) else {
            unreachable!()
        };
        if sequence {
            append.batches[0].first_sequence = ProducerSequence::new(u64::MAX);
        } else {
            append.batches[0].first_offset = Offset::new(u64::MAX);
        }
        assert!(
            encode_operation_body(&OperationBody::Append(append), OperationLimits::default())
                .is_err()
        );
    }
    let empty = body(AppendRecordList::borrowed(&[]));
    assert_eq!(
        encode_operation_body(&empty, OperationLimits::default()),
        Err(OperationCodecError::EmptyAppend)
    );
}

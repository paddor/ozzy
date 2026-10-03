use bytes::Bytes;
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecordList, OperationBody, OperationLimits, append_operation_body,
    encode_operation_body,
};
use ozzy_proto::data::{Authority, DataLimits, Encoding, IndexedRecords, Record, RecordBatch};
use ozzy_proto::reader::{self, RecordHeader, Source, Subscription};
use ozzy_proto::{
    Envelope, GroupId, LinkSessionId, MessageId, NodeId, Offset, Opcode, OwnerEpoch,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, RequestId, SubscriptionId,
};

fn input() -> RecordBatch {
    let large = vec![7; 1024];
    let compressed = [0, 0, 0, 1, 0, 0, 4, 0, 0xff];
    let shapes: &[&[&[u8]]] = &[&[b""], &[b"small"], &[b"", &large, b""], &[&compressed]];
    let records: Vec<_> = (0..12)
        .map(|i| Record {
            message_id: MessageId::from_bytes([i + 1; 16]),
            encoding: if i % 4 == 3 {
                Encoding::Lz4 {
                    decoded_bytes: 1024,
                }
            } else {
                Encoding::Raw
            },
            parts: shapes[i as usize % 4],
        })
        .collect();
    let envelope = Envelope {
        opcode: Opcode::Records,
        response: false,
        request_id: Some(RequestId::from_bytes([1; 16])),
        sender: NodeId::from_bytes([2; 16]),
        session: Some(LinkSessionId::from_bytes([3; 16])),
    };
    let limits = DataLimits::default();
    let mut metadata = Vec::with_capacity(limits.envelope.max_metadata_bytes);
    let mut payload = Vec::with_capacity(limits.envelope.max_payload_bytes);
    let header = reader::encode_records(
        envelope,
        RecordHeader {
            subscription: Subscription {
                id: SubscriptionId::from_bytes([4; 16]),
                generation: 1,
            },
            source: Source::Group {
                authority: Authority {
                    group_id: GroupId::from_bytes([1; 16]),
                    config_epoch: 1,
                    view: 0,
                },
                partition: PartitionIncarnation::from_bytes([2; 16]),
                owner_epoch: 1,
            },
            first_offset: 0,
        },
        &records,
        &mut metadata,
        &mut payload,
        limits,
    )
    .unwrap();
    let metadata = Bytes::from(metadata);
    let payload = Bytes::from(payload);
    let records = reader::decode_records(
        ozzy_proto::decode_packet(&[&header, &metadata, &payload], limits.envelope).unwrap(),
        limits,
    )
    .unwrap()
    .records
    .to_owned(&metadata, &payload);
    RecordBatch::Encoded(IndexedRecords::new(records))
}

fn body(records: AppendRecordList<'_>) -> OperationBody<'_> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: PartitionIncarnation::from_bytes([2; 16]),
            owner_epoch: OwnerEpoch::new(1),
            producer_id: ProducerId::from_bytes([3; 16]),
            producer_epoch: ProducerEpoch::new(1),
            first_sequence: ProducerSequence::new(7),
            first_offset: Offset::new(11),
            append_timestamp_millis: 19,
            records,
        }],
    })
}

#[test]
fn received_spans_match_general_encoding_across_partial_mixed_and_tiny_ranges() {
    let wire = input();
    let general = RecordBatch::General(wire.clone().into_owned());
    for start in 0..wire.len() {
        for end in start + 1..=wire.len() {
            let expected = body(AppendRecordList::batch(&general, start..end));
            let mut selected = AppendRecordList::batch(&wire, start..start + 1);
            selected.extend_batch(&wire, start + 1..end).unwrap();
            let encoded = body(selected);
            for limits in [
                OperationLimits::default(),
                OperationLimits {
                    max_body_bytes: 128,
                    ..OperationLimits::default()
                },
                OperationLimits {
                    max_parts: 2,
                    ..OperationLimits::default()
                },
                OperationLimits {
                    max_records: 1,
                    ..OperationLimits::default()
                },
                OperationLimits {
                    max_payload_bytes: 1023,
                    ..OperationLimits::default()
                },
            ] {
                let expected = encode_operation_body(&expected, limits);
                let actual = encode_operation_body(&encoded, limits);
                assert_eq!(
                    actual.is_ok(),
                    expected.is_ok(),
                    "{start}..{end}, {limits:?}"
                );
                if let Ok(bytes) = expected {
                    assert_eq!(actual.unwrap(), bytes);
                } else {
                    let mut output = vec![0xaa; 7];
                    assert!(append_operation_body(&mut output, &encoded, limits).is_err());
                    assert_eq!(output, [0xaa; 7]);
                }
            }
            // Mixed representations use the ordinary codec with identical bytes.
            let mut mixed = AppendRecordList::batch(&wire, start..start + 1);
            mixed.extend_batch(&general, start + 1..end).unwrap();
            assert_eq!(
                encode_operation_body(&body(mixed), OperationLimits::default()).unwrap(),
                encode_operation_body(&expected, OperationLimits::default()).unwrap(),
            );
        }
    }
}

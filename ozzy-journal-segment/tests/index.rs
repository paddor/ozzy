use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Barrier, CanonicalOperation, ChainPosition, OperationBody,
    OperationLimits, encode_operation_body, logical_operation_digest,
};
use ozzy_journal_segment::{
    DecodeLimits, Digest, IndexError, SEGMENT_HEADER_BYTES, SegmentHeader, decode_group,
    derive_index_entries, encode_group,
};
use ozzy_proto::{
    GroupId, MessageId, Offset, OperationId, OwnerEpoch, PartitionIncarnation, ProducerEpoch,
    ProducerId, ProducerSequence,
};

fn partition(byte: u8) -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([byte; 16])
}

fn append_body() -> OperationBody<'static> {
    OperationBody::Append(Append {
        batches: vec![
            AppendBatch {
                partition: partition(0x20),
                owner_epoch: OwnerEpoch::new(2),
                producer_id: ProducerId::from_bytes([0x30; 16]),
                producer_epoch: ProducerEpoch::new(3),
                first_sequence: ProducerSequence::new(4),
                first_offset: Offset::new(40),
                append_timestamp_millis: 123,
                records: vec![
                    AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes([0x40; 16]),
                        parts: vec![b"header".as_slice(), b"payload".as_slice()].into(),
                    },
                    AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: MessageId::from_bytes([0x41; 16]),
                        parts: vec![b"second".as_slice()].into(),
                    },
                ]
                .into(),
            },
            AppendBatch {
                partition: partition(0x21),
                owner_epoch: OwnerEpoch::new(2),
                producer_id: ProducerId::from_bytes([0x31; 16]),
                producer_epoch: ProducerEpoch::new(3),
                first_sequence: ProducerSequence::new(8),
                first_offset: Offset::new(80),
                append_timestamp_millis: 123,
                records: vec![AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([0x42; 16]),
                    parts: vec![b"third".as_slice()].into(),
                }]
                .into(),
            },
        ],
    })
}

#[test]
fn validated_operations_yield_exact_record_and_retry_entries() {
    let group_id = GroupId::from_bytes([0x10; 16]);
    let header = SegmentHeader::new(group_id, 7, None, Digest::ZERO, 16 * 1024).unwrap();
    let append = append_body();
    let append_bytes = encode_operation_body(&append, OperationLimits::default()).unwrap();
    let append_operation = CanonicalOperation {
        group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: append.kind(),
        body: &append_bytes,
    };
    let barrier = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([0x50; 16]),
    });
    let barrier_bytes = encode_operation_body(&barrier, OperationLimits::default()).unwrap();
    let barrier_operation = CanonicalOperation {
        op_number: 2,
        previous_digest: logical_operation_digest(&append_operation),
        kind: barrier.kind(),
        body: &barrier_bytes,
        ..append_operation
    };
    let encoded = encode_group(
        &header,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &[append_operation, barrier_operation],
    )
    .unwrap();
    let decoded = decode_group(
        &header,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        encoded.as_bytes(),
        DecodeLimits::default(),
    )
    .unwrap();
    assert_eq!(
        decoded.operations[0].entry_offset,
        SEGMENT_HEADER_BYTES as u64
    );
    assert_eq!(
        decoded.operations[1].entry_offset,
        decoded.operations[0].entry_offset + decoded.operations[0].entry_bytes
    );

    let entries =
        derive_index_entries(&header, &decoded.operations[0], OperationLimits::default()).unwrap();
    assert_eq!(entries.offsets.len(), 3);
    assert_eq!(entries.messages.len(), 3);
    assert!(entries.operation.is_none());
    assert_eq!(entries.offsets[0].offset, Offset::new(40));
    assert_eq!(entries.offsets[1].offset, Offset::new(41));
    assert_eq!(entries.offsets[2].offset, Offset::new(80));
    assert_eq!(entries.offsets[2].location.batch_index, 1);
    assert_eq!(entries.offsets[2].location.record_index, 0);
    assert_eq!(entries.offsets[0].location.operation.segment_id, 7);
    assert_eq!(entries.offsets[0].location.operation.op_number, 1);
    assert_eq!(entries.messages[1].offset, Offset::new(41));
    assert_eq!(
        entries.messages[0].operation_digest,
        logical_operation_digest(&append_operation)
    );
    let wrong_header = SegmentHeader::new(
        GroupId::from_bytes([0x11; 16]),
        8,
        None,
        Digest::ZERO,
        16 * 1024,
    )
    .unwrap();
    assert_eq!(
        derive_index_entries(
            &wrong_header,
            &decoded.operations[0],
            OperationLimits::default()
        ),
        Err(IndexError::SourceMismatch)
    );

    let entries =
        derive_index_entries(&header, &decoded.operations[1], OperationLimits::default()).unwrap();
    let operation = entries.operation.unwrap();
    assert_eq!(operation.operation_id, OperationId::from_bytes([0x50; 16]));
    assert_eq!(operation.location.op_number, 2);
    assert_eq!(
        operation.location.entry_offset,
        decoded.operations[1].entry_offset
    );
    assert!(entries.offsets.is_empty());
    assert!(entries.messages.is_empty());
}

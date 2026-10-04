use ozzy_journal_segment::{
    Digest, INDEX_HEADER_BYTES, IndexFileError, IndexLimits, IndexSource, MessageIndexEntry,
    OffsetIndexEntry, OperationIndexEntry, OperationLocation, RecordLocation, SegmentIndexImage,
    decode_segment_index, encode_segment_index,
};
use ozzy_proto::{GroupId, MessageId, Offset, OperationId, PartitionIncarnation};

fn partition(byte: u8) -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([byte; 16])
}

fn operation_location(number: u64, byte: u8, entry_offset: u64) -> OperationLocation {
    OperationLocation {
        segment_id: 7,
        entry_offset,
        entry_bytes: 192,
        op_number: number,
        operation_digest: Digest::from_bytes([byte; 32]),
    }
}

fn image() -> SegmentIndexImage {
    let first_location = operation_location(1, 0x70, 4096);
    let last_location = operation_location(3, 0x90, 4480);
    let first_partition = partition(0x20);
    let second_partition = partition(0x21);
    SegmentIndexImage::new(
        IndexSource {
            group_id: GroupId::from_bytes([0x10; 16]),
            segment_id: 7,
            valid_bytes: 8192,
            segment_digest: Digest::from_bytes([0x60; 32]),
            first_op_number: 1,
            last_op_number: 3,
            last_operation_digest: last_location.operation_digest,
        },
        64 * 1024,
        vec![
            OffsetIndexEntry {
                append_timestamp_millis: 77,
                partition: second_partition,
                offset: Offset::new(8),
                location: RecordLocation {
                    operation: first_location,
                    batch_index: 1,
                    record_index: 0,
                },
            },
            OffsetIndexEntry {
                append_timestamp_millis: 0,
                partition: first_partition,
                offset: Offset::new(4),
                location: RecordLocation {
                    operation: first_location,
                    batch_index: 0,
                    record_index: 2,
                },
            },
        ],
        vec![
            MessageIndexEntry {
                partition: second_partition,
                message_id: MessageId::from_bytes([0x41; 16]),
                offset: Offset::new(8),
                operation_digest: Digest::from_bytes([0x51; 32]),
            },
            MessageIndexEntry {
                partition: first_partition,
                message_id: MessageId::from_bytes([0x40; 16]),
                offset: Offset::new(4),
                operation_digest: Digest::from_bytes([0x50; 32]),
            },
        ],
        vec![OperationIndexEntry {
            operation_id: OperationId::from_bytes([0x30; 16]),
            location: last_location,
        }],
    )
    .unwrap()
}

#[test]
fn immutable_index_round_trips_and_binary_searches_without_entry_allocation() {
    let image = image();
    let encoded = encode_segment_index(&image, IndexLimits::default()).unwrap();
    assert_eq!(&encoded[..8], b"OZYIDX01");
    assert_eq!(u16::from_be_bytes(encoded[8..10].try_into().unwrap()), 2);
    assert_eq!(
        u16::from_be_bytes(encoded[10..12].try_into().unwrap()),
        INDEX_HEADER_BYTES as u16
    );

    let view = decode_segment_index(&encoded, IndexLimits::default()).unwrap();
    assert_eq!(view.source(), image.source());
    assert_eq!(view.build_memory_limit(), 64 * 1024);
    assert_eq!(view.offset_count(), 2);
    assert_eq!(view.message_count(), 2);
    assert_eq!(view.operation_count(), 1);
    assert_eq!(
        view.find_offset(partition(0x20), Offset::new(4)),
        Some(image.offsets()[0])
    );
    assert_eq!(
        view.find_message(partition(0x21), MessageId::from_bytes([0x41; 16])),
        Some(image.messages()[1])
    );
    assert_eq!(
        view.find_operation(OperationId::from_bytes([0x30; 16])),
        Some(image.operations()[0])
    );
    assert!(view.find_offset(partition(0x20), Offset::new(5)).is_none());
}

#[test]
fn every_truncated_prefix_and_corruption_is_rejected() {
    let encoded = encode_segment_index(&image(), IndexLimits::default()).unwrap();
    for length in 0..encoded.len() {
        assert!(
            decode_segment_index(&encoded[..length], IndexLimits::default()).is_err(),
            "accepted {length} bytes"
        );
    }

    let mut changed = encoded.clone();
    changed[INDEX_HEADER_BYTES + 40] ^= 1;
    assert_eq!(
        decode_segment_index(&changed, IndexLimits::default()).unwrap_err(),
        IndexFileError::DigestMismatch
    );
    let mut reserved = encoded;
    reserved[300] = 1;
    assert_eq!(
        decode_segment_index(&reserved, IndexLimits::default()).unwrap_err(),
        IndexFileError::NonZeroReserved
    );
}

#[test]
fn construction_rejects_duplicate_keys_and_invalid_locations() {
    let image = image();
    let duplicate = image.offsets()[0];
    assert_eq!(
        SegmentIndexImage::new(
            image.source(),
            image.build_memory_limit(),
            vec![duplicate, duplicate],
            image.messages().to_vec(),
            image.operations().to_vec(),
        )
        .unwrap_err(),
        IndexFileError::UnsortedOrDuplicate("offset")
    );

    let mut invalid = image.offsets()[0];
    invalid.location.operation.entry_offset = 8184;
    assert_eq!(
        SegmentIndexImage::new(
            image.source(),
            image.build_memory_limit(),
            vec![invalid, image.offsets()[1]],
            image.messages().to_vec(),
            image.operations().to_vec(),
        )
        .unwrap_err(),
        IndexFileError::InvalidLocation
    );
}

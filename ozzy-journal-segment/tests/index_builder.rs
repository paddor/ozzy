use std::fs;

use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, Barrier, CanonicalOperation, ChainPosition, OperationBody,
    OperationLimits, encode_operation_body, logical_operation_digest,
};
use ozzy_journal_segment::{
    DecodeLimits, Digest, IndexBuildError, IndexBuildLimits, IndexFileError, IndexSource,
    IndexedReadError, SEGMENT_HEADER_BYTES, SegmentHeader, build_segment_index, encode_group,
    encode_segment_header, read_indexed_message, scan_segment, segment_index_name,
};
use ozzy_proto::{
    GroupId, MessageId, Offset, OperationId, OwnerEpoch, PartitionIncarnation, ProducerEpoch,
    ProducerId, ProducerSequence,
};
use tempfile::TempDir;

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([0x20; 16])
}

fn append_body(message: u8, sequence: u64, offset: u64) -> Vec<u8> {
    encode_operation_body(
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: partition(),
                owner_epoch: OwnerEpoch::new(1),
                producer_id: ProducerId::from_bytes([0x30; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(sequence),
                first_offset: Offset::new(offset),
                append_timestamp_millis: 100,
                records: vec![AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([message; 16]),
                    parts: vec![b"payload".as_slice()].into(),
                }]
                .into(),
            }],
        }),
        OperationLimits::default(),
    )
    .unwrap()
}

fn segment_bytes(second_message: u8) -> Vec<u8> {
    let group_id = GroupId::from_bytes([0x10; 16]);
    let header = SegmentHeader::new(group_id, 7, None, Digest::ZERO, 32 * 1024).unwrap();
    let first_body = append_body(0x40, 0, 4);
    let first = CanonicalOperation {
        group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationBody::Append(Append {
            batches: Vec::new(),
        })
        .kind(),
        body: &first_body,
    };
    let second_body = append_body(second_message, 1, 5);
    let second = CanonicalOperation {
        op_number: 2,
        previous_digest: logical_operation_digest(&first),
        body: &second_body,
        ..first
    };
    let barrier_body = encode_operation_body(
        &OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes([0x50; 16]),
        }),
        OperationLimits::default(),
    )
    .unwrap();
    let barrier = CanonicalOperation {
        op_number: 3,
        previous_digest: logical_operation_digest(&second),
        kind: OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes([0x50; 16]),
        })
        .kind(),
        body: &barrier_body,
        ..first
    };
    let group = encode_group(
        &header,
        1,
        SEGMENT_HEADER_BYTES as u64,
        ChainPosition::GENESIS,
        &[first, second, barrier],
    )
    .unwrap();
    let mut bytes = encode_segment_header(&header).to_vec();
    bytes.extend_from_slice(group.as_bytes());
    bytes
}

fn source(scan: &ozzy_journal_segment::SegmentScan<'_>) -> IndexSource {
    let operations = scan
        .groups
        .iter()
        .flat_map(|group| &group.operations)
        .collect::<Vec<_>>();
    IndexSource {
        group_id: scan.header.group_id(),
        segment_id: scan.header.segment_id(),
        valid_bytes: scan.valid_bytes,
        segment_digest: scan.digest,
        first_op_number: operations.first().unwrap().op_number,
        last_op_number: operations.last().unwrap().op_number,
        last_operation_digest: operations.last().unwrap().digest,
    }
}

fn directories() -> (TempDir, std::path::PathBuf, std::path::PathBuf) {
    let temporary = TempDir::new().unwrap();
    let indexes = temporary.path().join("indexes");
    let staging = temporary.path().join("staging");
    fs::create_dir(&indexes).unwrap();
    fs::create_dir(&staging).unwrap();
    (temporary, indexes, staging)
}

#[test]
fn bounded_multi_run_build_publishes_and_reuses_exact_index() {
    let bytes = segment_bytes(0x41);
    let scan = scan_segment(&bytes, 1, ChainPosition::GENESIS, DecodeLimits::default()).unwrap();
    let source = source(&scan);
    let (temporary, indexes, staging) = directories();
    let segment_path = temporary.path().join("segment.log");
    fs::write(&segment_path, &bytes).unwrap();
    let limits = IndexBuildLimits {
        max_entry_buffer_bytes: 200,
        max_merge_fan_in: 2,
        ..IndexBuildLimits::default()
    };
    let index = build_segment_index(
        &scan,
        source,
        &indexes,
        &staging,
        OperationLimits::default(),
        limits,
    )
    .unwrap();
    assert_eq!(index.offset_count(), 2);
    assert_eq!(index.message_count(), 2);
    assert_eq!(index.operation_count(), 1);
    assert_eq!(
        index
            .find_message(partition(), MessageId::from_bytes([0x41; 16]))
            .unwrap()
            .offset,
        Offset::new(5)
    );
    assert_eq!(
        index
            .find_operation(OperationId::from_bytes([0x50; 16]))
            .unwrap()
            .location
            .op_number,
        3
    );
    let message = index
        .find_message(partition(), MessageId::from_bytes([0x41; 16]))
        .unwrap();
    let offset = index.find_offset(partition(), message.offset).unwrap();
    let record = read_indexed_message(
        &segment_path,
        source,
        offset,
        message,
        DecodeLimits::default(),
        OperationLimits::default(),
    )
    .unwrap();
    assert_eq!(record.offset, Offset::new(5));
    assert_eq!(record.producer_sequence, ProducerSequence::new(1));
    assert_eq!(record.parts.len(), 1);
    assert_eq!(record.parts[0], b"payload"[..]);
    assert!(indexes.join(segment_index_name(source)).is_file());
    assert_eq!(fs::read_dir(&staging).unwrap().count(), 0);

    let reused = build_segment_index(
        &scan,
        source,
        &indexes,
        &staging,
        OperationLimits::default(),
        limits,
    )
    .unwrap();
    assert_eq!(reused.as_bytes(), index.as_bytes());

    let mut corrupt_segment = bytes;
    corrupt_segment[offset.location.operation.entry_offset as usize + 192] ^= 1;
    fs::write(&segment_path, corrupt_segment).unwrap();
    assert!(matches!(
        read_indexed_message(
            &segment_path,
            source,
            offset,
            message,
            DecodeLimits::default(),
            OperationLimits::default(),
        ),
        Err(IndexedReadError::Codec(_))
    ));
}

#[test]
fn repeated_message_ids_are_ordered_by_offset_across_staging_runs() {
    let bytes = segment_bytes(0x40);
    let scan = scan_segment(&bytes, 1, ChainPosition::GENESIS, DecodeLimits::default()).unwrap();
    let source = source(&scan);
    let (_temporary, indexes, staging) = directories();
    let index = build_segment_index(
        &scan,
        source,
        &indexes,
        &staging,
        OperationLimits::default(),
        IndexBuildLimits {
            max_entry_buffer_bytes: 200,
            max_merge_fan_in: 2,
            ..IndexBuildLimits::default()
        },
    )
    .unwrap();
    assert_eq!(index.message_count(), 2);
    assert_eq!(
        index
            .find_message(partition(), MessageId::from_bytes([0x40; 16]))
            .unwrap()
            .offset,
        Offset::new(4)
    );
    assert_eq!(fs::read_dir(&indexes).unwrap().count(), 1);
    assert_eq!(fs::read_dir(&staging).unwrap().count(), 0);
}

#[test]
fn corrupt_published_index_is_never_silently_replaced() {
    let bytes = segment_bytes(0x41);
    let scan = scan_segment(&bytes, 1, ChainPosition::GENESIS, DecodeLimits::default()).unwrap();
    let source = source(&scan);
    let (_temporary, indexes, staging) = directories();
    let limits = IndexBuildLimits::default();
    build_segment_index(
        &scan,
        source,
        &indexes,
        &staging,
        OperationLimits::default(),
        limits,
    )
    .unwrap();
    let path = indexes.join(segment_index_name(source));
    let mut corrupt = fs::read(&path).unwrap();
    corrupt[INDEX_DIGEST_BYTE] ^= 1;
    fs::write(path, corrupt).unwrap();

    assert!(matches!(
        build_segment_index(
            &scan,
            source,
            &indexes,
            &staging,
            OperationLimits::default(),
            limits,
        ),
        Err(IndexBuildError::File(IndexFileError::DigestMismatch))
    ));
}

#[test]
fn staging_run_count_is_bounded() {
    let bytes = segment_bytes(0x41);
    let scan = scan_segment(&bytes, 1, ChainPosition::GENESIS, DecodeLimits::default()).unwrap();
    let source = source(&scan);
    let (_temporary, indexes, staging) = directories();
    assert!(matches!(
        build_segment_index(
            &scan,
            source,
            indexes,
            &staging,
            OperationLimits::default(),
            IndexBuildLimits {
                max_entry_buffer_bytes: 200,
                max_run_files: 1,
                ..IndexBuildLimits::default()
            },
        ),
        Err(IndexBuildError::RunLimitExceeded(1))
    ));
    assert_eq!(fs::read_dir(staging).unwrap().count(), 0);
}

const INDEX_DIGEST_BYTE: usize = 256;

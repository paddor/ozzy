use super::*;
use crate::index_file::{
    INDEX_HEADER_BYTES, MESSAGE_INDEX_ENTRY_BYTES, OFFSET_INDEX_ENTRY_BYTES,
    OPERATION_INDEX_ENTRY_BYTES, decode_segment_index, encode_segment_index, index_digest,
};
use crate::{
    IndexSource, MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry, OperationLocation,
    RecordLocation, SegmentIndexImage, test_io::Scheduling,
};
use ozzy_proto::{GroupId, MessageId, Offset, OperationId, PartitionIncarnation};
use std::{future::Future, pin::pin, sync::Arc, task::Poll};

fn fixture(count: usize) -> (Vec<u8>, SegmentIndexImage) {
    let partition = PartitionIncarnation::from_bytes([7; 16]);
    let source = IndexSource {
        group_id: GroupId::from_bytes([1; 16]),
        segment_id: 1,
        valid_bytes: 4 * 1024 * 1024,
        segment_digest: Digest::from_bytes([2; 32]),
        first_op_number: 1,
        last_op_number: count as u64,
        last_operation_digest: Digest::from_bytes([3; 32]),
    };
    let mut offsets = Vec::new();
    let mut messages = Vec::new();
    let mut operations = Vec::new();
    for index in 0..count {
        let location = OperationLocation {
            segment_id: 1,
            entry_offset: 4096 + 192 * index as u64,
            entry_bytes: 192,
            op_number: index as u64 + 1,
            operation_digest: source.last_operation_digest,
        };
        let id = (index as u128 + 1).to_be_bytes();
        offsets.push(OffsetIndexEntry {
            partition,
            offset: Offset::new(index as u64),
            location: RecordLocation {
                operation: location,
                batch_index: 0,
                record_index: 0,
            },
        });
        messages.push(MessageIndexEntry {
            partition,
            message_id: MessageId::from_bytes(id),
            offset: Offset::new(index as u64),
            operation_digest: location.operation_digest,
        });
        operations.push(OperationIndexEntry {
            operation_id: OperationId::from_bytes(id),
            location,
        });
    }
    let image = SegmentIndexImage::new(source, 64 * 1024, offsets, messages, operations).unwrap();
    (
        encode_segment_index(&image, IndexLimits::default()).unwrap(),
        image,
    )
}

fn run<T>(future: impl Future<Output = T>) -> (T, usize) {
    let mut future = pin!(future);
    let scheduling = Arc::new(Scheduling::default());
    for polls in 1..10_000 {
        if let Poll::Ready(result) = scheduling.poll(future.as_mut()) {
            return (result, polls);
        }
        assert!(
            scheduling.woken(),
            "index validation must schedule its next turn"
        );
    }
    panic!("index validation did not finish")
}

#[test]
fn cooperative_index_open_preserves_every_lookup_and_yields_within_each_table() {
    for count in [1, 130, 3000] {
        let (bytes, image) = fixture(count);
        let (result, polls) = run(decode_segment_index_async(&bytes, IndexLimits::default()));
        let view = result.unwrap();
        assert_eq!(view.source(), image.source());
        assert_eq!(view.build_memory_limit(), image.build_memory_limit());
        assert_eq!(view.offset_count(), count);
        assert_eq!(view.message_count(), count);
        assert_eq!(view.operation_count(), count);
        assert!(polls > (6 * count - 3) / 64);
        for ((offset, message), operation) in image
            .offsets()
            .iter()
            .zip(image.messages())
            .zip(image.operations())
        {
            assert_eq!(
                view.find_offset(offset.partition, offset.offset),
                Some(*offset)
            );
            assert_eq!(
                view.find_message(message.partition, message.message_id),
                Some(*message)
            );
            assert_eq!(
                view.find_operation(operation.operation_id),
                Some(*operation)
            );
        }
        let (owned, _) = run(crate::SegmentIndex::from_bytes_async(
            bytes.clone(),
            image.source(),
            IndexLimits::default(),
        ));
        assert_eq!(owned.unwrap().as_bytes(), bytes);
        let wrong = IndexSource {
            segment_id: 9,
            ..image.source()
        };
        let (result, _) = run(crate::SegmentIndex::from_bytes_async(
            bytes,
            wrong,
            IndexLimits::default(),
        ));
        assert!(matches!(
            result,
            Err(crate::IndexBuildError::SourceMismatch)
        ));
    }
}

fn repair_digest(bytes: &mut [u8]) {
    let digest = index_digest(bytes);
    bytes[INDEX_DIGEST_START..INDEX_DIGEST_END].copy_from_slice(digest.as_bytes());
}

fn rejects(bytes: &[u8], expected: &IndexFileError) {
    let (actual, _) = run(decode_segment_index_async(bytes, IndexLimits::default()));
    assert_eq!(&actual.unwrap_err(), expected);
    assert_eq!(
        &decode_segment_index(bytes, IndexLimits::default()).unwrap_err(),
        expected
    );
}

#[test]
fn cooperative_index_open_refuses_late_table_corruption_even_with_a_valid_checksum() {
    let count = 130;
    let (original, _) = fixture(count);
    let message_start = INDEX_HEADER_BYTES + count * OFFSET_INDEX_ENTRY_BYTES;
    let operation_start = message_start + count * MESSAGE_INDEX_ENTRY_BYTES;
    for (start, width, key, kind) in [
        (INDEX_HEADER_BYTES, OFFSET_INDEX_ENTRY_BYTES, 24, "offset"),
        (message_start, MESSAGE_INDEX_ENTRY_BYTES, 40, "message"),
        (
            operation_start,
            OPERATION_INDEX_ENTRY_BYTES,
            16,
            "operation",
        ),
    ] {
        let mut bytes = original.clone();
        let last = start + (count - 1) * width;
        bytes.copy_within(last - width..last - width + key, last);
        repair_digest(&mut bytes);
        rejects(&bytes, &IndexFileError::UnsortedOrDuplicate(kind));
        let mut bytes = original.clone();
        bytes[last + width - 1] = 1;
        repair_digest(&mut bytes);
        rejects(&bytes, &IndexFileError::NonZeroReserved);
    }
    let mut bytes = original.clone();
    let last_message = message_start + (count - 1) * MESSAGE_INDEX_ENTRY_BYTES;
    bytes[last_message + 32..last_message + 40].copy_from_slice(&9999u64.to_be_bytes());
    repair_digest(&mut bytes);
    rejects(&bytes, &IndexFileError::MissingOffsetEntry);
    let mut bytes = original.clone();
    let last_operation = operation_start + (count - 1) * OPERATION_INDEX_ENTRY_BYTES;
    bytes[last_operation + 16..last_operation + 24].copy_from_slice(&0u64.to_be_bytes());
    repair_digest(&mut bytes);
    rejects(&bytes, &IndexFileError::InvalidLocation);
    let mut bytes = original;
    bytes[last_operation + 40] ^= 1;
    rejects(&bytes, &IndexFileError::DigestMismatch);
}

#[test]
fn cooperative_index_open_cancellation_and_limits_never_expose_partial_tables() {
    let (bytes, _) = fixture(3000);
    let before = bytes.clone();
    // First turn hashes a partial file. Later turns validate complete checksum
    // but only a prefix of the tables. Neither permits an index to escape.
    for cut in [1, 2, 3, 64] {
        let mut future = Box::pin(decode_segment_index_async(&bytes, IndexLimits::default()));
        let scheduling = Arc::new(Scheduling::default());
        for _ in 0..cut {
            assert!(scheduling.poll(future.as_mut()).is_pending());
            assert!(scheduling.woken());
        }
        drop(future);
        assert_eq!(bytes, before);
    }
    for limits in [
        IndexLimits {
            max_file_bytes: bytes.len() - 1,
            ..IndexLimits::default()
        },
        IndexLimits {
            max_offset_entries: 2999,
            ..IndexLimits::default()
        },
        IndexLimits {
            max_message_entries: 2999,
            ..IndexLimits::default()
        },
        IndexLimits {
            max_operation_entries: 2999,
            ..IndexLimits::default()
        },
    ] {
        let (result, _) = run(decode_segment_index_async(&bytes, limits));
        assert!(matches!(result, Err(IndexFileError::LimitExceeded { .. })));
        assert_eq!(
            result.unwrap_err(),
            decode_segment_index(&bytes, limits).unwrap_err()
        );
    }
    let (result, polls) = run(decode_segment_index_async(&bytes, IndexLimits::default()));
    assert!(polls > 64);
    assert_eq!(result.unwrap().operation_count(), 3000);
    for length in [
        0,
        INDEX_HEADER_BYTES - 1,
        INDEX_HEADER_BYTES,
        bytes.len() - 1,
    ] {
        let (result, polls) = run(decode_segment_index_async(
            &bytes[..length],
            IndexLimits::default(),
        ));
        assert_eq!(polls, 1);
        assert!(result.is_err());
    }
}

#[test]
fn native_index_open_drives_cooperative_validation_after_owned_file_reads() {
    use crate::{async_files::Access, test_io::drive};
    use ozzy_io::{
        Local, OpenMode, Quota,
        simulation::{Controller, Image, ImageLimits},
    };
    let (bytes, image) = fixture(3000);
    let quota = Quota {
        operations: 4,
        bytes: 2 * 1024 * 1024,
    };
    let (mut controller, mut clients) = Controller::new(
        ozzy_io::simulation::Config {
            limits: ozzy_io::Limits {
                shards: 1,
                data: quota,
                progress: quota,
            },
            handles: 8,
            image: ImageLimits {
                nodes: 8,
                directory_entries: 8,
                file_bytes: 1024 * 1024,
                total_bytes: 4 * 1024 * 1024,
            },
            trace_events: 10_000,
        },
        Image::default(),
    )
    .unwrap();
    let access = Access {
        io: Local::new(clients.remove(0)),
        protection: None,
        readers: std::rc::Rc::default(),
    };
    let handle = drive(
        &mut controller,
        access.open("/index".into(), OpenMode::CreateNew, false, false),
    )
    .unwrap();
    drive(&mut controller, access.write_all(&handle, 0, &bytes)).unwrap();
    let opened = drive(
        &mut controller,
        crate::index_builder::asynchronous::open(
            &access,
            "/index".into(),
            image.source(),
            IndexLimits::default(),
            4096,
        ),
    )
    .unwrap();
    assert_eq!(opened.as_bytes(), bytes);
    assert_eq!(opened.operation_count(), 3000);
    assert_eq!(
        opened.find_offset(image.offsets()[2999].partition, Offset::new(2999)),
        Some(image.offsets()[2999])
    );
}

#[test]
fn cooperative_index_construction_sorts_all_tables_and_builds_identical_read_runs() {
    let (bytes, image) = fixture(130);
    let (actual, polls) = run(SegmentIndexImage::new_async(
        image.source(),
        image.build_memory_limit(),
        image.offsets().iter().rev().copied().collect(),
        image.messages().iter().rev().copied().collect(),
        image.operations().iter().rev().copied().collect(),
    ));
    assert!(polls > 10);
    assert_eq!(actual.unwrap(), image);
    let mut messages = image.messages().to_vec();
    messages[129].offset = Offset::new(9999);
    let (invalid, polls) = run(SegmentIndexImage::new_async(
        image.source(),
        0,
        image.offsets().to_vec(),
        messages,
        image.operations().to_vec(),
    ));
    assert!(polls > 1);
    assert_eq!(invalid.unwrap_err(), IndexFileError::MissingOffsetEntry);
    let persisted =
        crate::SegmentIndex::from_bytes(bytes, image.source(), IndexLimits::default()).unwrap();
    let (read, polls) =
        run(crate::active_read_index::ActiveReadIndex::from_persisted_async(&persisted));
    let read = read.unwrap();
    assert!(polls > 1);
    for offset in image.offsets() {
        assert_eq!(
            read.entry(offset.partition, offset.offset).unwrap(),
            *offset
        );
    }
}

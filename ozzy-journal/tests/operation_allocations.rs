//! Allocation regressions for the canonical journal codec.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, OperationKind, OperationLimits,
    decode_operation_body, encode_operation_body, validate_operation_body,
};
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};

thread_local! {
    static ALLOCATIONS: Cell<Option<usize>> = const { Cell::new(None) };
}

struct CountedAllocator;

#[global_allocator]
static ALLOCATOR: CountedAllocator = CountedAllocator;

fn count() {
    // Ignore other test threads and allocations during thread-local teardown.
    let _ = ALLOCATIONS.try_with(|counter| {
        if let Some(value) = counter.get() {
            counter.set(Some(value.saturating_add(1)));
        }
    });
}

// SAFETY: Every operation forwards its unchanged pointer/layout to System.
// Accounting uses allocation-free thread-local cells and never touches memory.
unsafe impl GlobalAlloc for CountedAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: The caller supplies the allocator's required valid layout.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: The caller supplies the allocator's required valid layout.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: All allocations came from System with the same layout.
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        count();
        // SAFETY: The caller supplies System's original allocation and valid size.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

fn measure<T>(operation: impl FnOnce() -> T) -> (T, usize) {
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            ALLOCATIONS.with(|counter| counter.set(None));
        }
    }
    ALLOCATIONS.with(|counter| assert!(counter.replace(Some(0)).is_none()));
    let reset = Reset;
    let output = operation();
    let allocations = ALLOCATIONS.with(|counter| counter.get().unwrap());
    drop(reset);
    (output, allocations)
}

fn fixture(part_count: usize) -> Vec<u8> {
    let body = OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: PartitionIncarnation::from_bytes([1; 16]),
            owner_epoch: OwnerEpoch::new(1),
            producer_id: ProducerId::from_bytes([2; 16]),
            producer_epoch: ProducerEpoch::new(1),
            first_sequence: ProducerSequence::new(0),
            first_offset: Offset::ZERO,
            append_timestamp_millis: 42,
            records: (0_u128..1_000)
                .map(|index| AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes(index.to_be_bytes()),
                    parts: std::iter::repeat_n(b"payload".as_slice(), part_count).collect(),
                })
                .collect(),
        }],
    });
    encode_operation_body(&body, OperationLimits::default()).unwrap()
}

#[test]
fn indexed_summary_keeps_borrowed_boundaries_without_record_allocations() {
    use ozzy_journal::operation::{
        decode_append_summary_and_batches, decode_append_summary_and_view,
    };

    for parts in [1, 3, 8] {
        let body = fixture(parts);
        let limits = OperationLimits::default();
        let (expected, view) = decode_append_summary_and_view(&body, limits).unwrap();
        let ((summary, batches), allocations) =
            measure(|| decode_append_summary_and_batches(black_box(&body), limits).unwrap());
        assert_eq!(allocations, 0);
        assert_eq!(summary, expected);
        for (batch, previous) in batches.iter().zip(view.batches()) {
            assert_eq!(batch.summary, previous.summary);
            assert_eq!(
                batch.append_timestamp_millis,
                previous.append_timestamp_millis
            );
            assert_eq!(batch.records().len(), previous.records().len());
            let (table, payload) = batch.records().remaining_bytes();
            let (old_table, old_payload) = previous.records().remaining_bytes();
            assert_eq!(
                (table.as_ptr(), table.len()),
                (old_table.as_ptr(), old_table.len())
            );
            assert_eq!(
                (payload.as_ptr(), payload.len()),
                (old_payload.as_ptr(), old_payload.len())
            );
        }
        for end in [0, 4, body.len() / 2, body.len() - 1] {
            assert_eq!(
                decode_append_summary_and_batches(&body[..end], limits).unwrap_err(),
                decode_append_summary_and_view(&body[..end], limits).unwrap_err(),
            );
        }
    }
}

#[test]
fn iterator_encoding_preserves_canonical_bytes_without_allocating() {
    use ozzy_journal::operation::{AppendHeader, DescriptorLayout, append_record_batches};

    for parts in [1, 3, 8] {
        let expected = fixture(parts);
        let OperationBody::Append(append) =
            decode_operation_body(OperationKind::Append, &expected, OperationLimits::default())
                .unwrap()
        else {
            panic!("append fixture");
        };
        let mut output = Vec::with_capacity(expected.len() + 3);
        output.extend_from_slice(b"old");
        let (range, allocations) = measure(|| {
            append_record_batches(
                &mut output,
                append.batches.iter().map(|batch| {
                    (
                        AppendHeader::from(batch),
                        batch.records.described().unwrap().iter().map(|record| {
                            (record.message_id, record.encoding, record.parts.as_slice())
                        }),
                    )
                }),
                DescriptorLayout::Compact,
                OperationLimits::default(),
            )
            .unwrap()
        });
        assert_eq!(allocations, 0);
        assert_eq!(&output[..3], b"old");
        assert_eq!(&output[range], expected);
    }
}

#[test]
fn iterator_encoding_rolls_back_late_failures_like_typed_encoding() {
    use ozzy_journal::operation::{AppendHeader, DescriptorLayout, append_record_batches};

    let bytes = fixture(1);
    let OperationBody::Append(mut append) =
        decode_operation_body(OperationKind::Append, &bytes, OperationLimits::default()).unwrap()
    else {
        panic!("append fixture");
    };
    append.batches.push(append.batches[0].clone());
    let defaults = OperationLimits::default();
    let limits = [
        OperationLimits {
            max_records: 1001,
            ..defaults
        },
        OperationLimits {
            max_parts: 1001,
            ..defaults
        },
        OperationLimits {
            max_payload_bytes: 7001,
            ..defaults
        },
        OperationLimits {
            max_body_bytes: bytes.len() + 10,
            ..defaults
        },
    ];
    for limits in limits {
        let expected =
            encode_operation_body(&OperationBody::Append(append.clone()), limits).unwrap_err();
        let mut output = b"prefix".to_vec();
        let error = append_record_batches(
            &mut output,
            append.batches.iter().map(|batch| {
                (
                    AppendHeader::from(batch),
                    batch.records.described().unwrap().iter().map(|record| {
                        (record.message_id, record.encoding, record.parts.as_slice())
                    }),
                )
            }),
            DescriptorLayout::Compact,
            limits,
        )
        .unwrap_err();
        assert_eq!(error, expected);
        assert_eq!(output, b"prefix");
    }
}

#[test]
fn native_canonical_descriptors_and_reused_encoding_allocate_nothing() {
    use ozzy_journal::operation::{AppendRecordList, append_operation_body};
    use ozzy_proto::data::OwnedRecord;

    for parts in [1, 2, 3, 8] {
        let records = (0..1024_u128)
            .map(|index| OwnedRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes(index.to_be_bytes()),
                payload: std::iter::repeat_n(bytes::Bytes::from_static(b"payload"), parts)
                    .collect(),
            })
            .collect::<Vec<_>>();
        let mut output = Vec::with_capacity(256 * 1024);
        let (list, allocations) = measure(|| {
            let mut list = AppendRecordList::borrowed(&[]);
            list.extend_borrowed(&records[..512]).unwrap();
            list.extend_borrowed(&records[512..]).unwrap();
            assert_eq!(list.iter().count(), 1024);
            for record in &list {
                for part in record.parts.iter() {
                    black_box(part);
                }
            }
            list
        });
        assert_eq!(allocations, 0, "{parts} parts per record");
        let body = OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition: PartitionIncarnation::from_bytes([1; 16]),
                owner_epoch: OwnerEpoch::INITIAL,
                producer_id: ProducerId::from_bytes([2; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(0),
                first_offset: Offset::ZERO,
                append_timestamp_millis: 42,
                records: list,
            }],
        });
        let expected = encode_operation_body(&body, OperationLimits::default()).unwrap();
        let ((), allocations) = measure(|| {
            append_operation_body(&mut output, &body, OperationLimits::default()).unwrap();
        });
        assert_eq!(allocations, 0, "{parts} parts per record");
        assert_eq!(output, expected);
        assert_eq!(
            decode_operation_body(OperationKind::Append, &output, OperationLimits::default())
                .unwrap(),
            body
        );
    }
}

#[test]
fn materialized_decode_has_no_per_record_scratch_allocations() {
    for parts in [1, 2] {
        let bytes = fixture(parts);
        let (body, allocations) = measure(|| {
            decode_operation_body(
                OperationKind::Append,
                black_box(&bytes),
                OperationLimits::default(),
            )
            .unwrap()
        });
        assert_eq!(
            encode_operation_body(&body, OperationLimits::default()).unwrap(),
            bytes
        );
        // Only the output batch vector and its output record vector allocate.
        assert_eq!(allocations, 2, "{parts} parts per record");
    }
}

#[test]
fn schema_only_validation_never_allocates_even_for_multipart_or_truncation() {
    for parts in [1, 2, 3, 8] {
        let bytes = fixture(parts);
        for length in [bytes.len(), bytes.len() - 1] {
            let (result, allocations) = measure(|| {
                validate_operation_body(
                    OperationKind::Append,
                    black_box(&bytes[..length]),
                    OperationLimits::default(),
                )
            });
            assert_eq!(result.is_ok(), length == bytes.len());
            assert_eq!(allocations, 0, "{parts} parts per record, {length} bytes");
        }
    }
}

#[test]
fn borrowed_view_and_all_record_parts_allocate_nothing() {
    for parts in [1, 2, 3, 8] {
        let bytes = fixture(parts);
        let (payload_bytes, allocations) = measure(|| {
            let view = ozzy_journal::operation::decode_append_view(
                black_box(&bytes),
                OperationLimits::default(),
            )
            .unwrap();
            view.batches()
                .flat_map(|batch| batch.records())
                .flat_map(|record| record.parts)
                .map(|part| black_box(part).len())
                .sum::<usize>()
        });
        assert_eq!(payload_bytes, 1_000 * parts * b"payload".len());
        assert_eq!(allocations, 0, "{parts} parts per record");
    }
}

#[test]
fn captured_batch_views_reuse_validated_descriptors_without_allocating() {
    for parts in [1, 2, 3, 8] {
        let bytes = fixture(parts);
        let (payload_bytes, allocations) = measure(|| {
            ozzy_journal::operation::decode_append_batches(
                black_box(&bytes),
                OperationLimits::default(),
            )
            .unwrap()
            .iter()
            .flat_map(ozzy_journal::operation::AppendBatchView::records)
            .flat_map(|record| record.parts)
            .map(|part| black_box(part).len())
            .sum::<usize>()
        });
        assert_eq!(payload_bytes, 1_000 * parts * b"payload".len());
        assert_eq!(allocations, 0, "{parts} parts per record");
    }
}

#[test]
fn captured_batch_views_bound_allocations_by_validated_batches() {
    use ozzy_journal::operation::decode_append_batches;

    let limits = OperationLimits {
        max_append_batches: u32::MAX as usize,
        max_records: 5_000,
        max_parts: 5_000,
        ..OperationLimits::default()
    };
    let (result, allocations) =
        measure(|| decode_append_batches(&u32::MAX.to_be_bytes(), limits).map(|_| ()));
    assert!(result.is_err());
    assert_eq!(allocations, 0);

    let single = fixture(1);
    for count in [4_u32, 5] {
        let mut bytes = count.to_be_bytes().to_vec();
        for _ in 0..count {
            bytes.extend_from_slice(&single[4..]);
        }
        let (batches, allocations) = measure(|| decode_append_batches(&bytes, limits).unwrap());
        assert_eq!(batches.len(), count as usize);
        assert_eq!(allocations, usize::from(count > 4));
        drop(batches);
        assert!(decode_append_batches(&bytes[..bytes.len() - 1], limits).is_err());
        bytes.push(0);
        assert!(decode_append_batches(&bytes, limits).is_err());
    }
}

#[test]
fn indexed_fragments_validate_only_selected_descriptors_without_allocating() {
    use ozzy_journal::operation::{decode_append_records, decode_append_view};

    let bytes = fixture(3);
    let batch = decode_append_view(&bytes, OperationLimits::default())
        .unwrap()
        .batches()
        .next()
        .unwrap();
    let mut records = batch.records();
    while records.len() != 0 {
        let (descriptors, payload) = records.remaining_bytes();
        let expected = records.next().unwrap();
        let (after_descriptors, after_payload) = records.remaining_bytes();
        let descriptors = &descriptors[..descriptors.len() - after_descriptors.len()];
        let payload = &payload[..payload.len() - after_payload.len()];
        let (record, allocations) = measure(|| {
            decode_append_records(descriptors, payload, 1, OperationLimits::default())
                .unwrap()
                .next()
                .unwrap()
        });
        assert_eq!(allocations, 0);
        assert_eq!(record.message_id, expected.message_id);
        assert_eq!(
            record.parts.collect::<Vec<_>>(),
            expected.parts.collect::<Vec<_>>()
        );
        assert!(
            decode_append_records(
                &descriptors[..descriptors.len() - 1],
                payload,
                1,
                OperationLimits::default()
            )
            .is_err()
        );
        assert!(
            decode_append_records(
                descriptors,
                &payload[..payload.len() - 1],
                1,
                OperationLimits::default()
            )
            .is_err()
        );
        assert!(
            decode_append_records(descriptors, payload, 2, OperationLimits::default()).is_err()
        );
    }
}

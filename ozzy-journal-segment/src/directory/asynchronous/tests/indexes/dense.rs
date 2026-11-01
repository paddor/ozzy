//! Segments of many small records: one row per batch, and rows that count from
//! the segment's own first offset.

use super::*;
use crate::journal_index::seek::SeekQuery;
use crate::{
    BATCH_INDEX_ENTRY_BYTES, INDEX_HEADER_BYTES, IndexFileError, MESSAGE_INDEX_ENTRY_BYTES,
    OPERATION_INDEX_ENTRY_BYTES, OPERATION_LOCATION_BYTES,
};
use ozzy_proto::reader::Start;

const BATCHES: u32 = 3;
const BATCH_RECORDS: u32 = 500;
const RECORDS: u32 = BATCHES * BATCH_RECORDS;

/// Identities sort against offset order, so builds must really sort them.
fn id(offset: u32) -> MessageId {
    MessageId::from_bytes(u128::from(RECORDS - offset).to_be_bytes())
}

fn batch_time(batch: u32) -> u64 {
    100 + u64::from(batch) * 500
}

fn batch_body(batch: u32, partition: PartitionIncarnation) -> Vec<u8> {
    let first = batch * BATCH_RECORDS;
    let payload = b"x";
    encode_operation_body(
        &OperationBody::Append(Append {
            batches: vec![AppendBatch {
                partition,
                owner_epoch: OwnerEpoch::new(1),
                producer_id: ProducerId::from_bytes([0x30; 16]),
                producer_epoch: ProducerEpoch::INITIAL,
                first_sequence: ProducerSequence::new(u64::from(first)),
                first_offset: Offset::new(u64::from(first)),
                append_timestamp_millis: batch_time(batch),
                records: (first..first + BATCH_RECORDS)
                    .map(|offset| AppendRecord {
                        encoding: ozzy_proto::data::Encoding::Raw,
                        message_id: id(offset),
                        parts: vec![payload.as_slice()].into(),
                    })
                    .collect::<Vec<_>>()
                    .into(),
            }],
        }),
        OperationLimits::default(),
    )
    .unwrap()
}

fn dense_limits() -> IndexBuildLimits {
    IndexBuildLimits {
        max_entry_buffer_bytes: 16 * 1024,
        max_merge_fan_in: 2,
        max_run_files: 16,
        file: IndexLimits::default(),
        max_resident_bytes: 0,
    }
}

/// A sealed first segment of `bodies`, then a barrier.
fn sealed_journal(bodies: &[Vec<u8>]) -> (Controller, Journal) {
    let (mut controller, io) = setup(Image::default());
    let capacity = 256 * 1024;
    let format = Format {
        first_segment: SegmentHeader::new(identity().group_id, 1, None, Digest::ZERO, capacity)
            .unwrap(),
        ..spec(CommitMode::External)
    };
    let mut roomy = limits();
    roomy.io.max_segment_bytes = capacity;
    let mut journal = drive(
        &mut controller,
        Journal::format("/group".into(), io, format, JournalGeneration(1), roomy),
    )
    .unwrap();
    drive(&mut controller, async {
        for body in bodies {
            let template = operation(&journal);
            let op = CanonicalOperation {
                kind: OperationKind::Append,
                body,
                ..template
            };
            journal.append(&[op], BodyEncoding::Raw).await.unwrap();
        }
        append_confirmed(&mut journal).await;
        journal.roll_active(32768, 4).await.unwrap();
    });
    (controller, journal)
}

#[test]
fn a_dense_segment_keeps_one_row_per_batch_and_resolves_every_record() {
    let bodies: Vec<_> = (0..BATCHES)
        .map(|batch| batch_body(batch, partition()))
        .collect();
    let (mut controller, mut journal) = sealed_journal(&bodies);
    let index = drive(
        &mut controller,
        journal.build_sealed_index(1, dense_limits()),
    )
    .unwrap();
    assert_eq!(index.batch_count(), BATCHES as usize);
    assert_eq!(index.message_count(), RECORDS as usize);
    assert_eq!(index.operation_count(), 1);
    assert_eq!(index.location_count(), BATCHES as usize + 1);
    assert_eq!(
        index.as_bytes().len(),
        INDEX_HEADER_BYTES
            + BATCHES as usize * BATCH_INDEX_ENTRY_BYTES
            + RECORDS as usize * MESSAGE_INDEX_ENTRY_BYTES
            + OPERATION_INDEX_ENTRY_BYTES
            + (BATCHES as usize + 1) * OPERATION_LOCATION_BYTES
    );
    for offset in 0..RECORDS {
        let batch = offset / BATCH_RECORDS;
        let record = offset % BATCH_RECORDS;
        let entry = index
            .find_offset(partition(), Offset::new(u64::from(offset)))
            .unwrap();
        assert_eq!(entry.location.operation.op_number, u64::from(batch) + 1);
        assert_eq!(entry.location.batch_index, 0);
        assert_eq!(entry.location.record_index, record);
        let time = if record == 0 { batch_time(batch) } else { 0 };
        assert_eq!(entry.append_timestamp_millis, time);
        let message = index.find_message(partition(), id(offset)).unwrap();
        assert_eq!(message.offset, entry.offset);
        assert_eq!(
            message.operation_digest,
            entry.location.operation.operation_digest
        );
    }
    assert!(
        index
            .find_offset(partition(), Offset::new(u64::from(RECORDS)))
            .is_none()
    );

    // Many staged runs produce the bytes of one sorted pass.
    let source = index.source();
    let reference = journal.manifest.segments[0];
    let image = drive(&mut controller, journal.segment_image(reference)).unwrap();
    assert_eq!(
        index.as_bytes(),
        one_pass(&image, reference, source, dense_limits())
    );
}

#[test]
fn timestamp_seeks_read_batch_rows_without_touching_records() {
    let bodies: Vec<_> = (0..BATCHES)
        .map(|batch| batch_body(batch, partition()))
        .collect();
    let (mut controller, mut journal) = sealed_journal(&bodies);
    let index = drive(
        &mut controller,
        journal.build_sealed_index(1, dense_limits()),
    )
    .unwrap();
    let seek = |timestamp, retained_from: u32, confirmed_end: u32, through| {
        SeekQuery {
            partition: partition(),
            start: Start::Timestamp(timestamp),
            retained_from: Offset::new(u64::from(retained_from)),
            confirmed_end: Offset::new(u64::from(confirmed_end)),
            through,
        }
        .timestamp_offset(
            timestamp,
            index.view().batch_heads(),
            |partition, offset| index.find_offset(partition, offset),
        )
        .map(Offset::get)
    };
    assert_eq!(seek(batch_time(0), 0, RECORDS, 4), Some(0));
    assert_eq!(seek(batch_time(0) + 1, 0, RECORDS, 4), Some(500));
    assert_eq!(seek(batch_time(2), 0, RECORDS, 4), Some(1000));
    assert_eq!(seek(batch_time(2) + 1, 0, RECORDS, 4), None);
    // A batch starting below the retained floor answers with the floor.
    assert_eq!(seek(batch_time(0), 250, RECORDS, 4), Some(250));
    assert_eq!(seek(batch_time(0) + 1, 250, RECORDS, 4), Some(500));
    // Later batches past the visible bounds do not answer.
    assert_eq!(seek(batch_time(0) + 1, 0, 500, 4), None);
    assert_eq!(seek(batch_time(0) + 1, 0, RECORDS, 1), None);
}

#[test]
fn a_later_segment_counts_its_rows_from_its_own_first_offset() {
    let (mut controller, io) = setup(Image::default());
    let mut journal = drive(
        &mut controller,
        Journal::format(
            "/group".into(),
            io,
            spec(CommitMode::External),
            JournalGeneration(1),
            limits(),
        ),
    )
    .unwrap();
    drive(&mut controller, async {
        populate(&mut journal).await;
        for number in 6..12 {
            let body = append_body(number);
            let template = operation(&journal);
            let op = CanonicalOperation {
                kind: OperationKind::Append,
                body: &body,
                ..template
            };
            journal.append(&[op], BodyEncoding::Raw).await.unwrap();
        }
        append_confirmed(&mut journal).await;
        journal.roll_active(32768, 4).await.unwrap();
    });
    let index = drive(
        &mut controller,
        journal.build_sealed_index(2, build_limits()),
    )
    .unwrap();
    assert_eq!(index.batch_count(), 6);
    assert_eq!(
        u64::from_be_bytes(index.as_bytes()[312..320].try_into().unwrap()),
        6
    );
    assert!(index.find_offset(partition(), Offset::new(5)).is_none());
    for number in 6..12 {
        let entry = index.find_offset(partition(), Offset::new(number)).unwrap();
        assert_eq!(entry.location.operation.op_number, number + 2);
    }
    assert!(index.find_offset(partition(), Offset::new(12)).is_none());
    let odd = MessageId::from_bytes([0x41; 16]);
    assert_eq!(
        index.find_message(partition(), odd).unwrap().offset,
        Offset::new(7)
    );
    let matches = |floor, end| {
        index
            .view()
            .message_offsets(partition(), odd, Offset::new(floor), Offset::new(end))
    };
    let offsets = |first, last| Some((Offset::new(first), Offset::new(last)));
    assert_eq!(matches(0, 100), offsets(7, 11));
    assert_eq!(matches(8, 100), offsets(9, 11));
    assert_eq!(matches(0, 8), offsets(7, 7));
    assert_eq!(matches(12, 100), None);
}

#[test]
fn a_segment_of_two_partitions_has_no_index_and_stays_readable() {
    let other = PartitionIncarnation::from_bytes([0x21; 16]);
    let bodies = [batch_body(0, partition()), batch_body(1, other)];
    let (mut controller, mut journal) = sealed_journal(&bodies);
    assert!(matches!(
        drive(
            &mut controller,
            journal.build_sealed_index(1, dense_limits())
        ),
        Err(DirectoryError::Index(IndexBuildError::File(
            IndexFileError::MixedPartitions
        )))
    ));
    let reference = journal.manifest.segments[0];
    assert!(drive(&mut controller, journal.segment_image(reference)).is_ok());
}

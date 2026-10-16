use ozzy_journal::operation::{Append, AppendBatch, AppendRecord, OperationBody};
use ozzy_proto::{GroupId, MessageId, OwnerEpoch, ProducerEpoch, ProducerId, ProducerSequence};
use smallvec::smallvec;

use super::*;
use crate::Digest;

fn source(last: u64, valid_bytes: u64) -> IndexSource {
    IndexSource {
        group_id: GroupId::from_bytes([1; 16]),
        segment_id: 1,
        valid_bytes,
        segment_digest: Digest::from_bytes([last as u8; 32]),
        first_op_number: 1,
        last_op_number: last,
        last_operation_digest: Digest::from_bytes([(last + 10) as u8; 32]),
    }
}

fn location(op_number: u64, entry_offset: u64) -> OperationLocation {
    OperationLocation {
        segment_id: 1,
        entry_offset,
        entry_bytes: 4_096,
        op_number,
        operation_digest: Digest::from_bytes([(op_number + 10) as u8; 32]),
    }
}

fn append_body(first: u64, records: usize) -> OperationBody<'static> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: PartitionIncarnation::from_bytes([2; 16]),
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id: ProducerId::from_bytes([3; 16]),
            producer_epoch: ProducerEpoch::INITIAL,
            first_sequence: ProducerSequence::new(first),
            first_offset: Offset::new(first),
            append_timestamp_millis: 1,
            records: (0..records)
                .map(|index| AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([index as u8 + 1; 16]),
                    parts: smallvec![b"x".as_slice()],
                })
                .collect(),
        }],
    })
}

#[test]
fn packed_segment_can_exceed_default_index_file_limit() {
    let body = append_body(0, 1);
    let mut index = ActiveReadIndex::from_group(
        source(1, 8192),
        &[location(1, 4096)],
        &[body],
        IndexLimits::default(),
    )
    .unwrap();
    // A 256 MiB canonical body can hold roughly eight million 16-byte
    // records. Compact RAM runs do not shrink their persisted index entries.
    index.records = 256 * 1024 * 1024 / (16 + 17);
    assert_eq!(
        ActiveReadIndex::capacity_after(Some(&index), &[append_body(1, 1)], IndexLimits::default()),
        Err(ActiveReadIndexError::LimitExceeded),
    );
    assert!(
        ActiveReadIndex::capacity_after(
            Some(&index),
            &[append_body(1, 1)],
            IndexLimits {
                max_file_bytes: 2 * 1024 * 1024 * 1024,
                ..IndexLimits::default()
            }
        )
        .is_ok()
    );
}

#[test]
fn incremental_runs_resolve_exact_record_selectors() {
    let partition = PartitionIncarnation::from_bytes([2; 16]);
    let mut index = ActiveReadIndex::from_group(
        source(1, 8_192),
        &[location(1, 4_096)],
        &[append_body(0, 2)],
        IndexLimits::default(),
    )
    .unwrap();
    index
        .append_group(
            source(2, 12_288),
            &[location(2, 8_192)],
            &[append_body(2, 1)],
            IndexLimits::default(),
        )
        .unwrap();

    let entries = [0, 1, 2].map(|offset| index.entry(partition, Offset::new(offset)).unwrap());
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].location.record_index, 0);
    assert_eq!(entries[1].location.record_index, 1);
    assert_eq!(entries[2].location.operation.op_number, 2);
    assert_eq!(entries[2].location.record_index, 0);
}

#[test]
fn sequential_entries_match_point_lookups_at_every_range_boundary() {
    let partition = PartitionIncarnation::from_bytes([2; 16]);
    let mut index = ActiveReadIndex::from_group(
        source(1, 8192),
        &[location(1, 4096)],
        &[append_body(10, 3)],
        IndexLimits::default(),
    )
    .unwrap();
    index
        .append_group(
            source(2, 12288),
            &[location(2, 8192)],
            &[append_body(13, 2)],
            IndexLimits::default(),
        )
        .unwrap();
    for key in [partition, PartitionIncarnation::new()] {
        for start in 9..=16 {
            for end in start..=17 {
                for count in 0..=8 {
                    let expected = (start..end)
                        .take(count)
                        .map(|offset| index.entry(key, Offset::new(offset)))
                        .collect::<Vec<_>>();
                    assert_eq!(
                        index
                            .entries(key, Offset::new(start), Offset::new(end))
                            .take(count)
                            .collect::<Vec<_>>(),
                        expected
                    );
                }
            }
        }
    }
    let maximum = Offset::new(u64::MAX);
    assert_eq!(index.entries(partition, maximum, maximum).count(), 0);
}

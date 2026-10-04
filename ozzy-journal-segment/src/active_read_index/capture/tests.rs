use super::*;
use crate::{Digest, IndexSource, OperationLocation};
use ozzy_proto::GroupId;

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([1; 16])
}

fn run(first: u64, count: u32, operation: u64, batch: u32) -> ActiveReadRun {
    ActiveReadRun {
        append_timestamp_millis: 0,
        first_offset: Offset::new(first),
        records: count,
        batch_index: batch,
        first_record_index: batch,
        operation: OperationLocation {
            segment_id: 1,
            entry_offset: operation * 4096,
            entry_bytes: 4096,
            op_number: operation,
            operation_digest: Digest::from_bytes([operation as u8; 32]),
        },
    }
}

fn index() -> ActiveReadIndex {
    ActiveReadIndex {
        source: IndexSource {
            group_id: GroupId::new(),
            segment_id: 1,
            valid_bytes: 16384,
            segment_digest: Digest::ZERO,
            first_op_number: 1,
            last_op_number: 3,
            last_operation_digest: Digest::from_bytes([3; 32]),
        },
        partitions: ahash::AHashMap::from([(
            partition(),
            vec![
                run(10, 5, 1, 0),
                run(15, 7, 1, 1),
                run(22, 3, 2, 0),
                run(25, 2, 3, 0),
            ],
        )]),
        records: 17,
        operations: 3,
    }
}

#[test]
fn captured_ranges_match_individual_selection_under_every_boundary() {
    let index = index();
    for partition in [partition(), PartitionIncarnation::new()] {
        for start in 9..=29 {
            for end in 9..=29 {
                for cap in 0..=20 {
                    for through in 0..=3 {
                        let mut expected = Ok(Vec::new());
                        for entry in index
                            .entries(partition, Offset::new(start), Offset::new(end))
                            .take(cap)
                        {
                            match entry {
                                Ok(entry) if entry.location.operation.op_number > through => break,
                                Ok(entry) => expected.as_mut().unwrap().push(entry),
                                Err(error) => {
                                    expected = Err(error);
                                    break;
                                }
                            }
                        }
                        let captured = index
                            .capture(
                                partition,
                                Offset::new(start),
                                Offset::new(end),
                                cap,
                                through,
                            )
                            .map(|capture| capture.entries().collect::<Vec<_>>());
                        assert_eq!(
                            captured, expected,
                            "start={start} end={end} cap={cap} through={through}"
                        );
                    }
                }
            }
        }
    }
}

#[test]
fn common_captures_keep_ranges_inline_and_do_not_retain_the_index() {
    let mut index = index();
    for (end, runs, spilled) in [
        (10, 0, false),
        (14, 1, false),
        (22, 2, false),
        (27, 4, true),
    ] {
        let capture = index
            .capture(partition(), Offset::new(10), Offset::new(end), 1024, 3)
            .unwrap();
        assert_eq!(capture.runs.len(), runs);
        assert_eq!(capture.runs.spilled(), spilled);
    }
    let capture = index
        .capture(partition(), Offset::new(12), Offset::new(27), 1024, 2)
        .unwrap();
    let expected = capture.runs.clone();
    index
        .partitions
        .get_mut(&partition())
        .unwrap()
        .push(run(27, 5, 4, 0));
    drop(index);
    assert_eq!(capture.runs, expected);
    let entries = capture.entries().collect::<Vec<_>>();
    assert_eq!(entries.len(), 13);
    assert_eq!(entries.first().unwrap().offset, Offset::new(12));
    assert_eq!(entries.last().unwrap().offset, Offset::new(24));
}

#[test]
fn capture_handles_extreme_offsets_without_wrapping() {
    let mut index = index();
    index
        .partitions
        .insert(partition(), vec![run(u64::MAX - 2, 2, 1, 0)]);
    let entries = index
        .capture(
            partition(),
            Offset::new(u64::MAX - 2),
            Offset::new(u64::MAX),
            usize::MAX,
            1,
        )
        .unwrap()
        .entries()
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[1].offset.get(), u64::MAX - 1);
    let empty = index
        .capture(
            partition(),
            Offset::new(u64::MAX),
            Offset::new(u64::MAX),
            usize::MAX,
            1,
        )
        .unwrap();
    assert_eq!(empty.entries().count(), 0);
    let mut invalid = run(10, 2, 1, 0);
    invalid.first_record_index = u32::MAX;
    index.partitions.insert(partition(), vec![invalid]);
    assert!(
        index
            .capture(partition(), Offset::new(10), Offset::new(12), 2, 1)
            .is_err()
    );
}

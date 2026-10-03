use super::*;
use crate::{
    CanonicalOperation, ChainPosition, Digest, OperationKind, SegmentHeader, encode_group,
    encode_segment_header, scan_segment, test_io::Scheduling,
};
use ozzy_journal::operation::logical_operation_digest;
use ozzy_proto::GroupId;
use std::{future::Future, pin::pin, sync::Arc, task::Poll};

fn fixture(duplicate: bool) -> Vec<u8> {
    let header =
        SegmentHeader::new(GroupId::from_bytes([1; 16]), 1, None, Digest::ZERO, 32768).unwrap();
    let mut bodies: Vec<_> = (1u128..=130).rev().map(u128::to_be_bytes).collect();
    if duplicate {
        bodies[129] = bodies[0];
    }
    let mut chain = ChainPosition::GENESIS;
    let mut operations = Vec::new();
    for body in &bodies {
        let operation = CanonicalOperation {
            group_id: header.group_id(),
            configuration_epoch: 1,
            original_view: 0,
            op_number: chain.next_op_number(),
            previous_digest: chain.previous_digest(),
            kind: OperationKind::Barrier,
            body,
        };
        chain = ChainPosition::new(
            operation.op_number + 1,
            logical_operation_digest(&operation),
        );
        operations.push(operation);
    }
    let mut image = encode_segment_header(&header).to_vec();
    let group = encode_group(
        &header,
        1,
        image.len() as u64,
        ChainPosition::GENESIS,
        &operations,
    )
    .unwrap();
    image.extend_from_slice(group.as_bytes());
    image
}

fn run<T>(future: impl Future<Output = T>) -> (T, usize) {
    let mut future = pin!(future);
    let scheduling = Arc::new(Scheduling::default());
    for polls in 1..1000 {
        if let Poll::Ready(result) = scheduling.poll(future.as_mut()) {
            return (result, polls);
        }
        assert!(scheduling.woken());
    }
    panic!("active index construction stalled")
}

#[test]
fn cooperative_active_index_preserves_prefix_bounds_and_exact_sorted_image() {
    let bytes = fixture(false);
    let scan = scan_segment(
        &bytes,
        1,
        ChainPosition::GENESIS,
        crate::DecodeLimits::default(),
    )
    .unwrap();
    for through in [0, 1, 64, 129, 130] {
        let expected = ActiveSegmentIndex::build(
            &scan,
            through,
            OperationLimits::default(),
            IndexLimits::default(),
        )
        .unwrap();
        let (actual, polls) = run(ActiveSegmentIndex::build_async(
            &scan,
            through,
            OperationLimits::default(),
            IndexLimits::default(),
        ));
        let actual = actual.unwrap();
        assert_eq!(
            actual.as_ref().map(|index| &index.image),
            expected.as_ref().map(|index| &index.image)
        );
        if through >= 64 {
            assert!(polls > 1);
        }
        if let Some(actual) = actual {
            assert_eq!(actual.source().last_op_number, through);
            assert_eq!(actual.operation_count(), through as usize);
            for number in 1..=through {
                let id = OperationId::from_bytes(u128::from(131 - number).to_be_bytes());
                assert_eq!(
                    actual.find_operation(id).unwrap().location.op_number,
                    number
                );
            }
        }
    }
    let (missing, _) = run(ActiveSegmentIndex::build_async(
        &scan,
        131,
        OperationLimits::default(),
        IndexLimits::default(),
    ));
    assert!(matches!(
        missing,
        Err(ActiveIndexError::PositionNotCovered {
            requested: 131,
            last: 130
        })
    ));
}

#[test]
fn cooperative_active_index_cancellation_limits_and_duplicate_keys_keep_scan_unchanged() {
    let bytes = fixture(false);
    let scan = scan_segment(
        &bytes,
        1,
        ChainPosition::GENESIS,
        crate::DecodeLimits::default(),
    )
    .unwrap();
    for cut in [1, 2, 3, 10] {
        let mut future = Box::pin(ActiveSegmentIndex::build_async(
            &scan,
            130,
            OperationLimits::default(),
            IndexLimits::default(),
        ));
        let scheduling = Arc::new(Scheduling::default());
        for _ in 0..cut {
            assert!(scheduling.poll(future.as_mut()).is_pending());
        }
        drop(future);
        let (index, _) = run(ActiveSegmentIndex::build_async(
            &scan,
            130,
            OperationLimits::default(),
            IndexLimits::default(),
        ));
        assert_eq!(
            index.unwrap().unwrap().source().last_operation_digest,
            scan.next_chain.previous_digest()
        );
    }
    let (limited, polls) = run(ActiveSegmentIndex::build_async(
        &scan,
        130,
        OperationLimits::default(),
        IndexLimits {
            max_operation_entries: 64,
            ..IndexLimits::default()
        },
    ));
    assert!(polls > 1);
    assert!(matches!(
        limited,
        Err(ActiveIndexError::LimitExceeded {
            actual: 65,
            limit: 64,
            ..
        })
    ));
    let (limited, _) = run(ActiveSegmentIndex::build_async(
        &scan,
        130,
        OperationLimits::default(),
        IndexLimits {
            max_file_bytes: INDEX_HEADER_BYTES,
            ..IndexLimits::default()
        },
    ));
    assert!(matches!(
        limited,
        Err(ActiveIndexError::LimitExceeded {
            kind: "active index footprint",
            ..
        })
    ));
    let bytes = fixture(true);
    let scan = scan_segment(
        &bytes,
        1,
        ChainPosition::GENESIS,
        crate::DecodeLimits::default(),
    )
    .unwrap();
    let (duplicate, polls) = run(ActiveSegmentIndex::build_async(
        &scan,
        130,
        OperationLimits::default(),
        IndexLimits::default(),
    ));
    assert!(polls > 2);
    assert!(matches!(
        duplicate,
        Err(ActiveIndexError::File(IndexFileError::UnsortedOrDuplicate(
            "operation"
        )))
    ));
}

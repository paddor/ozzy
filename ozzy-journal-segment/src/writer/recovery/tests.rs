use super::*;
use crate::test_io::Scheduling;
use crate::{
    CanonicalOperation, Digest, OperationKind, SegmentHeader, encode_group, encode_segment_header,
};
use ozzy_journal::operation::{OperationLimits, logical_operation_digest};
use ozzy_proto::GroupId;
use std::{future::Future, pin::pin, sync::Arc, task::Poll};

/// Synchronous oracle for the cooperative recovery decision.
fn prepare(
    image: &[u8],
    generation: JournalGeneration,
    first_group_number: u64,
    initial_chain: ChainPosition,
    limits: DecodeLimits,
    validation: RecoveryValidation,
) -> Result<Recovery, WriterError> {
    let scan = crate::codec::scan(
        image,
        first_group_number,
        initial_chain,
        limits,
        validation.discard_damaged_tail,
    )?;
    let mut checks = Checks::new(initial_chain, validation);
    let mut segment_digest = SegmentDigestBuilder::new(&scan.header);
    for group in &scan.groups {
        for operation in &group.operations {
            checks.operation(operation)?;
        }
        segment_digest.push(group.digest);
    }
    checks.finish(scan.next_chain)?;
    let truncate = matches!(scan.tail, TailState::Truncated { .. }).then_some(scan.valid_bytes);
    let zero = if matches!(scan.tail, TailState::Damaged { .. }) {
        let start = scan.valid_bytes as usize;
        let mut end = image.len();
        while end > start {
            let block = ((end - 1) / crate::WRITE_GROUP_ALIGNMENT * crate::WRITE_GROUP_ALIGNMENT)
                .max(start);
            if image[block..end].iter().any(|byte| *byte != 0) {
                break;
            }
            end = block;
        }
        scan.valid_bytes..end as u64
    } else {
        0..0
    };
    Ok(finish(scan, generation, segment_digest, truncate, zero))
}

fn fixture(initial: ChainPosition, fault: u8) -> (Vec<u8>, Vec<ChainPosition>) {
    let header = SegmentHeader::new(
        GroupId::from_bytes([1; 16]),
        1,
        None,
        Digest::ZERO,
        2 * 1024 * 1024,
    )
    .unwrap();
    let mut chain = initial;
    let mut positions = vec![chain];
    let mut operations = Vec::new();
    for index in 0..130 {
        let last = index == 129;
        let operation = CanonicalOperation {
            group_id: header.group_id(),
            configuration_epoch: if last && fault == 1 { 2 } else { 1 },
            original_view: if last && fault == 2 { 2 } else { 1 },
            op_number: chain.next_op_number(),
            previous_digest: chain.previous_digest(),
            kind: OperationKind::Barrier,
            body: if last && fault == 3 {
                &[3; 15]
            } else {
                &[3; 16]
            },
        };
        chain = ChainPosition::new(
            operation.op_number + 1,
            logical_operation_digest(&operation),
        );
        positions.push(chain);
        operations.push(operation);
    }
    let mut image = encode_segment_header(&header).to_vec();
    let group = encode_group(
        &header,
        crate::GroupStart::new(1, image.len() as u64, initial),
        &operations,
    )
    .unwrap();
    image.extend_from_slice(group.as_bytes());
    (image, positions)
}

fn validation(protected: [Option<ChainPosition>; 2]) -> RecoveryValidation {
    RecoveryValidation {
        protected,
        operation_limits: Some(OperationLimits::default()),
        configuration_epoch: Some(1),
        promised_view: Some(1),
        discard_damaged_tail: true,
    }
}

fn run<T>(future: impl Future<Output = T>) -> (T, usize) {
    let mut future = pin!(future);
    let scheduling = Arc::new(Scheduling::default());
    for polls in 1..100 {
        if let Poll::Ready(result) = scheduling.poll(future.as_mut()) {
            return (result, polls);
        }
        assert!(scheduling.woken(), "CPU work must schedule its next turn");
    }
    panic!("recovery exceeded scheduling bound")
}

fn asynchronous(
    image: &[u8],
    initial: ChainPosition,
    validation: RecoveryValidation,
) -> (Result<Recovery, WriterError>, usize) {
    run(prepare_async(
        image,
        Start {
            generation: JournalGeneration(2),
            first_group_number: 1,
            initial_chain: initial,
        },
        DecodeLimits::default(),
        validation,
    ))
}

#[test]
fn cooperative_recovery_validates_each_operation_inside_one_group() {
    let initial = ChainPosition::new(10, Digest::ZERO);
    let (image, positions) = fixture(initial, 0);
    let requirements = validation([Some(initial), Some(positions[130])]);
    let (result, polls) = asynchronous(&image, initial, requirements);
    assert_eq!(
        polls, 3,
        "one small physical group still needs validation turns"
    );
    let result = result.unwrap();
    let expected = prepare(
        &image,
        JournalGeneration(2),
        1,
        initial,
        DecodeLimits::default(),
        requirements,
    )
    .unwrap();
    assert_eq!(
        result.writer.written_position(),
        expected.writer.written_position()
    );
    assert_eq!(
        result.writer.written_position().next_chain(),
        positions[130]
    );
    assert_eq!(
        result.writer.structural_digest(),
        expected.writer.structural_digest()
    );
    assert_eq!(result.truncate, None);
    assert_eq!(result.zero, 0..0);
}

#[test]
fn cooperative_recovery_refuses_either_protected_prefix_outside_the_verified_chain() {
    let initial = ChainPosition::new(10, Digest::ZERO);
    let (image, positions) = fixture(initial, 0);
    for invalid in [
        ChainPosition::new(0, Digest::ZERO),
        ChainPosition::new(9, Digest::ZERO),
        ChainPosition::new(141, Digest::ZERO),
        ChainPosition::new(74, Digest::ZERO),
        ChainPosition::new(10, positions[1].previous_digest()),
    ] {
        for protected in [
            [Some(invalid), Some(positions[64])],
            [Some(positions[64]), Some(invalid)],
        ] {
            let (result, polls) = asynchronous(&image, initial, validation(protected));
            assert!(polls > 1);
            let expected = if invalid.next_op_number() == 0 {
                u64::MAX
            } else {
                invalid.next_op_number() - 1
            };
            assert!(
                matches!(result, Err(WriterError::ProtectedPrefixMismatch(actual)) if actual == expected)
            );
        }
    }
    let (result, _) = asynchronous(
        &image,
        initial,
        validation([Some(positions[64]), Some(positions[130])]),
    );
    assert!(result.is_ok());
}

#[test]
fn cooperative_recovery_never_repairs_before_late_canonical_validation() {
    for fault in 1..=3 {
        let (mut image, _) = fixture(ChainPosition::GENESIS, fault);
        image.extend_from_slice(&[99; 4096]);
        image.resize(image.len() + 512 * 1024, 0);
        let (result, polls) = asynchronous(&image, ChainPosition::GENESIS, validation([None; 2]));
        assert!(polls > 1);
        match (fault, result) {
            (1, Err(WriterError::ConfigurationMismatch { op_number: 130, .. }))
            | (2, Err(WriterError::ViewBeyondPromise { op_number: 130, .. }))
            | (3, Err(WriterError::Operation(_))) => {}
            _ => panic!("late invalid operation must refuse the whole repair"),
        }
    }
}

#[test]
fn cooperative_recovery_cancellation_keeps_exact_damaged_tail_decisions() {
    let (mut image, positions) = fixture(ChainPosition::GENESIS, 0);
    let valid = image.len() as u64;
    image.extend_from_slice(&[99; 4096]);
    image.resize(image.len() + 3 * crate::cooperative::TURN_BYTES, 0);
    let before = image.clone();
    let requirements = validation([Some(positions[130]), None]);
    let (result, polls) = asynchronous(&image, ChainPosition::GENESIS, requirements);
    let result = result.unwrap();
    assert!(
        polls >= 6,
        "validation and long reverse-tail scan must yield"
    );
    assert_eq!(result.zero, valid..valid + 4096);
    assert_eq!(result.truncate, None);
    for cut in 1..polls {
        let mut future = Box::pin(prepare_async(
            &image,
            Start {
                generation: JournalGeneration(2),
                first_group_number: 1,
                initial_chain: ChainPosition::GENESIS,
            },
            DecodeLimits::default(),
            requirements,
        ));
        let scheduling = Arc::new(Scheduling::default());
        for _ in 0..cut {
            assert!(scheduling.poll(future.as_mut()).is_pending());
            assert!(scheduling.woken());
        }
        drop(future);
        assert_eq!(image, before);
        let (result, _) = asynchronous(&image, ChainPosition::GENESIS, requirements);
        assert_eq!(result.unwrap().zero, valid..valid + 4096);
    }
    image.truncate(valid as usize - 1);
    let (result, _) = asynchronous(
        &image,
        ChainPosition::GENESIS,
        validation([Some(ChainPosition::GENESIS), None]),
    );
    let result = result.unwrap();
    assert_eq!(result.truncate, Some(crate::SEGMENT_HEADER_BYTES as u64));
    assert_eq!(result.zero, 0..0);
}

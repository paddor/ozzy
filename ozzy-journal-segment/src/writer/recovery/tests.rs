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
    let mut later = Vec::new();
    if matches!(
        scan.tail,
        TailState::Truncated { .. } | TailState::Damaged { .. }
    ) {
        let damaged = scan.next_group_number;
        let start = scan.valid_bytes as usize;
        for end in (start + crate::WRITE_GROUP_ALIGNMENT..=image.len())
            .step_by(crate::WRITE_GROUP_ALIGNMENT)
        {
            match crate::codec::durable_mark_ending_at(
                &scan.header,
                image,
                (damaged, scan.valid_bytes),
                end,
                limits,
            ) {
                Some(mark) if mark >= damaged => {
                    return Err(WriterError::DurableGroupDamaged(damaged));
                }
                Some(_) => later.push(end as u64 - 4096..end as u64),
                None => {}
            }
        }
    }
    let truncate = matches!(scan.tail, TailState::Truncated { .. }).then_some(scan.valid_bytes);
    if truncate.is_some() {
        later.clear();
    }
    Ok(finish(scan, generation, segment_digest, truncate, later))
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

/// Three groups of two operations, stamped with `marks` as a writer with
/// writes in flight would. Returns the image and each group's end offset.
fn marked_groups(marks: [u64; 3]) -> (Vec<u8>, Vec<u64>) {
    let header = SegmentHeader::new(
        GroupId::from_bytes([1; 16]),
        1,
        None,
        Digest::ZERO,
        2 * 1024 * 1024,
    )
    .unwrap();
    let mut image = encode_segment_header(&header).to_vec();
    let mut chain = ChainPosition::GENESIS;
    let mut ends = Vec::new();
    for (number, mark) in (1..).zip(marks) {
        let start = crate::GroupStart::new(number, image.len() as u64, chain);
        let mut operations = Vec::new();
        for _ in 0..2 {
            let operation = CanonicalOperation {
                group_id: header.group_id(),
                configuration_epoch: 1,
                original_view: 1,
                op_number: chain.next_op_number(),
                previous_digest: chain.previous_digest(),
                kind: OperationKind::Barrier,
                body: &[3; 16],
            };
            chain = ChainPosition::new(
                operation.op_number + 1,
                logical_operation_digest(&operation),
            );
            operations.push(operation);
        }
        let digests = operations
            .iter()
            .map(|operation| ozzy_journal::operation::canonical_body_digest(operation.body))
            .collect::<Vec<_>>();
        let mut bodies = crate::codec::prepare_group_bodies(
            operations.iter().map(|operation| operation.body),
            crate::BodyEncoding::Raw,
            Vec::new(),
            &mut crate::codec::BodyEncodeScratch::default(),
        )
        .unwrap();
        let finalized = crate::codec::finalize_group_bodies(
            &header,
            crate::codec::GroupWrite {
                start,
                durable_through: mark,
            },
            &operations,
            &digests,
            &mut bodies,
        )
        .unwrap();
        image.extend_from_slice(bodies.finish(finalized).as_bytes());
        ends.push(image.len() as u64);
    }
    (image, ends)
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
    assert_eq!(result.invalidate, [] as [Range<u64>; 0]);
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
    assert_eq!(result.invalidate, [] as [Range<u64>; 0]);
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
        let result = result.unwrap();
        assert_eq!(result.writer.written_position().end_offset(), valid);
        assert_eq!(result.invalidate, [] as [Range<u64>; 0]);
    }
    image.truncate(valid as usize - 1);
    let (result, _) = asynchronous(
        &image,
        ChainPosition::GENESIS,
        validation([Some(ChainPosition::GENESIS), None]),
    );
    let result = result.unwrap();
    assert_eq!(result.truncate, Some(crate::SEGMENT_HEADER_BYTES as u64));
    assert_eq!(result.invalidate, [] as [Range<u64>; 0]);
}

#[test]
fn recovery_refuses_a_damaged_group_that_a_later_group_marks_durable() {
    let (mut image, ends) = marked_groups([0, 1, 2]);
    // Group 3 was prepared after group 2 completed, so group 2 is durable.
    image[ends[0] as usize + 200] ^= 1;
    let (result, _) = asynchronous(&image, ChainPosition::GENESIS, validation([None; 2]));
    assert!(matches!(result, Err(WriterError::DurableGroupDamaged(2))));
}

#[test]
fn recovery_ends_the_log_at_a_damaged_group_no_later_group_marks() {
    // Group 3 was prepared while group 2 was in flight and completed first;
    // a crash left group 2 unfinished. Neither was ever voted.
    let (mut image, ends) = marked_groups([0, 1, 1]);
    image[ends[0] as usize + 200] ^= 1;
    let (result, _) = asynchronous(&image, ChainPosition::GENESIS, validation([None; 2]));
    let result = result.unwrap();
    assert_eq!(result.writer.written_position().end_offset(), ends[0]);
    // Group 3 loses its seal, so a rewritten group 2 cannot chain into it.
    assert_eq!(result.invalidate, vec![ends[2] - 4096..ends[2]]);
    // A torn newest group ends the log the same way, with nothing after it.
    let (mut image, ends) = marked_groups([0, 1, 2]);
    image[ends[1] as usize + 200] ^= 1;
    let (result, _) = asynchronous(&image, ChainPosition::GENESIS, validation([None; 2]));
    let result = result.unwrap();
    assert_eq!(result.writer.written_position().end_offset(), ends[1]);
    assert_eq!(result.invalidate, [] as [Range<u64>; 0]);
}

#[test]
fn a_rewritten_damaged_group_cannot_chain_into_an_invalidated_later_group() {
    // Group 3 completed while group 2 was in flight; a crash tore group 2.
    let (original, ends) = marked_groups([0, 1, 1]);
    let (whole, _) = asynchronous(&original, ChainPosition::GENESIS, validation([None; 2]));
    assert_eq!(
        whole.unwrap().writer.written_position().end_offset(),
        ends[2],
        "intact, group 3 chains after group 2"
    );
    let mut image = original.clone();
    image[ends[0] as usize + 200] ^= 1;
    let (result, _) = asynchronous(&image, ChainPosition::GENESIS, validation([None; 2]));
    for range in result.unwrap().invalidate {
        image[range.start as usize..range.end as usize].fill(0);
    }
    // The writer then writes group 2 again, byte for byte.
    let group = ends[0] as usize..ends[1] as usize;
    image[group.clone()].copy_from_slice(&original[group]);
    let (result, _) = asynchronous(&image, ChainPosition::GENESIS, validation([None; 2]));
    let result = result.unwrap();
    assert_eq!(result.writer.written_position().end_offset(), ends[1]);
    assert_eq!(result.invalidate, [] as [Range<u64>; 0]);
}

#[test]
fn bytes_after_the_log_end_it_even_beyond_decode_limits() {
    let (mut image, ends) = marked_groups([0, 1, 2]);
    // An earlier use of the file left a group with a larger entry here.
    let old = SegmentHeader::new(
        GroupId::from_bytes([1; 16]),
        7,
        None,
        Digest::ZERO,
        2 * 1024 * 1024,
    )
    .unwrap();
    let operation = CanonicalOperation {
        group_id: old.group_id(),
        configuration_epoch: 1,
        original_view: 1,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Barrier,
        body: &[5; 2048],
    };
    let start = crate::GroupStart::new(1, image.len() as u64, ChainPosition::GENESIS);
    let group = encode_group(&old, start, &[operation]).unwrap();
    image.extend_from_slice(group.as_bytes());
    let limits = DecodeLimits {
        max_entry_bytes: 1024,
        max_decoded_body_bytes: 1024,
        ..DecodeLimits::default()
    };
    let start = Start {
        generation: JournalGeneration(2),
        first_group_number: 1,
        initial_chain: ChainPosition::GENESIS,
    };
    let (result, _) = run(prepare_async(&image, start, limits, validation([None; 2])));
    let result = result.unwrap();
    assert_eq!(result.writer.written_position().end_offset(), ends[2]);
    assert_eq!(result.invalidate, [] as [Range<u64>; 0]);
}

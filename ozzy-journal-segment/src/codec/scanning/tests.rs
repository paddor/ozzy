use super::*;
use crate::codec::{
    CanonicalOperation, ChainPosition, Digest, OperationKind, SegmentHeader, encode_group,
    encode_segment_header, scan_segment, scan_segment_prefix,
};
use ozzy_proto::GroupId;
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

fn image(groups: u64, zero_bytes: usize) -> Vec<u8> {
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
    for number in 1..=groups {
        let operation = CanonicalOperation {
            group_id: header.group_id(),
            configuration_epoch: 1,
            original_view: 0,
            op_number: number,
            previous_digest: chain.previous_digest(),
            kind: OperationKind::Barrier,
            body: &u128::from(number).to_be_bytes(),
        };
        let group = encode_group(&header, number, image.len() as u64, chain, &[operation]).unwrap();
        chain = group.next_chain();
        image.extend_from_slice(group.as_bytes());
    }
    image.resize(image.len() + zero_bytes, 0);
    image
}

fn asynchronous(
    image: &[u8],
    limits: DecodeLimits,
) -> (Result<SegmentScan<'_>, CodecError>, usize) {
    let mut future = pin!(scan_segment_async(image, 1, ChainPosition::GENESIS, limits));
    for polls in 1..100 {
        if let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return (result, polls);
        }
    }
    panic!("cooperative scan did not finish");
}

#[test]
fn cooperative_scan_preserves_chain_and_yields_across_groups_and_zero_tail() {
    for (groups, zero_bytes) in [(0, 0), (130, 0), (0, TURN_BYTES * 3), (130, TURN_BYTES * 3)] {
        let image = image(groups, zero_bytes);
        let limits = DecodeLimits::default();
        let expected = scan_segment(&image, 1, ChainPosition::GENESIS, limits).unwrap();
        let (actual, polls) = asynchronous(&image, limits);
        let actual = actual.unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.groups.len() as u64, groups);
        assert_eq!(actual.next_chain.next_op_number(), groups + 1);
        assert_eq!(
            actual.valid_bytes,
            SEGMENT_HEADER_BYTES as u64 * (groups + 1)
        );
        assert_eq!(
            actual.tail,
            if zero_bytes == 0 {
                TailState::Clean
            } else {
                TailState::ZeroFilled { bytes: zero_bytes }
            }
        );
        assert_eq!(polls > 1, groups > 64 || zero_bytes > TURN_BYTES);
    }
}

#[test]
fn cooperative_scan_distinguishes_truncation_corruption_and_late_nonzero_tail() {
    let mut truncated = image(130, 0);
    truncated.truncate(truncated.len() - 1);
    let (scan, polls) = asynchronous(&truncated, DecodeLimits::default());
    let scan = scan.unwrap();
    assert!(polls > 1);
    assert_eq!(scan.groups.len(), 129);
    assert!(matches!(scan.tail, TailState::Truncated { .. }));

    let mut corrupt = image(130, 0);
    corrupt[SEGMENT_HEADER_BYTES * 129] ^= 1;
    let (scan, polls) = asynchronous(&corrupt, DecodeLimits::default());
    assert!(polls > 1);
    assert!(matches!(scan, Err(CodecError::WrongMagic("journal entry"))));
    let prefix =
        scan_segment_prefix(&corrupt, 1, ChainPosition::GENESIS, DecodeLimits::default()).unwrap();
    assert_eq!(prefix.groups.len(), 128);
    assert!(matches!(prefix.tail, TailState::Damaged { .. }));

    let mut trailing = image(1, TURN_BYTES * 3);
    *trailing.last_mut().unwrap() = 7;
    let (scan, polls) = asynchronous(&trailing, DecodeLimits::default());
    assert!(polls > 1);
    assert!(matches!(scan, Err(CodecError::WrongMagic("journal entry"))));
}

#[test]
fn cooperative_scan_keeps_group_limit_and_cancelled_scan_has_no_result() {
    let image = image(130, TURN_BYTES * 2);
    let limits = DecodeLimits {
        max_groups: 100,
        ..DecodeLimits::default()
    };
    let (result, polls) = asynchronous(&image, limits);
    assert!(polls > 1);
    assert!(matches!(
        result,
        Err(CodecError::LimitExceeded {
            kind: "physical group count",
            actual: 101,
            limit: 100
        })
    ));
    {
        let mut scanning = pin!(scan_segment_async(
            &image,
            1,
            ChainPosition::GENESIS,
            DecodeLimits::default()
        ));
        assert!(
            scanning
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    let (result, polls) = asynchronous(&image, DecodeLimits::default());
    assert!(polls > 1);
    assert_eq!(result.unwrap().groups.len(), 130);
}

#[cfg(feature = "lz4")]
#[test]
fn cooperative_scan_charges_decoded_work_for_small_compressed_groups() {
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
    let body = vec![42; TURN_BYTES];
    for number in 1..=12 {
        let operation = CanonicalOperation {
            group_id: header.group_id(),
            configuration_epoch: 1,
            original_view: 0,
            op_number: number,
            previous_digest: chain.previous_digest(),
            kind: OperationKind::Append,
            body: &body,
        };
        let group = crate::codec::encode_group_with_body_encoding(
            &header,
            number,
            image.len() as u64,
            chain,
            &[operation],
            crate::BodyEncoding::Lz4 {
                min_savings_bytes: 0,
            },
        )
        .unwrap();
        chain = group.next_chain();
        image.extend_from_slice(group.as_bytes());
    }
    assert!(image.len() < TURN_BYTES);
    let (result, polls) = asynchronous(&image, DecodeLimits::default());
    assert!(polls >= 12, "encoded size cannot hide decoded CPU work");
    let result = result.unwrap();
    assert_eq!(result.decoded_body_bytes, 12 * TURN_BYTES);
    assert_eq!(result.next_chain, chain);
}

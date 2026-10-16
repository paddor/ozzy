use super::*;
use ozzy_journal::operation::{DedupWatermark, validate_operation_body};

#[test]
fn idle_watermark_has_fixed_network_bytes_without_client_retry_identity() {
    let expected = [
        [0x10; 16].as_slice(),
        &[0, 0, 0, 0, 0, 0, 0, 9],
        &[0, 0, 0, 0, 0, 4, 0x93, 0xe0],
    ]
    .concat();
    let body = OperationBody::DedupWatermark(DedupWatermark {
        partition: PartitionIncarnation::from_bytes([0x10; 16]),
        owner_epoch: OwnerEpoch::new(9),
        logical_time: 300_000,
    });
    let limits = OperationLimits::default();
    assert_eq!(
        OperationKind::try_from(11).unwrap(),
        OperationKind::DedupWatermark
    );
    assert_eq!(body.kind(), OperationKind::DedupWatermark);
    assert_eq!(body.operation_id(), None);
    assert!(!body.kind().has_operation_id());
    assert_eq!(encode_operation_body(&body, limits).unwrap(), expected);
    assert_eq!(
        decode_operation_body(body.kind(), &expected, limits).unwrap(),
        body
    );
    for end in 0..expected.len() {
        assert!(decode_operation_body(body.kind(), &expected[..end], limits).is_err());
    }
    let mut extended = expected.clone();
    extended.push(0);
    assert!(decode_operation_body(body.kind(), &extended, limits).is_err());
    assert!(validate_operation_body(body.kind(), &extended, limits).is_err());
    let mut output = vec![0xaa; 3];
    assert!(
        append_operation_body(
            &mut output,
            &body,
            OperationLimits {
                max_body_bytes: 31,
                ..limits
            },
        )
        .is_err()
    );
    assert_eq!(output, [0xaa; 3]);
}

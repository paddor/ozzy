use super::*;

#[test]
fn create_partition_carries_fixed_dedup_window_and_rejects_previous_layout() {
    let OperationBody::CreatePartition(mut create) = bodies().remove(0) else {
        unreachable!()
    };
    for window in [None, NonZeroU64::new(300_000), NonZeroU64::new(u64::MAX)] {
        create.dedup_window_millis = window;
        let body = OperationBody::CreatePartition(create.clone());
        let encoded = encode_operation_body(&body, OperationLimits::default()).unwrap();
        assert_eq!(
            &encoded[47..55],
            &window.map_or(0, NonZeroU64::get).to_be_bytes(),
        );
        let OperationBody::CreatePartition(decoded) =
            decode_operation_body(body.kind(), &encoded, OperationLimits::default()).unwrap()
        else {
            unreachable!()
        };
        assert_eq!(decoded, create);
        let mut old_layout = encoded;
        old_layout.drain(47..55);
        assert!(
            decode_operation_body(body.kind(), &old_layout, OperationLimits::default()).is_err()
        );
    }
}

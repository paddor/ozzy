use super::*;
use ozzy_journal::operation::{
    GroupDelete, GroupMember, GroupPlan, GroupTarget, validate_operation_body,
};
use ozzy_proto::{NodeId, group::Start};

fn fixture<'a>() -> OperationBody<'a> {
    let member = GroupMember {
        id: ConsumerMemberId::from_bytes([0xc1; 16]),
        node: NodeId::from_bytes([0xd1; 16]),
        join: OperationId::from_bytes([0xe1; 16]),
    };
    OperationBody::GroupPlan(GroupPlan {
        coordinator: PartitionIncarnation::from_bytes([0xa1; 16]),
        group: ConsumerGroupId::from_bytes([0xb1; 16]),
        name: "g",
        expected_revision: 0,
        revision: 1,
        initial: Start::Earliest,
        members: vec![member],
        targets: vec![GroupTarget {
            partition: PartitionIncarnation::from_bytes([0xa1; 16]),
            plan_revision: 1,
            member: Some(member),
            prior_member: None,
        }],
        operation_id: OperationId::from_bytes([0xf1; 16]),
    })
}

#[test]
fn coordinator_plan_has_independent_bytes_and_strict_framing() {
    let bytes = concat!(
        "02",
        "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        "b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1",
        "0000000167",
        "0000000000000000",
        "0000000000000001",
        "000000000000000000",
        "00000001",
        "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1",
        "d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1",
        "e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1",
        "00000001",
        "a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1",
        "0000000000000001",
        "01",
        "c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1c1",
        "d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1",
        "e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1",
        "00",
        "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1",
    );
    let bytes: Vec<_> = bytes
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    assert_eq!(bytes.len(), 209);
    let limits = OperationLimits::default();
    assert_eq!(encode_operation_body(&fixture(), limits).unwrap(), bytes);
    assert_eq!(
        decode_operation_body(OperationKind::GroupPlan, &bytes, limits).unwrap(),
        fixture()
    );
    validate_operation_body(OperationKind::GroupPlan, &bytes, limits).unwrap();
    for end in 0..bytes.len() {
        assert!(decode_operation_body(OperationKind::GroupPlan, &bytes[..end], limits).is_err());
        assert!(validate_operation_body(OperationKind::GroupPlan, &bytes[..end], limits).is_err());
    }
    for (index, value) in [(0, 0), (0, 1), (54, 3), (62, 1), (143, 2), (192, 2)] {
        let mut malformed = bytes.clone();
        malformed[index] = value;
        assert!(
            decode_operation_body(OperationKind::GroupPlan, &malformed, limits).is_err(),
            "byte {index} = {value}"
        );
    }
    let mut latest = bytes.clone();
    latest[54] = 1;
    let OperationBody::GroupPlan(decoded) =
        decode_operation_body(OperationKind::GroupPlan, &latest, limits).unwrap()
    else {
        unreachable!()
    };
    assert_eq!(decoded.initial, Start::Latest);
    let mut offset = bytes.clone();
    offset[54] = 2;
    offset[62] = 7;
    let OperationBody::GroupPlan(decoded) =
        decode_operation_body(OperationKind::GroupPlan, &offset, limits).unwrap()
    else {
        unreachable!()
    };
    assert_eq!(decoded.initial, Start::Offset(Offset::new(7)));
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(matches!(
        decode_operation_body(OperationKind::GroupPlan, &trailing, limits),
        Err(OperationCodecError::TrailingBytes(1))
    ));
    for limits in [
        OperationLimits {
            max_group_members: 0,
            ..limits
        },
        OperationLimits {
            max_group_partitions: 0,
            ..limits
        },
        OperationLimits {
            max_body_bytes: bytes.len() - 1,
            ..limits
        },
    ] {
        assert!(encode_operation_body(&fixture(), limits).is_err());
        assert!(decode_operation_body(OperationKind::GroupPlan, &bytes, limits).is_err());
    }
}

#[test]
fn group_deletion_has_independent_bytes_and_strict_framing() {
    let mut bytes = vec![1];
    bytes.extend([0xa1; 16]);
    bytes.extend([0xb1; 16]);
    bytes.extend(3_u64.to_be_bytes());
    bytes.extend([0xf1; 16]);
    let body = OperationBody::GroupDelete(GroupDelete {
        coordinator: PartitionIncarnation::from_bytes([0xa1; 16]),
        group: ConsumerGroupId::from_bytes([0xb1; 16]),
        revision: 3,
        operation_id: OperationId::from_bytes([0xf1; 16]),
    });
    let limits = OperationLimits::default();
    assert_eq!(body.kind(), OperationKind::GroupDelete);
    assert!(body.kind().has_operation_id());
    assert_eq!(
        body.operation_id(),
        Some(OperationId::from_bytes([0xf1; 16]))
    );
    assert_eq!(encode_operation_body(&body, limits).unwrap(), bytes);
    assert_eq!(
        decode_operation_body(OperationKind::GroupDelete, &bytes, limits).unwrap(),
        body
    );
    validate_operation_body(OperationKind::GroupDelete, &bytes, limits).unwrap();
    assert_eq!(
        OperationKind::try_from(13).unwrap(),
        OperationKind::GroupDelete
    );
    for end in 0..bytes.len() {
        assert!(decode_operation_body(OperationKind::GroupDelete, &bytes[..end], limits).is_err());
    }
    let mut format = bytes.clone();
    format[0] = 2;
    assert!(decode_operation_body(OperationKind::GroupDelete, &format, limits).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(matches!(
        decode_operation_body(OperationKind::GroupDelete, &trailing, limits),
        Err(OperationCodecError::TrailingBytes(1))
    ));
}

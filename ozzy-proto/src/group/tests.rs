use super::*;
use crate::{LinkSessionId, NodeId, RequestId, decode_packet};

fn envelope(opcode: Opcode) -> Envelope {
    Envelope {
        opcode,
        response: matches!(
            opcode,
            Opcode::GroupJoined
                | Opcode::GroupAssigned
                | Opcode::GroupInspected
                | Opcode::GroupDeleted
        ),
        request_id: Some(RequestId::from_bytes([10; 16])),
        sender: NodeId::from_bytes([11; 16]),
        session: Some(LinkSessionId::from_bytes([12; 16])),
    }
}

fn authority() -> Authority {
    Authority {
        group_id: GroupId::from_bytes([1; 16]),
        config_epoch: 2,
        view: 3,
    }
}

fn join() -> JoinRequest {
    JoinRequest {
        authority: authority(),
        coordinator: PartitionIncarnation::from_bytes([4; 16]),
        name: "workers".into(),
        member: ConsumerMemberId::from_bytes([6; 16]),
        join: OperationId::from_bytes([7; 16]),
        group: Some(ConsumerGroupId::from_bytes([5; 16])),
        mode: Mode::Join,
        initial: Start::Offset(Offset::new(9)),
        owned: vec![Owned {
            partition: PartitionIncarnation::from_bytes([8; 16]),
            plan_revision: 2,
            epoch: 5,
        }],
    }
}

fn joined() -> JoinReply {
    JoinReply {
        authority: authority(),
        coordinator: PartitionIncarnation::from_bytes([4; 16]),
        group: ConsumerGroupId::from_bytes([5; 16]),
        revision: 3,
        status: Status::Member,
        targets: vec![
            Target {
                partition: PartitionIncarnation::from_bytes([8; 16]),
                number: 0,
                plan_revision: 2,
                activatable: true,
                prior_member: None,
            },
            Target {
                partition: PartitionIncarnation::from_bytes([9; 16]),
                number: 3,
                plan_revision: 3,
                activatable: false,
                prior_member: Some(ConsumerMemberId::from_bytes([13; 16])),
            },
        ],
    }
}

fn assign() -> AssignRequest {
    AssignRequest {
        authority: authority(),
        partition: PartitionIncarnation::from_bytes([8; 16]),
        group: ConsumerGroupId::from_bytes([5; 16]),
        member: Some(ConsumerMemberId::from_bytes([6; 16])),
        plan_revision: 2,
        initial: Start::Latest,
        policy: Policy::QuorumDurable,
    }
}

fn assigned() -> AssignReply {
    AssignReply {
        authority: authority(),
        partition: PartitionIncarnation::from_bytes([8; 16]),
        group: ConsumerGroupId::from_bytes([5; 16]),
        member: Some(ConsumerMemberId::from_bytes([6; 16])),
        epoch: 4,
        plan_revision: 2,
        cursor: Offset::new(7),
        retained_from: Offset::new(1),
        end: Offset::new(9),
        policy: Policy::QuorumDurable,
    }
}

fn packet<'a>(header: &'a [u8; ENVELOPE_BYTES], metadata: &'a [u8]) -> Packet<'a> {
    decode_packet(&[header, metadata, &[]], EnvelopeLimits::default()).unwrap()
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one wire fixture covers exact bytes and malformed coordinates"
)]
fn membership_commands_round_trip_with_exact_sizes_and_strict_framing() {
    let limits = EnvelopeLimits::default();
    let mut metadata = Vec::with_capacity(4096);
    let header = encode_join(envelope(Opcode::GroupJoin), &join(), &mut metadata, limits).unwrap();
    assert_eq!(metadata.len(), JOIN_FIXED_BYTES + 7 + OWNED_BYTES);
    assert_eq!(metadata.len(), join_bytes("workers", 1).unwrap());
    assert_eq!(
        decode_join(packet(&header, &metadata), limits).unwrap(),
        join()
    );
    for end in 1..metadata.len() {
        let header = envelope(Opcode::GroupJoin)
            .encode_header(end, 0, limits)
            .unwrap();
        assert!(decode_join(packet(&header, &metadata[..end]), limits).is_err());
    }
    let mut trailing = metadata.clone();
    trailing.push(0);
    let header = envelope(Opcode::GroupJoin)
        .encode_header(trailing.len(), 0, limits)
        .unwrap();
    assert!(decode_join(packet(&header, &trailing), limits).is_err());
    // Offsets: tag 0, authority 1..33, coordinator 33..49, name length 49,
    // member 57..73, join 73..89, group 89..105, mode 105, start 106.
    let header = envelope(Opcode::GroupJoin)
        .encode_header(metadata.len(), 0, limits)
        .unwrap();
    for (index, value) in [(0, 0), (49, 0), (105, 3), (106, 3), (106, 0)] {
        let mut malformed = metadata.clone();
        malformed[index] = value;
        assert!(
            decode_join(packet(&header, &malformed[..metadata.len()]), limits).is_err(),
            "byte {index} = {value}"
        );
    }
    let fresh = JoinRequest {
        group: None,
        ..join()
    };
    let header = encode_join(envelope(Opcode::GroupJoin), &fresh, &mut metadata, limits).unwrap();
    assert_eq!(metadata[89..105], [0; 16]);
    assert_eq!(
        decode_join(packet(&header, &metadata), limits).unwrap(),
        fresh
    );
    let deleted = JoinReply {
        group: ConsumerGroupId::from_bytes([0; 16]),
        revision: 0,
        status: Status::Deleted,
        targets: Vec::new(),
        ..joined()
    };
    let header = encode_joined(
        envelope(Opcode::GroupJoined),
        &deleted,
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(
        decode_joined(packet(&header, &metadata), limits).unwrap(),
        deleted
    );
    let mut small = Vec::with_capacity(8);
    assert!(matches!(
        encode_join(envelope(Opcode::GroupJoin), &join(), &mut small, limits),
        Err(CodecError::Capacity)
    ));
    for invalid in [
        JoinRequest {
            name: String::new(),
            ..join()
        },
        JoinRequest {
            name: "x".repeat(256),
            ..join()
        },
        JoinRequest {
            member: ConsumerMemberId::from_bytes([0; 16]),
            ..join()
        },
        JoinRequest {
            owned: vec![Owned {
                epoch: 0,
                ..join().owned[0]
            }],
            ..join()
        },
    ] {
        assert!(encode_join(envelope(Opcode::GroupJoin), &invalid, &mut metadata, limits).is_err());
    }

    let header = encode_joined(
        envelope(Opcode::GroupJoined),
        &joined(),
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(metadata.len(), JOINED_FIXED_BYTES + 2 * TARGET_BYTES);
    assert_eq!(
        decode_joined(packet(&header, &metadata), limits).unwrap(),
        joined()
    );
    let joining = JoinReply {
        group: ConsumerGroupId::from_bytes([0; 16]),
        revision: 0,
        status: Status::Joining,
        targets: Vec::new(),
        ..joined()
    };
    let header = encode_joined(
        envelope(Opcode::GroupJoined),
        &joining,
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(metadata.len(), JOINED_FIXED_BYTES);
    assert_eq!(
        decode_joined(packet(&header, &metadata), limits).unwrap(),
        joining
    );
    for invalid in [
        JoinReply {
            status: Status::Joining,
            ..joined()
        },
        JoinReply {
            revision: 0,
            ..joined()
        },
        JoinReply {
            group: ConsumerGroupId::from_bytes([0; 16]),
            ..joined()
        },
        JoinReply {
            targets: vec![Target {
                plan_revision: 4,
                ..joined().targets[0]
            }],
            ..joined()
        },
    ] {
        assert!(
            encode_joined(
                envelope(Opcode::GroupJoined),
                &invalid,
                &mut metadata,
                limits
            )
            .is_err()
        );
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one wire fixture covers exact bytes and malformed coordinates"
)]
fn activation_commands_have_fixed_bytes_and_check_coordinates() {
    let limits = EnvelopeLimits::default();
    let mut metadata = Vec::with_capacity(256);
    let header = encode_assign(
        envelope(Opcode::GroupAssign),
        assign(),
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(metadata.len(), ASSIGN_BYTES);
    assert_eq!(
        decode_assign(packet(&header, &metadata), limits).unwrap(),
        assign()
    );
    for (index, value) in [(0, 2), (88, 0), (89, 3), (98, 9)] {
        let mut malformed = metadata.clone();
        malformed[index] = value;
        assert!(
            decode_assign(packet(&header, &malformed), limits).is_err(),
            "byte {index} = {value}"
        );
    }
    let mut initial = metadata.clone();
    initial[89] = 2;
    initial[97] = 5;
    assert_eq!(
        decode_assign(packet(&header, &initial), limits)
            .unwrap()
            .initial,
        Start::Offset(Offset::new(5))
    );
    for invalid in [
        AssignRequest {
            plan_revision: 0,
            ..assign()
        },
        AssignRequest {
            group: ConsumerGroupId::from_bytes([0; 16]),
            ..assign()
        },
    ] {
        assert!(
            encode_assign(
                envelope(Opcode::GroupAssign),
                invalid,
                &mut metadata,
                limits
            )
            .is_err()
        );
    }
    assert!(encode_assign(envelope(Opcode::GroupJoin), assign(), &mut metadata, limits).is_err());

    let header = encode_assigned(
        envelope(Opcode::GroupAssigned),
        assigned(),
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(metadata.len(), ASSIGNED_BYTES);
    assert_eq!(
        decode_assigned(packet(&header, &metadata), limits).unwrap(),
        assigned()
    );
    for invalid in [
        AssignReply {
            epoch: 0,
            ..assigned()
        },
        AssignReply {
            cursor: Offset::new(10),
            ..assigned()
        },
        AssignReply {
            retained_from: Offset::new(10),
            ..assigned()
        },
    ] {
        assert!(
            encode_assigned(
                envelope(Opcode::GroupAssigned),
                invalid,
                &mut metadata,
                limits
            )
            .is_err()
        );
    }
    let detail = InitialPosition {
        cursor: Offset::new(12),
        retained_from: Offset::new(1),
        end: Offset::new(9),
    };
    let bytes = detail.encode().unwrap();
    assert_eq!(InitialPosition::decode(&bytes).unwrap(), detail);
    assert!(InitialPosition::decode(&bytes[..23]).is_err());
    assert!(
        InitialPosition {
            cursor: Offset::new(5),
            ..detail
        }
        .encode()
        .is_err()
    );
}

#[test]
fn group_profile_requires_processing_and_reader_capabilities() {
    let limits = crate::append::DataLimits::default();
    let group = crate::handshake::Parameters::group(limits, crate::handshake::CONSUMER).unwrap();
    assert_eq!(
        group.required_capabilities
            & (crate::handshake::OWNER_GROUPS
                | crate::handshake::OWNER_PROGRESS
                | crate::handshake::OWNER_READ),
        crate::handshake::OWNER_GROUPS
            | crate::handshake::OWNER_PROGRESS
            | crate::handshake::OWNER_READ
    );
    let mut invalid = group;
    invalid.capabilities &= !crate::handshake::OWNER_PROGRESS;
    invalid.required_capabilities &= !crate::handshake::OWNER_PROGRESS;
    assert!(invalid.validate().is_err());
}

#[test]
fn revocations_name_no_member_and_round_trip() {
    let limits = EnvelopeLimits::default();
    let mut metadata = Vec::with_capacity(ASSIGNED_BYTES);
    let revoke = AssignRequest {
        member: None,
        plan_revision: u64::MAX,
        ..assign()
    };
    let header =
        encode_assign(envelope(Opcode::GroupAssign), revoke, &mut metadata, limits).unwrap();
    assert_eq!(metadata[65..81], [0; 16]);
    assert_eq!(
        decode_assign(packet(&header, &metadata), limits).unwrap(),
        revoke
    );
    let revoked = AssignReply {
        member: None,
        plan_revision: u64::MAX,
        ..assigned()
    };
    let header = encode_assigned(
        envelope(Opcode::GroupAssigned),
        revoked,
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(
        decode_assigned(packet(&header, &metadata), limits).unwrap(),
        revoked
    );
}

fn inspection() -> Inspection {
    Inspection {
        authority: authority(),
        coordinator: PartitionIncarnation::from_bytes([4; 16]),
        group: Some(ConsumerGroupId::from_bytes([5; 16])),
        revision: 3,
        initial: Start::Latest,
        members: vec![InspectedMember {
            id: ConsumerMemberId::from_bytes([6; 16]),
            node: NodeId::from_bytes([14; 16]),
            join: OperationId::from_bytes([7; 16]),
        }],
        targets: vec![
            InspectedTarget {
                partition: PartitionIncarnation::from_bytes([8; 16]),
                number: 0,
                plan_revision: 2,
                member: Some(ConsumerMemberId::from_bytes([6; 16])),
            },
            InspectedTarget {
                partition: PartitionIncarnation::from_bytes([9; 16]),
                number: 1,
                plan_revision: 3,
                member: None,
            },
        ],
    }
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one wire fixture covers both command pairs and their invalid shapes"
)]
fn inspection_and_deletion_round_trip_with_exact_sizes_and_strict_framing() {
    let limits = EnvelopeLimits::default();
    let mut metadata = Vec::with_capacity(4096);
    let request = InspectRequest {
        authority: authority(),
        coordinator: PartitionIncarnation::from_bytes([4; 16]),
        name: "workers".into(),
    };
    let header = encode_inspect(
        envelope(Opcode::GroupInspect),
        &request,
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(metadata.len(), inspect_bytes("workers").unwrap());
    assert_eq!(metadata.len(), INSPECT_FIXED_BYTES + 7);
    assert_eq!(
        decode_inspect(packet(&header, &metadata), limits).unwrap(),
        request
    );
    for end in 1..metadata.len() {
        let header = envelope(Opcode::GroupInspect)
            .encode_header(end, 0, limits)
            .unwrap();
        assert!(decode_inspect(packet(&header, &metadata[..end]), limits).is_err());
    }

    let header = encode_inspected(
        envelope(Opcode::GroupInspected),
        &inspection(),
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(
        metadata.len(),
        INSPECTED_FIXED_BYTES + INSPECTED_MEMBER_BYTES + 2 * INSPECTED_TARGET_BYTES
    );
    assert_eq!(metadata.len(), inspected_bytes(1, 2).unwrap());
    assert_eq!(
        decode_inspected(packet(&header, &metadata), limits).unwrap(),
        inspection()
    );
    for end in 1..metadata.len() {
        let header = envelope(Opcode::GroupInspected)
            .encode_header(end, 0, limits)
            .unwrap();
        assert!(decode_inspected(packet(&header, &metadata[..end]), limits).is_err());
    }
    let absent = Inspection {
        group: None,
        revision: 0,
        initial: Start::Earliest,
        members: Vec::new(),
        targets: Vec::new(),
        ..inspection()
    };
    let header = encode_inspected(
        envelope(Opcode::GroupInspected),
        &absent,
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(metadata.len(), INSPECTED_FIXED_BYTES);
    assert_eq!(
        decode_inspected(packet(&header, &metadata), limits).unwrap(),
        absent
    );
    for invalid in [
        Inspection {
            revision: 0,
            ..inspection()
        },
        Inspection {
            group: None,
            ..inspection()
        },
        Inspection {
            targets: vec![InspectedTarget {
                plan_revision: 4,
                ..inspection().targets[0]
            }],
            ..inspection()
        },
        Inspection {
            members: vec![InspectedMember {
                join: OperationId::from_bytes([0; 16]),
                ..inspection().members[0]
            }],
            ..inspection()
        },
    ] {
        assert!(
            encode_inspected(
                envelope(Opcode::GroupInspected),
                &invalid,
                &mut metadata,
                limits
            )
            .is_err()
        );
    }

    let request = DeleteRequest {
        authority: authority(),
        coordinator: PartitionIncarnation::from_bytes([4; 16]),
        group: ConsumerGroupId::from_bytes([5; 16]),
        name: "workers".into(),
    };
    let header = encode_delete(
        envelope(Opcode::GroupDelete),
        &request,
        &mut metadata,
        limits,
    )
    .unwrap();
    assert_eq!(metadata.len(), delete_bytes("workers").unwrap());
    assert_eq!(metadata.len(), DELETE_FIXED_BYTES + 7);
    assert_eq!(
        decode_delete(packet(&header, &metadata), limits).unwrap(),
        request
    );
    assert!(
        encode_delete(
            envelope(Opcode::GroupDelete),
            &DeleteRequest {
                group: ConsumerGroupId::from_bytes([0; 16]),
                ..request.clone()
            },
            &mut metadata,
            limits
        )
        .is_err()
    );
    for deleted in [true, false] {
        let reply = DeleteReply {
            authority: authority(),
            coordinator: PartitionIncarnation::from_bytes([4; 16]),
            group: ConsumerGroupId::from_bytes([5; 16]),
            deleted,
        };
        let header =
            encode_deleted(envelope(Opcode::GroupDeleted), reply, &mut metadata, limits).unwrap();
        assert_eq!(metadata.len(), DELETED_BYTES);
        assert_eq!(
            decode_deleted(packet(&header, &metadata), limits).unwrap(),
            reply
        );
        let mut malformed = metadata.clone();
        malformed[DELETED_BYTES - 1] = 2;
        assert!(decode_deleted(packet(&header, &malformed), limits).is_err());
    }
    assert!(
        encode_inspect(
            envelope(Opcode::GroupDelete),
            &InspectRequest {
                authority: authority(),
                coordinator: PartitionIncarnation::from_bytes([4; 16]),
                name: "workers".into(),
            },
            &mut metadata,
            limits
        )
        .is_err()
    );
}

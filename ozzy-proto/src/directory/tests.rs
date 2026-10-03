use super::*;
use crate::{LinkSessionId, decode_packet};

fn peer(value: u8) -> NodeId {
    NodeId::from_bytes([value; 16])
}

fn group(value: u8) -> GroupId {
    GroupId::from_bytes([value; 16])
}

fn route(value: u8) -> RouteState {
    RouteState {
        group: group(value),
        config_epoch: 6,
        partition: PartitionIncarnation::from_bytes([value + 20; 16]),
        members: [peer(1), peer(2), peer(3)].into(),
        view: 7,
        leader: Some(peer(2)),
    }
}

fn envelope(opcode: Opcode) -> Envelope {
    Envelope {
        opcode,
        response: opcode == Opcode::StateSnapshot,
        request_id: matches!(opcode, Opcode::StateSnapshotRequest | Opcode::StateSnapshot)
            .then(|| RequestId::from_bytes([3; 16])),
        sender: peer(9),
        session: Some(LinkSessionId::from_bytes([8; 16])),
    }
}

fn packet<'a>(header: &'a [u8; ENVELOPE_BYTES], metadata: &'a [u8]) -> Packet<'a> {
    decode_packet(&[header, metadata, &[]], EnvelopeLimits::default()).unwrap()
}

#[test]
fn watch_request_snapshot_update_and_resync_round_trip() {
    let frame = EnvelopeLimits::default();
    let limits = Limits::default();
    let watch = RequestId::from_bytes([4; 16]);
    let mut metadata = Vec::with_capacity(4096);
    let request = SnapshotRequest {
        watch,
        groups: vec![group(10), group(11)],
    };
    let header = encode_request(
        envelope(Opcode::StateSnapshotRequest),
        &request,
        &mut metadata,
        frame,
        limits,
    )
    .unwrap();
    assert_eq!(
        decode_request(packet(&header, &metadata), frame, limits),
        Ok(request)
    );

    let snapshot = Snapshot {
        watch,
        routes: vec![route(10), route(11)],
    };
    let header = encode_snapshot(
        envelope(Opcode::StateSnapshot),
        &snapshot,
        &mut metadata,
        frame,
        limits,
    )
    .unwrap();
    assert_eq!(
        decode_snapshot(packet(&header, &metadata), frame, limits),
        Ok(snapshot)
    );

    let update = Update {
        watch,
        route: route(10),
    };
    let header = encode_update(
        envelope(Opcode::StateUpdate),
        &update,
        &mut metadata,
        frame,
        limits,
    )
    .unwrap();
    assert_eq!(
        decode_update(packet(&header, &metadata), frame, limits),
        Ok(update)
    );

    let resync = Resync { watch };
    let header =
        encode_resync(envelope(Opcode::StateResync), resync, &mut metadata, frame).unwrap();
    assert_eq!(metadata.len(), 17);
    assert_eq!(decode_resync(packet(&header, &metadata), frame), Ok(resync));
}

#[test]
fn malformed_identity_length_and_direction_fail_closed() {
    let frame = EnvelopeLimits::default();
    let limits = Limits::default();
    let mut metadata = Vec::with_capacity(2048);
    let request = SnapshotRequest {
        watch: RequestId::from_bytes([4; 16]),
        groups: vec![group(10)],
    };
    let header = encode_request(
        envelope(Opcode::StateSnapshotRequest),
        &request,
        &mut metadata,
        frame,
        limits,
    )
    .unwrap();
    metadata[19..35].fill(0);
    assert_eq!(
        decode_request(packet(&header, &metadata), frame, limits),
        Err(CodecError::Profile)
    );
    metadata[19..35].fill(10);
    metadata[17..19].copy_from_slice(&2_u16.to_be_bytes());
    assert_eq!(
        decode_request(packet(&header, &metadata), frame, limits),
        Err(CodecError::Length)
    );
    let mut wrong = envelope(Opcode::StateSnapshotRequest);
    wrong.response = true;
    assert_eq!(
        encode_request(wrong, &request, &mut metadata, frame, limits),
        Err(CodecError::Command)
    );
    let mut invalid = route(10);
    invalid.members = [peer(1), peer(1), peer(3)].into();
    assert_eq!(
        encode_update(
            envelope(Opcode::StateUpdate),
            &Update {
                watch: request.watch,
                route: invalid,
            },
            &mut metadata,
            frame,
            limits,
        ),
        Err(CodecError::Profile)
    );
}

#[test]
fn output_capacity_failure_preserves_caller_bytes() {
    let mut metadata = Vec::with_capacity(2);
    metadata.extend_from_slice(&[0xaa, 0xbb]);
    let before = metadata.clone();
    assert_eq!(
        encode_request(
            envelope(Opcode::StateSnapshotRequest),
            &SnapshotRequest {
                watch: RequestId::from_bytes([4; 16]),
                groups: vec![group(10)],
            },
            &mut metadata,
            EnvelopeLimits::default(),
            Limits::default(),
        ),
        Err(CodecError::Capacity)
    );
    assert_eq!(metadata, before);
}

#[test]
fn zero_configuration_epoch_is_rejected_before_a_hint_is_exposed() {
    let frame = EnvelopeLimits::default();
    let limits = Limits::default();
    let mut metadata = Vec::with_capacity(2048);
    let mut update = Update {
        watch: RequestId::from_bytes([4; 16]),
        route: route(10),
    };
    let header = encode_update(
        envelope(Opcode::StateUpdate),
        &update,
        &mut metadata,
        frame,
        limits,
    )
    .unwrap();
    metadata[33..41].fill(0);
    assert_eq!(
        decode_update(packet(&header, &metadata), frame, limits),
        Err(CodecError::Profile)
    );
    update.route.config_epoch = 0;
    assert_eq!(
        encode_update(
            envelope(Opcode::StateUpdate),
            &update,
            &mut metadata,
            frame,
            limits,
        ),
        Err(CodecError::Profile)
    );
}

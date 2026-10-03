use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationKind, OperationLimits,
    canonical_body_digest, encode_operation_body,
};
use ozzy_proto::{GroupId, LinkSessionId, NodeId, OperationId, RequestId};
use ozzy_replication::flow::{Channel, FlowError, ReceiveEpoch, Receiver};
use ozzy_replication::wire::{
    self, FlowMessage, FlowProbe, FlowState, Operation, PeerBinding, Prepare, ReplicaMessage,
    WireError, WireLimits,
};
use ozzy_replication::{Configuration, Digest, PipelineLimits, Prefix};

fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}
fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [node(0), node(1), node(2)],
    )
    .unwrap()
}
fn session() -> LinkSessionId {
    LinkSessionId::from_bytes([9; 16])
}
fn epoch() -> ReceiveEpoch {
    ReceiveEpoch::new(20).unwrap()
}
fn binding(peer: u8) -> PeerBinding {
    PeerBinding::new(configuration(), node(peer), session()).unwrap()
}
fn probe() -> FlowProbe {
    FlowProbe {
        scope: configuration().scope(),
        request_id: RequestId::from_bytes([10; 16]),
        tail: Prefix::GENESIS,
        available: ozzy_replication::OpNumber(7),
    }
}
fn state() -> FlowState {
    FlowState {
        handle: 1,
        repair_limit: None,
        request_id: Some(probe().request_id),
        report: Receiver::new(
            Channel {
                scope: configuration().scope(),
                epoch: epoch(),
            },
            Prefix::GENESIS,
            PipelineLimits {
                max_operations: 4,
                max_body_bytes: 256,
            },
        )
        .unwrap()
        .report(),
    }
}

#[test]
fn flow_probe_has_golden_bytes_and_keeps_its_exact_correlation() {
    let mut metadata = [0xff; 144];
    let encoded = wire::encode_flow_probe(
        node(0),
        session(),
        probe(),
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    let mut golden = [0; 128];
    golden[..16].fill(7);
    golden[23] = 1;
    golden[32..48].fill(1);
    golden[48..80].fill(8);
    golden[127] = 7; // Available work, independently of the sent/repair tail.
    assert_eq!(encoded.metadata_bytes, 128);
    assert_eq!(&metadata[..128], &golden);
    assert_eq!(&metadata[128..], &[0xff; 16]);
    assert_eq!(encoded.header[5], 0x30);
    assert_eq!(&encoded.header[6..8], &[0, 0]);
    assert_eq!(&encoded.header[8..24], probe().request_id.as_bytes());
    assert_eq!(
        wire::decode(
            &[&encoded.header, &golden, &[]],
            binding(0),
            WireLimits::default()
        )
        .unwrap(),
        ReplicaMessage::Flow(FlowMessage::Probe(probe()))
    );
}

#[test]
fn flow_report_has_golden_bytes_and_supports_coalesced_notifications() {
    let mut metadata = [0xff; 212];
    let mut golden = [0; 204];
    golden[..16].fill(7);
    golden[23] = 1;
    golden[32..48].fill(2);
    golden[48..80].fill(8);
    golden[95] = 20; // Receive epoch, unrelated to writer generation.
    golden[103] = 1; // Report revision.
    golden[192..200].fill(0xff); // Unrestricted repair.
    for request_id in [Some(probe().request_id), None] {
        let state = FlowState {
            handle: 1,
            repair_limit: None,
            request_id,
            ..state()
        };
        let encoded = wire::encode_flow_state(
            node(1),
            session(),
            state,
            &mut metadata,
            WireLimits::default(),
        )
        .unwrap();
        golden[203] = 1;
        assert_eq!(encoded.metadata_bytes, 204);
        assert_eq!(&metadata[..204], &golden);
        assert_eq!(&metadata[204..], &[0xff; 8]);
        assert_eq!(encoded.header[5], 0x31);
        assert_eq!(encoded.header[7], u8::from(request_id.is_some()));
        assert_eq!(
            wire::decode(
                &[&encoded.header, &golden, &[]],
                binding(1),
                WireLimits::default()
            )
            .unwrap(),
            ReplicaMessage::Flow(FlowMessage::State(state))
        );
    }
}

#[test]
fn only_the_matching_live_probe_can_authorize_opening_a_reported_epoch() {
    let state = state();
    state.validate_response(probe()).unwrap();
    let mut replaced = probe();
    replaced.request_id = RequestId::from_bytes([11; 16]);
    assert_eq!(
        state.validate_response(replaced),
        Err(WireError::Correlation)
    );
    replaced = probe();
    replaced.scope.view += 1;
    assert_eq!(
        state.validate_response(replaced),
        Err(WireError::Correlation)
    );
    assert_eq!(
        FlowState {
            handle: 1,
            repair_limit: None,
            request_id: None,
            ..state
        }
        .validate_response(probe()),
        Err(WireError::Correlation)
    );
}

fn bodies() -> Vec<Vec<u8>> {
    [12, 13]
        .map(|value| {
            encode_operation_body(
                &OperationBody::Barrier(Barrier {
                    operation_id: OperationId::from_bytes([value; 16]),
                }),
                OperationLimits::default(),
            )
            .unwrap()
        })
        .into()
}
fn operations(bodies: &[Vec<u8>]) -> Vec<Operation<'_>> {
    let mut previous = Prefix::GENESIS;
    bodies
        .iter()
        .map(|body| {
            let operation = Operation::from_verified(
                CanonicalOperation {
                    group_id: configuration().scope().group_id,
                    configuration_epoch: 1,
                    original_view: 0,
                    op_number: previous.op.0 + 1,
                    previous_digest: previous.digest,
                    kind: OperationKind::Barrier,
                    body,
                },
                canonical_body_digest(body),
            );
            previous = operation.prefix();
            operation
        })
        .collect()
}

#[test]
fn credited_prepare_reuses_canonical_bytes_and_adds_one_epoch_per_batch() {
    let bodies = bodies();
    let ops = operations(&bodies);
    let prepare = Prepare {
        scope: configuration().scope(),
        committed: Prefix::GENESIS,
        operations: &ops,
    };
    let mut metadata = [0xff; 512];
    let mut payload = [0xff; 64];
    let encoded = wire::encode_flow_prepare(
        node(0),
        session(),
        epoch(),
        prepare,
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    assert_eq!(encoded.header[5], 0x51);
    assert_eq!(encoded.metadata_bytes, 180 + 2 * 86);
    assert_eq!(&metadata[80..96], &epoch().get().to_be_bytes());
    let ReplicaMessage::Flow(FlowMessage::Prepare {
        epoch: decoded_epoch,
        batch,
    }) = wire::decode(
        &[
            &encoded.header,
            &metadata[..encoded.metadata_bytes],
            &payload[..encoded.payload_bytes],
        ],
        binding(0).with_receive_epoch(epoch()),
        WireLimits::default(),
    )
    .unwrap()
    else {
        panic!("not an epoch-bound PREPARE");
    };
    assert_eq!(decoded_epoch, epoch());
    assert_eq!(batch.end(), ops[1].prefix());
    assert_eq!(batch.operations().collect::<Vec<_>>(), ops);
    assert_eq!(
        batch.operations().next().unwrap().canonical().body.as_ptr(),
        payload.as_ptr()
    );
}

#[test]
fn wrong_epoch_is_rejected_before_body_hashing_and_legacy_cannot_bypass_credit() {
    let bodies = bodies();
    let ops = operations(&bodies);
    let prepare = Prepare {
        scope: configuration().scope(),
        committed: Prefix::GENESIS,
        operations: &ops,
    };
    let mut metadata = [0xff; 512];
    let mut payload = [0xff; 64];
    let encoded = wire::encode_flow_prepare(
        node(0),
        session(),
        epoch(),
        prepare,
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    payload[0] ^= 1;
    for peer in [
        binding(0),
        binding(0).with_receive_epoch(ReceiveEpoch::new(21).unwrap()),
    ] {
        assert_eq!(
            wire::decode(
                &[
                    &encoded.header,
                    &metadata[..encoded.metadata_bytes],
                    &payload[..encoded.payload_bytes]
                ],
                peer,
                WireLimits::default()
            ),
            Err(WireError::Flow(FlowError::Channel))
        );
    }
    assert_eq!(
        wire::decode(
            &[
                &encoded.header,
                &metadata[..encoded.metadata_bytes],
                &payload[..encoded.payload_bytes]
            ],
            binding(0).with_receive_epoch(epoch()),
            WireLimits::default()
        ),
        Err(WireError::Digest)
    );
    let legacy = wire::encode_prepare(
        node(0),
        session(),
        prepare,
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    assert_eq!(
        wire::decode(
            &[
                &legacy.header,
                &metadata[..legacy.metadata_bytes],
                &payload[..legacy.payload_bytes]
            ],
            binding(0).with_receive_epoch(epoch()),
            WireLimits::default()
        ),
        Err(WireError::Flow(FlowError::Channel))
    );
}

#[test]
fn invalid_flow_encodings_leave_reusable_output_unchanged() {
    let mut output = [0xff; 200];
    let mut invalid = state();
    invalid.report.revision = 0;
    assert!(
        wire::encode_flow_state(
            node(1),
            session(),
            invalid,
            &mut output,
            WireLimits::default()
        )
        .is_err()
    );
    assert_eq!(output, [0xff; 200]);
    let limits = WireLimits {
        envelope: ozzy_proto::EnvelopeLimits {
            max_metadata_bytes: 100,
            max_payload_bytes: 0,
        },
        ..WireLimits::default()
    };
    assert!(wire::encode_flow_probe(node(0), session(), probe(), &mut output, limits).is_err());
    assert!(wire::encode_flow_state(node(1), session(), state(), &mut output, limits).is_err());
    assert_eq!(output, [0xff; 200]);
}

#[test]
fn credit_window_is_not_limited_to_one_wire_payload() {
    let mut output = [0; 204];
    let limits = WireLimits {
        envelope: ozzy_proto::EnvelopeLimits {
            max_metadata_bytes: 204,
            max_payload_bytes: 1,
        },
        max_operations: 1,
    };
    let encoded =
        wire::encode_flow_state(node(1), session(), state(), &mut output, limits).unwrap();
    assert_eq!(
        wire::decode(&[&encoded.header, &output, &[]], binding(1), limits).unwrap(),
        ReplicaMessage::Flow(FlowMessage::State(state()))
    );
}

fn header_lengths(mut header: [u8; 64], metadata: usize, payload: usize) -> [u8; 64] {
    header[56..60].copy_from_slice(&u32::try_from(metadata).unwrap().to_be_bytes());
    header[60..64].copy_from_slice(&u32::try_from(payload).unwrap().to_be_bytes());
    header
}

#[test]
fn flow_controls_reject_truncation_extensions_payload_and_bad_correlation() {
    for is_state in [false, true] {
        let mut metadata = [0; 221];
        let (encoded, peer) = if is_state {
            (
                wire::encode_flow_state(
                    node(1),
                    session(),
                    state(),
                    &mut metadata,
                    WireLimits::default(),
                )
                .unwrap(),
                binding(1),
            )
        } else {
            (
                wire::encode_flow_probe(
                    node(0),
                    session(),
                    probe(),
                    &mut metadata,
                    WireLimits::default(),
                )
                .unwrap(),
                binding(0),
            )
        };
        for length in 0..=encoded.metadata_bytes + 1 {
            if length == encoded.metadata_bytes {
                continue;
            }
            let header = header_lengths(encoded.header, length, 0);
            assert!(
                wire::decode(
                    &[&header, &metadata[..length], &[]],
                    peer,
                    WireLimits::default()
                )
                .is_err()
            );
        }
        let bytes = &metadata[..encoded.metadata_bytes];
        let header = header_lengths(encoded.header, bytes.len(), 1);
        assert_eq!(
            wire::decode(&[&header, bytes, &[1]], peer, WireLimits::default()),
            Err(WireError::Payload)
        );
        let mut header = encoded.header;
        header[7] ^= 1;
        assert_eq!(
            wire::decode(&[&header, bytes, &[]], peer, WireLimits::default()),
            Err(WireError::Correlation)
        );
        header = encoded.header;
        header[8..24].fill(0);
        assert!(wire::decode(&[&header, bytes, &[]], peer, WireLimits::default()).is_err());
        header = encoded.header;
        header[40..56].fill(17);
        assert_eq!(
            wire::decode(&[&header, bytes, &[]], peer, WireLimits::default()),
            Err(WireError::Peer)
        );
        header = encoded.header;
        header[24..40].fill(17);
        assert_eq!(
            wire::decode(&[&header, bytes, &[]], peer, WireLimits::default()),
            Err(WireError::Peer)
        );
    }
}

#[test]
fn flow_report_rejects_impossible_credit_receipt_and_epoch_fields() {
    let mut metadata = [0; 204];
    let encoded = wire::encode_flow_state(
        node(1),
        session(),
        state(),
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    for (start, end) in [(80, 96), (96, 104), (200, 204)] {
        let mut invalid = metadata;
        invalid[start..end].fill(0); // Epoch/revision cannot be zero.
        assert!(
            wire::decode(
                &[&encoded.header, &invalid, &[]],
                binding(1),
                WireLimits::default()
            )
            .is_err()
        );
    }
    let mut invalid = metadata;
    invalid[191] = 1; // Nonzero cumulative bytes with genesis receipt.
    assert!(
        wire::decode(
            &[&encoded.header, &invalid, &[]],
            binding(1),
            WireLimits::default()
        )
        .is_err()
    );
    invalid = metadata;
    invalid[183] = 1; // Nonzero digest at genesis.
    assert_eq!(
        wire::decode(
            &[&encoded.header, &invalid, &[]],
            binding(1),
            WireLimits::default()
        ),
        Err(WireError::Prefix)
    );
}

#[test]
fn credited_prepare_rejects_truncated_extended_and_empty_batches_atomically() {
    let bodies = bodies();
    let ops = operations(&bodies);
    let prepare = Prepare {
        scope: configuration().scope(),
        committed: Prefix::GENESIS,
        operations: &ops,
    };
    let mut metadata = [0xff; 512];
    let mut payload = [0xff; 64];
    let encoded = wire::encode_flow_prepare(
        node(0),
        session(),
        epoch(),
        prepare,
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    for length in 0..=encoded.metadata_bytes + 1 {
        if length == encoded.metadata_bytes {
            continue;
        }
        let header = header_lengths(encoded.header, length, encoded.payload_bytes);
        assert!(
            wire::decode(
                &[
                    &header,
                    &metadata[..length],
                    &payload[..encoded.payload_bytes]
                ],
                binding(0).with_receive_epoch(epoch()),
                WireLimits::default()
            )
            .is_err()
        );
    }
    let before = (metadata, payload);
    assert!(
        wire::encode_flow_prepare(
            node(0),
            session(),
            epoch(),
            Prepare {
                operations: &[],
                ..prepare
            },
            &mut metadata,
            &mut payload,
            WireLimits::default()
        )
        .is_err()
    );
    assert_eq!((metadata, payload), before);
    assert!(
        wire::encode_flow_prepare(
            node(0),
            session(),
            epoch(),
            prepare,
            &mut metadata[..100],
            &mut payload,
            WireLimits::default()
        )
        .is_err()
    );
    assert_eq!((metadata, payload), before);
}

#[test]
fn held_publication_repair_limit_roundtrips_including_genesis() {
    for limit in [0, 1, 123] {
        let mut state = state();
        state.repair_limit = Some(ozzy_replication::OpNumber(limit));
        let mut metadata = [0; 204];
        let encoded = wire::encode_flow_state(
            node(1),
            session(),
            state,
            &mut metadata,
            WireLimits::default(),
        )
        .unwrap();
        assert_eq!(&metadata[192..200], &limit.to_be_bytes());
        assert_eq!(
            wire::decode(
                &[&encoded.header, &metadata, &[]],
                binding(1),
                WireLimits::default()
            )
            .unwrap(),
            ReplicaMessage::Flow(FlowMessage::State(state))
        );
    }
}

#[test]
fn compact_receipt_is_exactly_29_bytes_and_keeps_bound_history() {
    let bound = state().report;
    let compact = wire::CompactState::from_report(0x0102_0304, bound).unwrap();
    let bytes = compact.encode().unwrap();
    assert_eq!(bytes.len(), 29);
    assert_eq!(&bytes[..5], &[0x54, 1, 2, 3, 4]);
    assert_eq!(wire::CompactState::decode(&bytes).unwrap(), compact);
    assert_eq!(compact.report(bound, bound.received).unwrap(), bound);
    for end in 0..29 {
        assert!(wire::CompactState::decode(&bytes[..end]).is_err());
    }
    let mut extended = bytes.to_vec();
    extended.push(0);
    assert!(wire::CompactState::decode(&extended).is_err());
    for range in [1..5, 5..13] {
        let mut invalid = bytes;
        invalid[range].fill(0);
        assert!(wire::CompactState::decode(&invalid).is_err());
    }
    let mut ahead = compact;
    ahead.received = ozzy_replication::OpNumber(1);
    assert!(ahead.report(bound, bound.received).is_err());
}

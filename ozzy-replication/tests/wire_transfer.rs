use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
    encode_operation_body,
};
use ozzy_proto::OperationId;
use ozzy_proto::{GroupId, LinkSessionId, NodeId, RequestId};
use ozzy_replication::wire::{
    FetchOps, Operation, PeerBinding, ReplicaMessage, WireError, WireLimits, decode, encode_fetch,
    encode_ops,
};
use ozzy_replication::{Configuration, Digest, JournalGeneration, LogSource, OpNumber, Prefix};

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

fn request() -> FetchOps {
    let mut scope = configuration().scope();
    scope.view = 2;
    FetchOps {
        scope,
        request_id: RequestId::from_bytes([10; 16]),
        source: LogSource {
            voter: node(1),
            generation: JournalGeneration(20),
            accepted: Prefix {
                op: OpNumber(3),
                digest: Digest::from_bytes([11; 32]),
            },
        },
        predecessor: Prefix::GENESIS,
        max_operations: 2,
        max_body_bytes: 1024,
    }
}

#[test]
fn fetch_names_exact_frozen_history_and_bounded_correlated_range() {
    let request = request();
    let mut metadata = [0xff; 208];
    let encoded = encode_fetch(
        node(2),
        session(),
        request,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    let mut golden = [0; 200];
    golden[..16].fill(7);
    golden[23] = 1;
    golden[31] = 2;
    golden[32..48].fill(3);
    golden[48..80].fill(8);
    golden[80..96].fill(2); // Selected source, distinct from request sender.
    golden[111] = 20;
    golden[119] = 3;
    golden[120..152].fill(11);
    golden[195] = 2;
    golden[198] = 4;
    assert_eq!(encoded.metadata_bytes, 200);
    assert_eq!(&metadata[..200], &golden);
    assert_eq!(&metadata[200..], &[0xff; 8]);
    assert_eq!(encoded.header[5], 0x3a);
    assert_eq!(&encoded.header[6..8], &[0, 0]);
    assert_eq!(&encoded.header[8..24], &[10; 16]);
    assert_eq!(
        decode(
            &[&encoded.header, &golden, &[]],
            PeerBinding::new(configuration(), node(2), session()).unwrap(),
            WireLimits::default()
        )
        .unwrap(),
        ReplicaMessage::FetchOps(request),
    );
}

fn bodies() -> Vec<Vec<u8>> {
    [12, 13, 14]
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
        .enumerate()
        .map(|(index, bytes)| {
            let operation = Operation::from_verified(
                CanonicalOperation {
                    group_id: configuration().scope().group_id,
                    configuration_epoch: 1,
                    original_view: u64::from(index > 0),
                    op_number: index as u64 + 1,
                    previous_digest: previous.digest,
                    kind: ozzy_journal::operation::OperationKind::Barrier,
                    body: bytes,
                },
                canonical_body_digest(bytes),
            );
            previous = operation.prefix();
            operation
        })
        .collect()
}

#[test]
fn dispatch_fence_checks_exact_history_metadata_without_hashing_payload() {
    use ozzy_proto::{EnvelopeLimits, decode_packet};
    use ozzy_replication::wire::ReceiveFence;

    let bodies = bodies();
    let operations = operations(&bodies);
    let mut request = request();
    request.source.accepted = operations[2].prefix();
    let mut metadata = [0; 512];
    let mut payload = [0; 64];
    let encoded = encode_ops(
        node(1),
        session(),
        request,
        &operations[..2],
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    // Dispatch handles fixed routing metadata. The actor must still reject the
    // corrupt body through its complete decoder before accepting any history.
    payload[0] ^= 1;
    let frames: [&[u8]; 3] = [
        &encoded.header,
        &metadata[..encoded.metadata_bytes],
        &payload[..encoded.payload_bytes],
    ];
    let packet = decode_packet(&frames, EnvelopeLimits::default()).unwrap();
    ReceiveFence::History(request)
        .validate_routing(packet, EnvelopeLimits::default())
        .unwrap();
    assert!(
        decode(
            &frames,
            PeerBinding::new(configuration(), node(1), session()).unwrap(),
            WireLimits::default()
        )
        .is_err()
    );
    let mut stale = [request; 7];
    stale[0].scope.view += 1;
    stale[1].request_id = RequestId::from_bytes([12; 16]);
    stale[2].source.generation = JournalGeneration(21);
    stale[3].source.accepted.digest = Digest::from_bytes([12; 32]);
    stale[4].predecessor = operations[0].prefix();
    stale[5].max_operations = 1;
    stale[6].max_body_bytes = encoded.payload_bytes as u32 - 1;
    for request in stale {
        assert!(
            ReceiveFence::History(request)
                .validate_routing(packet, EnvelopeLimits::default())
                .is_err()
        );
    }
}

#[test]
fn ops_transfers_borrowed_consecutive_chunks_bound_to_the_request_and_source() {
    let bodies = bodies();
    let operations = operations(&bodies);
    let mut request = request();
    request.source.accepted = operations[2].prefix();
    let mut metadata = [0xff; 512];
    let mut payload = [0xff; 64];
    let first = encode_ops(
        node(1),
        session(),
        request,
        &operations[..2],
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    assert_eq!(first.metadata_bytes, 368); // 196 fixed bytes + two 86-byte descriptors.
    assert_eq!(first.payload_bytes, 32);
    assert_eq!(first.header[5], 0x3b);
    assert_eq!(&first.header[6..8], &[0, 1]);
    assert_eq!(&first.header[8..24], request.request_id.as_bytes());
    assert_eq!(&metadata[80..96], node(1).as_bytes());
    assert_eq!(&metadata[96..112], &20_u128.to_be_bytes());
    assert_eq!(&metadata[112..120], &3_u64.to_be_bytes());
    assert_eq!(
        &metadata[120..152],
        operations[2].prefix().digest.as_bytes()
    );
    assert_eq!(&metadata[152..192], &[0; 40]);
    assert_eq!(&metadata[192..196], &2_u32.to_be_bytes());
    let binding = PeerBinding::new(configuration(), node(1), session()).unwrap();
    let ReplicaMessage::Ops(first) = decode(
        &[&first.header, &metadata[..368], &payload[..32]],
        binding,
        WireLimits::default(),
    )
    .unwrap() else {
        panic!("ops");
    };
    first.validate_response(request).unwrap();
    assert_eq!(first.end(), operations[1].prefix());
    assert_eq!(first.source(), request.source);
    assert_eq!(first.operations().collect::<Vec<_>>(), &operations[..2]);
    assert_eq!(
        first.operations().next().unwrap().canonical().body.as_ptr(),
        payload.as_ptr()
    );
    request.predecessor = first.end();
    request.request_id = RequestId::from_bytes([15; 16]);
    let last = encode_ops(
        node(1),
        session(),
        request,
        &operations[2..],
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    let ReplicaMessage::Ops(last) = decode(
        &[
            &last.header,
            &metadata[..last.metadata_bytes],
            &payload[..last.payload_bytes],
        ],
        binding,
        WireLimits::default(),
    )
    .unwrap() else {
        panic!("ops");
    };
    last.validate_response(request).unwrap();
    assert_eq!(last.end(), request.source.accepted);
    assert_eq!(last.operations().len(), 1);
    assert_eq!(
        last.operations().next().unwrap().canonical().original_view,
        1
    );
}

#[test]
fn delayed_or_oversized_ops_cannot_match_a_replaced_request() {
    let bodies = bodies();
    let operations = operations(&bodies);
    let mut original = request();
    original.source.accepted = operations[2].prefix();
    let mut metadata = [0; 368];
    let mut payload = [0; 32];
    let encoded = encode_ops(
        node(1),
        session(),
        original,
        &operations[..2],
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    let binding = PeerBinding::new(configuration(), node(1), session()).unwrap();
    let ReplicaMessage::Ops(batch) = decode(
        &[&encoded.header, &metadata, &payload],
        binding,
        WireLimits::default(),
    )
    .unwrap() else {
        panic!("ops");
    };
    for field in 0..7 {
        let mut next = original;
        match field {
            0 => next.request_id = RequestId::from_bytes([99; 16]),
            1 => next.scope.view += 1,
            2 => next.source.generation.0 += 1,
            3 => next.source.voter = node(0),
            4 => next.predecessor = operations[0].prefix(),
            5 => next.max_operations = 1,
            6 => next.max_body_bytes = 31,
            _ => unreachable!(),
        }
        assert_eq!(
            batch.validate_response(next),
            Err(WireError::Transfer),
            "field {field}"
        );
    }
    let new_session = LinkSessionId::from_bytes([16; 16]);
    assert_eq!(
        decode(
            &[&encoded.header, &metadata, &payload],
            PeerBinding::new(configuration(), node(1), new_session).unwrap(),
            WireLimits::default()
        )
        .map(|_| ()),
        Err(WireError::Peer)
    );
    batch.validate_response(original).unwrap(); // Rejection did not mutate the request or batch.
}

#[test]
fn fetch_rejects_malformed_ranges_without_allocating_from_requested_limits() {
    let original = request();
    let mut metadata = [0; 201];
    let encoded = encode_fetch(
        node(2),
        session(),
        original,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    let binding = PeerBinding::new(configuration(), node(2), session()).unwrap();
    let decode_request = |header: &[u8], bytes: &[u8], payload: &[u8]| {
        decode(&[header, bytes, payload], binding, WireLimits::default()).map(|_| ())
    };
    for length in 0..200 {
        let mut header = encoded.header;
        header[56..60].copy_from_slice(&(length as u32).to_be_bytes());
        assert!(decode_request(&header, &metadata[..length], &[]).is_err());
    }
    for (offset, length) in [(96, 16), (192, 4), (196, 4)] {
        let mut invalid = metadata;
        invalid[offset..offset + length].fill(0);
        assert!(decode_request(&encoded.header, &invalid[..200], &[]).is_err());
    }
    let mut invalid = metadata;
    invalid[152..192].copy_from_slice(&metadata[112..152]); // Already at source tail.
    assert_eq!(
        decode_request(&encoded.header, &invalid[..200], &[]),
        Err(WireError::History)
    );
    let mut invalid = metadata;
    invalid[80..96].fill(99); // Source is not a configured voter.
    assert!(decode_request(&encoded.header, &invalid[..200], &[]).is_err());
    let mut header = encoded.header;
    header[7] = 1;
    assert_eq!(
        decode_request(&header, &metadata[..200], &[]),
        Err(WireError::Correlation)
    );
    header = encoded.header;
    header[8..24].fill(0);
    assert_eq!(
        decode_request(&header, &metadata[..200], &[]),
        Err(WireError::Correlation)
    );
    header = encoded.header;
    header[63] = 1;
    assert_eq!(
        decode_request(&header, &metadata[..200], &[1]),
        Err(WireError::Payload)
    );
    header = encoded.header;
    header[59] = 201;
    assert_eq!(
        decode_request(&header, &metadata, &[]),
        Err(WireError::Length)
    );
    let large = FetchOps {
        max_operations: u32::MAX,
        max_body_bytes: u32::MAX,
        ..original
    };
    let encoded = encode_fetch(
        node(2),
        session(),
        large,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    // The request remains a fixed descriptor, not a count-sized vector.
    assert!(decode_request(&encoded.header, &metadata[..200], &[]).is_ok());
}

#[test]
fn ops_rejects_bad_chains_and_preserves_output_on_capacity_or_bound_errors() {
    let bodies = bodies();
    let operations = operations(&bodies);
    let mut request = request();
    request.source.accepted = operations[2].prefix();
    let mut metadata = [0xaa; 368];
    let mut payload = [0xbb; 32];
    for (metadata_size, payload_size, bound) in [(367, 32, 1024), (368, 31, 1024), (368, 32, 31)] {
        let request = FetchOps {
            max_body_bytes: bound,
            ..request
        };
        assert!(
            encode_ops(
                node(1),
                session(),
                request,
                &operations[..2],
                &mut metadata[..metadata_size],
                &mut payload[..payload_size],
                WireLimits::default()
            )
            .is_err()
        );
        assert_eq!(metadata, [0xaa; 368]);
        assert_eq!(payload, [0xbb; 32]);
    }
    let encoded = encode_ops(
        node(1),
        session(),
        request,
        &operations[..2],
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    let binding = PeerBinding::new(configuration(), node(1), session()).unwrap();
    for offset in [195, 203, 217, 246, 280] {
        // Count, op, body length, body digest, chain digest.
        let mut invalid = metadata;
        invalid[offset] ^= 1;
        assert!(
            decode(
                &[&encoded.header, &invalid, &payload],
                binding,
                WireLimits::default()
            )
            .is_err(),
            "offset {offset}"
        );
    }
    let mut invalid = metadata;
    invalid[80..96].copy_from_slice(node(0).as_bytes());
    assert_eq!(
        decode(
            &[&encoded.header, &invalid, &payload],
            binding,
            WireLimits::default()
        )
        .map(|_| ()),
        Err(WireError::Peer)
    );
    let mut invalid = payload;
    invalid[0] ^= 1;
    assert_eq!(
        decode(
            &[&encoded.header, &metadata, &invalid],
            binding,
            WireLimits::default()
        )
        .map(|_| ()),
        Err(WireError::Digest)
    );
    let mut header = encoded.header;
    header[7] = 0;
    assert_eq!(
        decode(
            &[&header, &metadata, &payload],
            binding,
            WireLimits::default()
        )
        .map(|_| ()),
        Err(WireError::Correlation)
    );
    // A valid chunk that claims to finish a different selected tail still fails.
    let mut invalid = metadata;
    invalid[112..120].copy_from_slice(&2_u64.to_be_bytes());
    assert_eq!(
        decode(
            &[&encoded.header, &invalid, &payload],
            binding,
            WireLimits::default()
        )
        .map(|_| ()),
        Err(WireError::History)
    );
}

use ozzy_journal::operation::{
    Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
    encode_operation_body,
};
use ozzy_proto::OperationId;
use ozzy_proto::{GroupId, LinkSessionId, NodeId};
use ozzy_replication::wire::Grant;
use ozzy_replication::wire::{
    Control, PeerBinding, ReplicaMessage, WireLimits, decode, encode_control,
};
use ozzy_replication::{
    Admission, JournalGeneration, NormalReplica, PipelineLimits, PreparedOperation,
};
use ozzy_replication::{Commit, Configuration, Digest, Prefix};

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

#[test]
fn replica_routing_uses_fixed_scope_without_accepting_its_history() {
    use ozzy_proto::{EnvelopeLimits, Packet, decode_packet};
    use ozzy_replication::wire::route;
    let configuration = configuration();
    let mut scope = configuration.scope();
    scope.view = 7;
    let message = Control::Commit(Commit {
        scope,
        committed: Prefix::GENESIS,
    });
    let mut metadata = [0; 120];
    let encoded = encode_control(node(1), session(), message, &mut metadata).unwrap();
    let limits = EnvelopeLimits::default();
    let packet = decode_packet(&[&encoded.header, &metadata, &[]], limits).unwrap();
    assert_eq!(route(packet, limits).unwrap(), scope);
    for length in 0..80 {
        assert!(
            route(
                Packet {
                    metadata: &metadata[..length],
                    ..packet
                },
                limits
            )
            .is_err()
        );
    }
    let mut invalid = metadata;
    invalid[80..88].copy_from_slice(&1_u64.to_be_bytes());
    assert_eq!(
        route(
            Packet {
                metadata: &invalid,
                ..packet
            },
            limits
        )
        .unwrap(),
        scope
    );
    let binding = PeerBinding::new(configuration, node(1), session()).unwrap();
    assert!(
        decode(
            &[&encoded.header, &invalid, &[]],
            binding,
            WireLimits::default()
        )
        .is_err()
    );
    invalid = metadata;
    invalid[32..48].copy_from_slice(node(2).as_bytes());
    assert!(
        route(
            Packet {
                metadata: &invalid,
                ..packet
            },
            limits
        )
        .is_err()
    );
    for range in [0..16, 16..24, 48..80] {
        let mut invalid = metadata;
        invalid[range].fill(0);
        assert!(
            route(
                Packet {
                    metadata: &invalid,
                    ..packet
                },
                limits
            )
            .is_err()
        );
    }
}

#[test]
fn commit_uses_exact_scope_voter_and_prefix_bytes() {
    let mut scope = configuration().scope();
    scope.view = 2;
    let message = Control::Commit(Commit {
        scope,
        committed: Prefix::GENESIS,
    });
    let mut metadata = [0xff; 160];
    let encoded = encode_control(node(2), session(), message, &mut metadata).unwrap();
    // Independent fixed-field fixture. No native Rust layout or file padding.
    let mut golden = [0; 120];
    golden[..16].fill(7);
    golden[23] = 1;
    golden[31] = 2;
    golden[32..48].fill(3);
    golden[48..80].fill(8);
    assert_eq!(encoded.metadata_bytes, golden.len());
    assert_eq!(&metadata[..120], &golden);
    assert!(metadata[120..].iter().all(|&byte| byte == 0xff));
    assert_eq!(encoded.header[5], 0x34);
    let peer = PeerBinding::new(configuration(), node(2), session()).unwrap();
    assert_eq!(
        decode(
            &[&encoded.header, &golden, &[]],
            peer,
            WireLimits::default()
        )
        .unwrap(),
        ReplicaMessage::Control(message)
    );
}

#[test]
fn decoded_durable_ack_still_waits_for_the_primary_disk_boundary() {
    let scope = configuration().scope();
    let limits = PipelineLimits {
        max_operations: 2,
        max_body_bytes: 1024,
    };
    let mut primary =
        NormalReplica::bootstrap(configuration(), node(0), JournalGeneration(1), limits).unwrap();
    let mut backup =
        NormalReplica::bootstrap(configuration(), node(1), JournalGeneration(2), limits).unwrap();
    let body = OperationBody::Barrier(Barrier {
        operation_id: OperationId::from_bytes([10; 16]),
    });
    let bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
    let operation = CanonicalOperation {
        group_id: scope.group_id,
        configuration_epoch: scope.configuration_epoch,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: body.kind(),
        body: &bytes,
    };
    let prepared = PreparedOperation::from_verified(&operation, canonical_body_digest(&bytes));
    for replica in [&mut primary, &mut backup] {
        let Admission::Write { ticket, .. } = replica.prepare(node(0), scope, &[prepared]).unwrap()
        else {
            panic!("fresh prepare");
        };
        replica.complete_write(ticket).unwrap();
    }
    backup.complete_sync(backup.begin_sync().unwrap()).unwrap();
    let message = Control::PrepareOk {
        ack: backup.acknowledgment().unwrap(),
        grant: Grant {
            revision: 7,
            record_limit: 2,
            byte_limit: 1024,
        },
    };
    let mut metadata = [0; 145];
    let encoded = encode_control(node(1), session(), message, &mut metadata).unwrap();
    assert_eq!(metadata[120], 2); // Durable evidence, not RAM or socket receipt.
    let binding = PeerBinding::new(configuration(), node(1), session()).unwrap();
    let ReplicaMessage::Control(Control::PrepareOk { ack, grant }) = decode(
        &[&encoded.header, &metadata, &[]],
        binding,
        WireLimits::default(),
    )
    .unwrap() else {
        panic!("prepare ACK");
    };
    assert_eq!(grant.revision, 7);
    primary.receive_ack(node(1), ack).unwrap();
    assert_eq!(primary.snapshot().committed, Prefix::GENESIS);
    primary
        .complete_sync(primary.begin_sync().unwrap())
        .unwrap();
    assert_eq!(primary.snapshot().committed, prepared.prefix());
}

#[test]
fn prepare_roundtrip_preserves_canonical_bodies_views_and_hash_chain() {
    use ozzy_replication::wire::{Operation, Prepare, encode_prepare};
    let mut scope = configuration().scope();
    scope.view = 2;
    let bodies: Vec<_> = [10, 11]
        .map(|value| {
            encode_operation_body(
                &OperationBody::Barrier(Barrier {
                    operation_id: OperationId::from_bytes([value; 16]),
                }),
                OperationLimits::default(),
            )
            .unwrap()
        })
        .into();
    let mut operations = Vec::new();
    let mut previous = Prefix::GENESIS;
    for (index, body) in bodies.iter().enumerate() {
        let canonical = CanonicalOperation {
            group_id: scope.group_id,
            configuration_epoch: 1,
            original_view: index as u64,
            op_number: index as u64 + 1,
            previous_digest: previous.digest,
            kind: ozzy_journal::operation::OperationKind::Barrier,
            body,
        };
        let operation = Operation::from_verified(canonical, canonical_body_digest(body));
        previous = operation.prefix();
        operations.push(operation);
    }
    let request = Prepare {
        scope,
        committed: operations[0].prefix(),
        operations: &operations,
    };
    let mut metadata = [0xff; 512];
    let mut payload = [0xff; 64];
    let encoded = encode_prepare(
        node(2),
        session(),
        request,
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    assert_eq!(encoded.metadata_bytes, 336);
    assert_eq!(encoded.payload_bytes, 32);
    assert_eq!(&metadata[80..88], &1_u64.to_be_bytes());
    assert_eq!(&metadata[88..120], &[0; 32]);
    assert_eq!(&metadata[160..164], &2_u32.to_be_bytes());
    assert_eq!(&metadata[180..182], &8_u16.to_be_bytes());
    assert_eq!(&metadata[182..186], &16_u32.to_be_bytes());
    let ReplicaMessage::Prepare(decoded) = decode(
        &[&encoded.header, &metadata[..336], &payload[..32]],
        PeerBinding::new(configuration(), node(2), session()).unwrap(),
        WireLimits::default(),
    )
    .unwrap() else {
        panic!("prepare");
    };
    assert_eq!(decoded.scope(), scope);
    assert_eq!(decoded.committed(), request.committed);
    assert_eq!(decoded.predecessor(), Prefix::GENESIS);
    assert_eq!(decoded.end(), previous);
    let decoded_operations: Vec<_> = decoded.operations().collect();
    assert_eq!(decoded_operations, operations);
    assert_eq!(
        decoded_operations[0].canonical().body.as_ptr(),
        payload.as_ptr()
    );
    assert_eq!(
        decoded_operations[1].canonical().body.as_ptr(),
        payload[16..].as_ptr()
    );
}

#[test]
fn unbound_prepare_metadata_requires_real_framing_and_preserves_publication_bytes() {
    use ozzy_replication::wire::{
        Operation, Prepare, PublicationPart, encode_prepare_metadata, encode_prepare_unbound,
        encode_publication, encode_publication_group,
    };
    let scope = configuration().scope();
    let body = [11; 16];
    let operation = Operation::from_verified(
        CanonicalOperation {
            group_id: scope.group_id,
            configuration_epoch: scope.configuration_epoch,
            original_view: 0,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind: ozzy_journal::operation::OperationKind::Barrier,
            body: &body,
        },
        canonical_body_digest(&body),
    );
    let operations = [operation];
    let prepare = Prepare {
        scope,
        committed: Prefix::GENESIS,
        operations: &operations,
    };
    let limits = WireLimits::default();
    let mut metadata = [0xff; 512];
    let sizes = encode_prepare_unbound(node(0), prepare, &mut metadata, limits).unwrap();
    let mut bound = [0xff; 512];
    let wire = encode_prepare_metadata(node(0), session(), prepare, &mut bound, limits).unwrap();
    assert_eq!(sizes.metadata_bytes, wire.metadata_bytes);
    assert_eq!(sizes.payload_bytes, body.len());
    assert_eq!(metadata, bound);
    let unbound: [&[u8]; 3] = [&[], &metadata[..sizes.metadata_bytes], &body];
    let binding = PeerBinding::new(configuration(), node(0), session()).unwrap();
    assert!(decode(&unbound, binding, limits).is_err());
    let frames: [&[u8]; 3] = [&wire.header, &bound[..wire.metadata_bytes], &body];
    assert!(decode(&frames, binding, limits).is_ok());

    let mut published = [0xff; 512];
    let expected =
        encode_publication(node(0), &frames, operation.prefix(), &mut published, limits).unwrap();
    let mut output = [0xff; 512];
    assert_eq!(
        encode_publication(node(0), &unbound, operation.prefix(), &mut output, limits).unwrap(),
        expected
    );
    assert_eq!(output, published);
    let part = PublicationPart {
        frames: unbound,
        predecessor: Prefix::GENESIS,
        end: operation.prefix(),
    };
    let mut payload = Vec::new();
    assert_eq!(
        encode_publication_group(
            node(0),
            &[part],
            operation.prefix(),
            &mut output,
            &mut payload,
            limits
        )
        .unwrap(),
        expected
    );
    assert_eq!(payload, body);
    assert_eq!(output, published);

    for length in 0..sizes.metadata_bytes {
        let mut truncated = [0xab; 512];
        assert!(
            encode_prepare_unbound(node(0), prepare, &mut truncated[..length], limits).is_err()
        );
        assert_eq!(truncated, [0xab; 512]);
    }
    let mut invalid = [0xab; 512];
    assert!(
        encode_prepare_unbound(NodeId::from_bytes([0; 16]), prepare, &mut invalid, limits).is_err()
    );
    assert_eq!(invalid, [0xab; 512]);
}

#[test]
fn consecutive_prepares_combine_into_one_publication() {
    use ozzy_replication::wire::{
        Operation, Prepare, PublicationPart, decode_publication, encode_prepare_metadata,
        encode_publication_group, publication_topic,
    };
    let scope = configuration().scope();
    let bodies = [[10; 16], [11; 16]];
    let mut previous = Prefix::GENESIS;
    let mut headers = Vec::new();
    let mut metadata = Vec::new();
    let mut operations = Vec::new();
    for body in &bodies {
        let canonical = CanonicalOperation {
            group_id: scope.group_id,
            configuration_epoch: scope.configuration_epoch,
            original_view: scope.view,
            op_number: previous.op.0 + 1,
            previous_digest: previous.digest,
            kind: ozzy_journal::operation::OperationKind::Barrier,
            body,
        };
        let operation = Operation::from_verified(canonical, canonical_body_digest(body));
        let mut encoded_metadata = [0; 250];
        let encoded = encode_prepare_metadata(
            node(0),
            session(),
            Prepare {
                scope,
                committed: Prefix::GENESIS,
                operations: &[operation],
            },
            &mut encoded_metadata,
            WireLimits::default(),
        )
        .unwrap();
        headers.push(encoded.header);
        metadata.push(encoded_metadata[..encoded.metadata_bytes].to_vec());
        operations.push((previous, operation.prefix()));
        previous = operation.prefix();
    }
    let parts = (0..2)
        .map(|index| PublicationPart {
            frames: [&headers[index], &metadata[index], &bodies[index]],
            predecessor: operations[index].0,
            end: operations[index].1,
        })
        .collect::<Vec<_>>();
    let mut grouped_metadata = [0; 512];
    let mut payload = Vec::new();
    let encoded = encode_publication_group(
        node(0),
        &parts,
        Prefix::GENESIS,
        &mut grouped_metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    let topic = publication_topic(scope.group_id);
    let decoded = decode_publication(
        &[
            &topic,
            &encoded.header,
            &grouped_metadata[..encoded.metadata_bytes],
            &payload,
        ],
        PeerBinding::new(configuration(), node(0), session()).unwrap(),
        node(1),
        scope,
        WireLimits::default(),
    )
    .unwrap();
    assert_eq!(decoded.predecessor(), Prefix::GENESIS);
    assert_eq!(decoded.end(), previous);
    assert_eq!(
        decoded
            .operations()
            .map(|operation| operation.canonical().body)
            .collect::<Vec<_>>(),
        bodies.iter().map(<[u8; 16]>::as_slice).collect::<Vec<_>>()
    );
}

fn one_prepare() -> ([u8; 64], [u8; 250], [u8; 16]) {
    use ozzy_replication::wire::{Operation, Prepare, encode_prepare};
    let scope = configuration().scope();
    let body = [10; 16];
    let canonical = CanonicalOperation {
        group_id: scope.group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: ozzy_journal::operation::OperationKind::Barrier,
        body: &body,
    };
    let operation = Operation::from_verified(canonical, canonical_body_digest(&body));
    let mut metadata = [0; 250];
    let mut payload = [0; 16];
    let encoded = encode_prepare(
        node(0),
        session(),
        Prepare {
            scope,
            committed: Prefix::GENESIS,
            operations: &[operation],
        },
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    (encoded.header, metadata, payload)
}

#[test]
fn prepare_rejects_malformed_counts_chains_digests_and_trailing_bytes() {
    use ozzy_replication::wire::WireError;
    let binding = PeerBinding::new(configuration(), node(0), session()).unwrap();
    let limits = WireLimits::default();
    let (header, metadata, payload) = one_prepare();
    for (offset, value, expected) in [
        (163, 0, WireError::Limit),    // Empty operation list.
        (160, 0xff, WireError::Limit), // Huge count, rejected before descriptor work.
        (171, 2, WireError::Chain),    // Operation number gap.
        (179, 1, WireError::Chain),    // Original view beyond active view.
        (181, 0xff, WireError::OperationKind(255)),
        (185, 0, WireError::Limit),   // Empty canonical body.
        (185, 17, WireError::Length), // Body extends past payload.
        (186, metadata[186] ^ 1, WireError::Digest),
        (218, metadata[218] ^ 1, WireError::Digest),
    ] {
        let mut bad = metadata;
        bad[offset] = value;
        assert_eq!(
            decode(&[&header, &bad, &payload], binding, limits),
            Err(expected),
            "offset {offset}"
        );
    }
    let mut bad_payload = payload;
    bad_payload[0] ^= 1;
    assert_eq!(
        decode(&[&header, &metadata, &bad_payload], binding, limits),
        Err(WireError::Digest)
    );
    for length in 0..metadata.len() {
        let mut short_header = header;
        short_header[56..60].copy_from_slice(&(length as u32).to_be_bytes());
        assert!(
            decode(
                &[&short_header, &metadata[..length], &payload],
                binding,
                limits
            )
            .is_err()
        );
    }
    let mut long_header = header;
    long_header[56..60].copy_from_slice(&251_u32.to_be_bytes());
    let mut trailing = metadata.to_vec();
    trailing.push(0);
    assert_eq!(
        decode(&[&long_header, &trailing, &payload], binding, limits),
        Err(WireError::Length)
    );
    assert_eq!(
        decode(
            &[&header, &metadata, &payload],
            binding,
            WireLimits {
                max_operations: 0,
                ..limits
            }
        ),
        Err(WireError::Limit)
    );
}

#[test]
fn peer_session_configuration_and_weaker_votes_cannot_reach_the_core() {
    use ozzy_replication::wire::WireError;
    let limits = WireLimits::default();
    let binding = PeerBinding::new(configuration(), node(1), session()).unwrap();
    let message = Control::PrepareOk {
        ack: ozzy_replication::PrepareOk {
            scope: configuration().scope(),
            durable: Prefix::GENESIS,
        },
        grant: Grant {
            revision: 1,
            record_limit: 2,
            byte_limit: 1024,
        },
    };
    let mut metadata = [0; 145];
    let encoded = encode_control(node(1), session(), message, &mut metadata).unwrap();
    for evidence in [0, 1, 3, 255] {
        let mut bad = metadata;
        bad[120] = evidence;
        assert_eq!(
            decode(&[&encoded.header, &bad, &[]], binding, limits),
            Err(WireError::Evidence)
        );
    }
    for offset in [24, 40] {
        let mut bad = encoded.header;
        bad[offset] ^= 1;
        assert_eq!(
            decode(&[&bad, &metadata, &[]], binding, limits),
            Err(WireError::Peer)
        );
    }
    for (offset, expected) in [
        (0, WireError::Scope),
        (23, WireError::Scope),
        (32, WireError::Peer),
        (48, WireError::Scope),
    ] {
        let mut bad = metadata;
        bad[offset] ^= 1;
        assert_eq!(
            decode(&[&encoded.header, &bad, &[]], binding, limits),
            Err(expected)
        );
    }
    assert!(PeerBinding::new(configuration(), node(3), session()).is_err());
    assert!(
        PeerBinding::new(configuration(), node(1), LinkSessionId::from_bytes([0; 16])).is_err()
    );
}

#[test]
fn encoding_rejections_leave_reusable_output_buffers_unchanged() {
    use ozzy_replication::wire::{Operation, Prepare, WireError, encode_prepare};
    let scope = configuration().scope();
    let body = [10; 16];
    let canonical = CanonicalOperation {
        group_id: scope.group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: ozzy_journal::operation::OperationKind::Barrier,
        body: &body,
    };
    let operations = [Operation::from_verified(
        canonical,
        canonical_body_digest(&body),
    )];
    let request = Prepare {
        scope,
        committed: Prefix::GENESIS,
        operations: &operations,
    };
    for (metadata_capacity, payload_capacity, max_operations, expected) in [
        (249, 16, 1, WireError::Capacity),
        (250, 15, 1, WireError::Capacity),
        (250, 16, 0, WireError::Limit),
    ] {
        let mut metadata = [0xaa; 250];
        let mut payload = [0xbb; 16];
        assert_eq!(
            encode_prepare(
                node(0),
                session(),
                request,
                &mut metadata[..metadata_capacity],
                &mut payload[..payload_capacity],
                WireLimits {
                    max_operations,
                    ..WireLimits::default()
                }
            ),
            Err(expected)
        );
        assert_eq!(metadata, [0xaa; 250]);
        assert_eq!(payload, [0xbb; 16]);
    }
    let mut metadata = [0xaa; 120];
    assert_eq!(
        encode_control(
            node(0),
            session(),
            Control::Commit(Commit {
                scope,
                committed: Prefix {
                    op: ozzy_replication::OpNumber(1),
                    digest: Digest::ZERO
                }
            }),
            &mut metadata
        ),
        Err(WireError::Prefix)
    );
    assert_eq!(metadata, [0xaa; 120]);
}

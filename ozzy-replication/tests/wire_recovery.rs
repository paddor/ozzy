//! Recovery wire evidence remains distinct from normal durable votes.

#[path = "../../ozzy-journal-segment/tests/support/allocations.rs"]
mod allocations;

use ozzy_proto::{GroupId, LinkSessionId, NodeId, RequestId};
use ozzy_replication::recovery::{RecoveryLog, RecoveryResponse};
use ozzy_replication::wire::{
    PeerBinding, RecoveryMessage, RecoveryRequest, RecoveryState, ReplicaMessage, WireError,
    WireLimits, decode, encode_recovery, encode_recovery_state,
};
use ozzy_replication::{Configuration, Digest, JournalGeneration, OpNumber, Prefix};

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

fn request() -> RecoveryRequest {
    RecoveryRequest {
        scope: configuration().scope(),
        request_id: RequestId::from_bytes([11; 16]),
        nonce: RequestId::from_bytes([10; 16]),
    }
}

#[test]
fn recovery_request_has_frozen_bytes_and_separate_attempt_and_link_identities() {
    let mut metadata = [0xff; 112];
    let encoded = encode_recovery(
        node(1),
        session(),
        request(),
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    let mut header = [0; 64];
    header[..6].copy_from_slice(b"OZY\0\x01\x38");
    header[8..24].fill(11);
    header[24..40].fill(2);
    header[40..56].fill(9);
    header[59] = 96;
    let mut golden = [0; 96];
    golden[..16].fill(7);
    golden[23] = 1;
    golden[32..48].fill(2);
    golden[48..80].fill(8);
    golden[80..96].fill(10);
    assert_eq!(encoded.header, header);
    assert_eq!(encoded.metadata_bytes, golden.len());
    assert_eq!(&metadata[..96], &golden);
    assert_eq!(&metadata[96..], &[0xff; 16]);
    assert_eq!(
        decode(
            &[&header, &golden, &[]],
            PeerBinding::new(configuration(), node(1), session()).unwrap(),
            WireLimits::default(),
        )
        .unwrap(),
        ReplicaMessage::Recovery(RecoveryMessage::Request(request())),
    );
}

fn state() -> RecoveryState {
    RecoveryState {
        request_id: request().request_id,
        response: RecoveryResponse {
            scope: configuration().scope(),
            nonce: request().nonce,
            primary: Some(RecoveryLog {
                checkpoint: None,
                generation: JournalGeneration(14),
                accepted: Prefix {
                    op: OpNumber(2),
                    digest: Digest::from_bytes([12; 32]),
                },
                committed: Prefix {
                    op: OpNumber(1),
                    digest: Digest::from_bytes([13; 32]),
                },
            }),
        },
    }
}

#[test]
fn checkpoint_anchor_roundtrips_and_rejects_every_partial_descriptor() {
    let mut state = state();
    let log = state.response.primary.as_mut().unwrap();
    log.checkpoint = Some(ozzy_replication::recovery::CheckpointAnchor {
        predecessor: Prefix::GENESIS,
        position: log.committed,
        schema: Digest::from_bytes([41; 32]),
        state_digest: Digest::from_bytes([42; 32]),
        state_bytes: 4096,
        chunk_bytes: 1024,
    });
    let mut metadata = [0xff; 512];
    let encoded = encode_recovery_state(
        node(0),
        session(),
        state,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    assert_eq!(encoded.metadata_bytes, 350);
    assert_eq!(metadata[193], 1);
    let binding = PeerBinding::new(configuration(), node(0), session()).unwrap();
    assert_eq!(
        decode(
            &[&encoded.header, &metadata[..350], &[]],
            binding,
            WireLimits::default()
        )
        .unwrap(),
        ReplicaMessage::Recovery(RecoveryMessage::State(state))
    );
    for length in 194..350 {
        assert!(
            decode(
                &[&encoded.header, &metadata[..length], &[]],
                binding,
                WireLimits::default()
            )
            .is_err()
        );
    }
    let anchor = state
        .response
        .primary
        .as_mut()
        .unwrap()
        .checkpoint
        .as_mut()
        .unwrap();
    anchor.position = Prefix {
        op: OpNumber(3),
        digest: Digest::from_bytes([43; 32]),
    };
    let untouched = metadata;
    assert!(
        encode_recovery_state(
            node(0),
            session(),
            state,
            &mut metadata,
            WireLimits::default()
        )
        .is_err()
    );
    assert_eq!(metadata, untouched);
}

#[test]
fn recovery_state_has_frozen_primary_and_backup_bytes_without_durable_vote_evidence() {
    for primary in [true, false] {
        let mut state = state();
        if !primary {
            state.response.primary = None;
        }
        let from = if primary { node(0) } else { node(2) };
        let mut metadata = [0xff; 208];
        let encoded =
            encode_recovery_state(from, session(), state, &mut metadata, WireLimits::default())
                .unwrap();
        let size = if primary { 194 } else { 97 };
        let mut header = [0; 64];
        header[..6].copy_from_slice(b"OZY\0\x01\x39");
        header[7] = 1;
        header[8..24].fill(11);
        header[24..40].copy_from_slice(from.as_bytes());
        header[40..56].fill(9);
        header[59] = size as u8;
        let mut golden = [0; 194];
        golden[..16].fill(7);
        golden[23] = 1;
        golden[32..48].copy_from_slice(from.as_bytes());
        golden[48..80].fill(8);
        golden[80..96].fill(10);
        if primary {
            golden[96] = 1;
            golden[112] = 14;
            golden[120] = 2;
            golden[121..153].fill(12);
            golden[160] = 1;
            golden[161..193].fill(13);
        }
        assert_eq!(encoded.header, header);
        assert_eq!(encoded.metadata_bytes, size);
        assert_eq!(&metadata[..size], &golden[..size]);
        assert!(metadata[size..].iter().all(|&byte| byte == 0xff));
        assert_eq!(
            decode(
                &[&header, &golden[..size], &[]],
                PeerBinding::new(configuration(), from, session()).unwrap(),
                WireLimits::default(),
            )
            .unwrap(),
            ReplicaMessage::Recovery(RecoveryMessage::State(state)),
        );
    }
}

#[test]
fn current_link_exchange_and_recovery_nonce_must_both_match_without_fixing_reply_view() {
    let mut state = state();
    state.response.scope.view = 3;
    state.validate_response(request()).unwrap();
    let mut changed = request();
    changed.scope.view = 8; // The request's view is a hint, not authority over the donor.
    state.validate_response(changed).unwrap();
    changed.request_id = RequestId::from_bytes([12; 16]);
    assert_eq!(
        state.validate_response(changed),
        Err(WireError::Correlation)
    );
    changed = request();
    changed.nonce = RequestId::from_bytes([12; 16]);
    assert_eq!(
        state.validate_response(changed),
        Err(WireError::Correlation)
    );
    changed = request();
    changed.scope.configuration_epoch += 1;
    assert_eq!(state.validate_response(changed), Err(WireError::Scope));
    changed = request();
    changed.scope.configuration_digest = Digest::from_bytes([19; 32]);
    assert_eq!(state.validate_response(changed), Err(WireError::Scope));
}

fn packet(from: NodeId, message: &RecoveryMessage) -> ([u8; 64], Vec<u8>) {
    let mut metadata = [0xff; 208];
    let encoded = match *message {
        RecoveryMessage::Request(request) => encode_recovery(
            from,
            session(),
            request,
            &mut metadata,
            WireLimits::default(),
        ),
        RecoveryMessage::State(state) => {
            encode_recovery_state(from, session(), state, &mut metadata, WireLimits::default())
        }
    }
    .unwrap();
    (encoded.header, metadata[..encoded.metadata_bytes].to_vec())
}

fn decode_bytes<'a>(
    from: NodeId,
    header: &'a [u8],
    metadata: &'a [u8],
    payload: &'a [u8],
) -> Result<ReplicaMessage<'a>, WireError> {
    decode(
        &[header, metadata, payload],
        PeerBinding::new(configuration(), from, session()).unwrap(),
        WireLimits::default(),
    )
}

fn lengths(mut header: [u8; 64], metadata: usize, payload: usize) -> [u8; 64] {
    header[56..60].copy_from_slice(&(metadata as u32).to_be_bytes());
    header[60..64].copy_from_slice(&(payload as u32).to_be_bytes());
    header
}

fn messages() -> [(NodeId, RecoveryMessage); 3] {
    let mut backup = state();
    backup.response.primary = None;
    [
        (node(1), RecoveryMessage::Request(request())),
        (node(0), RecoveryMessage::State(state())),
        (node(2), RecoveryMessage::State(backup)),
    ]
}

#[test]
fn recovery_encoders_preserve_caller_storage_on_capacity_and_limit_errors() {
    for (from, message) in messages() {
        let (_, bytes) = packet(from, &message);
        let encode = |output: &mut [u8], limits| match message {
            RecoveryMessage::Request(request) => {
                encode_recovery(from, session(), request, output, limits)
            }
            RecoveryMessage::State(state) => {
                encode_recovery_state(from, session(), state, output, limits)
            }
        };
        let mut output = [0xff; 208];
        for end in 0..bytes.len() {
            assert_eq!(
                encode(&mut output[..end], WireLimits::default()),
                Err(WireError::Capacity)
            );
            assert_eq!(output, [0xff; 208]);
        }
        let mut limits = WireLimits::default();
        limits.envelope.max_metadata_bytes = bytes.len() - 1;
        assert!(encode(&mut output, limits).is_err());
        assert_eq!(output, [0xff; 208]);
        let binding = PeerBinding::new(configuration(), from, session()).unwrap();
        let ((), count) = allocations::measure(|| {
            let encoded = encode(&mut output, WireLimits::default()).unwrap();
            assert_eq!(
                decode(
                    &[&encoded.header, &output[..encoded.metadata_bytes], &[]],
                    binding,
                    WireLimits::default()
                )
                .unwrap(),
                ReplicaMessage::Recovery(message)
            );
        });
        assert_eq!(count, 0);
    }
}

fn exchange(from: NodeId, response: &RecoveryResponse) -> RecoveryResponse {
    let (header, metadata) = packet(
        from,
        &RecoveryMessage::State(RecoveryState {
            request_id: request().request_id,
            response: *response,
        }),
    );
    let ReplicaMessage::Recovery(RecoveryMessage::State(state)) =
        decode_bytes(from, &header, &metadata, &[]).unwrap()
    else {
        panic!("recovery state");
    };
    state.validate_response(request()).unwrap();
    state.response
}

#[test]
fn decoded_quorum_and_history_still_require_publication_before_fenced_rejoin() {
    use ozzy_journal::operation::{
        Barrier, CanonicalOperation, OperationBody, OperationLimits, canonical_body_digest,
        encode_operation_body,
    };
    use ozzy_proto::OperationId;
    use ozzy_replication::recovery::{Recovery, RecoveryError};
    use ozzy_replication::wire::Operation;
    use ozzy_replication::{NormalReplica, PipelineLimits, PreparedOperation, Status, ViewChange};

    let config = configuration();
    let bounds = PipelineLimits {
        max_operations: 1,
        max_body_bytes: 16,
    };
    let bytes = encode_operation_body(
        &OperationBody::Barrier(Barrier {
            operation_id: OperationId::from_bytes([17; 16]),
        }),
        OperationLimits::default(),
    )
    .unwrap();
    let operation = Operation::from_verified(
        CanonicalOperation {
            group_id: config.scope().group_id,
            configuration_epoch: config.scope().configuration_epoch,
            original_view: 0,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind: ozzy_journal::operation::OperationKind::Barrier,
            body: &bytes,
        },
        canonical_body_digest(&bytes),
    );
    let prepared =
        PreparedOperation::from_verified(&operation.canonical(), operation.body_digest());
    let mut primary =
        NormalReplica::bootstrap(config, node(0), JournalGeneration(20), bounds).unwrap();
    let third = NormalReplica::bootstrap(config, node(2), JournalGeneration(22), bounds).unwrap();
    primary
        .prepare(node(0), config.scope(), &[prepared])
        .unwrap();
    assert_eq!(primary.snapshot().journal.durable.0, 0);
    let mut recovery = Recovery::new(
        config,
        node(1),
        JournalGeneration(100),
        request().nonce,
        bounds,
    )
    .unwrap();
    let response = exchange(
        node(0),
        &primary.recovery_response(request().nonce).unwrap(),
    );
    recovery.receive(node(0), response).unwrap();
    recovery.receive(node(0), response).unwrap();
    assert_eq!(recovery.begin_transfer(), Err(RecoveryError::QuorumMissing));
    recovery
        .receive(
            node(2),
            exchange(node(2), &third.recovery_response(request().nonce).unwrap()),
        )
        .unwrap();
    let ticket = recovery.begin_transfer().unwrap();
    assert_eq!(ticket.source().accepted, operation.prefix());
    assert_eq!(
        recovery.complete(ticket, operation.prefix(), Prefix::GENESIS),
        Err(RecoveryError::HistoryMissing)
    );
    recovery
        .validate_chunk(ticket, &[transfer(&ticket, operation)])
        .unwrap();
    assert_eq!(
        recovery.complete(ticket, Prefix::GENESIS, Prefix::GENESIS),
        Err(RecoveryError::StoragePending)
    );
    // Explicit external publication assertion, not a simulated disk write.
    let restored = recovery
        .complete(ticket, operation.prefix(), Prefix::GENESIS)
        .unwrap();
    let changing =
        ViewChange::recover_intact(config, node(1), ticket.generation(), restored, bounds).unwrap();
    assert_eq!(changing.normal_snapshot().status, Status::Fenced);
    assert_eq!(changing.scope().view, 1);
}

fn transfer(
    ticket: &ozzy_replication::recovery::RecoveryTicket,
    operation: ozzy_replication::wire::Operation<'_>,
) -> ozzy_replication::PreparedOperation {
    use ozzy_replication::wire::{FetchOps, encode_fetch, encode_ops};
    let fetch = FetchOps {
        scope: ticket.scope(),
        request_id: RequestId::from_bytes([18; 16]),
        source: ticket.source(),
        predecessor: Prefix::GENESIS,
        max_operations: 1,
        max_body_bytes: 16,
    };
    let mut metadata = [0; 512];
    let encoded = encode_fetch(
        node(1),
        session(),
        fetch,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    let ReplicaMessage::FetchOps(decoded_fetch) = decode_bytes(
        node(1),
        &encoded.header,
        &metadata[..encoded.metadata_bytes],
        &[],
    )
    .unwrap() else {
        panic!("fetch");
    };
    assert_eq!(decoded_fetch, fetch);
    let mut payload = [0; 16];
    let encoded = encode_ops(
        node(0),
        session(),
        decoded_fetch,
        &[operation],
        &mut metadata,
        &mut payload,
        WireLimits::default(),
    )
    .unwrap();
    let ReplicaMessage::Ops(batch) = decode_bytes(
        node(0),
        &encoded.header,
        &metadata[..encoded.metadata_bytes],
        &payload,
    )
    .unwrap() else {
        panic!("history");
    };
    batch.validate_response(fetch).unwrap();
    let received = batch.operations().next().unwrap();
    ozzy_replication::PreparedOperation::from_verified(
        &received.canonical(),
        received.body_digest(),
    )
}

#[test]
fn recovery_schemas_reject_every_truncation_trailing_metadata_and_payload() {
    for (from, message) in messages() {
        let (header, mut metadata) = packet(from, &message);
        for end in 0..metadata.len() {
            let truncated = lengths(header, end, 0);
            assert_eq!(
                decode_bytes(from, &truncated, &metadata[..end], &[]),
                Err(WireError::Length)
            );
        }
        let payload = lengths(header, metadata.len(), 1);
        assert_eq!(
            decode_bytes(from, &payload, &metadata, &[1]),
            Err(WireError::Payload)
        );
        metadata.push(0);
        let trailing = lengths(header, metadata.len(), 0);
        assert_eq!(
            decode_bytes(from, &trailing, &metadata, &[]),
            Err(WireError::Length)
        );
    }
}

#[test]
fn recovery_rejects_stale_sessions_wrong_voters_foreign_scope_and_missing_correlation() {
    for (from, message) in messages() {
        let (header, metadata) = packet(from, &message);
        for offset in [24, 40] {
            let mut changed = header;
            changed[offset] ^= 1;
            assert_eq!(
                decode_bytes(from, &changed, &metadata, &[]),
                Err(WireError::Peer)
            );
        }
        for (offset, error) in [
            (0, WireError::Scope),
            (23, WireError::Scope),
            (32, WireError::Peer),
            (48, WireError::Scope),
        ] {
            let mut changed = metadata.clone();
            changed[offset] ^= 1;
            assert_eq!(decode_bytes(from, &header, &changed, &[]), Err(error));
        }
        let mut changed = metadata.clone();
        changed[80..96].fill(0);
        assert_eq!(
            decode_bytes(from, &header, &changed, &[]),
            Err(WireError::Correlation)
        );
        let mut changed = header;
        changed[7] ^= 1;
        assert_eq!(
            decode_bytes(from, &changed, &metadata, &[]),
            Err(WireError::Correlation)
        );
        changed = header;
        changed[8..24].fill(0);
        assert!(decode_bytes(from, &changed, &metadata, &[]).is_err());
    }
}

#[test]
fn recovery_rejects_forged_primary_roles_and_inconsistent_history_descriptors() {
    let (header, metadata) = packet(node(0), &RecoveryMessage::State(state()));
    for (range, value, error) in [
        (96..97, 2, WireError::History),
        (97..113, 0, WireError::History),
        (113..121, 0, WireError::Prefix),
        (113..121, 255, WireError::Prefix),
        (121..153, 0, WireError::Prefix),
        (160..161, 3, WireError::History),
        (160..161, 2, WireError::History),
        (161..193, 0, WireError::Prefix),
    ] {
        let mut changed = metadata.clone();
        changed[range].fill(value);
        assert_eq!(decode_bytes(node(0), &header, &changed, &[]), Err(error));
    }
    let mut claimed_sender = header;
    claimed_sender[24..40].copy_from_slice(node(2).as_bytes());
    let mut changed = metadata;
    changed[32..48].copy_from_slice(node(2).as_bytes());
    assert_eq!(
        decode_bytes(node(2), &claimed_sender, &changed, &[]),
        Err(WireError::History)
    );
    let mut empty = state();
    empty.response.primary = None;
    let (header, metadata) = packet(node(0), &RecoveryMessage::State(empty));
    assert_eq!(
        decode_bytes(node(0), &header, &metadata, &[]),
        Err(WireError::History)
    );
    empty.response.primary = Some(RecoveryLog {
        checkpoint: None,
        generation: JournalGeneration(14),
        accepted: Prefix::GENESIS,
        committed: Prefix::GENESIS,
    });
    let (header, metadata) = packet(node(0), &RecoveryMessage::State(empty));
    assert_eq!(
        decode_bytes(node(0), &header, &metadata, &[]).unwrap(),
        ReplicaMessage::Recovery(RecoveryMessage::State(empty)),
    );
}

#[test]
fn checkpoint_chunks_bind_source_nonce_range_and_live_link_session() {
    use ozzy_replication::{
        LogSource,
        wire::{CheckpointMessage, CheckpointRequest, encode_checkpoint},
    };
    let request = CheckpointRequest {
        scope: configuration().scope(),
        request_id: RequestId::from_bytes([51; 16]),
        nonce: RequestId::from_bytes([52; 16]),
        source: LogSource {
            voter: node(0),
            generation: JournalGeneration(5),
            accepted: state().response.primary.unwrap().accepted,
        },
        offset: 4096,
        max_bytes: 1024,
    };
    let mut metadata = [0xff; 256];
    for (sender, message, payload) in [
        (node(1), CheckpointMessage::Request(request), &[][..]),
        (
            node(0),
            CheckpointMessage::Chunk {
                request,
                bytes: b"canonical-state",
            },
            &b"canonical-state"[..],
        ),
    ] {
        let encoded = encode_checkpoint(
            sender,
            session(),
            message,
            &mut metadata,
            WireLimits::default(),
        )
        .unwrap();
        assert_eq!(encoded.metadata_bytes, 180);
        let binding = PeerBinding::new(configuration(), sender, session()).unwrap();
        let frames = [&encoded.header[..], &metadata[..180], payload];
        assert_eq!(
            decode(&frames, binding, WireLimits::default()).unwrap(),
            ReplicaMessage::Checkpoint(message)
        );
        let stale =
            PeerBinding::new(configuration(), sender, LinkSessionId::from_bytes([53; 16])).unwrap();
        assert!(decode(&frames, stale, WireLimits::default()).is_err());
        for length in 0..180 {
            assert!(
                decode(
                    &[&encoded.header, &metadata[..length], payload],
                    binding,
                    WireLimits::default()
                )
                .is_err()
            );
        }
    }
    let unchanged = metadata;
    let oversized = [1; 1025];
    assert!(
        encode_checkpoint(
            node(0),
            session(),
            CheckpointMessage::Chunk {
                request,
                bytes: &oversized
            },
            &mut metadata,
            WireLimits::default()
        )
        .is_err()
    );
    assert_eq!(metadata, unchanged);
    assert!(
        encode_checkpoint(
            node(1),
            session(),
            CheckpointMessage::Chunk {
                request,
                bytes: b"bad donor"
            },
            &mut metadata,
            WireLimits::default()
        )
        .is_err()
    );
    assert_eq!(metadata, unchanged);
}

#[test]
fn retired_history_notice_is_bound_to_scope_session_and_live_receive_or_fetch() {
    use ozzy_replication::wire::{HistoryFence, HistoryRetired, encode_history_retired};
    let mut output = [0xff; 160];
    for fence in [
        HistoryFence::Receive(ozzy_replication::flow::ReceiveEpoch::new(15).unwrap()),
        HistoryFence::Fetch(RequestId::from_bytes([16; 16])),
    ] {
        let notice = HistoryRetired {
            scope: configuration().scope(),
            fence,
            before: Prefix {
                op: OpNumber(8),
                digest: Digest::from_bytes([17; 32]),
            },
        };
        let encoded = encode_history_retired(
            node(0),
            session(),
            notice,
            &mut output,
            WireLimits::default(),
        )
        .unwrap();
        assert_eq!(encoded.metadata_bytes, 137);
        assert_eq!(
            decode_bytes(node(0), &encoded.header, &output[..137], &[]).unwrap(),
            ReplicaMessage::HistoryRetired(notice)
        );
        for end in 0..137 {
            assert!(decode_bytes(node(0), &encoded.header, &output[..end], &[]).is_err());
        }
        let mut stale = encoded.header;
        stale[49] ^= 1;
        assert!(decode_bytes(node(0), &stale, &output[..137], &[]).is_err());
        let mut invalid = notice;
        invalid.fence = HistoryFence::Fetch(RequestId::from_bytes([0; 16]));
        let before = output;
        assert!(
            encode_history_retired(
                node(0),
                session(),
                invalid,
                &mut output,
                WireLimits::default()
            )
            .is_err()
        );
        assert_eq!(output, before);
    }
}

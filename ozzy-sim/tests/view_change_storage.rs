//! Real segment publication/reopen composition, not a device power-loss test.

use std::convert::Infallible;

use ozzy_core::state::{
    CanonicalImages, IdentityIndex, IdentityKey, MemoryIdentityIndex, StateLimits,
};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, CanonicalOperation, CreatePartition, OpenProducer,
    OperationBody, OperationKind, OperationLimits, RetentionPolicy, canonical_body_digest,
    decode_operation_body, encode_operation_body, logical_operation_digest,
};
use ozzy_journal_segment::{
    BodyEncoding, CanonicalRecoveryCandidate, CanonicalRecoveryLimits, DecodeLimits,
    GroupDirectory, GroupIdentity, JournalIdentityIndex, LogPosition, MetadataLimits,
    OpenGroupJournal, SegmentHeader, SuffixReplacement, SuffixStreamLimits,
};
use ozzy_proto::{
    GroupId, LinkSessionId, MessageId, NodeId, Offset, OperationId, OwnerEpoch, PartitionId,
    PartitionIncarnation, ProducerEpoch, ProducerId, ProducerSequence, RequestId, StoreId,
    VolumeId,
};
use ozzy_replication::wire::{
    Control, FetchOps, Operation, PeerBinding, ReplicaMessage, WireLimits, decode, encode_control,
    encode_fetch, encode_ops,
};
use ozzy_replication::{
    Admission, Configuration, Digest, DoViewChange, FrozenLog, InstallOutcome, InstallTicket,
    InstallingView, JournalGeneration, NormalReplica, OpNumber, PipelineLimits, Prefix,
    PromiseTicket, RecoveredState, Scope, SelectedView, StartView, StartViewChange, ViewChange,
    ViewChangeError, WriteTicket,
};
use tempfile::TempDir;

const CAPACITY: u64 = 32 * 1024;

fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}

fn wire_control(from: u8, message: Control) -> Control {
    // Explicit trusted fixture binding, not authentication inferred from bytes.
    let session = LinkSessionId::from_bytes([9; 16]);
    let mut metadata = [0; 184];
    let encoded = encode_control(node(from), session, message, &mut metadata).unwrap();
    let ReplicaMessage::Control(decoded) = decode(
        &[&encoded.header, &metadata[..encoded.metadata_bytes], &[]],
        PeerBinding::new(config(), node(from), session).unwrap(),
        WireLimits::default(),
    )
    .unwrap() else {
        panic!("control");
    };
    assert_eq!(decoded, message);
    decoded
}

fn wire_start(from: u8, message: StartViewChange) -> StartViewChange {
    let Control::StartViewChange(decoded) = wire_control(from, Control::StartViewChange(message))
    else {
        panic!("start view change");
    };
    decoded
}

fn wire_report(from: u8, message: DoViewChange) -> DoViewChange {
    let Control::DoViewChange(decoded) = wire_control(from, Control::DoViewChange(message)) else {
        panic!("view report");
    };
    decoded
}

fn wire_install(from: u8, message: StartView) -> StartView {
    let Control::StartView(decoded) = wire_control(from, Control::StartView(message)) else {
        panic!("start view");
    };
    decoded
}

fn transfer_history(
    journal: &OpenGroupJournal,
    selected: SelectedView,
) -> Vec<(OperationKind, Vec<u8>)> {
    let source = selected.source();
    assert_eq!(
        source.generation,
        journal.writer().durable_position().generation()
    );
    assert_eq!(
        position(source.accepted),
        journal.accepted_position().unwrap()
    );
    assert!(source.accepted.op.0 <= 8); // This fixture retains at most eight returned bodies.
    let session = LinkSessionId::from_bytes([9; 16]);
    let requester = config().primary(selected.scope().view);
    let mut request = FetchOps {
        scope: selected.scope(),
        request_id: RequestId::from_bytes([1; 16]),
        source,
        predecessor: Prefix::GENESIS,
        max_operations: 2,
        max_body_bytes: 8192,
    };
    let mut metadata = [0; 512];
    let mut payload = [0; 8192];
    let mut transferred = Vec::with_capacity(source.accepted.op.0 as usize);
    journal
        .replay_accepted(|replayed| {
            assert!(!replayed.committed);
            let operation = replayed.operation;
            request.request_id =
                RequestId::from_bytes(u128::from(operation.op_number).to_be_bytes());
            let encoded = encode_fetch(
                requester,
                session,
                request,
                &mut metadata,
                WireLimits::default(),
            )
            .unwrap();
            let ReplicaMessage::FetchOps(fetched) = decode(
                &[&encoded.header, &metadata[..encoded.metadata_bytes], &[]],
                PeerBinding::new(config(), requester, session).unwrap(),
                WireLimits::default(),
            )
            .unwrap() else {
                panic!("fetch");
            };
            assert_eq!(fetched.source, source);
            let wire = Operation::from_verified(
                CanonicalOperation {
                    group_id: operation.group_id,
                    configuration_epoch: operation.configuration_epoch,
                    original_view: operation.original_view,
                    op_number: operation.op_number,
                    previous_digest: operation.previous_digest,
                    kind: operation.kind,
                    body: &operation.body,
                },
                canonical_body_digest(&operation.body),
            );
            assert_eq!(wire.prefix().digest, operation.digest);
            let encoded = encode_ops(
                source.voter,
                session,
                fetched,
                &[wire],
                &mut metadata,
                &mut payload,
                WireLimits::default(),
            )
            .unwrap();
            let ReplicaMessage::Ops(received) = decode(
                &[
                    &encoded.header,
                    &metadata[..encoded.metadata_bytes],
                    &payload[..encoded.payload_bytes],
                ],
                PeerBinding::new(config(), source.voter, session).unwrap(),
                WireLimits::default(),
            )
            .unwrap() else {
                panic!("ops");
            };
            received.validate_response(request).unwrap();
            let received_operation = received.operations().next().unwrap().canonical();
            assert_eq!(received_operation.original_view, operation.original_view);
            transferred.push((received_operation.kind, received_operation.body.to_vec()));
            request.predecessor = received.end();
            Ok::<_, Infallible>(())
        })
        .unwrap();
    assert_eq!(request.predecessor, source.accepted);
    transferred
}

fn config() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [node(0), node(1), node(2)],
    )
    .unwrap()
}

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([10; 16])
}

fn append(sequence: u64) -> OperationBody<'static> {
    OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::new(1),
            producer_id: ProducerId::from_bytes([11; 16]),
            producer_epoch: ProducerEpoch::new(1),
            first_sequence: ProducerSequence::new(sequence),
            first_offset: Offset::new(sequence),
            append_timestamp_millis: 123,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes(u128::from(sequence + 100).to_be_bytes()),
                parts: vec![
                    b"order.created".as_slice(),
                    b"{\"sku\":\"sku-42\",\"qty\":3}".as_slice(),
                ]
                .into(),
            }]
            .into(),
        }],
    })
}

fn bodies() -> Vec<(OperationKind, Vec<u8>)> {
    [
        OperationBody::CreatePartition(CreatePartition {
            partition: partition(),
            stream: "events",
            topic: "orders",
            partition_id: PartitionId::new(0),
            owner_epoch: OwnerEpoch::new(1),
            retention: RetentionPolicy::default(),
        }),
        OperationBody::OpenProducer(OpenProducer {
            partition: partition(),
            producer_id: ProducerId::from_bytes([11; 16]),
            expected_epoch: None,
            new_epoch: ProducerEpoch::new(1),
            operation_id: OperationId::from_bytes([12; 16]),
        }),
        append(0),
        append(1),
    ]
    .iter()
    .map(|body| {
        (
            body.kind(),
            encode_operation_body(body, OperationLimits::default()).unwrap(),
        )
    })
    .collect()
}

fn operations(bodies: &[(OperationKind, Vec<u8>)]) -> Vec<CanonicalOperation<'_>> {
    let mut previous_digest = Digest::ZERO;
    bodies
        .iter()
        .enumerate()
        .map(|(index, (kind, bytes))| {
            let operation = CanonicalOperation {
                group_id: config().scope().group_id,
                configuration_epoch: 1,
                original_view: 0,
                op_number: index as u64 + 1,
                previous_digest,
                kind: *kind,
                body: bytes,
            };
            previous_digest = logical_operation_digest(&operation);
            operation
        })
        .collect()
}

#[derive(Debug)]
struct Replica<I = MemoryIdentityIndex> {
    temporary: TempDir,
    journal: OpenGroupJournal,
    core: NormalReplica,
    images: CanonicalImages<I>,
}

fn replica(index: u8, operations: &[CanonicalOperation<'_>]) -> Replica {
    // Use the repository filesystem, not a possibly memory-backed /tmp mount.
    // Successful fsync/reopen still does not establish device power-loss safety.
    let temporary = tempfile::Builder::new()
        .prefix(".vsr-storage-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .unwrap();
    let identity = GroupIdentity {
        group_id: config().scope().group_id,
        replica_node_id: node(index),
        volume_id: VolumeId::from_bytes([index + 10; 16]),
        store_id: StoreId::from_bytes([index + 20; 16]),
        store_generation: 1,
    };
    let generation = JournalGeneration(u128::from(index) + 1);
    let header = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, CAPACITY).unwrap();
    let mut journal =
        GroupDirectory::format_new(temporary.path().join("group"), identity, 1, &header)
            .unwrap()
            .recover(
                generation,
                DecodeLimits::default(),
                OperationLimits::default(),
            )
            .unwrap();
    let mut core = NormalReplica::bootstrap(
        config(),
        node(index),
        generation,
        PipelineLimits {
            max_operations: 8,
            max_body_bytes: 8192,
        },
    )
    .unwrap();
    let mut images = CanonicalImages::new(StateLimits::default(), 8, 8);
    if !operations.is_empty() {
        let typed: Vec<_> = operations
            .iter()
            .map(|operation| {
                (
                    operation.op_number,
                    decode_operation_body(
                        operation.kind,
                        operation.body,
                        OperationLimits::default(),
                    )
                    .unwrap(),
                )
            })
            .collect();
        let plans = images.prepare_group(&typed).unwrap();
        let prepared: Vec<_> = operations
            .iter()
            .map(|operation| {
                ozzy_replication::PreparedOperation::from_verified(
                    operation,
                    canonical_body_digest(operation.body),
                )
            })
            .collect();
        let Admission::Write { ticket, .. } =
            core.prepare(node(0), config().scope(), &prepared).unwrap()
        else {
            panic!("fresh journal");
        };
        images.install_prepared_group(plans).unwrap();
        let written = journal.append(operations).unwrap();
        core.complete_write(ticket).unwrap();
        let sync = core.begin_sync().unwrap();
        journal.sync_through(written).unwrap();
        core.complete_sync(sync).unwrap();
    }
    Replica {
        temporary,
        journal,
        core,
        images,
    }
}

fn position(prefix: Prefix) -> LogPosition {
    LogPosition {
        op_number: prefix.op.0,
        digest: prefix.digest,
    }
}

// Synchronous test adapter. Runtime integration must submit this work to the
// dedicated journal worker, never execute it on a protocol/OMQ runtime.
fn publish_promise(journal: OpenGroupJournal, ticket: PromiseTicket) -> OpenGroupJournal {
    let current = journal.directory().manifest();
    assert_eq!(
        ticket.generation(),
        journal.writer().written_position().generation()
    );
    assert_eq!(ticket.scope().group_id, current.identity.group_id);
    assert_eq!(
        ticket.scope().configuration_epoch,
        current.configuration_epoch
    );
    assert_eq!(
        ticket.scope().configuration_digest,
        config().scope().configuration_digest
    );
    assert_eq!(ticket.log().last_normal_view, current.last_normal_view);
    assert_eq!(
        position(ticket.log().accepted),
        journal.accepted_position().unwrap()
    );
    let mut next = current.clone();
    next.generation += 1;
    next.parent_generation = current.generation;
    next.promised_view = ticket.scope().view;
    next.last_normal_view = ticket.log().last_normal_view;
    next.accepted = position(ticket.log().accepted);
    next.committed = position(ticket.log().committed);
    journal.install_metadata(next).unwrap()
}

fn install_selection(
    journal: OpenGroupJournal,
    selected: InstallTicket,
    operations: &[CanonicalOperation<'_>],
) -> OpenGroupJournal {
    assert_eq!(
        journal.writer().written_position().generation(),
        selected.previous_generation()
    );
    assert_eq!(
        journal.directory().identity().group_id,
        selected.scope().group_id
    );
    let accepted = operations
        .last()
        .map_or(selected.protected_committed(), |operation| Prefix {
            op: OpNumber(operation.op_number),
            digest: logical_operation_digest(operation),
        });
    assert_eq!(accepted, selected.accepted());
    let protected = position(selected.protected_committed());
    let current = journal.directory().current();
    let mut staging = journal
        .begin_suffix_replacement(
            SuffixReplacement {
                expected_current: current,
                protected_committed: protected,
                promised_view: selected.scope().view,
                last_normal_view: selected.scope().view,
                committed: position(selected.committed()),
                writer_generation: selected.generation(),
                segment_capacity: CAPACITY,
                body_encoding: BodyEncoding::Lz4 {
                    min_savings_bytes: 0,
                },
            },
            position(accepted),
            SuffixStreamLimits {
                max_group_operations: 2,
                max_group_body_bytes: 8192,
                max_segments: 8,
                max_staged_bytes: CAPACITY * 8,
                max_source_segment_bytes: CAPACITY,
                max_orphan_probes: 8,
            },
        )
        .unwrap();
    for chunk in operations.chunks(2) {
        staging.append_chunk(chunk).unwrap();
    }
    staging.finish().unwrap()
}

#[derive(Debug)]
struct RecoveringReplica {
    temporary: TempDir,
    journal: OpenGroupJournal,
    core: NormalReplica,
    candidate: CanonicalRecoveryCandidate,
}

fn activated(
    mut pending: InstallingView,
    journal: OpenGroupJournal,
    temporary: TempDir,
) -> RecoveringReplica {
    let ticket = pending.ticket();
    assert_eq!(
        journal.writer().durable_position().generation(),
        ticket.generation()
    );
    assert_eq!(
        journal.directory().manifest().promised_view,
        ticket.scope().view
    );
    assert_eq!(
        journal.directory().manifest().last_normal_view,
        ticket.scope().view
    );
    assert_eq!(
        journal.accepted_position().unwrap(),
        position(ticket.accepted())
    );
    assert_eq!(
        journal.committed_position().unwrap(),
        position(ticket.committed())
    );
    let candidate = journal
        .recover_canonical_candidate(CanonicalRecoveryLimits {
            accepted_transitions: 1,
            retained_identities: 1,
            ..CanonicalRecoveryLimits::default()
        })
        .unwrap();
    assert_eq!(
        candidate.committed_images().committed().revision(),
        ticket.committed().op.0
    );
    assert_eq!(candidate.committed_images().pending_len(), 0);
    let InstallOutcome::Normal(core) = pending.complete(ticket, ticket.committed()).unwrap() else {
        panic!("no superseding view");
    };
    RecoveringReplica {
        temporary,
        journal,
        core,
        candidate,
    }
}

fn finish_recovery(mut replica: RecoveringReplica) -> Replica<JournalIdentityIndex> {
    let committed = replica.core.snapshot().committed;
    assert_eq!(position(committed), replica.candidate.accepted_position());
    assert!(!replica.core.snapshot().ready_for_appends);
    // One activation publication after actual new-view quorum. Normal appends
    // below still commit without a per-response manifest synchronization.
    let mut next = replica.journal.directory().manifest().clone();
    next.parent_generation = next.generation;
    next.generation += 1;
    next.accepted = position(committed);
    next.committed = position(committed);
    let journal = replica.journal.install_metadata(next).unwrap();
    let images = replica.candidate.activate(&journal).unwrap();
    assert_eq!(images.pending_len(), 0);
    replica.core.apply_through(committed).unwrap();
    assert!(replica.core.snapshot().ready_for_appends);
    Replica {
        temporary: replica.temporary,
        journal,
        core: replica.core,
        images,
    }
}

fn confirm_recovered_tail(
    mut primary: RecoveringReplica,
    mut backup: RecoveringReplica,
) -> (Replica<JournalIdentityIndex>, Replica<JournalIdentityIndex>) {
    primary
        .core
        .receive_ack(
            backup.journal.directory().identity().replica_node_id,
            backup.core.acknowledgment().unwrap(),
        )
        .unwrap();
    backup
        .core
        .receive_commit(
            primary.journal.directory().identity().replica_node_id,
            primary.core.announcement().unwrap(),
        )
        .unwrap();
    let primary = finish_recovery(primary);
    let backup = finish_recovery(backup);
    assert_eq!(primary.images.committed(), backup.images.committed());
    (primary, backup)
}

fn apply_committed(replica: &mut Replica<JournalIdentityIndex>) {
    let committed = replica.core.snapshot().committed;
    replica.images.commit_through(committed.op.0).unwrap();
    replica.core.apply_through(committed).unwrap();
}

fn confirm_tail(
    primary: &mut Replica<JournalIdentityIndex>,
    backup: &mut Replica<JournalIdentityIndex>,
) {
    primary
        .core
        .receive_ack(
            backup.journal.directory().identity().replica_node_id,
            backup.core.acknowledgment().unwrap(),
        )
        .unwrap();
    apply_committed(primary);
    backup
        .core
        .receive_commit(
            primary.journal.directory().identity().replica_node_id,
            primary.core.announcement().unwrap(),
        )
        .unwrap();
    apply_committed(backup);
    assert_eq!(primary.images.committed(), backup.images.committed());
}

fn admit(
    replica: &mut Replica<JournalIdentityIndex>,
    operation: CanonicalOperation<'_>,
) -> WriteTicket {
    let body =
        decode_operation_body(operation.kind, operation.body, OperationLimits::default()).unwrap();
    let plan = replica
        .images
        .prepare_group(&[(operation.op_number, body)])
        .unwrap();
    let prepared = ozzy_replication::PreparedOperation::from_verified(
        &operation,
        canonical_body_digest(operation.body),
    );
    let scope = replica.core.snapshot().scope;
    let Admission::Write { ticket, .. } = replica
        .core
        .prepare(config().primary(scope.view), scope, &[prepared])
        .unwrap()
    else {
        panic!("new operation");
    };
    replica.images.install_prepared_group(plan).unwrap();
    ticket
}

fn write_sync(
    replica: &mut Replica<JournalIdentityIndex>,
    operation: CanonicalOperation<'_>,
    ticket: WriteTicket,
) {
    let written = replica.journal.append(&[operation]).unwrap();
    replica.core.complete_write(ticket).unwrap();
    let sync = replica.core.begin_sync().unwrap();
    replica.journal.sync_through(written).unwrap();
    replica.core.complete_sync(sync).unwrap();
}

fn append_after_failover(
    primary: &mut Replica<JournalIdentityIndex>,
    backup: &mut Replica<JournalIdentityIndex>,
) {
    assert!(primary.core.snapshot().ready_for_appends);
    let scope = primary.core.snapshot().scope;
    let previous = primary.core.snapshot().accepted;
    let body = append(2);
    let bytes = encode_operation_body(&body, OperationLimits::default()).unwrap();
    let operation = CanonicalOperation {
        group_id: scope.group_id,
        configuration_epoch: scope.configuration_epoch,
        original_view: scope.view,
        op_number: previous.op.0 + 1,
        previous_digest: previous.digest,
        kind: body.kind(),
        body: &bytes,
    };
    let primary_write = admit(primary, operation);
    let backup_write = admit(backup, operation);
    // Replica transmission/write completes before the primary starts its write.
    write_sync(backup, operation, backup_write);
    primary
        .core
        .receive_ack(
            backup.journal.directory().identity().replica_node_id,
            backup.core.acknowledgment().unwrap(),
        )
        .unwrap();
    assert_eq!(primary.core.snapshot().committed, previous);
    write_sync(primary, operation, primary_write);
    confirm_tail(primary, backup);
    assert_eq!(primary.core.snapshot().applied.op, OpNumber(5));
    assert_eq!(
        primary
            .images
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::new(3)
    );
}

fn verify_reopen(replica: Replica<JournalIdentityIndex>, generation: JournalGeneration) {
    let view = replica.core.snapshot().scope.view;
    let accepted = replica.core.snapshot().accepted;
    let identity = replica.journal.directory().identity();
    let expected = replica.images.committed().clone();
    let key = IdentityKey::operation(OperationId::from_bytes([12; 16]));
    let expected_identity = replica.images.committed_identities().lookup(key).unwrap();
    assert!(expected_identity.is_some());
    drop(replica.core);
    // Disk-backed identity snapshots pin old segments and the exclusive lock.
    // Release them before reopening this same store with a new writer generation.
    drop(replica.images);
    drop(replica.journal);
    let reopened = GroupDirectory::open(
        replica.temporary.path().join("group"),
        identity,
        MetadataLimits::default(),
    )
    .unwrap()
    .recover(
        generation,
        DecodeLimits::default(),
        OperationLimits::default(),
    )
    .unwrap();
    assert_eq!(reopened.directory().manifest().promised_view, view);
    assert_eq!(reopened.directory().manifest().last_normal_view, view);
    assert_eq!(reopened.accepted_position().unwrap(), position(accepted));
    let images = reopened
        .recover_canonical_images(CanonicalRecoveryLimits::default())
        .unwrap();
    assert_eq!(images.speculative(), &expected);
    assert_eq!(
        images.speculative_identities().lookup(key).unwrap(),
        expected_identity
    );
    // Only activation published a commit floor. The next acknowledged append
    // did not add a per-response metadata barrier and remains accepted on reopen.
    assert_eq!(images.committed().revision(), accepted.op.0 - 1);
    let mut records = 0;
    reopened
        .replay_accepted(|replayed| {
            let operation = replayed.operation;
            if let OperationBody::Append(append) =
                decode_operation_body(operation.kind, &operation.body, OperationLimits::default())
                    .unwrap()
            {
                let batch = &append.batches[0];
                assert_eq!(batch.first_offset.get(), records);
                assert_eq!(batch.first_sequence.get(), records);
                assert_eq!(
                    batch.records.get(0).unwrap().message_id,
                    MessageId::from_bytes(u128::from(records + 100).to_be_bytes())
                );
                let record = batch.records.get(0).unwrap();
                let parts = record.parts.iter().collect::<Vec<_>>();
                assert_eq!(parts[0], b"order.created");
                assert_eq!(parts[1], b"{\"sku\":\"sku-42\",\"qty\":3}");
                records += 1;
            }
            Ok::<_, Infallible>(())
        })
        .unwrap();
    assert_eq!(records, 3);
}

fn crash(replica: Replica) -> (TempDir, GroupIdentity) {
    let identity = replica.journal.directory().identity();
    drop(replica.core);
    drop(replica.images);
    drop(replica.journal);
    (replica.temporary, identity)
}

fn restart(
    (temporary, identity): (TempDir, GroupIdentity),
    generation: JournalGeneration,
) -> (TempDir, OpenGroupJournal, ViewChange) {
    let journal = GroupDirectory::open(
        temporary.path().join("group"),
        identity,
        MetadataLimits::default(),
    )
    .unwrap()
    .recover(
        generation,
        DecodeLimits::default(),
        OperationLimits::default(),
    )
    .unwrap();
    let manifest = journal.directory().manifest();
    let prefix = |position: LogPosition| Prefix {
        op: OpNumber(position.op_number),
        digest: position.digest,
    };
    // Fixed test configuration is supplied out of band. Runtime bootstrap must
    // bind that full configuration durably before admitting recovered voters.
    let changing = ViewChange::recover_intact(
        config(),
        identity.replica_node_id,
        generation,
        RecoveredState {
            scope: Scope {
                group_id: identity.group_id,
                configuration_epoch: manifest.configuration_epoch,
                view: manifest.promised_view,
                ..config().scope()
            },
            log: FrozenLog {
                last_normal_view: manifest.last_normal_view,
                accepted: prefix(journal.accepted_position().unwrap()),
                committed: prefix(journal.committed_position().unwrap()),
            },
        },
        PipelineLimits {
            max_operations: 8,
            max_body_bytes: 8192,
        },
    )
    .unwrap();
    (temporary, journal, changing)
}

#[test]
fn restarted_intact_quorum_recovers_unannounced_commit_then_appends() {
    let bytes = bodies();
    let operations = operations(&bytes);
    let mut old_primary = replica(0, &operations);
    let old_backup = replica(1, &operations);
    let offline = replica(2, &[]);
    old_primary
        .core
        .receive_ack(node(1), old_backup.core.acknowledgment().unwrap())
        .unwrap();
    let acknowledged = old_primary.core.snapshot().committed;
    old_primary
        .images
        .commit_through(acknowledged.op.0)
        .unwrap();
    old_primary.core.apply_through(acknowledged).unwrap();
    // Drop every old core before reopening anything. Neither surviving normal
    // authority nor a persisted COMMIT marker can supply the acknowledged result.
    let _offline_disk = crash(offline);
    let primary_disk = crash(old_primary);
    let backup_disk = crash(old_backup);
    let (backup_dir, backup_journal, mut backup) = restart(primary_disk, JournalGeneration(300));
    let (primary_dir, primary_journal, mut primary) = restart(backup_disk, JournalGeneration(301));
    for replica in [&primary, &backup] {
        assert_eq!(replica.normal_snapshot().committed, Prefix::GENESIS);
        assert_eq!(replica.normal_snapshot().accepted, acknowledged);
        assert_eq!(replica.scope().view, 1);
    }
    let ticket = primary.begin_promise().unwrap();
    let primary_journal = publish_promise(primary_journal, ticket);
    primary.complete_promise(ticket).unwrap();
    let ticket = backup.begin_promise().unwrap();
    let backup_journal = publish_promise(backup_journal, ticket);
    backup.complete_promise(ticket).unwrap();
    primary
        .receive_start(node(0), wire_start(0, backup.start_message().unwrap()))
        .unwrap();
    backup
        .receive_start(node(1), wire_start(1, primary.start_message().unwrap()))
        .unwrap();
    primary.report().unwrap();
    primary
        .receive_report(node(0), wire_report(0, backup.report().unwrap()))
        .unwrap();
    assert_eq!(
        primary.select(|_, _| None).unwrap().source().accepted,
        acknowledged
    );
    let prepared: Vec<_> = operations
        .iter()
        .map(|operation| {
            ozzy_replication::PreparedOperation::from_verified(
                operation,
                canonical_body_digest(operation.body),
            )
        })
        .collect();
    let mut pending = primary
        .begin_primary_install(JournalGeneration(302))
        .unwrap();
    for chunk in prepared.chunks(2) {
        pending.validate_suffix(chunk).unwrap();
    }
    let installed = install_selection(primary_journal, pending.ticket(), &operations);
    let primary = activated(pending, installed, primary_dir);
    let mut pending = backup
        .begin_backup_install(
            node(1),
            wire_install(1, primary.core.start_view().unwrap().unwrap()),
            JournalGeneration(303),
            |_, _| None,
        )
        .unwrap();
    for chunk in prepared.chunks(2) {
        pending.validate_suffix(chunk).unwrap();
    }
    let installed = install_selection(backup_journal, pending.ticket(), &operations);
    let backup = activated(pending, installed, backup_dir);
    let (mut primary, mut backup) = confirm_recovered_tail(primary, backup);
    append_after_failover(&mut primary, &mut backup);
    verify_reopen(primary, JournalGeneration(304));
    verify_reopen(backup, JournalGeneration(305));
}

#[test]
fn promise_reopen_keeps_promised_view_without_claiming_normal_installation() {
    for notify in [false, true] {
        let bytes = bodies();
        let operations = operations(&bytes);
        let replica = replica(1, &operations);
        let mut election = replica.core.into_view_change(1).unwrap();
        let ticket = election.begin_promise().unwrap();
        let journal = publish_promise(replica.journal, ticket);
        assert_eq!(
            election.start_message(),
            Err(ViewChangeError::PromiseRequired)
        );
        if notify {
            election.complete_promise(ticket).unwrap();
            assert_eq!(election.start_message().unwrap().scope.view, 1);
        }
        let identity = journal.directory().identity();
        drop(journal);
        // Successful publication followed by lost completion must still fence
        // restart. Reopening disk does not itself construct a normal replica.
        let reopened = GroupDirectory::open(
            replica.temporary.path().join("group"),
            identity,
            MetadataLimits::default(),
        )
        .unwrap()
        .recover(
            JournalGeneration(101),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
        let manifest = reopened.directory().manifest();
        assert_eq!(manifest.promised_view, 1);
        assert_eq!(manifest.last_normal_view, 0);
        assert_eq!(manifest.accepted, position(ticket.log().accepted));
        assert_eq!(manifest.committed, LogPosition::GENESIS);
        let images = reopened
            .recover_canonical_images(CanonicalRecoveryLimits::default())
            .unwrap();
        assert_eq!(images.committed().revision(), 0);
        assert_eq!(images.speculative().revision(), 4);
    }
}

#[test]
fn installed_view_resumes_record_appends_after_primary_loss_and_reopens_exact_history() {
    let bytes = bodies();
    let operations = operations(&bytes);
    let mut primary = replica(0, &operations);
    let source = replica(1, &operations);
    let candidate = replica(2, &[]);
    primary
        .core
        .receive_ack(node(1), source.core.acknowledgment().unwrap())
        .unwrap();
    let acknowledged = primary.core.snapshot().committed;
    primary.images.commit_through(acknowledged.op.0).unwrap();
    primary.core.apply_through(acknowledged).unwrap();
    assert_eq!(acknowledged.op, OpNumber(4));
    // Keep its disk, but make the old primary unavailable. No COMMIT
    // announcement or second per-response metadata synchronization happened.
    let _old_primary_disk = primary.temporary;
    drop(primary.core);
    drop(primary.journal);

    let mut source_election = source.core.into_view_change(2).unwrap();
    let source_promise = source_election.begin_promise().unwrap();
    let source_journal = publish_promise(source.journal, source_promise);
    source_election.complete_promise(source_promise).unwrap();
    let mut election = candidate.core.into_view_change(2).unwrap();
    let promise = election.begin_promise().unwrap();
    let journal = publish_promise(candidate.journal, promise);
    election.complete_promise(promise).unwrap();
    source_election
        .receive_start(node(2), wire_start(2, election.start_message().unwrap()))
        .unwrap();
    election
        .receive_start(
            node(1),
            wire_start(1, source_election.start_message().unwrap()),
        )
        .unwrap();
    election.report().unwrap();
    election
        .receive_report(node(1), wire_report(1, source_election.report().unwrap()))
        .unwrap();
    let selected = election.select(|_, _| None).unwrap();
    assert_eq!(selected.source().accepted, acknowledged);
    assert_eq!(selected.committed(), Prefix::GENESIS);
    assert_eq!(selected.source().generation, source_promise.generation());

    let pin = source_journal.pin_segments(&[1]).unwrap();
    let transferred = transfer_history(&source_journal, selected);
    assert_eq!(transferred, bytes);
    let transferred_operations = self::operations(&transferred);
    assert_eq!(
        logical_operation_digest(transferred_operations.last().unwrap()),
        selected.source().accepted.digest
    );
    let prepared: Vec<_> = transferred_operations
        .iter()
        .map(|operation| {
            ozzy_replication::PreparedOperation::from_verified(
                operation,
                canonical_body_digest(operation.body),
            )
        })
        .collect();
    let mut pending = election
        .begin_primary_install(JournalGeneration(200))
        .unwrap();
    for chunk in prepared.chunks(2) {
        pending.validate_suffix(chunk).unwrap();
    }
    let installed = install_selection(journal, pending.ticket(), &transferred_operations);
    drop(pin);
    let primary = activated(pending, installed, candidate.temporary);
    assert!(!primary.core.snapshot().ready_for_appends);
    let start = wire_install(2, primary.core.start_view().unwrap().unwrap());
    let mut pending = source_election
        .begin_backup_install(node(2), start, JournalGeneration(202), |_, _| None)
        .unwrap();
    for chunk in prepared.chunks(2) {
        pending.validate_suffix(chunk).unwrap();
    }
    let installed = install_selection(source_journal, pending.ticket(), &transferred_operations);
    let backup = activated(pending, installed, source.temporary);
    let (mut primary, mut backup) = confirm_recovered_tail(primary, backup);
    append_after_failover(&mut primary, &mut backup);
    verify_reopen(primary, JournalGeneration(201));
    verify_reopen(backup, JournalGeneration(203));
}

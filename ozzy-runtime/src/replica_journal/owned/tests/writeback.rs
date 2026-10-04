use super::*;
use crate::replica_journal::{JournalCompletion, OwnedWriteStep};
use ozzy_journal::operation::{CanonicalOperation, OperationKind, logical_operation_digest};
use ozzy_replication::{Admission, OpNumber, WriteTicket, driver::ReplicaDriver, wire::Control};

pub(super) struct Replica {
    pub(super) journal: OwnedJournal,
    pub(super) driver: ReplicaDriver,
    pub(super) config: OwnedConfig,
}

pub(super) fn replica(
    controller: &mut Controller,
    io: Local,
    index: u8,
    mode: QuorumPolicy,
    group_bytes: usize,
) -> Replica {
    let mut config = config(&format!("/broker-{index}"), 7, mode);
    config.identity.replica_node_id = NodeId::from_bytes([index + 1; 16]);
    config.append_limits.max_operations = 16;
    config.recovery.accepted_transitions = 16;
    config.writeback.max_operations = 16;
    config.write_group_bytes = group_bytes;
    // Match physical decode/scratch bounds to this fixture's admitted groups.
    config.limits.decode.max_entries = config.writeback.max_operations;
    config.limits.decode.max_decoded_body_bytes = config.writeback.max_body_bytes;
    config.limits.decode.max_group_decoded_body_bytes = config.writeback.max_body_bytes;
    config.limits.operations.max_payload_bytes = config.writeback.max_body_bytes;

    let (mut journal, startup) = drive(
        controller,
        OwnedJournal::format_new(
            config.clone(),
            io,
            JournalGeneration(u128::from(index) + 1),
            32768,
        ),
    )
    .unwrap();
    journal
        .bind_append_memory(&payload_owner(1024 * 1024))
        .unwrap();
    let driver = startup
        .into_driver(Duration::ZERO, timing(), config.append_limits)
        .unwrap();
    Replica {
        journal,
        driver,
        config,
    }
}

pub(super) fn admit(
    controller: &mut Controller,
    replica: &mut Replica,
    count: usize,
) -> (WriteTicket, AppendBuffer, JournalCompletion<WriteTicket>) {
    let first = replica.driver.begin_validation().unwrap().accepted().op.0 + 1;
    admit_encoded(
        controller,
        replica,
        (0..count).map(|index| {
            (
                OperationKind::Barrier,
                vec![(first + index as u64) as u8; 16],
            )
        }),
    )
}

pub(super) fn admit_encoded(
    controller: &mut Controller,
    replica: &mut Replica,
    bodies: impl IntoIterator<Item = (OperationKind, Vec<u8>)>,
) -> (WriteTicket, AppendBuffer, JournalCompletion<WriteTicket>) {
    let ticket = replica.driver.begin_validation().unwrap();
    let mut buffer = replica.journal.lease_append_buffer().unwrap();
    let mut end = ticket.accepted();
    for (kind, bytes) in bodies {
        let operation = CanonicalOperation {
            group_id: ticket.scope().group_id,
            configuration_epoch: ticket.scope().configuration_epoch,
            original_view: ticket.scope().view,
            op_number: end.op.0 + 1,
            previous_digest: end.digest,
            kind,
            body: &bytes,
        };
        end = Prefix {
            op: OpNumber(operation.op_number),
            digest: logical_operation_digest(&operation),
        };
        buffer.push(operation).unwrap();
    }
    let mut validated = drive(controller, replica.journal.validate_append(ticket, buffer)).unwrap();
    assert!(replica.journal.can_admit(&mut validated));
    let Admission::Write { ticket: write, .. } = replica
        .driver
        .prepare_validated(
            NodeId::from_bytes([1; 16]),
            ticket,
            validated.prepared(),
            Duration::ZERO,
        )
        .unwrap()
    else {
        panic!("write ticket")
    };
    replica
        .journal
        .admit_append(write, validated)
        .unwrap()
        .into_parts()
}

pub(super) fn prepare(replica: &mut Replica) -> PreparedWrite {
    let OwnedWriteStep::Write(work) = replica.journal.prepare_write().unwrap() else {
        panic!("physical write")
    };
    work
}

pub(super) fn finish(
    controller: &mut Controller,
    replica: &mut Replica,
    work: PreparedWrite,
) -> WrittenRecords {
    let completed = drive(controller, work.write());
    replica.journal.complete_write(completed).unwrap()
}

pub(super) fn settle(replica: &mut Replica, mut receipt: JournalCompletion<WriteTicket>) {
    let Poll::Ready(Ok(ticket)) = poll(Pin::new(&mut receipt)) else {
        panic!("installed write completion")
    };
    replica.driver.complete_write(ticket).unwrap();
}

pub(super) fn synchronize(controller: &mut Controller, replica: &mut Replica) {
    let ticket = replica.driver.begin_sync().unwrap();
    let ticket = drive(controller, replica.journal.sync(ticket)).unwrap();
    replica
        .driver
        .complete_sync(ticket, Duration::ZERO)
        .unwrap();
}

pub(super) fn confirm(primary: &mut Replica, backup: &Replica, mode: QuorumPolicy) {
    let normal = backup.driver.normal().unwrap();
    let message = match mode {
        QuorumPolicy::Durable => Control::PrepareOk {
            ack: normal.acknowledgment().unwrap(),
        },
        QuorumPolicy::Replicated => Control::PrepareRetained {
            ack: normal.retained_acknowledgment().unwrap(),
        },
    };
    primary
        .driver
        .receive(
            backup.config.identity.replica_node_id,
            message,
            Duration::ZERO,
        )
        .unwrap();
    let apply = primary.driver.begin_validation().unwrap();
    primary.journal.apply(apply).unwrap();
    primary.driver.apply_through(apply.committed()).unwrap();
}

#[test]
fn owned_admission_groups_physical_writes_and_keeps_reused_arena_bytes() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io.clone(), 0, QuorumPolicy::Durable, 8192);
    let (first, mut arena, receipt1) = admit(&mut controller, &mut replica, 1);
    let (second, _, receipt2) = admit(&mut controller, &mut replica, 1);
    let accepted = replica.driver.normal().unwrap().snapshot().accepted;
    assert!(
        controller.jobs().is_empty(),
        "logical admission submits no file jobs"
    );
    assert_eq!(
        replica.journal.images().unwrap().speculative().revision(),
        2
    );
    assert_eq!(replica.journal.images().unwrap().committed().revision(), 0);
    arena.clear();
    let work = prepare(&mut replica);
    let records = finish(&mut controller, &mut replica, work);
    assert_eq!(records.locations.len(), 2);
    assert_eq!(records.locations[0].op_number, first.through().0);
    assert_eq!(records.locations[1].op_number, second.through().0);
    settle(&mut replica, receipt1);
    settle(&mut replica, receipt2);
    synchronize(&mut controller, &mut replica);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
    let (journal, startup) = drive(
        &mut controller,
        OwnedJournal::open(replica.config, io, JournalGeneration(100)),
    )
    .unwrap();
    assert_eq!(startup.recovered().unwrap().log.accepted, accepted);
    drive(&mut controller, journal.shutdown()).unwrap();
}

pub(super) fn initialize_writer(
    controller: &mut Controller,
    replica: &mut Replica,
) -> JournalCompletion<WriteTicket> {
    use ozzy_journal::operation::{
        CreatePartition, OpenProducer, OperationBody, RetentionPolicy, encode_operation_body,
    };
    use ozzy_proto::{
        OperationId, OwnerEpoch, PartitionId, PartitionIncarnation, ProducerEpoch, ProducerId,
    };
    let limits = replica.config.limits.operations;
    let partition = PartitionIncarnation::from_bytes([11; 16]);
    let producer_id = ProducerId::from_bytes([12; 16]);
    let bodies = [
        OperationBody::CreatePartition(CreatePartition {
            partition,
            stream: "stream",
            topic: "orders",
            partition_id: PartitionId::ZERO,
            owner_epoch: OwnerEpoch::INITIAL,
            retention: RetentionPolicy::default(),
        }),
        OperationBody::OpenProducer(OpenProducer {
            partition,
            producer_id,
            expected_epoch: None,
            new_epoch: ProducerEpoch::INITIAL,
            operation_id: OperationId::from_bytes([13; 16]),
        }),
    ];
    let (_, _, receipt) = admit_encoded(
        controller,
        replica,
        bodies
            .iter()
            .map(|body| (body.kind(), encode_operation_body(body, limits).unwrap())),
    );
    receipt
}

fn packed_append_body(limits: ozzy_journal::operation::OperationLimits, payload: &[u8]) -> Vec<u8> {
    use ozzy_journal::operation::{
        Append, AppendBatch, AppendPackResult, AppendPackScratch, AppendRecord, OperationBody,
        encode_operation_body, pack_append_payload,
    };
    use ozzy_proto::{
        MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
        ProducerSequence,
    };
    let partition = PartitionIncarnation::from_bytes([11; 16]);
    let producer_id = ProducerId::from_bytes([12; 16]);
    let append = OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition,
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id,
            producer_epoch: ProducerEpoch::INITIAL,
            first_sequence: ProducerSequence::ZERO,
            first_offset: Offset::ZERO,
            append_timestamp_millis: 123,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([14; 16]),
                parts: vec![payload].into(),
            }]
            .into(),
        }],
    });
    let mut packed = encode_operation_body(&append, limits).unwrap();
    assert!(matches!(
        pack_append_payload(
            &mut packed,
            limits,
            &mut AppendPackScratch::new(limits.max_payload_bytes)
        )
        .unwrap(),
        AppendPackResult::Packed { .. }
    ));
    packed
}

#[test]
fn owned_sdk_lz4_payload_survives_write_history_and_recovery_byte_exact() {
    use ozzy_proto::{Offset, PartitionIncarnation};
    use ozzy_replication::{LogSource, wire::FetchOps};

    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io.clone(), 0, QuorumPolicy::Durable, 8192);
    let partition = PartitionIncarnation::from_bytes([11; 16]);
    let receipt = initialize_writer(&mut controller, &mut replica);
    let predecessor = replica.driver.normal().unwrap().snapshot().accepted;
    let payload = vec![42; 4096];
    let packed = packed_append_body(replica.config.limits.operations, &payload);
    let (_, mut reused, append_receipt) = admit_encoded(
        &mut controller,
        &mut replica,
        [(OperationKind::Append, packed.clone())],
    );
    reused.clear();
    let work = prepare(&mut replica);
    let records = finish(&mut controller, &mut replica, work);
    assert_eq!(records.locations.len(), 3);
    let append_records = &records.records[2];
    let decoded = append_records.decoded_batches();
    let record = append_records
        .record(partition, Offset::ZERO, &decoded)
        .unwrap();
    assert_eq!(record.parts().next().unwrap(), payload.as_slice());
    settle(&mut replica, receipt);
    settle(&mut replica, append_receipt);
    synchronize(&mut controller, &mut replica);
    let source = LogSource {
        voter: replica.config.identity.replica_node_id,
        generation: JournalGeneration(1),
        accepted: replica.driver.normal().unwrap().snapshot().accepted,
    };
    replica.journal.capture_history(source).unwrap();
    let request = FetchOps {
        scope: replica.journal.scope(),
        request_id: ozzy_proto::RequestId::from_bytes([71; 16]),
        source,
        predecessor,
        max_operations: 1,
        max_body_bytes: 8192,
    };
    let arena = replica.journal.lease_append_buffer().unwrap();
    let history = drive(
        &mut controller,
        replica.journal.fetch_history(request, arena),
    )
    .unwrap();
    assert_eq!(history.buffer().operations().next().unwrap().body, packed);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
    let (journal, startup) = drive(
        &mut controller,
        OwnedJournal::open(replica.config, io, JournalGeneration(100)),
    )
    .unwrap();
    assert_eq!(startup.recovered().unwrap().log.accepted, source.accepted);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owned_follower_writes_primary_validated_lz4_payload() {
    use ozzy_journal::operation::{canonical_body_digest, decode_append_summary};
    use ozzy_proto::{LinkSessionId, Offset, PartitionIncarnation};
    use ozzy_replication::wire::{
        PeerBinding, Prepare, ReplicaMessage, WireLimits, decode, encode_prepare_metadata,
    };

    let (mut controller, io) = setup();
    let mut follower = replica(&mut controller, io, 1, QuorumPolicy::Durable, 8192);
    let initial = initialize_writer(&mut controller, &mut follower);
    let limits = follower.config.limits.operations;
    let partition = PartitionIncarnation::from_bytes([11; 16]);
    let payload = vec![42; 4096];
    let packed = packed_append_body(limits, &payload);
    // The primary validates before canonical construction. Decode only proves
    // that the transmitted bytes match the primary's body digest.
    decode_append_summary(&packed, limits).unwrap();
    let ticket = follower.driver.begin_validation().unwrap();
    let predecessor = ticket.accepted();
    let primary = NodeId::from_bytes([1; 16]);
    let session = LinkSessionId::from_bytes([9; 16]);
    let operation = ozzy_replication::wire::Operation::from_verified(
        CanonicalOperation {
            group_id: ticket.scope().group_id,
            configuration_epoch: ticket.scope().configuration_epoch,
            original_view: ticket.scope().view,
            op_number: predecessor.op.0 + 1,
            previous_digest: predecessor.digest,
            kind: OperationKind::Append,
            body: &packed,
        },
        canonical_body_digest(&packed),
    );
    let operations = [operation];
    let prepare_message = Prepare {
        scope: ticket.scope(),
        committed: Prefix::GENESIS,
        operations: &operations,
    };
    let mut metadata = [0; 512];
    let wire = encode_prepare_metadata(
        primary,
        session,
        prepare_message,
        &mut metadata,
        WireLimits::default(),
    )
    .unwrap();
    let binding = PeerBinding::new(
        follower.config.configuration.configuration(),
        primary,
        session,
    )
    .unwrap();
    let frames: [&[u8]; 3] = [&wire.header, &metadata[..wire.metadata_bytes], &packed];
    let ReplicaMessage::Prepare(batch) = decode(&frames, binding, WireLimits::default()).unwrap()
    else {
        panic!("prepare");
    };
    let mut buffer = follower.journal.lease_append_buffer().unwrap();
    buffer
        .push_primary_verified(batch.verified_operations().next().unwrap())
        .unwrap();
    assert!(buffer.primary_payloads_validated());
    let mut validated = drive(
        &mut controller,
        follower.journal.validate_append(ticket, buffer),
    )
    .unwrap();
    assert!(follower.journal.can_admit(&mut validated));
    let Admission::Write { ticket: write, .. } = follower
        .driver
        .prepare_validated(primary, ticket, validated.prepared(), Duration::ZERO)
        .unwrap()
    else {
        panic!("write ticket");
    };
    let (_, _, receipt) = follower
        .journal
        .admit_append(write, validated)
        .unwrap()
        .into_parts();
    let work = prepare(&mut follower);
    let records = finish(&mut controller, &mut follower, work);
    let last = records.records.last().unwrap();
    let decoded = last.decoded_batches();
    let stored = last.record(partition, Offset::ZERO, &decoded).unwrap();
    assert_eq!(stored.parts().next().unwrap(), payload.as_slice());
    settle(&mut follower, initial);
    settle(&mut follower, receipt);
    synchronize(&mut controller, &mut follower);
    drive(&mut controller, follower.journal.shutdown()).unwrap();
}

#[test]
fn owned_split_request_completes_only_after_all_physical_parts() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 16);
    let (ticket, _, mut receipt) = admit(&mut controller, &mut replica, 3);
    let first = prepare(&mut replica);
    let second = prepare(&mut replica);
    let third = prepare(&mut replica);
    assert!(matches!(
        replica.journal.prepare_write().unwrap(),
        OwnedWriteStep::Waiting
    ));
    // Actual effects may finish out of order. Installation may not.
    let second = drive(&mut controller, second.write());
    let third = drive(&mut controller, third.write());
    finish(&mut controller, &mut replica, first);
    assert!(poll(Pin::new(&mut receipt)).is_pending());
    replica.journal.complete_write(second).unwrap();
    assert!(poll(Pin::new(&mut receipt)).is_pending());
    replica.journal.complete_write(third).unwrap();
    let Poll::Ready(Ok(completed)) = poll(Pin::new(&mut receipt)) else {
        panic!("whole request complete")
    };
    assert_eq!(completed, ticket);
    replica.driver.complete_write(completed).unwrap();
    synchronize(&mut controller, &mut replica);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_old_barrier_excludes_later_unobserved_physical_write() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 16);
    let (_, _, first) = admit(&mut controller, &mut replica, 1);
    let (_, _, second) = admit(&mut controller, &mut replica, 1);
    let work1 = prepare(&mut replica);
    let work2 = prepare(&mut replica);
    finish(&mut controller, &mut replica, work1);
    settle(&mut replica, first);
    let unobserved = drive(&mut controller, work2.write());
    synchronize(&mut controller, &mut replica);
    assert_eq!(
        replica.driver.normal().unwrap().snapshot().journal.durable,
        OpNumber(1)
    );
    assert_eq!(
        replica
            .journal
            .journal
            .readable()
            .unwrap()
            .accepted_position()
            .unwrap()
            .op_number,
        1
    );
    replica.journal.complete_write(unobserved).unwrap();
    settle(&mut replica, second);
    synchronize(&mut controller, &mut replica);
    assert_eq!(
        replica.driver.normal().unwrap().snapshot().journal.durable,
        OpNumber(2)
    );
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_both_confirmation_policies_apply_at_their_actual_boundary() {
    for mode in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut primary = replica(&mut controller, io.clone(), 0, mode, 8192);
        let mut backup = replica(&mut controller, io, 1, mode, 8192);
        let (_, _, first) = admit(&mut controller, &mut primary, 2);
        let (_, _, second) = admit(&mut controller, &mut backup, 2);
        if mode == QuorumPolicy::Replicated {
            confirm(&mut primary, &backup, mode);
            assert_eq!(primary.journal.images().unwrap().committed().revision(), 2);
            assert_eq!(
                primary
                    .journal
                    .journal
                    .readable()
                    .unwrap()
                    .written_position()
                    .unwrap()
                    .op_number,
                0
            );
        }
        let work = prepare(&mut primary);
        finish(&mut controller, &mut primary, work);
        settle(&mut primary, first);
        let work = prepare(&mut backup);
        finish(&mut controller, &mut backup, work);
        settle(&mut backup, second);
        synchronize(&mut controller, &mut primary);
        synchronize(&mut controller, &mut backup);
        if mode == QuorumPolicy::Durable {
            confirm(&mut primary, &backup, mode);
        }
        assert_eq!(primary.journal.images().unwrap().committed().revision(), 2);
        drive(&mut controller, primary.journal.shutdown()).unwrap();
        drive(&mut controller, backup.journal.shutdown()).unwrap();
    }
}

#[test]
fn owned_roll_keeps_partly_written_request_and_accepts_while_io_is_pending() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 16);
    let (_, _, first) = admit(&mut controller, &mut replica, 8);
    for _ in 0..7 {
        let work = prepare(&mut replica);
        finish(&mut controller, &mut replica, work);
    }
    assert!(matches!(
        replica.journal.prepare_write().unwrap(),
        OwnedWriteStep::RollRequired
    ));
    let roll = replica.journal.begin_roll(16).unwrap();
    let (_, _, second) = admit(&mut controller, &mut replica, 1);
    assert!(matches!(
        replica.journal.prepare_write().unwrap(),
        OwnedWriteStep::Waiting
    ));
    let done = drive(&mut controller, roll.publish());
    replica.journal.complete_roll(done).unwrap();
    let work = prepare(&mut replica);
    assert_eq!(finish(&mut controller, &mut replica, work).segment_id, 2);
    settle(&mut replica, first);
    let work = prepare(&mut replica);
    finish(&mut controller, &mut replica, work);
    settle(&mut replica, second);
    synchronize(&mut controller, &mut replica);
    assert_eq!(
        replica.driver.normal().unwrap().snapshot().journal.durable,
        OpNumber(9)
    );
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_canceled_physical_write_fences_owner_without_freeing_backend_work() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io.clone(), 0, QuorumPolicy::Durable, 16);
    let (_, _, receipt) = admit(&mut controller, &mut replica, 1);
    let work = prepare(&mut replica);
    let held = {
        let mut future = std::pin::pin!(work.write());
        assert!(poll(future.as_mut()).is_pending());
        controller.jobs()[0].0
    };
    assert!(replica.journal.is_faulted());
    assert!(matches!(
        replica.journal.prepare_write(),
        Err(JournalError::Faulted)
    ));
    drop(replica.journal);
    assert!(poll(std::pin::pin!(receipt)).is_ready());
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    let (journal, _) = drive(
        &mut controller,
        OwnedJournal::open(replica.config, io, JournalGeneration(100)),
    )
    .unwrap();
    assert!(journal.images().is_err());
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owned_reordered_installation_fences_even_when_both_writes_succeeded() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 16);
    let (_, _, _) = admit(&mut controller, &mut replica, 2);
    let first = prepare(&mut replica);
    let second = prepare(&mut replica);
    let second = drive(&mut controller, second.write());
    assert!(replica.journal.complete_write(second).is_err());
    assert!(replica.journal.is_faulted());
    drop(first);
}

#[test]
fn owned_dropped_response_does_not_cancel_admitted_data() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 16);
    let (ticket, _, receipt) = admit(&mut controller, &mut replica, 1);
    drop(receipt);
    let work = prepare(&mut replica);
    finish(&mut controller, &mut replica, work);
    assert!(!replica.journal.is_faulted());
    replica.driver.complete_write(ticket).unwrap();
    synchronize(&mut controller, &mut replica);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

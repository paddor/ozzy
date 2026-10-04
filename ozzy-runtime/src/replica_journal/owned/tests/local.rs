use super::proposal::{coordinates, request};
use super::*;
use crate::replica_journal::{
    JournalCompletion, PartitionReadLimits, ProposalBuffer, ProposalValidation,
};
use ozzy_journal::operation::{
    CreatePartition, OpenProducer, OperationBody, RetentionPolicy, encode_operation_body,
};
use ozzy_proto::{
    Offset, OperationId, OwnerEpoch, PartitionId, PartitionIncarnation, ProducerEpoch, ProducerId,
};
use ozzy_replication::{
    WriteTicket,
    local::{Configuration, Driver},
};

mod actor;
mod cooperation;
mod faults;
mod native;
mod partition;
mod producer_session;
mod retention;

fn local_config() -> OwnedConfig<Configuration> {
    let base = config("/local", 7, QuorumPolicy::Durable);
    OwnedConfig {
        configuration: Configuration::new(
            base.identity.group_id,
            1,
            base.identity.replica_node_id,
            Digest::from_bytes([20; 32]),
        )
        .unwrap(),
        root: base.root,
        identity: base.identity,
        limits: base.limits,
        recovery: base.recovery,
        append_buffers: base.append_buffers,
        append_limits: base.append_limits,
        writeback: base.writeback,
        write_group_bytes: base.write_group_bytes,
        reads: base.reads,
    }
}

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([11; 16])
}

fn admit(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    buffer: ProposalBuffer,
) -> JournalCompletion<WriteTicket> {
    let ticket = driver.begin_validation().unwrap();
    let result = drive(controller, journal.propose_append(ticket, buffer, 777));
    let ProposalValidation::Ready(mut validated) = result else {
        panic!("fresh proposal: {result:?}");
    };
    assert!(journal.can_admit(&mut validated));
    let write = driver
        .prepare_validated(ticket, validated.prepared())
        .unwrap();
    journal
        .admit_append(write, validated)
        .unwrap()
        .into_parts()
        .2
}

fn write(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    mut receipt: JournalCompletion<WriteTicket>,
) {
    loop {
        if let Poll::Ready(result) = poll(Pin::new(&mut receipt)) {
            driver.complete_write(result.unwrap()).unwrap();
            return;
        }
        match journal.prepare_write().unwrap() {
            WriteStep::Write(work) => {
                let completed = drive(controller, work.write());
                journal.complete_write(completed).unwrap();
            }
            WriteStep::RollRequired => {
                let work = journal.begin_roll(16).unwrap();
                let completed = drive(controller, work.publish());
                journal.complete_roll(completed).unwrap();
            }
            other => panic!("write did not progress: {other:?}"),
        }
    }
}

fn sync_apply(controller: &mut Controller, journal: &mut OwnedJournal, driver: &mut Driver) {
    let ticket = driver.begin_sync().unwrap();
    let work = journal.begin_sync(ticket).unwrap();
    let done = drive(controller, work.publish());
    driver
        .complete_sync(journal.complete_sync(done).unwrap())
        .unwrap();
    let ticket = driver.begin_validation().unwrap();
    journal.apply(ticket).unwrap();
    driver.apply_through(ticket.committed()).unwrap();
}

fn initialize(controller: &mut Controller, journal: &mut OwnedJournal, driver: &mut Driver) {
    initialize_with_writers(controller, journal, driver, &[12, 30]);
}

fn initialize_with_writers(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    writers: &[u8],
) {
    initialize_partition(
        controller,
        journal,
        driver,
        writers,
        partition(),
        PartitionId::ZERO,
    );
}

fn initialize_partition(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    writers: &[u8],
    incarnation: PartitionIncarnation,
    number: PartitionId,
) {
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    let create = OperationBody::CreatePartition(CreatePartition {
        partition: incarnation,
        stream: "stream",
        topic: "orders",
        partition_id: number,
        owner_epoch: OwnerEpoch::INITIAL,
        retention: RetentionPolicy::default(),
    });
    buffer
        .push(
            create.kind(),
            &encode_operation_body(&create, journal.limits.operations).unwrap(),
        )
        .unwrap();
    for &writer in writers {
        let body = OperationBody::OpenProducer(OpenProducer {
            partition: incarnation,
            producer_id: ProducerId::from_bytes([writer; 16]),
            expected_epoch: None,
            new_epoch: ProducerEpoch::INITIAL,
            operation_id: OperationId::from_bytes([writer + 1; 16]),
        });
        buffer
            .push(
                body.kind(),
                &encode_operation_body(&body, journal.limits.operations).unwrap(),
            )
            .unwrap();
    }
    let receipt = admit(controller, journal, driver, buffer);
    write(controller, journal, driver, receipt);
    sync_apply(controller, journal, driver);
}

fn append(journal: &OwnedJournal, writer: u8, sequence: u64) -> ProposalBuffer {
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer.prepare_append(request(writer, sequence, 1)).unwrap();
    buffer
}

fn read(controller: &mut Controller, journal: &mut OwnedJournal, driver: &Driver, expected: usize) {
    let cursor = journal
        .open_reader(
            driver.begin_validation().unwrap(),
            partition(),
            Some(Offset::ZERO),
        )
        .unwrap();
    let work = journal
        .prepare_read(
            cursor,
            PartitionReadLimits {
                max_records: 8,
                max_parts: 16,
                max_payload_bytes: 8192,
            },
            journal.lease_append_buffer().unwrap().into(),
        )
        .unwrap();
    let done = drive(controller, work.read());
    let result = journal.complete_read(done).unwrap();
    assert_eq!(result.buffer.records().len(), expected);
    assert_eq!(result.cursor.next_offset(), Offset::new(expected as u64));
}

#[test]
fn local_owned_journal_requires_observed_barrier_and_application_for_reader_visibility() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(local_config(), io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let first = append(&journal, 12, 0);
    let receipt = admit(&mut controller, &mut journal, &mut driver, first);
    let before = driver.snapshot().committed;
    read(&mut controller, &mut journal, &driver, 0);
    write(&mut controller, &mut journal, &mut driver, receipt);
    read(&mut controller, &mut journal, &driver, 0);
    let sync = driver.begin_sync().unwrap();
    let work = journal.begin_sync(sync).unwrap();
    let second = append(&journal, 30, 0);
    let receipt = admit(&mut controller, &mut journal, &mut driver, second);
    write(&mut controller, &mut journal, &mut driver, receipt);
    let done = drive(&mut controller, work.publish());
    assert_eq!(
        driver.snapshot().committed,
        before,
        "unobserved barrier cannot confirm"
    );
    driver
        .complete_sync(journal.complete_sync(done).unwrap())
        .unwrap();
    read(&mut controller, &mut journal, &driver, 0);
    let ticket = driver.begin_validation().unwrap();
    journal.apply(ticket).unwrap();
    driver.apply_through(ticket.committed()).unwrap();
    read(&mut controller, &mut journal, &driver, 1);
    sync_apply(&mut controller, &mut journal, &mut driver);
    read(&mut controller, &mut journal, &driver, 2);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn local_power_loss_restart_preserves_shared_writer_offsets_and_exact_retries() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(local_config(), io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    for (writer, sequence) in [(12, 0), (30, 0), (12, 1)] {
        let buffer = append(&journal, writer, sequence);
        let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
        write(&mut controller, &mut journal, &mut driver, receipt);
    }
    sync_apply(&mut controller, &mut journal, &mut driver);
    let confirmed = driver.snapshot().committed;
    let stale = driver.begin_validation().unwrap();
    // Crash the byte backend, without an orderly journal shutdown.
    let (image, _) = controller.crash(true).unwrap();
    drop(journal);
    let (mut controller, io) = setup_image(image);
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(local_config(), io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(driver.snapshot().applied, confirmed);
    assert!(journal.open_reader(stale, partition(), None).is_err());
    for (writer, sequence, offset) in [(12, 0, 0), (30, 0, 1), (12, 1, 2)] {
        let buffer = append(&journal, writer, sequence);
        let result = drive(
            &mut controller,
            journal.propose_append(driver.begin_validation().unwrap(), buffer, 999),
        );
        let ProposalValidation::Resolved { buffer, .. } = result else {
            panic!("exact retry: {result:?}");
        };
        assert_eq!(coordinates(&buffer.0), [(sequence, offset, 777, 1)]);
    }
    read(&mut controller, &mut journal, &driver, 3);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn local_open_rejects_group_configuration_and_changed_identity() {
    let (mut controller, io) = setup();
    let base = config("/local", 7, QuorumPolicy::Durable);
    let (journal, _) = drive(
        &mut controller,
        OwnedJournal::format_new(base, io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    drive(&mut controller, journal.shutdown()).unwrap();
    assert!(
        drive(
            &mut controller,
            OwnedJournal::open_local(local_config(), io.clone(), JournalGeneration(2))
        )
        .is_err()
    );
    let mut config = local_config();
    config.root = "/single".into();
    let (journal, _) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(3), 32768),
    )
    .unwrap();
    drive(&mut controller, journal.shutdown()).unwrap();
    config.configuration = Configuration::new(
        config.identity.group_id,
        2,
        config.identity.replica_node_id,
        Digest::from_bytes([20; 32]),
    )
    .unwrap();
    assert!(
        drive(
            &mut controller,
            OwnedJournal::open_local(config, io, JournalGeneration(4))
        )
        .is_err()
    );
}

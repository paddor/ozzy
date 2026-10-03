use super::writeback::{
    Replica, admit_encoded, confirm, finish, initialize_writer, prepare, replica, settle,
    synchronize,
};
use super::*;
mod delivery;
use crate::replica_journal::{
    JournalCompletion, PartitionReadError, PartitionReadLimits, ReadPartition,
};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendRecord, OperationBody, encode_operation_body,
};
use ozzy_proto::{
    MessageId, Offset, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};
use ozzy_replication::WriteTicket;

fn partition() -> PartitionIncarnation {
    PartitionIncarnation::from_bytes([11; 16])
}
fn bounds() -> PartitionReadLimits {
    PartitionReadLimits {
        max_records: 8,
        max_parts: 16,
        max_payload_bytes: 8192,
    }
}

fn data(
    controller: &mut Controller,
    replica: &mut Replica,
    first: u64,
) -> JournalCompletion<WriteTicket> {
    let body = OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id: ProducerId::from_bytes([12; 16]),
            producer_epoch: ProducerEpoch::INITIAL,
            first_sequence: ProducerSequence::new(first),
            first_offset: Offset::new(first),
            append_timestamp_millis: 123,
            records: vec![AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([(first + 14) as u8; 16]),
                parts: vec![b"payload".as_slice(), b"\0".as_slice()].into(),
            }]
            .into(),
        }],
    });
    let bytes = encode_operation_body(&body, replica.config.limits.operations).unwrap();
    admit_encoded(controller, replica, [(body.kind(), bytes)]).2
}

fn capture(replica: &Replica, from: u64, limits: PartitionReadLimits) -> PreparedRead {
    let ticket = replica.driver.begin_validation().unwrap();
    let cursor = replica
        .journal
        .open_reader(ticket, partition(), Some(Offset::new(from)))
        .unwrap();
    replica
        .journal
        .prepare_read(
            cursor,
            limits,
            replica.journal.lease_append_buffer().unwrap().into(),
        )
        .unwrap()
}

fn check(read: &ReadPartition, first: u64, count: usize) {
    assert_eq!(read.first_offset(), Offset::new(first));
    assert_eq!(
        read.cursor().next_offset(),
        Offset::new(first + count as u64)
    );
    assert_eq!(read.records().len(), count);
    for (id, _, parts) in read.records() {
        assert!(!id.as_bytes().iter().all(|b| *b == 0));
        assert_eq!(
            parts.collect::<Vec<_>>(),
            [b"payload".as_slice(), b"\0".as_slice()]
        );
    }
}

#[test]
fn owned_read_visibility_follows_confirmation_not_memory_or_disk_receipt() {
    for mode in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut primary = replica(&mut controller, io.clone(), 0, mode, 8192);
        let mut backup = replica(&mut controller, io, 1, mode, 8192);
        let initial1 = initialize_writer(&mut controller, &mut primary);
        let initial2 = initialize_writer(&mut controller, &mut backup);
        let receipt1 = data(&mut controller, &mut primary, 0);
        let receipt2 = data(&mut controller, &mut backup, 0);
        let ticket = primary.driver.begin_validation().unwrap();
        assert!(
            primary
                .journal
                .open_reader(ticket, partition(), Some(Offset::ZERO))
                .is_err()
        );
        if mode == QuorumPolicy::Durable {
            for voter in [&mut primary, &mut backup] {
                let work = prepare(voter);
                finish(&mut controller, voter, work);
            }
            settle(&mut primary, initial1);
            settle(&mut primary, receipt1);
            settle(&mut backup, initial2);
            settle(&mut backup, receipt2);
            synchronize(&mut controller, &mut primary);
            synchronize(&mut controller, &mut backup);
        } else {
            drop((initial1, receipt1, initial2, receipt2));
        }
        confirm(&mut primary, &backup, mode);
        let captured = capture(&primary, 0, bounds());
        if mode == QuorumPolicy::Replicated {
            // The read keeps admitted RAM records while writes install and
            // the original pending entries are released.
            for voter in [&mut primary, &mut backup] {
                let work = prepare(voter);
                finish(&mut controller, voter, work);
            }
        }
        let mut jobs = 0;
        let done = drive_except(&mut controller, captured.read(), None, |_| {
            jobs += 1;
            Effect::Normal
        });
        check(&primary.journal.complete_read(done).unwrap(), 0, 1);
        assert_eq!(jobs, 0);
        let stored = capture(&primary, 0, bounds());
        let done = drive(&mut controller, stored.read());
        check(&primary.journal.complete_read(done).unwrap(), 0, 1);
        let caught_up = capture(&primary, 1, bounds());
        let done = drive(&mut controller, caught_up.read());
        check(&primary.journal.complete_read(done).unwrap(), 1, 0);
        drive(&mut controller, primary.journal.shutdown()).unwrap();
        drive(&mut controller, backup.journal.shutdown()).unwrap();
    }
}

#[test]
fn owned_read_capacity_exhaustion_is_retryable_without_fencing_the_journal() {
    let (mut controller, io) = setup();
    let mut primary = replica(&mut controller, io.clone(), 0, QuorumPolicy::Durable, 8192);
    let mut backup = replica(&mut controller, io, 1, QuorumPolicy::Durable, 8192);
    for voter in [&mut primary, &mut backup] {
        let initial = initialize_writer(&mut controller, voter);
        let receipt = data(&mut controller, voter, 0);
        let work = prepare(voter);
        finish(&mut controller, voter, work);
        settle(voter, initial);
        settle(voter, receipt);
        synchronize(&mut controller, voter);
    }
    confirm(&mut primary, &backup, QuorumPolicy::Durable);
    let Some(memory) = primary.journal.append_memory.clone() else {
        panic!("fixture binds a shared payload owner")
    };
    let captured = capture(&primary, 0, bounds());
    memory.trim_cache();
    let held = memory
        .try_lease(1024 * 1024 - memory.allocated_bytes())
        .unwrap();
    let done = drive(&mut controller, captured.read());
    assert!(matches!(
        primary.journal.complete_read(done),
        Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock
    ));
    assert!(!primary.journal.is_faulted());
    drop(held);
    let captured = capture(&primary, 0, bounds());
    let done = drive(&mut controller, captured.read());
    check(&primary.journal.complete_read(done).unwrap(), 0, 1);
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}

#[test]
fn owned_read_leases_one_payload_arena_for_a_stored_group() {
    const RECORDS: usize = 32;
    const BYTES: usize = 64;
    let (mut controller, io) = setup();
    let mut primary = replica(&mut controller, io.clone(), 0, QuorumPolicy::Durable, 8192);
    let mut backup = replica(&mut controller, io, 1, QuorumPolicy::Durable, 8192);
    let payload = [7; BYTES];
    let body = OperationBody::Append(Append {
        batches: vec![AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id: ProducerId::from_bytes([12; 16]),
            producer_epoch: ProducerEpoch::INITIAL,
            first_sequence: ProducerSequence::new(0),
            first_offset: Offset::new(0),
            append_timestamp_millis: 123,
            records: (0..RECORDS)
                .map(|record| AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([record as u8 + 1; 16]),
                    parts: vec![payload.as_slice()].into(),
                })
                .collect::<Vec<_>>()
                .into(),
        }],
    });
    let mut receipts = Vec::new();
    for voter in [&mut primary, &mut backup] {
        let initial = initialize_writer(&mut controller, voter);
        let bytes = encode_operation_body(&body, voter.config.limits.operations).unwrap();
        let receipt = admit_encoded(&mut controller, voter, [(body.kind(), bytes)]).2;
        let work = prepare(voter);
        finish(&mut controller, voter, work);
        receipts.push((initial, receipt));
    }
    for (voter, (initial, receipt)) in [&mut primary, &mut backup].into_iter().zip(receipts) {
        settle(voter, initial);
        settle(voter, receipt);
        synchronize(&mut controller, voter);
    }
    confirm(&mut primary, &backup, QuorumPolicy::Durable);
    let Some(memory) = primary.journal.append_memory.clone() else {
        panic!("fixture binds a shared payload owner")
    };
    let limits = PartitionReadLimits {
        max_records: RECORDS,
        max_parts: RECORDS,
        max_payload_bytes: 8192,
    };
    let captured = capture(&primary, 0, limits);
    memory.trim_cache();
    let before = memory.allocated_bytes();
    let done = drive(&mut controller, captured.read());
    let read = primary.journal.complete_read(done).unwrap();
    assert_eq!(read.records().len(), RECORDS);
    for (_, _, parts) in read.records() {
        assert_eq!(parts.collect::<Vec<_>>(), [payload.as_slice()]);
    }
    // Growing per record leases one exact arena per record and keeps the
    // smaller ones cached. One reservation leases the group's bytes once.
    assert_eq!(memory.allocated_bytes() - before, RECORDS * BYTES);
    drop(read);
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}

#[test]
fn owned_detached_read_survives_roll_and_enforces_output_bounds() {
    let (mut controller, io) = setup();
    let mut primary = replica(
        &mut controller,
        io.clone(),
        0,
        QuorumPolicy::Replicated,
        8192,
    );
    let mut backup = replica(&mut controller, io, 1, QuorumPolicy::Replicated, 8192);
    for voter in [&mut primary, &mut backup] {
        drop(initialize_writer(&mut controller, voter));
        drop(data(&mut controller, voter, 0));
        let work = prepare(voter);
        finish(&mut controller, voter, work);
    }
    confirm(&mut primary, &backup, QuorumPolicy::Replicated);
    let captured = capture(&primary, 0, bounds());
    let roll = primary.journal.begin_roll(16).unwrap();
    let roll = drive(&mut controller, roll.publish());
    primary.journal.complete_roll(roll).unwrap();
    let done = drive(&mut controller, captured.read());
    check(&primary.journal.complete_read(done).unwrap(), 0, 1);
    for limits in [
        PartitionReadLimits {
            max_payload_bytes: 1,
            ..bounds()
        },
        PartitionReadLimits {
            max_parts: 1,
            ..bounds()
        },
    ] {
        let read = capture(&primary, 0, limits);
        let done = drive(&mut controller, read.read());
        assert!(matches!(
            primary.journal.complete_read(done),
            Err(JournalError::Read(PartitionReadError::RecordTooLarge {
                bytes: 8,
                parts: 2
            }))
        ));
        assert!(!primary.journal.is_faulted());
    }
    let captured = capture(&primary, 0, bounds());
    let done = drive(&mut controller, captured.read());
    assert!(backup.journal.complete_read(done).is_err());
    assert!(!backup.journal.is_faulted());
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}

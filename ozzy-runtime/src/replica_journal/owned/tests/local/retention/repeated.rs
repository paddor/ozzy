use super::*;
use ozzy_journal_segment::AsyncRetirementBudget;
use std::io;

mod adapter;
mod replicated;

#[test]
fn repeated_retirement_reuses_the_exact_checkpoint_without_new_operations() {
    let (mut controller, io) = setup();
    let config = local_config();
    let (mut journal, driver) = seed(&mut controller, io.clone(), config.clone());
    let ticket = driver.begin_validation().unwrap();
    let before = driver.snapshot();
    let selected = retire(&mut controller, &mut journal, ticket, 92, &[1]);
    assert_eq!(prefix(selected.position), ticket.applied());
    assert_eq!(
        retire(&mut controller, &mut journal, ticket, 93, &[2]),
        selected
    );
    assert_eq!(
        retire(&mut controller, &mut journal, ticket, 94, &[]),
        selected
    );
    assert_eq!(driver.snapshot(), before);
    assert!(!journal.is_faulted());
    verify_retained(&mut controller, &mut journal, &driver);
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(
        journal.journal.ready().unwrap().manifest().checkpoint,
        Some(selected)
    );
    verify_retained(&mut controller, &mut journal, &driver);
    append(&mut controller, &mut journal, &mut driver, 12, 4, 1);
    let next = retire(
        &mut controller,
        &mut journal,
        driver.begin_validation().unwrap(),
        95,
        &[],
    );
    assert_eq!(next.checkpoint_id, CheckpointId::from_bytes([95; 16]));
    assert!(next.position.op_number > selected.position.op_number);
    drive(&mut controller, journal.shutdown()).unwrap();
}

fn seed(
    controller: &mut Controller,
    io: Local,
    config: OwnedConfig<Configuration>,
) -> (OwnedJournal, Driver) {
    let (mut journal, mut driver) = drive(
        controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(controller, &mut journal, &mut driver);
    append(controller, &mut journal, &mut driver, 12, 0, 2);
    roll(controller, &mut journal);
    append(controller, &mut journal, &mut driver, 12, 2, 1);
    roll(controller, &mut journal);
    append(controller, &mut journal, &mut driver, 12, 3, 1);
    let proposal = journal
        .retention_proposal(
            partition(),
            Offset::new(3),
            OperationId::from_bytes([91; 16]),
        )
        .unwrap()
        .unwrap();
    let receipt = admit(controller, &mut journal, &mut driver, proposal);
    write(controller, &mut journal, &mut driver, receipt);
    sync_apply(controller, &mut journal, &mut driver);
    (journal, driver)
}

#[test]
fn reused_checkpoint_read_failure_fences_retirement_before_unreferencing_segments() {
    let (mut controller, io) = setup();
    let (mut journal, driver) = seed(&mut controller, io, local_config());
    let ticket = driver.begin_validation().unwrap();
    let selected = retire(&mut controller, &mut journal, ticket, 92, &[1]);
    let manifest = journal.journal.ready().unwrap().manifest().clone();
    let mut refused = false;
    let result = drive_except(
        &mut controller,
        journal.retire_confirmed_history(
            ticket,
            CheckpointId::from_bytes([93; 16]),
            AsyncRetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
        None,
        |operation| {
            if let Operation::Open { path, .. } = operation
                && path
                    .components()
                    .any(|part| part.as_os_str() == "checkpoints")
            {
                refused = true;
                Effect::FailBefore(io::ErrorKind::Other)
            } else {
                Effect::Normal
            }
        },
    );
    assert!(
        refused,
        "reuse must revalidate the selected checkpoint files"
    );
    assert!(result.is_err());
    assert!(journal.is_faulted());
    assert_eq!(journal.journal.ready().unwrap().manifest(), &manifest);
    assert_eq!(manifest.checkpoint, Some(selected));
    assert_eq!(
        manifest
            .segments
            .iter()
            .map(|r| r.segment_id)
            .collect::<Vec<_>>(),
        [2, 3]
    );
    assert!(drive(&mut controller, journal.shutdown()).is_err());
}

fn retire(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    ticket: ozzy_replication::driver::ValidationTicket,
    id: u8,
    expected: &[u64],
) -> ozzy_journal_segment::CheckpointReference {
    let retired = drive(
        controller,
        journal.retire_confirmed_history(
            ticket,
            CheckpointId::from_bytes([id; 16]),
            AsyncRetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .expect("bounded retirement must remain healthy at an unchanged checkpoint position");
    assert_eq!(retired.unreferenced_segment_ids, expected);
    journal
        .journal
        .ready()
        .unwrap()
        .manifest()
        .checkpoint
        .unwrap()
}

fn verify_retained(controller: &mut Controller, journal: &mut OwnedJournal, driver: &Driver) {
    let state = journal
        .images()
        .unwrap()
        .committed()
        .partition(partition())
        .unwrap();
    assert_eq!(state.retained_from, Offset::new(3));
    let producer = state.producer(ProducerId::from_bytes([12; 16])).unwrap();
    assert_eq!(producer.producer_result_floor, ProducerSequence::new(3));
    assert_eq!(producer.result_offset(ProducerSequence::new(2)), None);
    assert_eq!(
        producer.result_offset(ProducerSequence::new(3)),
        Some(Offset::new(3))
    );
    let cursor = journal
        .open_reader(
            driver.begin_validation().unwrap(),
            partition(),
            Some(Offset::new(3)),
        )
        .unwrap();
    let work = journal
        .prepare_read(
            cursor,
            PartitionReadLimits {
                max_records: 1,
                max_parts: 2,
                max_payload_bytes: 8192,
            },
            journal.lease_append_buffer().unwrap().into(),
        )
        .unwrap();
    let done = drive(controller, work.read());
    let read = journal.complete_read(done).unwrap();
    let mut records = read.records();
    assert_eq!(records.len(), 1);
    let (id, encoding, parts) = records.next().unwrap();
    assert_eq!(id, ozzy_proto::MessageId::from_bytes([4; 16]));
    assert_eq!(encoding, ozzy_proto::data::Encoding::Raw);
    assert_eq!(
        parts.collect::<Vec<_>>(),
        [b"opaque".as_slice(), b"\0\xff".as_slice()]
    );
}

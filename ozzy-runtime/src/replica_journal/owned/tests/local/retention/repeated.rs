use super::*;
use ozzy_journal_segment::RetirementBudget;
use std::io;

mod adapter;
mod replicated;

#[test]
fn retirement_does_not_reread_unaffected_sealed_bodies() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = seed(&mut controller, io, local_config());
    roll(&mut controller, &mut journal);
    append(&mut controller, &mut journal, &mut driver, 12, 4, 1);
    let limits = journal.recovery.index;
    let snapshot = drive(
        &mut controller,
        journal
            .journal
            .ready_mut()
            .unwrap()
            .build_index_snapshot(ozzy_journal_segment::JournalIndexBoundary::Written, limits),
    )
    .unwrap();
    drop(snapshot);
    let before = journal.images().unwrap().committed().clone();
    let mut reread = false;
    let mut unaffected = false;
    let result = drive_except(
        &mut controller,
        journal.retire_confirmed_history(
            driver.begin_validation().unwrap(),
            CheckpointId::from_bytes([92; 16]),
            RetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
        None,
        |operation| {
            match operation {
                Operation::Open { path, .. } => unaffected = path.ends_with("segments/3.log"),
                Operation::Close { .. } => unaffected = false,
                Operation::Read { offset, .. }
                    if unaffected
                        && *offset >= ozzy_journal_segment::SEGMENT_HEADER_BYTES as u64 =>
                {
                    reread = true;
                    return Effect::FailBefore(io::ErrorKind::Other);
                }
                _ => {}
            }
            Effect::Normal
        },
    );
    assert!(
        !reread,
        "one-segment retirement reread an unaffected sealed body"
    );
    assert_eq!(result.unwrap().unreferenced_segment_ids, [1]);
    assert_eq!(journal.images().unwrap().committed(), &before);
    verify_retained(&mut controller, &mut journal, &driver);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn retirement_checks_rolled_segments_from_their_read_index() {
    let (mut controller, io) = setup();
    let (mut journal, driver) = seed(&mut controller, io, local_config());
    drive(
        &mut controller,
        journal.begin_retirement(
            driver.begin_validation().unwrap(),
            CheckpointId::from_bytes([92; 16]),
            RetirementBudget {
                max_segments: 2,
                max_read_bytes: 1,
            },
        ),
    )
    .unwrap();
    let work = journal.take_retirement_work().unwrap();
    let mut read = false;
    let done = drive_except(&mut controller, Box::pin(work.run()), None, |operation| {
        read |= matches!(operation, Operation::Open { path, .. }
            if path.ends_with("segments/1.log") || path.ends_with("segments/2.log"));
        Effect::Normal
    });
    assert!(!read, "retirement read a segment this owner rolled");
    journal.complete_retirement(done);
    let retired = drive(&mut controller, journal.finish_retirement())
        .unwrap()
        .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [1, 2]);
    assert_eq!(retired.scanned_segments, 0);
    verify_retained(&mut controller, &mut journal, &driver);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn retirement_narrows_identity_coverage_without_indexing_surviving_segments() {
    let (mut controller, io) = setup();
    let (mut journal, driver) = seed(&mut controller, io, local_config());
    let retained_from = journal.journal.ready().unwrap().manifest().segments[1]
        .first_chain
        .next_op_number();
    let overlay = journal.images().unwrap().committed_identities().overlay();
    let retained = overlay
        .claims()
        .filter(|claim| claim.op_number >= retained_from)
        .count();
    assert!(retained != 0 && retained < overlay.len());
    let mut indexed = false;
    let retired = drive_except(
        &mut controller,
        journal.retire_confirmed_history(
            driver.begin_validation().unwrap(),
            CheckpointId::from_bytes([92; 16]),
            RetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
        None,
        |operation| {
            if let Operation::Open { path, .. } = operation
                && path.components().any(|part| part.as_os_str() == "indexes")
            {
                indexed = true;
            }
            Effect::Normal
        },
    )
    .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [1]);
    assert!(!indexed, "retirement indexed surviving segments");
    let ready = journal.journal.ready().unwrap();
    assert!(ready.pins_released(&[1], &[]));
    let first = ready.manifest().segments[0];
    let snapshot = journal.images().unwrap().committed_identities().snapshot();
    assert_eq!(snapshot.segment_references(), []);
    assert_eq!(
        snapshot.through().op_number + 1,
        first.first_chain.next_op_number()
    );
    assert_eq!(first.first_chain.next_op_number(), retained_from);
    assert_eq!(journal.identity_capacity().unwrap().0, retained);
    verify_retained(&mut controller, &mut journal, &driver);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn retirement_writes_its_checkpoint_beside_later_appends() {
    let (mut controller, io) = setup();
    let config = local_config();
    let (mut journal, mut driver) = seed(&mut controller, io.clone(), config.clone());
    let ticket = driver.begin_validation().unwrap();
    drive(
        &mut controller,
        journal.begin_retirement(
            ticket,
            CheckpointId::from_bytes([92; 16]),
            RetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .unwrap();
    let work = journal.take_retirement_work().unwrap();
    assert!(journal.retiring());
    // Admission continues while the checkpoint and prefix scan run.
    append(&mut controller, &mut journal, &mut driver, 12, 4, 1);
    let done = drive(&mut controller, Box::pin(work.run()));
    journal.complete_retirement(done);
    // An accepted, unconfirmed control operation keeps its identity claim.
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .push(ozzy_journal::operation::OperationKind::Barrier, &[98; 16])
        .unwrap();
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    let retired = drive(&mut controller, journal.finish_retirement())
        .unwrap()
        .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [1]);
    assert!(!journal.retiring());
    let barrier = ozzy_core::state::IdentityKey::operation(OperationId::from_bytes([98; 16]));
    let images = journal.images().unwrap();
    assert!(
        images
            .speculative_identities()
            .overlay()
            .get(barrier)
            .is_some()
    );
    assert!(
        images
            .committed_identities()
            .overlay()
            .get(barrier)
            .is_none()
    );
    let selected = journal
        .journal
        .ready()
        .unwrap()
        .manifest()
        .checkpoint
        .unwrap();
    assert_eq!(prefix(selected.position), ticket.applied());
    sync_apply(&mut controller, &mut journal, &mut driver);
    let applied = driver.snapshot().applied;
    verify_retained(&mut controller, &mut journal, &driver);
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(
        journal.journal.ready().unwrap().manifest().checkpoint,
        Some(selected)
    );
    assert_eq!(driver.snapshot().applied, applied);
    verify_retained(&mut controller, &mut journal, &driver);
    drive(&mut controller, journal.shutdown()).unwrap();
}

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
            0,
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
            RetirementBudget {
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
            RetirementBudget {
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

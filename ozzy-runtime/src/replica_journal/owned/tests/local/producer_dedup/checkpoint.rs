use super::*;
use ozzy_core::dedup::Match;
use ozzy_journal_segment::RetirementBudget;
use ozzy_proto::CheckpointId;

fn append(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    first: u64,
    records: usize,
) {
    let validated = ready(produce(
        controller,
        journal,
        driver,
        [request(12, first, records)],
        0,
    ));
    accept(controller, journal, driver, validated);
}

fn roll(controller: &mut Controller, journal: &mut OwnedJournal) {
    let work = journal.begin_roll(8).unwrap();
    let done = drive(controller, work.publish());
    journal.complete_roll(done).unwrap();
}

fn trim(controller: &mut Controller, journal: &mut OwnedJournal, driver: &mut Driver) {
    let proposal = journal
        .retention_proposal(
            partition(),
            Offset::new(3),
            0,
            OperationId::from_bytes([81; 16]),
        )
        .unwrap()
        .unwrap();
    let receipt = admit(controller, journal, driver, proposal);
    write(controller, journal, driver, receipt);
    sync_apply(controller, journal, driver);
}

fn retire(controller: &mut Controller, journal: &mut OwnedJournal, driver: &Driver, id: u8) {
    let retired = drive(
        controller,
        journal.retire_confirmed_history(
            driver.begin_validation().unwrap(),
            CheckpointId::from_bytes([id; 16]),
            RetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [u64::from(id - 81)]);
}

fn duplicate_original(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
) {
    let validated = ready(produce(controller, journal, driver, [request(30, 0, 1)], 0));
    assert_eq!(outcomes(&validated), [vec![duplicate(0)]]);
    accept(controller, journal, driver, validated);
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
    append(controller, &mut journal, &mut driver, 0, 2);
    roll(controller, &mut journal);
    append(controller, &mut journal, &mut driver, 2, 1);
    roll(controller, &mut journal);
    append(controller, &mut journal, &mut driver, 3, 1);
    trim(controller, &mut journal, &mut driver);
    (journal, driver)
}

#[tokio::test(start_paused = true)]
async fn checkpoint_keeps_live_ids_after_payload_deletion_and_replays_only_its_suffix() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, driver) = seed(&mut controller, io.clone(), config.clone());
    retire(&mut controller, &mut journal, &driver, 82);
    let selected = journal
        .journal
        .ready()
        .unwrap()
        .manifest()
        .checkpoint
        .unwrap();
    retire(&mut controller, &mut journal, &driver, 83);
    assert_eq!(
        journal.journal.ready().unwrap().manifest().checkpoint,
        Some(selected)
    );
    let ticket = driver.begin_validation().unwrap();
    drive(
        &mut controller,
        journal.cleanup_storage(
            ticket,
            crate::replica_journal::owned::StorageCleanup::Segments,
            16,
        ),
    )
    .unwrap();
    for id in [1, 2] {
        assert!(
            !controller
                .image()
                .exists(&config.root.join(format!("segments/{id}.log")), false)
        );
    }
    assert_eq!(retained_ids(&journal), 4);
    drive(&mut controller, journal.shutdown()).unwrap();
    tokio::time::advance(std::time::Duration::from_secs(60)).await;
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config.clone(), io.clone(), JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(retained_ids(&journal), 4);
    assert_eq!(
        journal
            .dedup
            .image
            .as_ref()
            .unwrap()
            .index
            .lookup(MessageId::from_bytes([1; 16])),
        Some(Match::Committed(Offset::ZERO))
    );
    assert_eq!(
        journal
            .images()
            .unwrap()
            .committed()
            .partition(partition())
            .unwrap()
            .retained_from,
        Offset::new(3)
    );
    duplicate_original(&mut controller, &mut journal, &mut driver);
    append(&mut controller, &mut journal, &mut driver, 4, 1);
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(3)),
    )
    .unwrap();
    assert_eq!(retained_ids(&journal), 5);
    assert_eq!(stored_end(&journal), Offset::new(5));
    let mut repeated = request(30, 1, 1);
    repeated.records[0].message_id = MessageId::from_bytes([5; 16]);
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [repeated],
        0,
    ));
    assert_eq!(outcomes(&validated), [vec![duplicate(4)]]);
    drop(validated);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[tokio::test(start_paused = true)]
async fn checkpoint_restore_failure_preserves_the_old_image_and_refuses_smaller_capacity() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, driver) = seed(&mut controller, io.clone(), config.clone());
    retire(&mut controller, &mut journal, &driver, 82);
    let before = journal.images().unwrap().committed().clone();
    let mut refused = false;
    let result = drive_except(
        &mut controller,
        journal.restore_dedup(),
        None,
        |operation| {
            if matches!(operation, Operation::Read { .. }) {
                refused = true;
                Effect::FailBefore(std::io::ErrorKind::Other)
            } else {
                Effect::Normal
            }
        },
    );
    assert!(refused);
    assert!(result.is_err());
    assert_eq!(journal.images().unwrap().committed(), &before);
    assert_eq!(retained_ids(&journal), 4);
    drive(&mut controller, journal.shutdown()).unwrap();
    config.deduplication = Some(ozzy_core::dedup::Limits {
        entries: 2,
        pending_records: 2,
        batch_records: 2,
        ..limits()
    });
    assert!(matches!(
        drive(
            &mut controller,
            OwnedJournal::open_local(config, io, JournalGeneration(2)),
        ),
        Err(JournalError::Checkpoint(
            ozzy_journal_segment::CanonicalCheckpointError::Sections(
                ozzy_core::checkpoint::Error::Identity(ozzy_core::dedup::Error::Snapshot)
            )
        ))
    ));
}

#[tokio::test(start_paused = true)]
async fn empty_identity_checkpoint_keeps_the_confirmed_clock_and_record_end() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = seed(&mut controller, io.clone(), config.clone());
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    let proposal = drive(
        &mut controller,
        journal.retention_turn(
            driver.begin_validation().unwrap(),
            0,
            OperationId::from_bytes([85; 16]),
            true,
        ),
    )
    .unwrap()
    .proposal
    .unwrap();
    let receipt = admit(&mut controller, &mut journal, &mut driver, proposal);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    retire(&mut controller, &mut journal, &driver, 82);
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(retained_ids(&journal), 0);
    assert_eq!(stored_end(&journal), Offset::new(4));
    assert_eq!(
        journal.dedup.image.as_ref().unwrap().index.committed_time(),
        10
    );
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [request(30, 0, 1)],
        0,
    ));
    assert_eq!(outcomes(&validated), [vec![stored(4)]]);
    drop(validated);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_confirmation_lost_beside_a_checkpoint_resolves_to_its_original_offset() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = seed(&mut controller, io, config.clone());
    let ticket = driver.begin_validation().unwrap();
    drive(
        &mut controller,
        journal.begin_retirement(
            ticket,
            CheckpointId::from_bytes([82; 16]),
            RetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .unwrap();
    let work = journal.take_retirement_work().unwrap();
    // This append commits after the checkpoint's position while the job
    // runs; its producer never receives the confirmation.
    append(&mut controller, &mut journal, &mut driver, 4, 1);
    let done = drive(&mut controller, Box::pin(work.run()));
    journal.complete_retirement(done);
    let retired = drive(&mut controller, journal.finish_retirement())
        .unwrap()
        .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [1]);
    let selected = journal
        .journal
        .ready()
        .unwrap()
        .manifest()
        .checkpoint
        .unwrap();
    assert_eq!(prefix(selected.position), ticket.applied());
    // Crash without an orderly shutdown.
    let (image, _) = controller.crash(true).unwrap();
    drop(journal);
    let (mut controller, io) = setup_image(image);
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(
        journal.journal.ready().unwrap().manifest().checkpoint,
        Some(selected)
    );
    assert_eq!(retained_ids(&journal), 5);
    assert_eq!(stored_end(&journal), Offset::new(5));
    // The producer's own retry resolves to its original result.
    let retried = produce(
        &mut controller,
        &mut journal,
        &driver,
        [request(12, 4, 1)],
        0,
    );
    let ProposalValidation::Resolved { buffer, .. } = retried else {
        panic!("lost confirmation retry: {retried:?}");
    };
    assert_eq!(coordinates(&buffer.0), [(4, 4, 0, 1)]);
    // A redundant producer's copy is a duplicate of the same offset.
    let mut redundant = request(30, 0, 1);
    redundant.records[0].message_id = MessageId::from_bytes([5; 16]);
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [redundant],
        0,
    ));
    assert_eq!(outcomes(&validated), [vec![duplicate(4)]]);
    drop(validated);
    drive(&mut controller, journal.shutdown()).unwrap();
}

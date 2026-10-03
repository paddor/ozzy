use super::replay::persist;
use super::writeback::{Replica, admit, confirm, prepare, replica, settle};
use super::*;
use ozzy_journal_segment::StorageValidationBudget;

fn budget() -> StorageValidationBudget {
    StorageValidationBudget {
        max_read_bytes: 4096,
        max_work: Duration::MAX,
    }
}

#[test]
fn owned_detached_storage_scan_keeps_original_prefix_while_append_and_roll_continue() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let first = persist(&mut controller, &mut replica);
    let ticket = replica.driver.begin_validation().unwrap();
    let work = drive(
        &mut controller,
        replica.journal.prepare_storage_validation(ticket, budget()),
    )
    .unwrap();
    let mut reading = Box::pin(work.validate());
    assert!(poll(reading.as_mut()).is_pending());
    let held = controller.jobs()[0].0;
    let (_, _, receipt) = admit(&mut controller, &mut replica, 1);
    let write = prepare(&mut replica);
    let done = drive_except(&mut controller, write.write(), Some(held), |_| {
        Effect::Normal
    });
    replica.journal.complete_write(done).unwrap();
    settle(&mut replica, receipt);
    let roll = replica.journal.begin_roll(8).unwrap();
    let done = drive_except(&mut controller, roll.publish(), Some(held), |_| {
        Effect::Normal
    });
    replica.journal.complete_roll(done).unwrap();
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    let done = drive(&mut controller, reading);
    let mut checked = replica.journal.complete_storage_validation(done).unwrap();
    assert_eq!(prefix(checked.step.through), first);
    assert!(checked.step.checked_bytes <= 4096);
    assert_ne!(checked.step.remaining_segments, 0);
    let mut turns = 1;
    while checked.step.remaining_segments != 0 {
        let ticket = replica.driver.begin_validation().unwrap();
        let work = drive(
            &mut controller,
            replica.journal.prepare_storage_validation(ticket, budget()),
        )
        .unwrap();
        let done = drive(&mut controller, work.validate());
        checked = replica.journal.complete_storage_validation(done).unwrap();
        assert_eq!(prefix(checked.step.through), first);
        assert!(checked.step.checked_bytes <= 4096);
        turns += 1;
        assert!(turns < 16);
    }
    assert_eq!(checked.step.segment_id, 1);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_storage_scan_cancellation_releases_capacity_and_observed_failure_fences_owner() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    persist(&mut controller, &mut replica);
    let ticket = replica.driver.begin_validation().unwrap();
    let work = drive(
        &mut controller,
        replica.journal.prepare_storage_validation(ticket, budget()),
    )
    .unwrap();
    assert!(matches!(
        drive(
            &mut controller,
            replica.journal.prepare_storage_validation(ticket, budget())
        ),
        Err(JournalError::AppendCapacity)
    ));
    drop(work);
    assert!(!replica.journal.is_faulted());
    let work = drive(
        &mut controller,
        replica.journal.prepare_storage_validation(ticket, budget()),
    )
    .unwrap();
    let mut failed = false;
    let done = drive_except(&mut controller, work.validate(), None, |operation| {
        if matches!(operation, Operation::Read { .. }) && !failed {
            failed = true;
            Effect::Short(0)
        } else {
            Effect::Normal
        }
    });
    assert!(failed);
    assert!(replica.journal.complete_storage_validation(done).is_err());
    assert!(replica.journal.is_faulted());
}

#[test]
fn owned_storage_scan_old_scope_cannot_reinstall_after_promise() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let ticket = replica.driver.begin_validation().unwrap();
    let work = drive(
        &mut controller,
        replica.journal.prepare_storage_validation(ticket, budget()),
    )
    .unwrap();
    let done = drive(&mut controller, work.validate());
    let promise = promise(&replica.config, 1);
    drive(&mut controller, replica.journal.persist_promise(promise)).unwrap();
    assert!(replica.journal.complete_storage_validation(done).is_err());
    assert!(!replica.journal.is_faulted());
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

fn orphan(controller: &mut Controller, io: &Local, path: PathBuf) {
    drive(controller, async {
        let opened = io
            .execute(
                ozzy_io::Class::Progress,
                Operation::Open {
                    path,
                    mode: ozzy_io::OpenMode::CreateNew,
                    direct: false,
                    data_sync: false,
                },
            )
            .await
            .unwrap();
        let ozzy_io::Outcome::Opened(handle) = &*opened else {
            panic!("file")
        };
        io.execute(
            ozzy_io::Class::Data,
            Operation::Write {
                handle: handle.clone(),
                offset: 0,
                data: ozzy_io::WriteBuffer::from_vec(vec![0; 16]),
            },
        )
        .await
        .unwrap();
        io.execute(
            ozzy_io::Class::Progress,
            Operation::Close {
                handle: handle.clone(),
            },
        )
        .await
        .unwrap();
    });
}

#[test]
fn owned_cleanup_obeys_removal_bound_and_canceled_mutation_requires_reopen() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io.clone(), 0, QuorumPolicy::Durable, 8192);
    persist(&mut controller, &mut replica);
    for file in ["999.log", "1000.log", "unknown"] {
        orphan(
            &mut controller,
            &io,
            replica.config.root.join("segments").join(file),
        );
    }
    let selected = controller
        .image()
        .bytes(&replica.config.root.join("segments/1.log"), false)
        .unwrap()
        .to_vec();
    let ticket = replica.driver.begin_validation().unwrap();
    for _ in 0..2 {
        let result = drive(
            &mut controller,
            replica
                .journal
                .cleanup_storage(ticket, StorageCleanup::Segments, 1),
        )
        .unwrap();
        assert_eq!(result.ticket, ticket);
        assert_eq!(result.removed_objects, 1);
        assert_eq!(result.reclaimed_bytes, 16);
    }
    assert_eq!(
        controller
            .image()
            .bytes(&replica.config.root.join("segments/1.log"), false)
            .unwrap(),
        selected
    );
    assert!(
        controller
            .image()
            .exists(&replica.config.root.join("segments/unknown"), false)
    );
    for kind in [
        StorageCleanup::Indexes,
        StorageCleanup::Checkpoints,
        StorageCleanup::Metadata,
    ] {
        drive(
            &mut controller,
            replica.journal.cleanup_storage(ticket, kind, 1),
        )
        .unwrap();
    }
    let mut work = Box::pin(
        replica
            .journal
            .cleanup_storage(ticket, StorageCleanup::Metadata, 1),
    );
    assert!(poll(work.as_mut()).is_pending());
    drop(work);
    assert!(replica.journal.is_faulted());
    for (id, _) in controller.jobs() {
        controller.execute(id, Effect::Normal).unwrap();
        controller.deliver(id).unwrap();
    }
    drop(replica.journal);
    let (journal, _) = drive(
        &mut controller,
        OwnedJournal::open(replica.config, io, JournalGeneration(20)),
    )
    .unwrap();
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owned_identity_refresh_frees_bounded_overlay_without_forgetting_old_controls() {
    use ozzy_journal::operation::{CanonicalOperation, OperationKind};
    let (mut controller, io) = setup();
    let mut config = config("/primary", 7, QuorumPolicy::Durable);
    config.recovery.retained_identities = 1;
    let (journal, startup) = drive(
        &mut controller,
        OwnedJournal::format_new(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    let driver = startup
        .into_driver(Duration::ZERO, timing(), pipeline())
        .unwrap();
    let mut primary = Replica {
        journal,
        driver,
        config,
    };
    let mut backup = replica(&mut controller, io, 2, QuorumPolicy::Durable, 8192);
    for number in 1..=4 {
        persist(&mut controller, &mut primary);
        persist(&mut controller, &mut backup);
        let pending = primary.driver.begin_validation().unwrap();
        assert!(drive(&mut controller, primary.journal.refresh_identities(pending)).is_err());
        assert!(!primary.journal.is_faulted());
        confirm(&mut primary, &backup, QuorumPolicy::Durable);
        assert_eq!(primary.journal.identity_capacity().unwrap(), (1, 1));
        let ticket = primary.driver.begin_validation().unwrap();
        assert_eq!(
            drive(&mut controller, primary.journal.refresh_identities(ticket)).unwrap(),
            ticket
        );
        assert_eq!(primary.journal.identity_capacity().unwrap(), (0, 1));
        assert_eq!(
            primary.journal.images().unwrap().committed().revision(),
            number
        );
    }
    let ticket = primary.driver.begin_validation().unwrap();
    let mut buffer = primary.journal.lease_append_buffer().unwrap();
    buffer
        .push(CanonicalOperation {
            group_id: ticket.scope().group_id,
            configuration_epoch: 1,
            original_view: 0,
            op_number: 5,
            previous_digest: ticket.accepted().digest,
            kind: OperationKind::Barrier,
            body: &[1; 16],
        })
        .unwrap();
    assert!(
        drive(
            &mut controller,
            primary.journal.validate_append(ticket, buffer)
        )
        .is_err()
    );
    assert!(!primary.journal.is_faulted());
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}

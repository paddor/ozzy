use super::*;

#[test]
fn local_sync_failure_or_cancellation_never_confirms_and_fences_the_owner() {
    for failure in 0..3 {
        let (mut controller, io) = setup();
        let (mut journal, mut driver) = drive(
            &mut controller,
            OwnedJournal::format_local(local_config(), io, JournalGeneration(1), 32768),
        )
        .unwrap();
        initialize(&mut controller, &mut journal, &mut driver);
        let before = driver.snapshot().committed;
        let buffer = append(&journal, 12, 0);
        let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
        write(&mut controller, &mut journal, &mut driver, receipt);
        let work = journal.begin_sync(driver.begin_sync().unwrap()).unwrap();
        match failure {
            0 => drop(work),
            1 => drop(drive(&mut controller, work.publish())),
            _ => {
                let mut injected = false;
                let done = drive_except(&mut controller, work.publish(), None, |operation| {
                    if !injected && matches!(operation, Operation::Write { .. }) {
                        injected = true;
                        Effect::FailAfter(std::io::ErrorKind::Other)
                    } else {
                        Effect::Normal
                    }
                });
                assert!(injected);
                assert!(journal.complete_sync(done).is_err());
            }
        }
        assert!(journal.is_faulted());
        assert_eq!(driver.snapshot().committed, before);
        assert!(journal.apply(driver.begin_validation().unwrap()).is_err());
        let (image, _) = controller.crash(true).unwrap();
        drop(journal);
        let (mut controller, io) = setup_image(image);
        let (journal, recovered) = drive(
            &mut controller,
            OwnedJournal::open_local(local_config(), io, JournalGeneration(2)),
        )
        .unwrap();
        assert!(recovered.snapshot().applied.op >= before.op);
        drive(&mut controller, journal.shutdown()).unwrap();
    }
}

#[test]
fn local_protected_corruption_is_refused_without_truncating_confirmed_history() {
    use ozzy_io::{Class, OpenMode, Outcome, SyncMode, WriteBuffer};
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(local_config(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let buffer = append(&journal, 12, 0);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    drive(&mut controller, journal.shutdown()).unwrap();
    let path = PathBuf::from("/local/segments/1.log");
    let offset = ozzy_journal_segment::SEGMENT_HEADER_BYTES + 16;
    let damaged = controller.image().bytes(&path, true).unwrap()[offset] ^ 1;
    drive(&mut controller, async {
        let opened = io
            .execute(
                Class::Progress,
                Operation::Open {
                    path: path.clone(),
                    mode: OpenMode::ReadWrite,
                    direct: false,
                    data_sync: false,
                },
            )
            .await
            .unwrap();
        let Outcome::Opened(handle) = &*opened else {
            panic!("file")
        };
        io.execute(
            Class::Data,
            Operation::Write {
                handle: handle.clone(),
                offset: offset as u64,
                data: WriteBuffer::from_vec(vec![damaged]),
            },
        )
        .await
        .unwrap();
        io.execute(
            Class::Progress,
            Operation::Sync {
                handle: handle.clone(),
                mode: SyncMode::All,
            },
        )
        .await
        .unwrap();
        io.execute(
            Class::Progress,
            Operation::Close {
                handle: handle.clone(),
            },
        )
        .await
        .unwrap();
    });
    let (image, _) = controller.crash(true).unwrap();
    let before = image.bytes(&path, true).unwrap().to_vec();
    let (mut controller, io) = setup_image(image);
    assert!(
        drive(
            &mut controller,
            OwnedJournal::open_local(local_config(), io, JournalGeneration(2))
        )
        .is_err()
    );
    assert_eq!(controller.image().bytes(&path, true).unwrap(), before);
}

#[test]
fn local_journal_rejects_group_confirmation_policy_without_admission() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(local_config(), io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let mut buffer = append(&journal, 12, 0);
    buffer.0.proposal_policy = Some(ozzy_proto::append::Policy::QuorumDurable);
    let before = driver.snapshot();
    let result = drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    );
    assert!(matches!(
        result,
        ProposalValidation::Rejected {
            reason: JournalError::ProducerAppend(
                crate::replica_journal::AppendAdmissionError::Policy
            ),
            ..
        }
    ));
    assert_eq!(driver.snapshot(), before);
    assert!(!journal.is_faulted());
    drive(&mut controller, journal.shutdown()).unwrap();
}

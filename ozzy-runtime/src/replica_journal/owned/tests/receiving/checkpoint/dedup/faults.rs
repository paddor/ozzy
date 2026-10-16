use super::*;

#[expect(
    clippy::large_types_passed_by_value,
    reason = "copyable tickets match production request APIs"
)]
fn checkpoint_bytes(
    controller: &mut Controller,
    primary: &mut Replica,
    ticket: RecoveryTicket,
    pin: PinnedRecovery,
    maximum: usize,
) -> bytes::Bytes {
    let work = primary
        .journal
        .prepare_checkpoint_read(
            pin,
            ozzy_replication::wire::CheckpointRequest {
                scope: ticket.scope(),
                request_id: RequestId::from_bytes([101; 16]),
                nonce: ticket.nonce(),
                source: ticket.source(),
                offset: 0,
                max_bytes: u32::try_from(maximum).unwrap(),
            },
        )
        .unwrap();
    let done = drive(controller, work.read());
    primary
        .journal
        .complete_checkpoint_read(done)
        .unwrap()
        .bytes
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
fn dedup_checkpoint_header_rejects_policy_and_capacity_before_core_allocation() {
    for configured in [
        None,
        Some(ozzy_core::dedup::Limits {
            entries: 2,
            pending_records: 2,
            batch_records: 2,
            ..limits()
        }),
        Some(ozzy_core::dedup::Limits {
            window_millis: 20,
            ..limits()
        }),
    ] {
        let (mut controller, io) = setup();
        let (mut primary, backup) = donors(&mut controller, &io);
        let mut config = receiver_config();
        config.deduplication = configured;
        let (mut receiver, startup) = drive(
            &mut controller,
            RecoveringJournal::start(
                config.clone(),
                io.clone(),
                generations(1000),
                RecoveryOpen::FormatNew {
                    segment_capacity: 32768,
                },
            ),
        )
        .unwrap();
        let memory = payload_owner(32768);
        receiver.bind_append_memory(&memory).unwrap();
        let (_, old, pin) = authorize(&mut controller, &mut primary, &backup, startup, 101);
        drive(&mut controller, receiver.begin_recovery(old, physical())).unwrap();
        let baseline = memory.allocated_bytes();
        let header = checkpoint_bytes(
            &mut controller,
            &mut primary,
            old,
            pin,
            ozzy_core::checkpoint::HEADER_BYTES,
        );
        assert_eq!(header.len(), ozzy_core::checkpoint::HEADER_BYTES);
        let result = drive(
            &mut controller,
            receiver.receive_checkpoint(old, 0, &header),
        );
        assert!(
            matches!(
                result,
                Err(JournalError::Checkpoint(
                    ozzy_journal_segment::CanonicalCheckpointError::Sections(
                        ozzy_core::checkpoint::Error::Identity(ozzy_core::dedup::Error::Snapshot)
                    )
                ))
            ),
            "configured: {configured:?}, result: {result:?}"
        );
        assert!(receiver.is_faulted());
        assert_eq!(memory.allocated_bytes(), baseline);
        assert!(matches!(
            drive(&mut controller, receiver.finish_recovery(old)),
            Err(JournalError::Faulted)
        ));
        drop(receiver);
        memory.trim_cache();
        assert_eq!(memory.allocated_bytes(), 0);
        primary.journal.release_recovery(pin).unwrap();
        config.deduplication = Some(limits());
        assert!(
            drive(
                &mut controller,
                OwnedJournal::open(config.clone(), io.clone(), JournalGeneration(1010)),
            )
            .is_err()
        );
        let (mut receiver, startup) = drive(
            &mut controller,
            RecoveringJournal::start(config, io, generations(1020), RecoveryOpen::Resume),
        )
        .unwrap();
        receiver.bind_append_memory(&memory).unwrap();
        let (mut recovery, fresh, pin) =
            authorize(&mut controller, &mut primary, &backup, startup, 102);
        let plan = drive(&mut controller, receiver.begin_recovery(fresh, physical())).unwrap();
        assert_ne!(old.nonce(), fresh.nonce());
        assert!(
            drive(
                &mut controller,
                receiver.receive_checkpoint(old, 0, &header)
            )
            .is_err()
        );
        assert!(!receiver.is_faulted());
        transfer(
            &mut controller,
            &mut primary,
            &mut receiver,
            &mut recovery,
            fresh,
            pin,
            plan,
        );
        let publication = drive(&mut controller, receiver.finish_recovery(fresh)).unwrap();
        let (journal, startup) = drive(
            &mut controller,
            receiver.into_journal(&mut recovery, publication, JournalGeneration(1030)),
        )
        .unwrap();
        assert_eq!(
            startup.recovered().unwrap().log.accepted,
            fresh.source().accepted
        );
        assert!(journal.images().is_err());
        drive(&mut controller, journal.shutdown()).unwrap();
        memory.trim_cache();
        assert_eq!(memory.allocated_bytes(), 0);
        primary.journal.release_recovery(pin).unwrap();
        drive(&mut controller, primary.journal.shutdown()).unwrap();
        drive(&mut controller, backup.journal.shutdown()).unwrap();
    }
}

#[test]
fn interrupted_dedup_rows_require_fresh_nonce_and_release_private_state() {
    let (mut controller, io) = setup();
    let (mut primary, backup) = donors(&mut controller, &io);
    let mut config = receiver_config();
    config.deduplication = Some(limits());
    let (mut receiver, startup) = drive(
        &mut controller,
        RecoveringJournal::start(
            config.clone(),
            io.clone(),
            generations(1040),
            RecoveryOpen::FormatNew {
                segment_capacity: 32768,
            },
        ),
    )
    .unwrap();
    let memory = payload_owner(32768);
    receiver.bind_append_memory(&memory).unwrap();
    let (_, old, pin) = authorize(&mut controller, &mut primary, &backup, startup, 103);
    drive(&mut controller, receiver.begin_recovery(old, physical())).unwrap();
    let anchor = old.checkpoint().unwrap();
    let prefix = checkpoint_bytes(
        &mut controller,
        &mut primary,
        old,
        pin,
        usize::try_from(anchor.state_bytes - 1).unwrap(),
    );
    let progress = drive(
        &mut controller,
        receiver.receive_checkpoint(old, 0, &prefix),
    )
    .unwrap();
    assert_eq!(progress.through, anchor.state_bytes - 1);
    assert!(progress.revision.is_none());
    assert!(memory.allocated_bytes() > 0);
    drive(&mut controller, receiver.shutdown()).unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
    primary.journal.release_recovery(pin).unwrap();
    assert!(
        drive(
            &mut controller,
            OwnedJournal::open(config.clone(), io.clone(), JournalGeneration(1050)),
        )
        .is_err()
    );
    let (mut receiver, startup) = drive(
        &mut controller,
        RecoveringJournal::start(config, io, generations(1060), RecoveryOpen::Resume),
    )
    .unwrap();
    receiver.bind_append_memory(&memory).unwrap();
    let (mut recovery, fresh, pin) =
        authorize(&mut controller, &mut primary, &backup, startup, 104);
    let plan = drive(&mut controller, receiver.begin_recovery(fresh, physical())).unwrap();
    assert_ne!(fresh.nonce(), old.nonce());
    assert!(
        drive(
            &mut controller,
            receiver.receive_checkpoint(old, 0, &prefix)
        )
        .is_err()
    );
    assert!(!receiver.is_faulted());
    transfer(
        &mut controller,
        &mut primary,
        &mut receiver,
        &mut recovery,
        fresh,
        pin,
        plan,
    );
    let publication = drive(&mut controller, receiver.finish_recovery(fresh)).unwrap();
    let (journal, startup) = drive(
        &mut controller,
        receiver.into_journal(&mut recovery, publication, JournalGeneration(1070)),
    )
    .unwrap();
    assert_eq!(
        startup.recovered().unwrap().log.accepted,
        fresh.source().accepted
    );
    assert!(journal.images().is_err());
    drive(&mut controller, journal.shutdown()).unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
    primary.journal.release_recovery(pin).unwrap();
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}

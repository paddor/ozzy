use super::*;

fn damaged(controller: &mut Controller, io: &Local) -> OwnedConfig {
    let mut replica = replica(controller, io.clone(), 1, QuorumPolicy::Durable, 8192);
    persist(controller, &mut replica);
    let roll = replica.journal.begin_roll(8).unwrap();
    let done = drive(controller, roll.publish());
    replica.journal.complete_roll(done).unwrap();
    persist(controller, &mut replica);
    drive(controller, replica.journal.shutdown()).unwrap();
    drive(controller, async {
        let opened = io
            .execute(
                ozzy_io::Class::Progress,
                Operation::Open {
                    path: replica.config.root.join("segments/1.log"),
                    mode: ozzy_io::OpenMode::ReadWrite,
                    direct: false,
                    data_sync: false,
                },
            )
            .await
            .unwrap();
        let ozzy_io::Outcome::Opened(handle) = &*opened else {
            panic!("file handle")
        };
        io.execute(
            ozzy_io::Class::Data,
            Operation::Write {
                handle: handle.clone(),
                offset: 4096 + 192,
                data: ozzy_io::WriteBuffer::from_vec(vec![99]),
            },
        )
        .await
        .unwrap();
        io.execute(
            ozzy_io::Class::Progress,
            Operation::Sync {
                handle: handle.clone(),
                mode: ozzy_io::SyncMode::All,
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
    replica.config
}

#[test]
fn owned_recovery_repairs_only_damaged_sealed_history_and_keeps_original_files() {
    let (mut controller, io) = setup();
    let mut primary = replica(&mut controller, io.clone(), 0, QuorumPolicy::Durable, 8192);
    let backup = replica(&mut controller, io.clone(), 2, QuorumPolicy::Durable, 8192);
    persist(&mut controller, &mut primary);
    let accepted = persist(&mut controller, &mut primary);
    let config = damaged(&mut controller, &io);
    let old = controller
        .image()
        .bytes(&config.root.join("segments/1.log"), false)
        .unwrap()
        .to_vec();
    let active = controller
        .image()
        .bytes(&config.root.join("segments/2.log"), false)
        .unwrap()
        .to_vec();
    assert!(
        drive(
            &mut controller,
            OwnedJournal::open(config.clone(), io.clone(), JournalGeneration(40))
        )
        .is_err()
    );
    let (mut receiver, startup) = drive(
        &mut controller,
        RecoveringJournal::start(
            config.clone(),
            io.clone(),
            generations(50),
            RecoveryOpen::Quarantine,
        ),
    )
    .unwrap();
    let (mut recovery, ticket, pin) =
        authorize(&mut controller, &mut primary, &backup, startup, 65);
    let plan = drive(&mut controller, receiver.begin_recovery(ticket, physical())).unwrap();
    assert!(matches!(plan, RecoveryPlan::Repair(Some(_))));
    transfer(
        &mut controller,
        &mut primary,
        &mut receiver,
        &mut recovery,
        ticket,
        pin,
        plan,
    );
    let publication = drive(&mut controller, receiver.finish_recovery(ticket)).unwrap();
    let (journal, startup) = drive(
        &mut controller,
        receiver.into_journal(&mut recovery, publication, JournalGeneration(70)),
    )
    .unwrap();
    assert_eq!(startup.recovered().unwrap().log.accepted, accepted);
    assert_eq!(startup.recovered().unwrap().log.last_normal_view, 0);
    assert_eq!(
        journal.journal.readable().unwrap().manifest().segments[0].file_generation,
        1
    );
    assert_eq!(
        controller
            .image()
            .bytes(&config.root.join("segments/1.log"), false)
            .unwrap(),
        old
    );
    assert_eq!(
        controller
            .image()
            .bytes(&config.root.join("segments/2.log"), false)
            .unwrap(),
        active
    );
    drive(&mut controller, journal.shutdown()).unwrap();
    primary.journal.release_recovery(pin).unwrap();
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}

#[test]
fn owned_recovery_conflicting_sealed_donor_requires_fresh_full_transfer() {
    use super::super::writeback::{admit_encoded, finish, prepare, settle, synchronize};
    let (mut controller, io) = setup();
    let mut primary = replica(&mut controller, io.clone(), 0, QuorumPolicy::Durable, 8192);
    let backup = replica(&mut controller, io.clone(), 2, QuorumPolicy::Durable, 8192);
    let (_, _, receipt) = admit_encoded(
        &mut controller,
        &mut primary,
        [(
            ozzy_journal::operation::OperationKind::Barrier,
            vec![99; 16],
        )],
    );
    let work = prepare(&mut primary);
    finish(&mut controller, &mut primary, work);
    settle(&mut primary, receipt);
    synchronize(&mut controller, &mut primary);
    persist(&mut controller, &mut primary);
    let config = damaged(&mut controller, &io);
    let (mut receiver, startup) = drive(
        &mut controller,
        RecoveringJournal::start(
            config.clone(),
            io.clone(),
            generations(50),
            RecoveryOpen::Quarantine,
        ),
    )
    .unwrap();
    let (_, ticket, pin) = authorize(&mut controller, &mut primary, &backup, startup, 65);
    assert!(matches!(
        drive(&mut controller, receiver.begin_recovery(ticket, physical())).unwrap(),
        RecoveryPlan::Repair(Some(_))
    ));
    let request = FetchOps {
        scope: ticket.scope(),
        source: ticket.source(),
        predecessor: Prefix::GENESIS,
        request_id: RequestId::from_bytes([66; 16]),
        max_operations: 1,
        max_body_bytes: 8192,
    };
    let arena = primary.journal.lease_append_buffer().unwrap();
    let work = primary
        .journal
        .prepare_recovery_read(pin, request, arena)
        .unwrap();
    let done = drive(&mut controller, work.read());
    let fetched = primary.journal.complete_recovery_read(done).unwrap();
    let mut arena = receiver.lease_append_buffer().unwrap();
    for operation in fetched.buffer().operations() {
        arena.push(operation).unwrap();
    }
    drop(fetched);
    let chunk = drive(&mut controller, receiver.receive_chunk(ticket, arena)).unwrap();
    assert_eq!(chunk.plan(), RecoveryPlan::RetryFull);
    assert!(chunk.prepared().is_empty());
    drop(chunk);
    drive(&mut controller, receiver.abort_recovery(ticket)).unwrap();
    drive(&mut controller, receiver.shutdown()).unwrap();
    primary.journal.release_recovery(pin).unwrap();
    let (mut receiver, startup) = drive(
        &mut controller,
        RecoveringJournal::start(config, io, generations(70), RecoveryOpen::ResumeFull),
    )
    .unwrap();
    let (mut recovery, ticket, pin) =
        authorize(&mut controller, &mut primary, &backup, startup, 67);
    let plan = drive(&mut controller, receiver.begin_recovery(ticket, physical())).unwrap();
    assert_eq!(plan, RecoveryPlan::Full);
    transfer(
        &mut controller,
        &mut primary,
        &mut receiver,
        &mut recovery,
        ticket,
        pin,
        plan,
    );
    let publication = drive(&mut controller, receiver.finish_recovery(ticket)).unwrap();
    let (journal, _) = drive(
        &mut controller,
        receiver.into_journal(&mut recovery, publication, JournalGeneration(90)),
    )
    .unwrap();
    drive(&mut controller, journal.shutdown()).unwrap();
    primary.journal.release_recovery(pin).unwrap();
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}

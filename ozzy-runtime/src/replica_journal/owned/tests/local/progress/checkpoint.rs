use super::*;
use ozzy_core::state::group_targets;
use ozzy_io::{Class, OpenMode, Outcome, SyncMode, WriteBuffer};
use ozzy_journal::operation::{GroupMember, GroupPlan};
use ozzy_proto::{NodeId, group::Start};

fn instance(join: u8) -> GroupMember {
    GroupMember {
        id: assignment().member,
        node: NodeId::from_bytes([1; 16]),
        join: OperationId::from_bytes([join; 16]),
    }
}

fn plan(journal: &OwnedJournal, join: u8) -> ProposalBuffer {
    let prior = journal
        .images()
        .unwrap()
        .speculative()
        .coordinator_group(assignment().group);
    let revision = prior.map_or(1, |image| image.revision + 1);
    let members = vec![instance(join)];
    let body = OperationBody::GroupPlan(GroupPlan {
        coordinator: partition(),
        group: assignment().group,
        name: "workers",
        expected_revision: revision - 1,
        revision,
        initial: Start::Earliest,
        targets: group_targets(prior.map(AsRef::as_ref), &members, &[partition()], revision)
            .unwrap(),
        members,
        operation_id: OperationId::from_bytes([join; 16]),
    });
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .push(
            body.kind(),
            &encode_operation_body(&body, journal.limits.operations).unwrap(),
        )
        .unwrap();
    buffer
}

fn checkpoint(controller: &mut Controller, journal: &mut OwnedJournal) {
    let state = journal.images().unwrap().committed().clone();
    let limits = local_config().recovery;
    let id = ozzy_proto::CheckpointId::from_bytes([93; 16]);
    let ready = journal.journal.ready_mut().unwrap();
    drive(controller, ready.publish_progress()).unwrap();
    let files = drive(
        controller,
        ready.build_canonical_checkpoint(id, 4096, &state, limits.snapshot),
    )
    .unwrap();
    drive(
        controller,
        ready.install_canonical_checkpoint(id, limits.state, limits.snapshot, limits.checkpoint),
    )
    .unwrap();
    drop(files);
}

#[test]
fn checkpoint_with_pending_plan_recovers_the_same_coordinator_and_source_as_uninterrupted_execution()
 {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = initialized(&mut controller, io);
    let buffer = request_progress(&journal, &driver, Some(2), 90);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let buffer = plan(&journal, 91);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let buffer = plan(&journal, 92);
    let pending = admit(&mut controller, &mut journal, &mut driver, buffer);
    assert_eq!(
        journal
            .images()
            .unwrap()
            .committed()
            .coordinator_group(assignment().group)
            .unwrap()
            .revision,
        1
    );
    assert_eq!(
        journal
            .images()
            .unwrap()
            .speculative()
            .coordinator_group(assignment().group)
            .unwrap()
            .revision,
        2
    );
    // The selected checkpoint must exclude the admitted plan. Its durable
    // suffix, rather than the checkpoint, carries that later transition.
    checkpoint(&mut controller, &mut journal);
    write(&mut controller, &mut journal, &mut driver, pending);
    let restart = controller.image().clone();
    sync_apply(&mut controller, &mut journal, &mut driver);
    let expected = journal.images().unwrap().committed().clone();
    let confirmed = driver.snapshot().committed;
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut controller, io) = setup_image(restart);
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(local_config(), io.clone(), JournalGeneration(2)),
    )
    .unwrap();
    journal
        .bind_append_memory(&payload_owner(1024 * 1024))
        .unwrap();
    assert_eq!(journal.images().unwrap().committed(), &expected);
    assert_eq!(driver.snapshot().committed, confirmed);
    let restored = journal
        .images()
        .unwrap()
        .committed()
        .coordinator_group_by_name(partition(), "workers")
        .unwrap();
    assert_eq!(
        (restored.revision, restored.members[0].join),
        (2, instance(92).join)
    );
    let source = journal
        .images()
        .unwrap()
        .committed()
        .assignment(assignment().group, partition())
        .unwrap();
    assert_eq!((source.epoch, source.resume_cursor), (1, Offset::ZERO));
    assert_eq!(
        journal.images().unwrap().committed().progress(
            ProgressOwner::ConsumerGroup(assignment().group),
            partition()
        ),
        Some(Offset::new(2))
    );
    drive(&mut controller, journal.shutdown()).unwrap();
    damaged_checkpoint_fails_closed(&mut controller, io);
}

fn damaged_checkpoint_fails_closed(controller: &mut Controller, io: Local) {
    let path = PathBuf::from("/local/checkpoints")
        .join(ozzy_journal_segment::checkpoint_name(
            ozzy_proto::CheckpointId::from_bytes([93; 16]),
        ))
        .join("CHUNK.00000000");
    let damaged = controller.image().bytes(&path, true).unwrap()[0] ^ 1;
    drive(controller, async {
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
            panic!("checkpoint chunk")
        };
        io.execute(
            Class::Data,
            Operation::Write {
                handle: handle.clone(),
                offset: 0,
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
    let before = controller.image().bytes(&path, true).unwrap().to_vec();
    assert!(
        drive(
            controller,
            OwnedJournal::open_local(local_config(), io, JournalGeneration(3))
        )
        .is_err()
    );
    assert_eq!(controller.image().bytes(&path, true).unwrap(), before);
}

use super::*;
use crate::replica_actor::{
    ActorIds, PartitionActor, PartitionActors, PartitionStatus, PendingProposal, ProposalOutcome,
    ProposalReply, ReplicaActor, ScheduledReplica,
};
use crate::replica_journal::owned::tests::{
    dedup::{limits, replica},
    proposal::{dedup::resolved, request},
    shard::actors::{actor_config, scheduled},
    writeback::{admit_encoded, finish, initialize_writer, prepare, settle, synchronize},
};
use ozzy_journal::operation::{
    OpenProducer, OperationBody, OperationKind, decode_resolved_append, encode_operation_body,
};
use ozzy_proto::{
    CheckpointId, MessageId, Offset, OperationId, PartitionIncarnation, ProducerEpoch, ProducerId,
    append::AppendDisposition,
};
use ozzy_replication::wire::Control;

mod faults;

fn confirm_pair(primary: &mut Replica, backup: &mut Replica) {
    confirm(primary, backup);
    backup
        .driver
        .receive(
            primary.config.identity.replica_node_id,
            Control::Commit(primary.driver.normal().unwrap().announcement().unwrap()),
            Duration::ZERO,
        )
        .unwrap();
    let ticket = backup.driver.begin_validation().unwrap();
    backup.journal.apply(ticket).unwrap();
    backup.driver.apply_through(ticket.committed()).unwrap();
}

fn donors(controller: &mut Controller, io: &Local) -> (Replica, Replica) {
    let mut primary = replica(controller, io.clone(), 0);
    let mut backup = replica(controller, io.clone(), 2);
    let mut receipts = Vec::new();
    for replica in [&mut primary, &mut backup] {
        let init = initialize_writer(controller, replica);
        let stored = resolved(&request(12, 0, 4), Offset::ZERO, &[None; 4], 0);
        let receipt = admit_encoded(controller, replica, [stored]).2;
        receipts.push([init, receipt]);
    }
    // Finish physical writes before presenting their receipts to replication.
    for (replica, receipts) in [&mut primary, &mut backup].into_iter().zip(receipts) {
        let work = prepare(replica);
        finish(controller, replica, work);
        for receipt in receipts {
            settle(replica, receipt);
        }
        synchronize(controller, replica);
    }
    confirm_pair(&mut primary, &mut backup);
    let roll = primary.journal.begin_roll(8).unwrap();
    let done = drive(controller, roll.publish());
    primary.journal.complete_roll(done).unwrap();
    let trim = primary
        .journal
        .retention_proposal(
            PartitionIncarnation::from_bytes([11; 16]),
            Offset::new(4),
            0,
            OperationId::from_bytes([81; 16]),
        )
        .unwrap()
        .unwrap();
    let bodies: Vec<_> = trim
        .bodies()
        .map(|(kind, bytes)| (kind, bytes.to_vec()))
        .collect();
    let left = admit_encoded(controller, &mut primary, bodies.clone()).2;
    let right = admit_encoded(controller, &mut backup, bodies).2;
    for replica in [&mut primary, &mut backup] {
        let work = prepare(replica);
        finish(controller, replica, work);
    }
    settle(&mut primary, left);
    settle(&mut backup, right);
    for replica in [&mut primary, &mut backup] {
        synchronize(controller, replica);
    }
    confirm_pair(&mut primary, &mut backup);
    let retired = drive(
        controller,
        primary.journal.retire_confirmed_history(
            primary.driver.begin_validation().unwrap(),
            CheckpointId::from_bytes([82; 16]),
            ozzy_journal_segment::RetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [1]);
    drive(
        controller,
        primary.journal.cleanup_storage(
            primary.driver.begin_validation().unwrap(),
            crate::replica_journal::owned::StorageCleanup::Segments,
            16,
        ),
    )
    .unwrap();
    assert!(
        !controller
            .image()
            .exists(&primary.config.root.join("segments/1.log"), false)
    );
    (primary, backup)
}

fn status(actors: &PartitionActors) -> crate::replica_actor::ActorStatus {
    let PartitionStatus::Replicated(status) = actors.status(GroupId::from_bytes([7; 16])).unwrap()
    else {
        panic!("replicated group changed its mode");
    };
    status
}

fn round(actors: &mut [PartitionActors], now: Duration) {
    let group = GroupId::from_bytes([7; 16]);
    let mut messages = Vec::new();
    for (broker, actors) in actors.iter_mut().enumerate() {
        let result = actors.poll_progress(
            &mut Context::from_waker(Waker::noop()),
            now,
            |_, mut message| {
                let to = usize::from(message.pop_front().unwrap()[0] - 1);
                messages.push((
                    to,
                    omq_tokio::Message::with_prefix(
                        bytes::Bytes::from(vec![broker as u8 + 1; 16]),
                        message,
                    ),
                ));
                Ok(())
            },
        );
        assert!(
            result.is_pending(),
            "partition scheduler stopped: {result:?}"
        );
    }
    for (to, message) in messages {
        actors[to].receive(group, &message, now).unwrap();
    }
}

fn complete(
    controller: &mut Controller,
    actors: &mut [PartitionActors],
    pending: PendingProposal,
    now: Duration,
) -> ProposalReply {
    let mut pending = Box::pin(pending);
    for _ in 0..10000 {
        round(actors, now);
        scheduled::settle(controller, &[]);
        if let Poll::Ready(reply) = poll(pending.as_mut()) {
            let reply = reply.unwrap();
            assert!(
                matches!(reply.outcome, ProposalOutcome::Committed { .. }),
                "{reply:?}"
            );
            return reply;
        }
    }
    panic!("proposal did not reach its quorum confirmation boundary");
}

#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
fn recovered_cluster(
    unconfirmed_tail: bool,
) -> (
    Controller,
    Vec<PartitionActors>,
    OwnedConfig,
    crate::memory::Owner,
) {
    let (mut controller, io) = setup();
    let (mut primary, backup) = donors(&mut controller, &io);
    if unconfirmed_tail {
        let stored = resolved(&request(12, 4, 1), Offset::new(4), &[None], 0);
        let receipt = admit_encoded(&mut controller, &mut primary, [stored]).2;
        let work = prepare(&mut primary);
        finish(&mut controller, &mut primary, work);
        settle(&mut primary, receipt);
        synchronize(&mut controller, &mut primary);
        let snapshot = primary.driver.normal().unwrap().snapshot();
        assert_eq!(snapshot.accepted.op.0, 6);
        assert_eq!(snapshot.committed.op.0, 5);
        assert_eq!(backup.driver.normal().unwrap().snapshot().accepted.op.0, 5);
    }
    let mut config = receiver_config();
    config.deduplication = Some(limits());
    let (mut receiver, startup) = drive(
        &mut controller,
        RecoveringJournal::start(
            config.clone(),
            io.clone(),
            generations(900),
            RecoveryOpen::FormatNew {
                segment_capacity: 32768,
            },
        ),
    )
    .unwrap();
    let memory = payload_owner(32768);
    receiver.bind_append_memory(&memory).unwrap();
    let (mut recovery, ticket, pin) =
        authorize(&mut controller, &mut primary, &backup, startup, 91);
    let anchor = ticket
        .checkpoint()
        .expect("donor must transfer the retained identity image");
    let plan = drive(&mut controller, receiver.begin_recovery(ticket, physical())).unwrap();
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
        receiver.into_journal(&mut recovery, publication, JournalGeneration(910)),
    )
    .unwrap();
    let adopted = journal
        .journal
        .ready()
        .unwrap()
        .manifest()
        .checkpoint
        .unwrap();
    assert_eq!(adopted.position.op_number, anchor.position.op.0);
    let checkpoint = journal
        .journal
        .ready()
        .unwrap()
        .capture_checkpoint()
        .unwrap();
    assert_eq!(checkpoint.manifest().state_digest, anchor.state_digest);
    drop(checkpoint);
    assert!(
        journal.images().is_err(),
        "complete bytes do not grant normal authority"
    );
    drive(&mut controller, journal.shutdown()).unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
    let (mut journal, startup) = drive(
        &mut controller,
        OwnedJournal::open(config.clone(), io.clone(), JournalGeneration(940)),
    )
    .unwrap();
    // Active actors reserve additional arenas and cold-read scratch beyond
    // this test's smaller, independent receive budget.
    let actor_memory = payload_owner(1024 * 1024);
    journal.bind_append_memory(&actor_memory).unwrap();
    primary.journal.release_recovery(pin).unwrap();
    let primary_config = primary.config.clone();
    let backup_config = backup.config.clone();
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
    let (primary, primary_startup) = drive(
        &mut controller,
        OwnedJournal::open(primary_config, io.clone(), JournalGeneration(920)),
    )
    .unwrap();
    let (backup, backup_startup) = drive(
        &mut controller,
        OwnedJournal::open(backup_config, io.clone(), JournalGeneration(930)),
    )
    .unwrap();
    let mut actors = Vec::new();
    for (broker, (journal, startup)) in vec![
        (primary, primary_startup),
        (journal, startup),
        (backup, backup_startup),
    ]
    .into_iter()
    .enumerate()
    {
        let shard = journal
            .into_shard_journal(ShardJournalConfig::default(), || 777)
            .unwrap();
        let mut actor = ReplicaActor::new_with_ids(
            shard,
            startup,
            actor_config(),
            ActorIds::deterministic(
                std::num::NonZeroU64::new(broker as u64 + 100).unwrap(),
                crate::replica_actor::ReceiptHandles::default(),
            ),
        )
        .unwrap();
        actor.enable_recovery();
        actors.push(
            PartitionActors::new(
                vec![PartitionActor::Replicated(
                    ScheduledReplica::new(actor).unwrap(),
                )],
                1,
                1,
            )
            .unwrap(),
        );
    }
    (controller, actors, config, actor_memory)
}

#[tokio::test(start_paused = true)]
async fn cluster_checkpoint_preserves_dedup_after_payload_deletion_and_new_view_activation() {
    // Manual storage/actor polling needs paused time without Tokio's task budget.
    tokio::task::coop::unconstrained(async { exercise_cluster(false) }).await;
}

#[tokio::test(start_paused = true)]
async fn accepted_only_dedup_tail_survives_checkpoint_recovery_and_new_view_confirmation() {
    tokio::task::coop::unconstrained(async { exercise_cluster(true) }).await;
}

#[expect(
    clippy::too_many_lines,
    reason = "keep the controlled fault schedule and its assertions together"
)]
fn exercise_cluster(unconfirmed_tail: bool) {
    let (mut controller, mut actors, config, actor_memory) = recovered_cluster(unconfirmed_tail);
    let mut now = Duration::ZERO;
    for step in 0..20000 {
        // Finish bounded storage and actor turns at each clock instant.
        // Advancing every turn would time out healthy multi-step recovery.
        now = Duration::from_millis(500 * (step / 2000));
        round(&mut actors, now);
        scheduled::settle(&mut controller, &[]);
        if actors.iter().all(|actor| status(actor).application_ready) {
            break;
        }
    }
    assert!(
        actors.iter().all(|actor| status(actor).application_ready),
        "activation: {:?}",
        actors.iter().map(status).collect::<Vec<_>>()
    );
    let view = status(&actors[1]).scope.view;
    assert!(view > 0);
    for actor in &actors {
        let normal = status(actor).normal.unwrap();
        assert_eq!(normal.applied.op.0, 5 + u64::from(unconfirmed_tail));
        assert_eq!(normal.accepted, normal.committed);
        assert_eq!(normal.committed, normal.applied);
    }
    let leader = config.configuration.configuration().primary(view);
    let actor = actors[usize::from(leader.as_bytes()[0] - 1)]
        .startup_actor(GroupId::from_bytes([7; 16]))
        .unwrap()
        .unwrap();
    let mut lane = actor.take_submitter().unwrap();
    let mut proposal = actor.lease_proposal_buffer_with_limits(pipeline()).unwrap();
    let open = OperationBody::OpenProducer(OpenProducer {
        partition: PartitionIncarnation::from_bytes([11; 16]),
        producer_id: ProducerId::from_bytes([30; 16]),
        expected_epoch: None,
        new_epoch: ProducerEpoch::INITIAL,
        operation_id: OperationId::from_bytes([31; 16]),
    });
    proposal
        .push(
            open.kind(),
            &encode_operation_body(&open, config.limits.operations).unwrap(),
        )
        .unwrap();
    let mut reply = complete(
        &mut controller,
        &mut actors,
        lane.try_submit(proposal).unwrap(),
        now,
    );
    reply.buffer.clear();
    let mut duplicate = request(30, 0, 1);
    duplicate.records[0].parts = vec![b"changed redundant payload".as_slice()].into();
    reply.buffer.prepare_append(duplicate).unwrap();
    let mut reply = complete(
        &mut controller,
        &mut actors,
        lane.try_submit(reply.buffer).unwrap(),
        now,
    );
    let (kind, bytes) = reply.buffer.bodies().next().unwrap();
    assert_eq!(kind, OperationKind::ResolvedAppend);
    let append = decode_resolved_append(bytes, config.limits.operations).unwrap();
    assert_eq!(append.results.stored(), 0);
    let result = append
        .results
        .attempts_with_fingerprints()
        .next()
        .unwrap()
        .0;
    assert_eq!(result.disposition, AppendDisposition::Duplicate);
    assert_eq!(result.offset, Offset::ZERO);
    reply.buffer.clear();
    if unconfirmed_tail {
        let mut duplicate = request(30, 1, 1);
        duplicate.records[0].message_id = MessageId::from_bytes([5; 16]);
        duplicate.records[0].parts = vec![b"redundant accepted-only event".as_slice()].into();
        reply.buffer.prepare_append(duplicate).unwrap();
        reply = complete(
            &mut controller,
            &mut actors,
            lane.try_submit(reply.buffer).unwrap(),
            now,
        );
        let append = decode_resolved_append(
            reply.buffer.bodies().next().unwrap().1,
            config.limits.operations,
        )
        .unwrap();
        assert_eq!(append.results.stored(), 0);
        let result = append
            .results
            .attempts_with_fingerprints()
            .next()
            .unwrap()
            .0;
        assert_eq!(result.disposition, AppendDisposition::Duplicate);
        assert_eq!(result.offset, Offset::new(4));
        reply.buffer.clear();
    }
    let mut unique = request(30, 1 + u64::from(unconfirmed_tail), 1);
    unique.records[0].message_id = MessageId::from_bytes([5 + u8::from(unconfirmed_tail); 16]);
    reply.buffer.prepare_append(unique).unwrap();
    let reply = complete(
        &mut controller,
        &mut actors,
        lane.try_submit(reply.buffer).unwrap(),
        now,
    );
    let append = decode_resolved_append(
        reply.buffer.bodies().next().unwrap().1,
        config.limits.operations,
    )
    .unwrap();
    assert_eq!(append.results.stored(), 1);
    assert_eq!(
        append.batch.summary.first_offset,
        Offset::new(4 + u64::from(unconfirmed_tail))
    );
    drop(reply);
    drop(lane);
    for actor in actors {
        drive(&mut controller, actor.shutdown()).unwrap();
    }
    actor_memory.trim_cache();
    assert_eq!(actor_memory.allocated_bytes(), 0);
}

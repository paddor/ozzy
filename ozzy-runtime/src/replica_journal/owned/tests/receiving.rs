use super::replay::persist;
use super::writeback::{Replica, confirm, replica};
use super::*;
mod checkpoint;
mod faults;
mod repair;
use crate::replica_journal::{InstallationConfig, PinnedRecovery, RecoveryPlan, RecoveryStartup};
use ozzy_journal_segment::BodyEncoding;
use ozzy_proto::RequestId;
use ozzy_replication::{
    recovery::{Recovery, RecoveryError, RecoveryTicket},
    wire::FetchOps,
};

fn generations(attempt: u128) -> RecoveryGenerations {
    RecoveryGenerations {
        attempt: JournalGeneration(attempt),
        temporary: JournalGeneration(attempt + 1),
    }
}

fn receiver_config(policy: QuorumPolicy) -> OwnedConfig {
    let mut config = config("/replacement", 7, policy);
    config.identity.replica_node_id = NodeId::from_bytes([2; 16]);
    config
}

fn physical() -> InstallationConfig {
    InstallationConfig {
        segment_capacity: 8192,
        body_encoding: BodyEncoding::Raw,
        max_staged_bytes: 128 * 1024,
        max_orphan_probes: 16,
    }
}

fn authorize(
    controller: &mut Controller,
    primary: &mut Replica,
    backup: &Replica,
    startup: RecoveryStartup,
    nonce: u8,
) -> (Recovery, RecoveryTicket, PinnedRecovery) {
    let local = startup.local();
    let nonce = RequestId::from_bytes([nonce; 16]);
    let mut recovery = startup.into_recovery(nonce, pipeline()).unwrap();
    let response = primary
        .driver
        .normal()
        .unwrap()
        .recovery_response(nonce)
        .unwrap();
    let pin = drive(controller, primary.journal.pin_recovery(local, response)).unwrap();
    assert_eq!(recovery.begin_transfer(), Err(RecoveryError::QuorumMissing));
    recovery
        .receive(primary.config.identity.replica_node_id, pin.response())
        .unwrap();
    assert_eq!(recovery.begin_transfer(), Err(RecoveryError::QuorumMissing));
    recovery
        .receive(
            backup.config.identity.replica_node_id,
            backup
                .driver
                .normal()
                .unwrap()
                .recovery_response(nonce)
                .unwrap(),
        )
        .unwrap();
    let ticket = recovery.begin_transfer().unwrap();
    (recovery, ticket, pin)
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "fixtures retain copied transfer evidence"
)]
fn transfer(
    controller: &mut Controller,
    primary: &mut Replica,
    receiver: &mut RecoveringJournal,
    recovery: &mut Recovery,
    ticket: RecoveryTicket,
    pin: PinnedRecovery,
    mut plan: RecoveryPlan,
) {
    let mut next = Prefix::GENESIS;
    if let Some(anchor) = ticket.checkpoint() {
        let mut offset = 0;
        while offset < anchor.state_bytes {
            let request = ozzy_replication::wire::CheckpointRequest {
                scope: ticket.scope(),
                request_id: RequestId::from_bytes([64; 16]),
                nonce: ticket.nonce(),
                source: ticket.source(),
                offset,
                max_bytes: 128,
            };
            let work = primary
                .journal
                .prepare_checkpoint_read(pin, request)
                .unwrap();
            let done = drive(controller, work.read());
            let chunk = primary.journal.complete_checkpoint_read(done).unwrap();
            let progress = drive(
                controller,
                receiver.receive_checkpoint(ticket, offset, &chunk.bytes),
            )
            .unwrap();
            offset = progress.through;
            if let Some(revision) = progress.revision {
                recovery
                    .complete_checkpoint(ticket, anchor, revision)
                    .unwrap();
            }
        }
        next = anchor.predecessor;
    }
    loop {
        let (predecessor, through) = match plan {
            RecoveryPlan::Full if next == ticket.source().accepted => break,
            RecoveryPlan::Full => (next, ticket.source().accepted),
            RecoveryPlan::Repair(None) => break,
            RecoveryPlan::Repair(Some(range)) => (prefix(range.after), prefix(range.through)),
            RecoveryPlan::RetryFull => panic!("matching donor"),
        };
        let request = FetchOps {
            scope: ticket.scope(),
            source: ticket.source(),
            predecessor,
            request_id: RequestId::from_bytes([63; 16]),
            max_operations: 1,
            max_body_bytes: 8192,
        };
        let buffer = primary.journal.lease_append_buffer().unwrap();
        let work = primary
            .journal
            .prepare_recovery_read(pin, request, buffer)
            .unwrap();
        let done = drive(controller, work.read());
        let fetched = primary.journal.complete_recovery_read(done).unwrap();
        assert!(fetched.end().op <= through.op);
        let mut buffer = receiver.lease_append_buffer().unwrap();
        for op in fetched.buffer().operations() {
            buffer.push(op).unwrap();
        }
        let chunk = drive(controller, receiver.receive_chunk(ticket, buffer)).unwrap();
        next = chunk.end();
        plan = chunk.plan();
        if plan == RecoveryPlan::Full {
            recovery.validate_chunk(ticket, chunk.prepared()).unwrap();
        }
    }
}

#[test]
fn owned_recovery_full_transfer_both_policies_preserve_unconfirmed_tail_and_fence_restart() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut primary = replica(&mut controller, io.clone(), 0, policy, 8192);
        let mut backup = replica(&mut controller, io.clone(), 2, policy, 8192);
        let committed = persist(&mut controller, &mut primary);
        persist(&mut controller, &mut backup);
        confirm(&mut primary, &backup, policy);
        let accepted = persist(&mut controller, &mut primary);
        let config = receiver_config(policy);
        let (mut receiver, startup) = drive(
            &mut controller,
            RecoveringJournal::start(
                config.clone(),
                io.clone(),
                generations(50),
                RecoveryOpen::FormatNew {
                    segment_capacity: 32768,
                },
            ),
        )
        .unwrap();
        let memory = payload_owner(32 * 1024);
        receiver.bind_append_memory(&memory).unwrap();
        let (mut recovery, ticket, pin) =
            authorize(&mut controller, &mut primary, &backup, startup, 62);
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
        assert_eq!(publication.applied(), committed);
        assert_eq!(
            drive(&mut controller, receiver.finish_recovery(ticket)).unwrap(),
            publication
        );
        let (journal, startup) = drive(
            &mut controller,
            receiver.into_journal(&mut recovery, publication, JournalGeneration(70)),
        )
        .unwrap();
        let restored = startup.recovered().unwrap();
        memory.trim_cache();
        assert_eq!(memory.allocated_bytes(), 0);
        let mut buffer = journal.lease_append_buffer().unwrap();
        buffer.push_read_part(&[7; 128]).unwrap();
        assert_eq!(
            memory.allocated_bytes(),
            128,
            "adoption preserved the same budget"
        );
        drop(buffer);
        assert_eq!(restored.log.accepted, accepted);
        assert_eq!(restored.log.committed, committed);
        assert!(journal.images().is_err());
        assert!(
            startup
                .into_driver(Duration::ZERO, timing(), pipeline())
                .unwrap()
                .normal()
                .is_none()
        );
        drive(&mut controller, journal.shutdown()).unwrap();
        primary.journal.release_recovery(pin).unwrap();
        drive(&mut controller, primary.journal.shutdown()).unwrap();
        drive(&mut controller, backup.journal.shutdown()).unwrap();
    }
}

#[test]
fn owned_recovery_canceled_begin_stays_nonvoting_and_does_not_stop_other_partitions() {
    let (mut controller, io) = setup();
    let mut primary = replica(&mut controller, io.clone(), 0, QuorumPolicy::Durable, 8192);
    let backup = replica(&mut controller, io.clone(), 2, QuorumPolicy::Durable, 8192);
    persist(&mut controller, &mut primary);
    let config = receiver_config(QuorumPolicy::Durable);
    let (mut receiver, startup) = drive(
        &mut controller,
        RecoveringJournal::start(
            config.clone(),
            io.clone(),
            generations(50),
            RecoveryOpen::FormatNew {
                segment_capacity: 32768,
            },
        ),
    )
    .unwrap();
    let (_, ticket, pin) = authorize(&mut controller, &mut primary, &backup, startup, 62);
    let mut pending = Box::pin(receiver.begin_recovery(ticket, physical()));
    assert!(poll(pending.as_mut()).is_pending());
    let held = controller.jobs()[0].0;
    drop(pending);
    assert!(receiver.is_faulted());
    // Independent owner makes progress while canceled work retains the old lock.
    let (_, _, receipt) = super::writeback::admit(&mut controller, &mut primary, 1);
    let work = super::writeback::prepare(&mut primary);
    let done = drive_except(&mut controller, work.write(), Some(held), |_| {
        Effect::Normal
    });
    primary.journal.complete_write(done).unwrap();
    super::writeback::settle(&mut primary, receipt);
    assert!(
        drive_except(
            &mut controller,
            OwnedJournal::open(config.clone(), io.clone(), JournalGeneration(60)),
            Some(held),
            |_| Effect::Normal
        )
        .is_err()
    );
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    drop(receiver);
    assert!(
        drive(
            &mut controller,
            OwnedJournal::open(config.clone(), io.clone(), JournalGeneration(60))
        )
        .is_err()
    );
    let (receiver, _) = drive(
        &mut controller,
        RecoveringJournal::start(config, io, generations(70), RecoveryOpen::Resume),
    )
    .unwrap();
    drive(&mut controller, receiver.shutdown()).unwrap();
    primary.journal.release_recovery(pin).unwrap();
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}

#[test]
fn owned_recovery_quarantine_rejects_unsupported_profile_before_marker_publication() {
    let (mut controller, io) = setup();
    let config = receiver_config(QuorumPolicy::Durable);
    let (journal, _) = drive(
        &mut controller,
        OwnedJournal::format_new(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    drive(&mut controller, journal.shutdown()).unwrap();
    let before = controller
        .image()
        .bytes(&config.root.join("CONFIGURATION"), false)
        .unwrap()
        .to_vec();
    let mut bad = config.clone();
    bad.limits.io.max_segment_bytes = 8192;
    assert!(
        drive(
            &mut controller,
            RecoveringJournal::start(bad, io.clone(), generations(50), RecoveryOpen::Quarantine)
        )
        .is_err()
    );
    assert_eq!(
        controller
            .image()
            .bytes(&config.root.join("CONFIGURATION"), false)
            .unwrap(),
        before
    );
    let (journal, startup) = drive(
        &mut controller,
        OwnedJournal::open(config, io, JournalGeneration(70)),
    )
    .unwrap();
    assert!(startup.recovered().is_some());
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owned_checkpoint_recovery_after_retirement_preserves_confirmed_anchor_and_accepted_tail() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut primary = replica(&mut controller, io.clone(), 0, policy, 8192);
        let mut backup = replica(&mut controller, io.clone(), 2, policy, 8192);
        let old = persist(&mut controller, &mut primary);
        persist(&mut controller, &mut backup);
        confirm(&mut primary, &backup, policy);
        let roll = primary.journal.begin_roll(8).unwrap();
        let done = drive(&mut controller, roll.publish());
        primary.journal.complete_roll(done).unwrap();
        let committed = persist(&mut controller, &mut primary);
        persist(&mut controller, &mut backup);
        confirm(&mut primary, &backup, policy);
        let ticket = primary.driver.begin_validation().unwrap();
        let retired = drive(
            &mut controller,
            primary.journal.retire_confirmed_history(
                ticket,
                ozzy_proto::CheckpointId::from_bytes([95; 16]),
                ozzy_journal_segment::AsyncRetirementBudget {
                    max_segments: 1,
                    max_read_bytes: 32768,
                },
            ),
        )
        .unwrap();
        assert_eq!(retired.unreferenced_segment_ids, [1]);
        let accepted = persist(&mut controller, &mut primary);
        let (mut receiver, startup) = drive(
            &mut controller,
            RecoveringJournal::start(
                receiver_config(policy),
                io.clone(),
                generations(150),
                RecoveryOpen::FormatNew {
                    segment_capacity: 32768,
                },
            ),
        )
        .unwrap();
        let memory = payload_owner(1024 * 1024);
        receiver.bind_append_memory(&memory).unwrap();
        let (mut recovery, ticket, pin) =
            authorize(&mut controller, &mut primary, &backup, startup, 96);
        let anchor = ticket
            .checkpoint()
            .expect("selected checkpoint required after retirement");
        assert_eq!(anchor.predecessor, old);
        assert_eq!(anchor.position, committed);
        assert_eq!(ticket.source().accepted, accepted);
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
        let (journal, startup) = drive(
            &mut controller,
            receiver.into_journal(&mut recovery, publication, JournalGeneration(170)),
        )
        .unwrap();
        let restored = startup.recovered().unwrap();
        assert_eq!(restored.log.accepted, accepted);
        assert_eq!(restored.log.committed, committed);
        assert!(
            startup
                .into_driver(Duration::ZERO, timing(), pipeline())
                .unwrap()
                .normal()
                .is_none()
        );
        let files = journal
            .journal
            .readable()
            .unwrap()
            .capture_checkpoint()
            .unwrap();
        assert_eq!(
            files.manifest().store_id,
            receiver_config(policy).identity.store_id
        );
        assert_eq!(
            files.manifest().position,
            crate::replica_journal::authority::position(committed)
        );
        drop(files);
        drive(&mut controller, journal.shutdown()).unwrap();
        primary.journal.release_recovery(pin).unwrap();
        drive(&mut controller, primary.journal.shutdown()).unwrap();
        drive(&mut controller, backup.journal.shutdown()).unwrap();
    }
}

#[test]
fn owned_checkpoint_recovery_at_the_accepted_tail_needs_no_operations() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut primary = replica(&mut controller, io.clone(), 0, policy, 8192);
        let mut backup = replica(&mut controller, io.clone(), 2, policy, 8192);
        let accepted = persist(&mut controller, &mut primary);
        persist(&mut controller, &mut backup);
        confirm(&mut primary, &backup, policy);
        let roll = primary.journal.begin_roll(8).unwrap();
        let done = drive(&mut controller, roll.publish());
        primary.journal.complete_roll(done).unwrap();
        let retired = drive(
            &mut controller,
            primary.journal.retire_confirmed_history(
                primary.driver.begin_validation().unwrap(),
                ozzy_proto::CheckpointId::from_bytes([97; 16]),
                ozzy_journal_segment::AsyncRetirementBudget {
                    max_segments: 1,
                    max_read_bytes: 32768,
                },
            ),
        )
        .unwrap();
        assert_eq!(retired.unreferenced_segment_ids, [1]);
        let (mut receiver, startup) = drive(
            &mut controller,
            RecoveringJournal::start(
                receiver_config(policy),
                io.clone(),
                generations(180),
                RecoveryOpen::FormatNew {
                    segment_capacity: 32768,
                },
            ),
        )
        .unwrap();
        let memory = payload_owner(1024 * 1024);
        receiver.bind_append_memory(&memory).unwrap();
        let (mut recovery, ticket, pin) =
            authorize(&mut controller, &mut primary, &backup, startup, 98);
        let anchor = ticket.checkpoint().unwrap();
        assert_eq!(anchor.predecessor, accepted);
        assert_eq!(anchor.position, accepted);
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
        let (journal, startup) = drive(
            &mut controller,
            receiver.into_journal(&mut recovery, publication, JournalGeneration(190)),
        )
        .unwrap();
        assert_eq!(startup.recovered().unwrap().log.accepted, accepted);
        assert_eq!(startup.recovered().unwrap().log.committed, accepted);
        memory.trim_cache();
        assert_eq!(memory.allocated_bytes(), 0);
        drive(&mut controller, journal.shutdown()).unwrap();
        primary.journal.release_recovery(pin).unwrap();
        drive(&mut controller, primary.journal.shutdown()).unwrap();
        drive(&mut controller, backup.journal.shutdown()).unwrap();
    }
}

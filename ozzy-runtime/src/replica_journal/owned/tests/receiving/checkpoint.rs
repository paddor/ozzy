use super::*;
use crate::replica_journal::{RecoveryStorage, ShardJournalConfig, ShardRecoveringJournal};

#[cfg(feature = "simulation")]
mod messages;

#[test]
fn checkpoint_receive_charges_pending_chunks_and_refuses_exhausted_memory() {
    let (mut controller, io) = setup();
    let (mut primary, backup) = donors(&mut controller, &io, QuorumPolicy::Durable);
    let (mut receiver, startup) = drive(
        &mut controller,
        RecoveringJournal::start(
            receiver_config(QuorumPolicy::Durable),
            io,
            generations(800),
            RecoveryOpen::FormatNew {
                segment_capacity: 32768,
            },
        ),
    )
    .unwrap();
    let memory = payload_owner(32768);
    receiver.bind_append_memory(&memory).unwrap();
    let (_, ticket, pin) = authorize(&mut controller, &mut primary, &backup, startup, 86);
    let mut receiver =
        ShardRecoveringJournal::from_owned(receiver, ShardJournalConfig::default(), || 123)
            .unwrap();
    let mut begin = Box::pin(receiver.begin_recovery(ticket, physical()).unwrap());
    drive(
        &mut controller,
        std::future::poll_fn(|cx| {
            assert!(receiver.poll_stopped(cx).is_pending());
            begin.as_mut().poll(cx)
        }),
    )
    .unwrap();
    let baseline = memory.allocated_bytes();
    let work = primary
        .journal
        .prepare_checkpoint_read(
            pin,
            ozzy_replication::wire::CheckpointRequest {
                scope: ticket.scope(),
                request_id: RequestId::from_bytes([88; 16]),
                nonce: ticket.nonce(),
                source: ticket.source(),
                offset: 0,
                max_bytes: 8192,
            },
        )
        .unwrap();
    let done = drive(&mut controller, work.read());
    let chunk = primary.journal.complete_checkpoint_read(done).unwrap();
    let held = memory.try_lease(32768 - baseline).unwrap();
    assert!(matches!(
        receiver.receive_checkpoint(ticket, 0, chunk.bytes.clone()),
        Err(crate::replica_journal::SubmitError::Full)
    ));
    drop(held);
    memory.trim_cache();
    // A rejected chunk must not queue work or fault the owner. Its next accepted
    // command owns compact, charged bytes while cooperative validation yields.
    let completion = receiver
        .receive_checkpoint(ticket, 0, chunk.bytes.clone())
        .unwrap();
    assert!(memory.allocated_bytes() >= baseline + chunk.bytes.len());
    drop(completion);
    drive(&mut controller, receiver.shutdown()).unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
    primary.journal.release_recovery(pin).unwrap();
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
}

fn donors(controller: &mut Controller, io: &Local, policy: QuorumPolicy) -> (Replica, Replica) {
    let mut primary = replica(controller, io.clone(), 0, policy, 8192);
    let mut backup = replica(controller, io.clone(), 2, policy, 8192);
    persist(controller, &mut primary);
    persist(controller, &mut backup);
    confirm(&mut primary, &backup, policy);
    let roll = primary.journal.begin_roll(8).unwrap();
    let done = drive(controller, roll.publish());
    primary.journal.complete_roll(done).unwrap();
    drive(
        controller,
        primary.journal.retire_confirmed_history(
            primary.driver.begin_validation().unwrap(),
            ozzy_proto::CheckpointId::from_bytes([87; 16]),
            ozzy_journal_segment::AsyncRetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .unwrap();
    (primary, backup)
}

#[test]
fn interrupted_checkpoint_receive_restarts_with_a_fresh_nonce_and_releases_memory() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let (mut primary, backup) = donors(&mut controller, &io, policy);
        let config = receiver_config(policy);
        let memory = payload_owner(32768);
        let (mut receiver, startup) = drive(
            &mut controller,
            RecoveringJournal::start(
                config.clone(),
                io.clone(),
                generations(840),
                RecoveryOpen::FormatNew {
                    segment_capacity: 32768,
                },
            ),
        )
        .unwrap();
        receiver.bind_append_memory(&memory).unwrap();
        let (_, old, pin) = authorize(&mut controller, &mut primary, &backup, startup, 93);
        drive(&mut controller, receiver.begin_recovery(old, physical())).unwrap();
        let work = primary
            .journal
            .prepare_checkpoint_read(
                pin,
                ozzy_replication::wire::CheckpointRequest {
                    scope: old.scope(),
                    request_id: RequestId::from_bytes([94; 16]),
                    nonce: old.nonce(),
                    source: old.source(),
                    offset: 0,
                    max_bytes: 64,
                },
            )
            .unwrap();
        let done = drive(&mut controller, work.read());
        let chunk = primary.journal.complete_checkpoint_read(done).unwrap();
        let progress = drive(
            &mut controller,
            receiver.receive_checkpoint(old, 0, &chunk.bytes),
        )
        .unwrap();
        assert!(progress.revision.is_none());
        assert!(memory.allocated_bytes() > 0);
        drive(&mut controller, receiver.shutdown()).unwrap();
        memory.trim_cache();
        assert_eq!(memory.allocated_bytes(), 0);
        primary.journal.release_recovery(pin).unwrap();
        assert!(
            drive(
                &mut controller,
                OwnedJournal::open(config.clone(), io.clone(), JournalGeneration(850))
            )
            .is_err()
        );
        let (mut receiver, startup) = drive(
            &mut controller,
            RecoveringJournal::start(config, io, generations(860), RecoveryOpen::Resume),
        )
        .unwrap();
        receiver.bind_append_memory(&memory).unwrap();
        let (mut recovery, fresh, pin) =
            authorize(&mut controller, &mut primary, &backup, startup, 95);
        assert_ne!(old.nonce(), fresh.nonce());
        let plan = drive(&mut controller, receiver.begin_recovery(fresh, physical())).unwrap();
        assert!(
            drive(
                &mut controller,
                receiver.receive_checkpoint(old, 0, &chunk.bytes)
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
            receiver.into_journal(&mut recovery, publication, JournalGeneration(870)),
        )
        .unwrap();
        assert_eq!(
            startup.recovered().unwrap().log.accepted,
            fresh.source().accepted
        );
        drive(&mut controller, journal.shutdown()).unwrap();
        memory.trim_cache();
        assert_eq!(memory.allocated_bytes(), 0);
        primary.journal.release_recovery(pin).unwrap();
        drive(&mut controller, primary.journal.shutdown()).unwrap();
        drive(&mut controller, backup.journal.shutdown()).unwrap();
    }
}

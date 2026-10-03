use super::writeback::{Replica, admit, finish, prepare, replica, settle, synchronize};
use super::*;
use ozzy_proto::RequestId;
use ozzy_replication::{LogSource, OpNumber};

fn capture(
    replica: &mut Replica,
    predecessor: Prefix,
    bytes: usize,
) -> super::super::PreparedReplay {
    let ticket = replica.driver.begin_validation().unwrap();
    let buffer = replica.journal.lease_append_buffer().unwrap();
    replica
        .journal
        .prepare_replay(
            ticket,
            predecessor,
            PipelineLimits {
                max_operations: 8,
                max_body_bytes: bytes,
            },
            buffer,
            RequestId::from_bytes([42; 16]),
        )
        .unwrap()
}

pub(super) fn persist(controller: &mut Controller, replica: &mut Replica) -> Prefix {
    let (_, _, receipt) = admit(controller, replica, 1);
    let work = prepare(replica);
    finish(controller, replica, work);
    settle(replica, receipt);
    synchronize(controller, replica);
    replica.driver.normal().unwrap().snapshot().accepted
}

#[test]
fn owned_replay_refresh_reads_only_the_new_segment_suffix() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let first = persist(&mut controller, &mut replica);
    let initial = capture(&mut replica, Prefix::GENESIS, 8192);
    let done = drive(&mut controller, initial.read());
    let result = replica.journal.complete_replay(done).unwrap();
    assert_eq!(result.end(), first);
    drop(result);

    let second = persist(&mut controller, &mut replica);
    let refresh = capture(&mut replica, first, 8192);
    let mut reads = Vec::new();
    let done = drive_except(&mut controller, refresh.read(), None, |operation| {
        if let Operation::Read { offset, .. } = operation {
            reads.push(*offset);
        }
        Effect::Normal
    });
    let result = replica.journal.complete_replay(done).unwrap();
    assert_eq!(result.end(), second);
    assert!(!reads.is_empty());
    assert!(reads.iter().all(|offset| *offset > 0), "{reads:?}");
    drop(result);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_detached_replay_preserves_election_source_and_never_replaces_newer_cache() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let first = persist(&mut controller, &mut replica);
    let source = LogSource {
        voter: replica.config.identity.replica_node_id,
        generation: JournalGeneration(1),
        accepted: first,
    };
    replica.journal.capture_history(source).unwrap();
    let old_read = capture(&mut replica, Prefix::GENESIS, 8192);
    let second = persist(&mut controller, &mut replica);
    let ticket = replica.driver.begin_validation().unwrap();
    let positions = drive(
        &mut controller,
        replica
            .journal
            .replication_positions(ticket, [first.op, second.op]),
    )
    .unwrap();
    assert_eq!(positions.positions, [Some(first), Some(second)]);
    let done = drive(&mut controller, old_read.read());
    let result = replica.journal.complete_replay(done).unwrap();
    assert_eq!(result.end(), first);
    assert_eq!(result.request().source, source);
    drop(result);
    let current = capture(&mut replica, first, 8192);
    let mut jobs = 0;
    let done = drive_except(&mut controller, current.read(), None, |_| {
        jobs += 1;
        Effect::Normal
    });
    let result = replica.journal.complete_replay(done).unwrap();
    assert_eq!(result.end(), second);
    assert_eq!(jobs, 0, "reuse newer verified source cache");
    let old = drive(
        &mut controller,
        replica.journal.history_position(source, first.op),
    )
    .unwrap();
    assert_eq!(old.position, Some(first));
    drop(result);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_replay_body_credit_reports_required_size_without_advancing_history() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let end = persist(&mut controller, &mut replica);
    let work = capture(&mut replica, Prefix::GENESIS, 15);
    let done = drive(&mut controller, work.read());
    let result = replica.journal.complete_replay(done).unwrap();
    assert_eq!(result.end(), Prefix::GENESIS);
    assert_eq!(result.minimum_body_bytes(), Some(16));
    assert!(result.buffer().is_empty());
    drop(result);
    let work = capture(&mut replica, Prefix::GENESIS, 16);
    let done = drive(&mut controller, work.read());
    let result = replica.journal.complete_replay(done).unwrap();
    assert_eq!(result.end(), end);
    assert_eq!(result.minimum_body_bytes(), None);
    drop(result);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_replay_cancellation_frees_its_slot_and_does_not_cancel_writes() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Replicated, 8192);
    let first = persist(&mut controller, &mut replica);
    let work = capture(&mut replica, Prefix::GENESIS, 8192);
    let ticket = replica.driver.begin_validation().unwrap();
    let arena = replica.journal.lease_append_buffer().unwrap();
    assert!(matches!(
        replica.journal.prepare_replay(
            ticket,
            Prefix::GENESIS,
            pipeline(),
            arena,
            RequestId::from_bytes([43; 16])
        ),
        Err(JournalError::AppendCapacity)
    ));
    let mut reading = Box::pin(work.read());
    assert!(poll(reading.as_mut()).is_pending());
    let held = controller.jobs()[0].0;
    drop(reading);
    assert!(!replica.journal.is_faulted());
    let (_, _, receipt) = admit(&mut controller, &mut replica, 1);
    let work = prepare(&mut replica);
    let done = drive_except(&mut controller, work.write(), Some(held), |_| {
        Effect::Normal
    });
    replica.journal.complete_write(done).unwrap();
    settle(&mut replica, receipt);
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    let work = capture(&mut replica, first, 8192);
    let done = drive(&mut controller, work.read());
    let result = replica.journal.complete_replay(done).unwrap();
    assert_eq!(result.end().op, OpNumber(2));
    drop(result);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_replay_reserves_whole_chunk_and_waits_for_shared_capacity() {
    for (free, cancel) in [(0, false), (0, true), (32, false)] {
        let (mut controller, io) = setup();
        let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
        persist(&mut controller, &mut replica);
        let end = persist(&mut controller, &mut replica);
        let work = capture(&mut replica, Prefix::GENESIS, 32);
        let Some(memory) = replica.journal.append_memory.clone() else {
            panic!("fixture binds a shared payload owner")
        };
        memory.trim_cache();
        let held = memory
            .try_lease(1024 * 1024 - memory.allocated_bytes() - free)
            .unwrap();
        let mut reading = Box::pin(work.read());
        let done = if free == 0 {
            loop {
                assert!(
                    poll(reading.as_mut()).is_pending(),
                    "allocation pressure waits"
                );
                let jobs = controller.jobs();
                if jobs.is_empty() {
                    break;
                }
                for (id, _) in jobs {
                    controller.execute(id, Effect::Normal).unwrap();
                    controller.deliver(id).unwrap();
                }
            }
            if cancel {
                drop(reading);
                let work = capture(&mut replica, Prefix::GENESIS, 32);
                reading = Box::pin(work.read());
            }
            drop(held);
            drive(&mut controller, reading)
        } else {
            let done = drive(&mut controller, reading);
            drop(held);
            done
        };
        let result = replica.journal.complete_replay(done).unwrap();
        assert_eq!(result.end(), end);
        assert_eq!(result.buffer().len(), 2);
        assert_eq!(result.buffer().body_bytes(), 32);
        assert!(!replica.journal.is_faulted());
        drop(result);
        drive(&mut controller, replica.journal.shutdown()).unwrap();
    }
}

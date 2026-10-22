use super::writeback::{
    Replica, admit, finish, prepare, replica_with_pending, settle, synchronize,
};
use super::*;
use crate::replica_journal::{OwnedWriteStep, ProposalValidation};
use ozzy_journal::operation::{CanonicalOperation, OperationKind};
use ozzy_replication::OpNumber;

/// A buffer holding one operation of `kind`; only its kind is read.
fn single(replica: &Replica, kind: OperationKind) -> AppendBuffer {
    let mut buffer = replica.journal.lease_append_buffer().unwrap();
    buffer
        .push(CanonicalOperation {
            group_id: GroupId::from_bytes([7; 16]),
            configuration_epoch: 1,
            original_view: 0,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind,
            body: &[1; 16],
        })
        .unwrap();
    buffer
}

fn room(replica: &mut Replica, kind: OperationKind) -> Result<(), JournalError> {
    let buffer = single(replica, kind);
    replica.journal.check_room(&buffer)
}

fn write(controller: &mut Controller, replica: &mut Replica, groups: usize) {
    for _ in 0..groups {
        let work = prepare(replica);
        finish(controller, replica, work);
    }
}

/// Admit, write, and settle `groups` one-operation groups.
fn fill(controller: &mut Controller, replica: &mut Replica, groups: usize) {
    let (_, _, done) = admit(controller, replica, groups);
    write(controller, replica, groups);
    settle(replica, done);
}

/// Roll once nothing waits to be written, or a queued group needs the roll.
fn roll(controller: &mut Controller, replica: &mut Replica) {
    assert!(matches!(
        replica.journal.prepare_write().unwrap(),
        OwnedWriteStep::Idle | OwnedWriteStep::RollRequired
    ));
    let work = replica.journal.begin_roll(16).unwrap();
    let done = drive(controller, work.publish());
    replica.journal.complete_roll(done).unwrap();
}

#[test]
fn full_journal_refuses_client_work_then_holds_writes_without_faulting() {
    let (mut controller, io) = setup();
    // One operation per 4 KiB group, seven groups per 32 KiB segment. None
    // applies here, so all 43 stay pending.
    let mut replica = replica_with_pending(&mut controller, io, 0, 16, 64);
    replica.journal.limits.metadata.max_segments = 6;
    for _ in 0..2 {
        fill(&mut controller, &mut replica, 7);
        roll(&mut controller, &mut replica);
    }
    fill(&mut controller, &mut replica, 1);

    // Fewer than four segments left: records stop, control requests still
    // pass.
    assert!(matches!(
        room(&mut replica, OperationKind::Barrier),
        Err(JournalError::RollCapacity)
    ));
    assert!(room(&mut replica, OperationKind::Progress).is_ok());
    let mut proposal = replica.journal.lease_proposal_buffer().unwrap();
    proposal.push(OperationKind::Barrier, &[200; 16]).unwrap();
    let ticket = replica.driver.begin_validation().unwrap();
    assert!(matches!(
        drive(
            &mut controller,
            replica.journal.propose_append(ticket, proposal, 777)
        ),
        ProposalValidation::Rejected {
            reason: JournalError::RollCapacity,
            ..
        }
    ));

    // Fewer than two left: control requests stop too. Trims and policy
    // changes free space and always pass.
    fill(&mut controller, &mut replica, 6);
    roll(&mut controller, &mut replica);
    fill(&mut controller, &mut replica, 7);
    roll(&mut controller, &mut replica);
    fill(&mut controller, &mut replica, 1);
    assert!(matches!(
        room(&mut replica, OperationKind::Progress),
        Err(JournalError::RollCapacity)
    ));
    assert!(room(&mut replica, OperationKind::Trim).is_ok());
    assert!(room(&mut replica, OperationKind::PartitionPolicy).is_ok());

    // An operation admitted anyway, as on a follower, holds its write.
    fill(&mut controller, &mut replica, 6);
    roll(&mut controller, &mut replica);
    fill(&mut controller, &mut replica, 7);
    let (_, _, held) = admit(&mut controller, &mut replica, 1);
    assert!(matches!(
        replica.journal.prepare_write().unwrap(),
        OwnedWriteStep::Waiting
    ));
    assert!(matches!(
        replica.journal.begin_roll(16),
        Err(JournalError::RollCapacity)
    ));
    assert!(!replica.journal.is_faulted());

    // Room for another segment resumes the held write.
    replica.journal.limits.metadata.max_segments = 7;
    roll(&mut controller, &mut replica);
    write(&mut controller, &mut replica, 1);
    settle(&mut replica, held);
    synchronize(&mut controller, &mut replica);
    assert_eq!(
        replica.driver.normal().unwrap().snapshot().journal.durable,
        OpNumber(43)
    );
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

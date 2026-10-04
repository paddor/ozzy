use super::replay::persist;
use super::writeback::{Replica, admit, finish, prepare, replica, settle};
use super::*;
use crate::replica_journal::PinnedRecovery;
use ozzy_proto::RequestId;
use ozzy_replication::{OpNumber, wire::FetchOps};

fn request(pin: &PinnedRecovery) -> FetchOps {
    FetchOps {
        scope: pin.response().scope,
        request_id: RequestId::from_bytes([42; 16]),
        source: pin.source(),
        predecessor: Prefix::GENESIS,
        max_operations: 8,
        max_body_bytes: 8192,
    }
}
fn read(replica: &mut Replica, pin: &PinnedRecovery) -> super::super::PreparedRecoveryRead {
    let buffer = replica.journal.lease_append_buffer().unwrap();
    replica
        .journal
        .prepare_recovery_read(*pin, request(pin), buffer)
        .unwrap()
}

#[test]
fn owned_donor_canceled_reads_keep_exact_source_while_writes_and_other_donor_continue() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let first = persist(&mut controller, &mut replica);
    let response = replica
        .driver
        .normal()
        .unwrap()
        .recovery_response(RequestId::from_bytes([91; 16]))
        .unwrap();
    let pin2 = drive(
        &mut controller,
        replica
            .journal
            .pin_recovery(NodeId::from_bytes([2; 16]), response),
    )
    .unwrap();
    let pin3 = drive(
        &mut controller,
        replica
            .journal
            .pin_recovery(NodeId::from_bytes([3; 16]), response),
    )
    .unwrap();
    // Lose the cache but retain the independently held segment metadata.
    drop(read(&mut replica, &pin2));
    assert_eq!(
        drive(
            &mut controller,
            replica.journal.pin_recovery(pin2.requester(), response)
        )
        .unwrap(),
        pin2
    );
    let mut reading = Box::pin(read(&mut replica, &pin2).read());
    assert!(poll(reading.as_mut()).is_pending());
    let held = controller.jobs()[0].0;
    drop(reading);
    let (_, _, receipt) = admit(&mut controller, &mut replica, 1);
    let work = prepare(&mut replica);
    let done = drive_except(&mut controller, work.write(), Some(held), |_| {
        Effect::Normal
    });
    replica.journal.complete_write(done).unwrap();
    settle(&mut replica, receipt);
    let other = read(&mut replica, &pin3);
    let done = drive_except(&mut controller, other.read(), Some(held), |_| {
        Effect::Normal
    });
    assert_eq!(
        replica.journal.complete_recovery_read(done).unwrap().end(),
        first
    );
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();
    let retried = read(&mut replica, &pin2);
    let done = drive(&mut controller, retried.read());
    let result = replica.journal.complete_recovery_read(done).unwrap();
    assert_eq!(result.end(), first);
    assert_eq!(result.buffer().len(), 1);
    drop(result);
    assert_eq!(
        replica.driver.normal().unwrap().snapshot().accepted.op,
        OpNumber(2)
    );
    replica.journal.release_recovery(pin2).unwrap();
    replica.journal.release_recovery(pin3).unwrap();
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_released_and_recreated_donor_rejects_old_completion_even_for_same_nonce() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    persist(&mut controller, &mut replica);
    let response = replica
        .driver
        .normal()
        .unwrap()
        .recovery_response(RequestId::from_bytes([91; 16]))
        .unwrap();
    let requester = NodeId::from_bytes([2; 16]);
    let pin = drive(
        &mut controller,
        replica.journal.pin_recovery(requester, response),
    )
    .unwrap();
    let old = read(&mut replica, &pin);
    replica.journal.release_recovery(pin).unwrap();
    let fresh = drive(
        &mut controller,
        replica.journal.pin_recovery(requester, response),
    )
    .unwrap();
    assert_eq!(fresh, pin);
    let done = drive(&mut controller, old.read());
    assert!(matches!(
        replica.journal.complete_recovery_read(done),
        Err(JournalError::HistorySourceMismatch)
    ));
    assert!(!replica.journal.is_faulted());
    let new = read(&mut replica, &fresh);
    let done = drive(&mut controller, new.read());
    assert!(replica.journal.complete_recovery_read(done).is_ok());
    replica.journal.release_recovery(fresh).unwrap();
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_donor_requires_installed_bytes_and_preserves_truncated_logical_snapshot() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Replicated, 8192);
    let (_, _, receipt1) = admit(&mut controller, &mut replica, 1);
    let response = replica
        .driver
        .normal()
        .unwrap()
        .recovery_response(RequestId::from_bytes([91; 16]))
        .unwrap();
    let requester = NodeId::from_bytes([2; 16]);
    assert!(
        drive(
            &mut controller,
            replica.journal.pin_recovery(requester, response)
        )
        .is_err()
    );
    assert!(!replica.journal.is_faulted());
    let (_, _, receipt2) = admit(&mut controller, &mut replica, 1);
    let work = prepare(&mut replica);
    finish(&mut controller, &mut replica, work);
    settle(&mut replica, receipt1);
    settle(&mut replica, receipt2);
    // Snapshot ends inside the physical group. Recreated readers retain that
    // logical end while validating the entire enclosing physical source.
    let pin = drive(
        &mut controller,
        replica.journal.pin_recovery(requester, response),
    )
    .unwrap();
    drop(read(&mut replica, &pin));
    let work = read(&mut replica, &pin);
    let done = drive(&mut controller, work.read());
    let result = replica.journal.complete_recovery_read(done).unwrap();
    assert_eq!(result.end(), response.primary.unwrap().accepted);
    assert_eq!(result.buffer().len(), 1);
    drop(result);
    replica.journal.release_recovery(pin).unwrap();
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

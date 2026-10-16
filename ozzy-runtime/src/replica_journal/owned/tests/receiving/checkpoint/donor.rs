use super::*;
use ozzy_replication::wire::CheckpointRequest;

fn pin(controller: &mut Controller, primary: &mut Replica) -> PinnedRecovery {
    let response = primary
        .driver
        .normal()
        .unwrap()
        .recovery_response(RequestId::from_bytes([110; 16]))
        .unwrap();
    drive(
        controller,
        primary
            .journal
            .pin_recovery(NodeId::from_bytes([2; 16]), response),
    )
    .unwrap()
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "copyable tickets match production request APIs"
)]
fn request(pin: PinnedRecovery, offset: u64) -> CheckpointRequest {
    CheckpointRequest {
        scope: pin.response().scope,
        request_id: RequestId::from_bytes([111; 16]),
        nonce: pin.response().nonce,
        source: pin.source(),
        offset,
        max_bytes: 128,
    }
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "copyable tickets match production request APIs"
)]
fn read(
    controller: &mut Controller,
    primary: &mut Replica,
    pin: PinnedRecovery,
    offset: u64,
) -> bytes::Bytes {
    let work = primary
        .journal
        .prepare_checkpoint_read(pin, request(pin, offset))
        .unwrap();
    let done = drive(controller, work.read());
    primary
        .journal
        .complete_checkpoint_read(done)
        .unwrap()
        .bytes
}

#[test]
fn checkpoint_donor_responses_stay_charged_through_their_final_alias() {
    let (mut controller, io) = setup();
    let (mut primary, backup) = donors(&mut controller, &io);
    let memory = primary.journal.append_memory.clone().unwrap();
    let pin = pin(&mut controller, &mut primary);
    let first = read(&mut controller, &mut primary, pin, 0);
    let alias = first.clone();
    assert_eq!(first.len(), 128);
    assert!(memory.allocated_bytes() >= 128);
    let repeated = read(&mut controller, &mut primary, pin, 0);
    assert_eq!(repeated, first);
    drop(repeated);
    drop(first);
    primary.journal.release_recovery(pin).unwrap();
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 128);
    drop(alias);
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
}

#[test]
fn checkpoint_donor_memory_exhaustion_preserves_its_cursor_and_source() {
    let (mut controller, io) = setup();
    let (mut primary, backup) = donors(&mut controller, &io);
    let memory = primary.journal.append_memory.clone().unwrap();
    let pin = pin(&mut controller, &mut primary);
    let first = read(&mut controller, &mut primary, pin, 0);
    drop(first);
    memory.trim_cache();
    let baseline = memory.allocated_bytes();
    let held = memory.try_lease(1024 * 1024 - baseline).unwrap();
    assert!(matches!(
        primary
            .journal
            .prepare_checkpoint_read(pin, request(pin, 128)),
        Err(JournalError::AppendCapacity)
    ));
    assert!(!primary.journal.is_faulted());
    drop(held);
    memory.trim_cache();
    let work = primary
        .journal
        .prepare_checkpoint_read(pin, request(pin, 128))
        .unwrap();
    let mut opens = 0;
    let done = drive_except(&mut controller, work.read(), None, |operation| {
        if matches!(operation, Operation::Open { .. }) {
            opens += 1;
        }
        Effect::Normal
    });
    let second = primary.journal.complete_checkpoint_read(done).unwrap();
    assert_eq!(opens, 0, "exhaustion must not discard the open cursor");
    assert!(!second.bytes.is_empty());
    drop(second);
    primary.journal.release_recovery(pin).unwrap();
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
}

#[test]
fn checkpoint_donor_cancellation_and_repin_fence_old_cursor_completions() {
    let (mut controller, io) = setup();
    let (mut primary, backup) = donors(&mut controller, &io);
    let memory = primary.journal.append_memory.clone().unwrap();
    let pin = pin(&mut controller, &mut primary);
    let first = read(&mut controller, &mut primary, pin, 0);
    drop(first);
    let work = primary
        .journal
        .prepare_checkpoint_read(pin, request(pin, 128))
        .unwrap();
    let mut reading = Box::pin(work.read());
    assert!(poll(reading.as_mut()).is_pending());
    let held = controller.jobs()[0].0;
    assert!(matches!(
        controller.operation(held).unwrap().unprotected(),
        Operation::Read { .. }
    ));
    drop(reading);
    let work = primary
        .journal
        .prepare_checkpoint_read(pin, request(pin, 128))
        .unwrap();
    let done = drive_except(&mut controller, work.read(), Some(held), |_| Effect::Normal);
    let second = primary.journal.complete_checkpoint_read(done).unwrap();
    assert!(!second.bytes.is_empty());
    let offset = second.request.offset + second.bytes.len() as u64;
    drop(second);
    assert!(!primary.journal.is_faulted());
    let work = primary
        .journal
        .prepare_checkpoint_read(pin, request(pin, offset))
        .unwrap();
    let mut read_bytes = 0;
    let done = drive_except(&mut controller, work.read(), Some(held), |operation| {
        if let Operation::Read { length, .. } = operation {
            read_bytes += length;
        }
        Effect::Normal
    });
    let third = primary.journal.complete_checkpoint_read(done).unwrap();
    assert_eq!(
        read_bytes,
        third.bytes.len(),
        "resumed cursor must not rescan"
    );
    drop(third);
    controller.execute(held, Effect::Normal).unwrap();
    controller.deliver(held).unwrap();

    let old = primary
        .journal
        .prepare_checkpoint_read(pin, request(pin, 0))
        .unwrap();
    primary.journal.release_recovery(pin).unwrap();
    let response = pin.response();
    let fresh = drive(
        &mut controller,
        primary.journal.pin_recovery(pin.requester(), response),
    )
    .unwrap();
    assert_eq!(pin, fresh);
    let done = drive(&mut controller, old.read());
    assert!(matches!(
        primary.journal.complete_checkpoint_read(done),
        Err(JournalError::HistorySourceMismatch)
    ));
    assert!(!primary.journal.is_faulted());
    let bytes = read(&mut controller, &mut primary, fresh, 0);
    drop(bytes);
    primary.journal.release_recovery(fresh).unwrap();
    drive(&mut controller, primary.journal.shutdown()).unwrap();
    drive(&mut controller, backup.journal.shutdown()).unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
}

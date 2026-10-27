use super::super::retention::{limit_bytes, roll};
use super::*;
use crate::replica_journal::ValidatedAppend;
use ozzy_journal::operation::{CanonicalOperation, ReleaseKey};
use ozzy_proto::MessageId;

/// Run one release step at `now`, with its reads inline.
fn release(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    now: u64,
) -> Option<ProposalBuffer> {
    drive(
        controller,
        journal.release_turn(driver.begin_validation().unwrap(), now),
    )
    .unwrap()
    .proposal
}

fn validate(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    buffer: ProposalBuffer,
    now: u64,
) -> ProposalValidation {
    drive(
        controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, now),
    )
}

/// The validated `Release`: first offset, timestamp, release key, and
/// record IDs.
fn released(result: &ProposalValidation) -> (Offset, u64, ReleaseKey, Vec<MessageId>) {
    let ProposalValidation::Ready(validated) = result else {
        panic!("ready: {result:?}")
    };
    let [operation] = validated.buffer().operations().collect::<Vec<_>>()[..] else {
        panic!("one release")
    };
    let OperationBody::Release(body) =
        decode_operation_body(operation.kind, operation.body, OperationLimits::default()).unwrap()
    else {
        panic!("release: {:?}", operation.kind)
    };
    (
        body.batch.first_offset,
        body.batch.append_timestamp_millis,
        body.through,
        body.batch
            .records
            .iter()
            .map(|record| record.message_id)
            .collect(),
    )
}

/// Record IDs readable from offset zero.
fn readable(controller: &mut Controller, journal: &mut OwnedJournal, driver: &Driver) -> Vec<u8> {
    let cursor = journal
        .open_reader(
            driver.begin_validation().unwrap(),
            partition(),
            Some(Offset::ZERO),
        )
        .unwrap();
    let work = journal
        .prepare_read(
            cursor,
            PartitionReadLimits {
                max_records: 8,
                max_parts: 16,
                max_payload_bytes: 8192,
            },
            journal.lease_append_buffer().unwrap().into(),
        )
        .unwrap();
    let done = drive(controller, work.read());
    let result = journal.complete_read(done).unwrap();
    result
        .records()
        .map(|(id, _, _)| id.as_bytes()[0])
        .collect()
}

fn scheduled(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    first: u64,
    due: &[u64],
) {
    let result = produce(
        controller,
        journal,
        driver,
        request(12, first, due.len()),
        Some(due),
        NOW,
    );
    accept(controller, journal, driver, result);
}

#[test]
fn release_copies_due_records_in_release_order_at_the_next_offsets() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    scheduled(
        &mut controller,
        &mut journal,
        &mut driver,
        0,
        &[NOW + 5_000, NOW + 1_000, NOW - 1],
    );
    let immediate = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(30, 0, 1),
        None,
        NOW,
    );
    accept(&mut controller, &mut journal, &mut driver, immediate);
    assert_eq!(journal.next_release(), Some(NOW));

    // A past due time releases at acceptance, after the immediate record.
    let proposal = release(&mut controller, &mut journal, &driver, NOW).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, NOW + 3);
    let through = ReleaseKey {
        millis: NOW,
        schedule: 2,
    };
    let ids = vec![MessageId::from_bytes([3; 16])];
    assert_eq!(released(&result), (Offset::new(1), NOW + 3, through, ids));
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (2, 3, Offset::new(2)));
    assert!(release(&mut controller, &mut journal, &driver, NOW + 999).is_none());

    // A clock behind the due time still stamps the release at its key.
    let proposal = release(&mut controller, &mut journal, &driver, NOW + 5_000).unwrap();
    let result = validate(
        &mut controller,
        &mut journal,
        &driver,
        proposal,
        NOW + 4_000,
    );
    let through = ReleaseKey {
        millis: NOW + 5_000,
        schedule: 0,
    };
    let ids = vec![
        MessageId::from_bytes([2; 16]),
        MessageId::from_bytes([1; 16]),
    ];
    assert_eq!(
        released(&result),
        (Offset::new(2), NOW + 5_000, through, ids)
    );
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (0, 3, Offset::new(4)));
    assert_eq!(journal.next_release(), None);
    assert!(release(&mut controller, &mut journal, &driver, NOW + 9_000).is_none());
    assert_eq!(
        readable(&mut controller, &mut journal, &driver),
        [1, 3, 2, 1]
    );
}

#[test]
fn release_waits_for_the_schedule_to_commit() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let result = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 1),
        Some(&[NOW]),
        NOW,
    );
    let ProposalValidation::Ready(mut validated) = result else {
        panic!("ready")
    };
    assert!(journal.can_admit(&mut validated));
    let ticket = driver
        .prepare_validated(validated.validation(), validated.prepared())
        .unwrap();
    let receipt = journal
        .admit_append(ticket, validated)
        .unwrap()
        .into_parts()
        .2;
    write(&mut controller, &mut journal, &mut driver, receipt);
    assert_eq!(journal.next_release(), None);
    assert!(release(&mut controller, &mut journal, &driver, NOW).is_none());
    sync_apply(&mut controller, &mut journal, &mut driver);
    assert_eq!(journal.next_release(), Some(NOW));
    assert!(release(&mut controller, &mut journal, &driver, NOW).is_some());
}

/// Validate one canonical operation from another copy and admit it.
fn replicate(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    kind: OperationKind,
    body: &[u8],
) -> Result<(), JournalError> {
    let ticket = driver.begin_validation().unwrap();
    let mut buffer = journal.lease_append_buffer().unwrap();
    buffer
        .push(CanonicalOperation {
            group_id: ticket.scope().group_id,
            configuration_epoch: ticket.scope().configuration_epoch,
            original_view: ticket.scope().view,
            op_number: ticket.accepted().op.0 + 1,
            previous_digest: ticket.accepted().digest,
            kind,
            body,
        })
        .unwrap();
    let validated = drive(controller, journal.validate_append(ticket, buffer))?;
    admit_validated(controller, journal, driver, validated);
    Ok(())
}

fn admit_validated(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    mut validated: ValidatedAppend,
) {
    assert!(journal.can_admit(&mut validated));
    let ticket = driver
        .prepare_validated(validated.validation(), validated.prepared())
        .unwrap();
    let receipt = journal
        .admit_append(ticket, validated)
        .unwrap()
        .into_parts()
        .2;
    write(controller, journal, driver, receipt);
    sync_apply(controller, journal, driver);
}

fn body(result: &ProposalValidation) -> (OperationKind, Vec<u8>) {
    let ProposalValidation::Ready(validated) = result else {
        panic!("ready: {result:?}")
    };
    let operation = validated.buffer().operations().next().unwrap();
    (operation.kind, operation.body.to_vec())
}

#[test]
fn another_copy_validates_the_release_and_refuses_a_changed_key() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let (mut other, mut copy, mut copy_driver) = delayed(16);
    let result = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 2),
        Some(&[NOW + 2, NOW + 1]),
        NOW,
    );
    let schedule = body(&result);
    accept(&mut controller, &mut journal, &mut driver, result);
    let proposal = release(&mut controller, &mut journal, &driver, NOW + 5).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, NOW + 5);
    let (kind, original) = body(&result);
    accept(&mut controller, &mut journal, &mut driver, result);

    replicate(
        &mut other,
        &mut copy,
        &mut copy_driver,
        schedule.0,
        &schedule.1,
    )
    .unwrap();
    assert_eq!(held(&copy), (2, 2, Offset::ZERO));
    // The release key must name the last copied record.
    let changed = body_with_key(&original, 1);
    assert!(matches!(
        replicate(&mut other, &mut copy, &mut copy_driver, kind, &changed),
        Err(JournalError::Images(
            ozzy_core::state::CanonicalImagesError::State(
                ozzy_core::state::StateError::ReleaseMismatch
            )
        ))
    ));
    replicate(&mut other, &mut copy, &mut copy_driver, kind, &original).unwrap();
    assert_eq!(held(&copy), (0, 2, Offset::new(2)));
    assert_eq!(readable(&mut other, &mut copy, &copy_driver), [2, 1]);
}

/// `body` with its trailing schedule sequence replaced.
fn body_with_key(body: &[u8], schedule: u64) -> Vec<u8> {
    let mut body = body.to_vec();
    let start = body.len() - 8;
    body[start..].copy_from_slice(&schedule.to_be_bytes());
    body
}

#[test]
fn restart_keeps_held_records_and_releases_them() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    scheduled(&mut controller, &mut journal, &mut driver, 0, &[NOW + 7]);
    let (image, _) = controller.crash(true).unwrap();
    drop(journal);
    let (mut controller, io) = setup_image(image);
    let mut config = local_config();
    config.delay = Some(DelayLimits { max_entries: 16 });
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(held(&journal), (1, 1, Offset::ZERO));
    assert_eq!(journal.next_release(), Some(NOW + 7));
    let proposal = release(&mut controller, &mut journal, &driver, NOW + 7).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, NOW + 7);
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (0, 1, Offset::new(1)));
    assert_eq!(readable(&mut controller, &mut journal, &driver), [1]);
}

#[test]
fn retention_keeps_a_held_schedule_until_its_release() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    scheduled(&mut controller, &mut journal, &mut driver, 0, &[NOW + 1]);
    roll(&mut controller, &mut journal);
    let immediate = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(30, 0, 1),
        None,
        NOW,
    );
    accept(&mut controller, &mut journal, &mut driver, immediate);
    roll(&mut controller, &mut journal);
    limit_bytes(&mut controller, &mut journal, &mut driver, 65536);
    let plan = |controller: &mut Controller, journal: &mut OwnedJournal, driver: &Driver| {
        drive(
            controller,
            journal.plan_retention(driver.begin_validation().unwrap(), NOW, 2),
        )
        .unwrap()
        .map(|(_, plan)| plan.retire)
    };
    assert_eq!(
        plan(&mut controller, &mut journal, &driver),
        Some(Vec::new())
    );
    let proposal = release(&mut controller, &mut journal, &driver, NOW + 1).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, NOW + 1);
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(plan(&mut controller, &mut journal, &driver), Some(vec![1]));
}

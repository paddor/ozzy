use super::super::retention::{limit, limit_bytes, roll, run_retention_jobs};
use super::*;
use crate::replica_journal::ValidatedAppend;
use ozzy_journal::operation::{CanonicalOperation, ReleaseKey};
use ozzy_proto::MessageId;

mod carry;

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

#[test]
fn another_copy_refuses_held_records_out_of_release_order() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let (mut other, mut copy, mut copy_driver) = delayed(16);
    // Equal due times release in schedule order.
    let result = produce(
        &mut controller,
        &mut journal,
        &driver,
        request(12, 0, 2),
        Some(&[NOW + 2, NOW + 2]),
        NOW,
    );
    let schedule = body(&result);
    accept(&mut controller, &mut journal, &mut driver, result);
    let proposal = release(&mut controller, &mut journal, &driver, NOW + 5).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, NOW + 5);
    let (kind, original) = body(&result);
    replicate(
        &mut other,
        &mut copy,
        &mut copy_driver,
        schedule.0,
        &schedule.1,
    )
    .unwrap();
    let OperationBody::Release(decoded) =
        decode_operation_body(kind, &original, OperationLimits::default()).unwrap()
    else {
        panic!("release")
    };
    let mut records: Vec<_> = decoded
        .batch
        .records
        .iter()
        .map(|record| ozzy_journal::operation::AppendRecord {
            encoding: record.encoding,
            message_id: record.message_id,
            parts: record.parts.iter().collect(),
        })
        .collect();
    records.swap(0, 1);
    let swapped = ozzy_journal::operation::Release {
        batch: ozzy_journal::operation::AppendBatch {
            records: records.into(),
            ..decoded.batch.clone()
        },
        through: decoded.through,
    };
    let swapped = ozzy_journal::operation::encode_operation_body(
        &OperationBody::Release(swapped),
        OperationLimits::default(),
    )
    .unwrap();
    assert!(matches!(
        replicate(&mut other, &mut copy, &mut copy_driver, kind, &swapped),
        Err(JournalError::Images(
            ozzy_core::state::CanonicalImagesError::State(
                ozzy_core::state::StateError::ReleaseMismatch
            )
        ))
    ));
    replicate(&mut other, &mut copy, &mut copy_driver, kind, &original).unwrap();
    assert_eq!(readable(&mut other, &mut copy, &copy_driver), [1, 2]);
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
    scheduled(
        &mut controller,
        &mut journal,
        &mut driver,
        0,
        &[NOW + 7, NOW + 9],
    );
    let (image, _) = controller.crash(true).unwrap();
    drop(journal);
    let (mut controller, io) = setup_image(image);
    // A lower limit than the held count only stops new schedules.
    let mut config = local_config();
    config.delay = Some(DelayLimits::new(1));
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(held(&journal), (2, 2, Offset::ZERO));
    assert_eq!(journal.next_release(), Some(NOW + 7));
    let more = |controller: &mut Controller, journal: &mut OwnedJournal, driver: &Driver| {
        produce(
            controller,
            journal,
            driver,
            request(12, 2, 1),
            Some(&[NOW + 9]),
            NOW,
        )
    };
    assert!(matches!(
        rejection(more(&mut controller, &mut journal, &driver)),
        JournalError::ProducerAppend(AppendAdmissionError::ScheduleCapacity)
    ));
    let proposal = release(&mut controller, &mut journal, &driver, NOW + 9).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, NOW + 9);
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (0, 2, Offset::new(2)));
    assert_eq!(readable(&mut controller, &mut journal, &driver), [1, 2]);
    let result = more(&mut controller, &mut journal, &driver);
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (1, 3, Offset::new(2)));
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

#[test]
fn retention_ages_a_schedule_only_segment_from_its_acceptance_time() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    scheduled(&mut controller, &mut journal, &mut driver, 0, &[NOW + 1]);
    roll(&mut controller, &mut journal);
    let proposal = release(&mut controller, &mut journal, &driver, NOW + 1).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, NOW + 1);
    accept(&mut controller, &mut journal, &mut driver, result);
    roll(&mut controller, &mut journal);
    limit(
        &mut controller,
        &mut journal,
        &mut driver,
        ozzy_journal::operation::RetentionPolicy {
            max_age_millis: std::num::NonZeroU64::new(60_000),
            max_bytes: None,
        },
    );
    let mut plan = |now| {
        drive(
            &mut controller,
            journal.plan_retention(driver.begin_validation().unwrap(), now, 2),
        )
        .unwrap()
        .map(|(_, plan)| plan.retire)
    };
    // The segment holds no stored record, but retries of its schedule
    // resolve for the retention age.
    assert_eq!(plan(NOW + 59_999), Some(Vec::new()));
    assert_eq!(plan(NOW + 60_000), Some(vec![1]));
}

#[test]
fn release_takes_a_lone_held_record_as_large_as_its_arena() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let room = journal
        .lease_proposal_buffer()
        .unwrap()
        .limits()
        .max_body_bytes;
    // The largest lone record a `Schedule` holds: batch header, descriptor,
    // payload, and the schedule trailer fill the arena.
    let payload: &'static [u8] = vec![7; room - 80 - 24 - 16].leak();
    let mut large = request(12, 0, 1);
    large.records[0].parts = smallvec::smallvec![payload];
    let result = produce(
        &mut controller,
        &mut journal,
        &driver,
        large,
        Some(&[NOW]),
        NOW,
    );
    accept(&mut controller, &mut journal, &mut driver, result);
    let proposal = release(&mut controller, &mut journal, &driver, NOW).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, NOW);
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (0, 1, Offset::new(1)));
}

/// Whether the journal has room for one control request.
fn control_room(journal: &mut OwnedJournal, driver: &Driver) -> bool {
    let ticket = driver.begin_validation().unwrap();
    let mut buffer = journal.lease_append_buffer().unwrap();
    buffer
        .push(CanonicalOperation {
            group_id: ticket.scope().group_id,
            configuration_epoch: ticket.scope().configuration_epoch,
            original_view: ticket.scope().view,
            op_number: ticket.accepted().op.0 + 1,
            previous_digest: ticket.accepted().digest,
            kind: OperationKind::Progress,
            body: &[1; 16],
        })
        .unwrap();
    match journal.check_room(&buffer) {
        Ok(()) => true,
        Err(JournalError::RollCapacity) => false,
        Err(error) => panic!("{error:?}"),
    }
}

#[test]
fn held_records_keep_room_to_release_after_control_requests_stop() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    scheduled(&mut controller, &mut journal, &mut driver, 0, &[NOW + 1; 8]);
    // A lower segment limit stands in for writes that used the journal up,
    // one segment at a time.
    while control_room(&mut journal, &driver) {
        let segments = &mut journal.limits.metadata.max_segments;
        assert!(*segments > 1, "control requests never stopped");
        *segments -= 1;
    }
    // Control requests leave room to release every held record.
    let proposal = release(&mut controller, &mut journal, &driver, NOW + 1).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, NOW + 1);
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (0, 8, Offset::new(8)));
    assert!(control_room(&mut journal, &driver));
}

type Copy<'a> = (&'a mut Controller, &'a mut OwnedJournal, &'a mut Driver);

/// Run release steps at `now` until nothing is held, copying each release
/// to `copy`. Each step keeps at most `window` sealed rows loaded. Returns
/// the released record IDs and how many steps loaded sealed records
/// instead.
fn release_all(
    primary: Copy<'_>,
    copy: Copy<'_>,
    window: usize,
    mut until: impl FnMut(usize) -> bool,
) -> (Vec<u8>, usize) {
    let (controller, journal, driver) = primary;
    let (other, copy, copy_driver) = copy;
    let (mut ids, mut loads) = (Vec::new(), 0);
    while held(journal).0 > 0 && !until(ids.len()) {
        let turn = drive(
            controller,
            journal.release_turn(driver.begin_validation().unwrap(), NOW + 1_000),
        )
        .unwrap();
        assert!(journal.held_rows() <= window);
        let Some(proposal) = turn.proposal else {
            assert!(turn.loaded);
            loads += 1;
            continue;
        };
        let result = validate(controller, journal, driver, proposal, NOW + 1_000);
        ids.extend(released(&result).3.iter().map(|id| id.as_bytes()[0]));
        let (kind, body) = body(&result);
        accept(controller, journal, driver, result);
        replicate(other, copy, copy_driver, kind, &body).unwrap();
    }
    (ids, loads)
}

/// Schedule `batches` batches of three records with due times out of
/// order, copying each to `copy`. After each batch, `rolls` says whether the
/// primary and the copy roll. Returns the record IDs in release order.
fn schedule_batches(
    primary: Copy<'_>,
    copy: Copy<'_>,
    batches: u64,
    mut rolls: impl FnMut(u64) -> (bool, bool),
    mut after: impl FnMut(&OwnedJournal, &OwnedJournal),
) -> Vec<u8> {
    let (controller, journal, driver) = primary;
    let (other, copy, copy_driver) = copy;
    let mut expected = Vec::new();
    for batch in 0..batches {
        let first = batch * 3;
        let due: Vec<_> = (0..3)
            .map(|at| NOW - 50 + (batch * 7 + at * 5) % 13 * 10)
            .collect();
        let result = produce(
            controller,
            journal,
            driver,
            request(12, first, 3),
            Some(&due),
            NOW,
        );
        let (kind, schedule) = body(&result);
        accept(controller, journal, driver, result);
        replicate(other, copy, copy_driver, kind, &schedule).unwrap();
        for (at, due) in (0..).zip(&due) {
            expected.push(((*due.max(&NOW), first + at), (first + at + 1) as u8));
        }
        let (primary, copied) = rolls(batch);
        if primary {
            roll(controller, journal);
        }
        if copied {
            roll(other, copy);
        }
        after(journal, copy);
    }
    expected.sort_unstable();
    expected.into_iter().map(|(_, id)| id).collect()
}

/// Reopen `journal` from its crash image after a power loss.
fn restart(
    controller: Controller,
    journal: OwnedJournal,
    limits: DelayLimits,
) -> (Controller, OwnedJournal, Driver) {
    let (image, _) = controller.crash(true).unwrap();
    drop(journal);
    let (mut controller, io) = setup_image(image);
    let mut config = local_config();
    config.delay = Some(limits);
    let (journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    (controller, journal, driver)
}

fn segments(journal: &OwnedJournal) -> usize {
    journal
        .journal
        .readable()
        .unwrap()
        .manifest()
        .segments
        .len()
}

#[test]
fn held_records_release_in_order_through_a_small_window_across_segments() {
    let limits = DelayLimits {
        window: 2,
        ..DelayLimits::new(64)
    };
    let (mut controller, mut journal, mut driver) = delayed_with(limits);
    let (mut other, mut copy, mut copy_driver) = delayed_with(limits);
    // Four sealed segments hold records; the last batch stays active.
    let expected = schedule_batches(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        5,
        |batch| (batch < 4, batch < 4),
        |_, _| {},
    );

    // The copy never loads sealed records; its releases pass them unread.
    let (mut ids, mut loads) = release_all(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        2,
        |released| released >= 6,
    );
    let (mut controller, mut journal, mut driver) = restart(controller, journal, limits);
    // Restart loads the window from the sealed indexes.
    assert!(journal.held_rows() > 0);
    let (rest, more) = release_all(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        2,
        |_| false,
    );
    ids.extend(rest);
    loads += more;
    assert_eq!(ids, expected);
    assert!(loads >= 3, "{loads} loads");
    assert_eq!(held(&journal), (0, 15, Offset::new(15)));
    assert_eq!(held(&copy), (0, 15, Offset::new(15)));
    assert_eq!(journal.next_release(), None);
}

/// Pack the sealed segments of a journal low on segments.
fn pack(controller: &mut Controller, journal: &mut OwnedJournal) {
    let max = journal.limits.metadata.max_segments;
    journal.limits.metadata.max_segments = segments(journal) + 1;
    let work = journal.take_pack_work().expect("a pack is due");
    journal.limits.metadata.max_segments = max;
    let done = drive(controller, work.run());
    journal.complete_pack(done);
    assert!(journal.pack_ready());
    drive(controller, journal.finish_pack()).unwrap();
}

#[test]
fn held_records_release_in_order_from_packed_segments() {
    let limits = DelayLimits {
        window: 2,
        ..DelayLimits::new(64)
    };
    let (mut controller, mut journal, mut driver) = delayed_with(limits);
    let (mut other, mut copy, mut copy_driver) = delayed_with(limits);
    let expected = schedule_batches(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        5,
        |batch| (batch < 4, batch < 4),
        |_, _| {},
    );
    let (mut ids, _) = release_all(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        2,
        |released| released >= 4,
    );
    // One packed segment replaces the sealed ones; loading continues from
    // its index, before and after a restart.
    let before = segments(&journal);
    pack(&mut controller, &mut journal);
    assert!(segments(&journal) < before - 1, "{before} segments");
    let (more, loads) = release_all(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        2,
        |released| released >= 4,
    );
    assert!(loads > 0);
    ids.extend(more);
    let (mut controller, mut journal, mut driver) = restart(controller, journal, limits);
    let (rest, _) = release_all(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        2,
        |_| false,
    );
    ids.extend(rest);
    assert_eq!(ids, expected);
    assert_eq!(held(&journal), (0, 15, Offset::new(15)));
    assert_eq!(held(&copy), (0, 15, Offset::new(15)));
}

#[test]
fn a_copy_loads_past_its_window_to_check_a_release_of_rows_it_sealed() {
    let (mut controller, mut journal, mut driver) = delayed(64);
    let limits = DelayLimits {
        window: 2,
        ..DelayLimits::new(64)
    };
    let (mut other, mut copy, mut copy_driver) = delayed_with(limits);
    // The primary keeps every held row in its active segment; the copy seals
    // them in four segments.
    let expected = schedule_batches(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        5,
        |batch| (false, batch < 4),
        |_, _| {},
    );
    // One release passes every row, so the copy loads past its window.
    let (ids, loads) = release_all(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        0,
        |_| false,
    );
    assert_eq!((ids, loads), (expected, 0));
    assert_eq!(held(&copy), (0, 15, Offset::new(15)));
    // The applied release leaves the copy's window bounded again.
    assert!(copy.held_rows() <= limits.window);
}

#[test]
fn held_records_in_memory_stay_within_the_window_and_the_active_cap() {
    let limits = DelayLimits {
        window: 4,
        active: 7,
        ..DelayLimits::new(64)
    };
    let (mut controller, mut journal, mut driver) = delayed_with(limits);
    let (mut other, mut copy, mut copy_driver) = delayed_with(limits);
    let (first, copied) = (segments(&journal), segments(&copy));
    // Each copy rolls early once seven rows are in memory, so the rows of
    // its sealed segments past the window stay on disk.
    let expected = schedule_batches(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        8,
        |_| (false, false),
        |journal, copy| {
            for journal in [journal, copy] {
                let rows = journal.held_memory();
                assert!(rows <= limits.window + limits.active, "{rows} rows");
            }
        },
    );
    assert_eq!(segments(&journal) - first, 3);
    assert_eq!(segments(&copy) - copied, 3);
    let (ids, loads) = release_all(
        (&mut controller, &mut journal, &mut driver),
        (&mut other, &mut copy, &mut copy_driver),
        4,
        |_| false,
    );
    assert_eq!(ids, expected);
    assert!(loads > 0);
    assert_eq!(held(&copy), (0, 24, Offset::new(24)));
}

#[test]
fn release_waits_for_shard_memory_instead_of_failing() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.delay = Some(DelayLimits::new(16));
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    let memory = payload_owner(4096);
    journal.bind_append_memory(&memory).unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    scheduled(&mut controller, &mut journal, &mut driver, 0, &[NOW]);
    memory.trim_cache();
    let mut occupied = journal.lease_append_buffer().unwrap();
    occupied.push_read_part(&[0; 4096]).unwrap();

    let proposal = {
        let mut step =
            std::pin::pin!(journal.release_turn(driver.begin_validation().unwrap(), NOW));
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..100 {
            assert!(step.as_mut().poll(&mut context).is_pending());
            for (id, _) in controller.jobs() {
                controller.execute(id, Effect::Normal).unwrap();
                controller.deliver(id).unwrap();
            }
        }
        drop(occupied);
        drive(&mut controller, step).unwrap().proposal.unwrap()
    };
    let result = validate(&mut controller, &mut journal, &driver, proposal, NOW);
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(held(&journal), (0, 1, Offset::new(1)));
}

#[test]
fn release_reads_kept_bodies_instead_of_storage() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    scheduled(
        &mut controller,
        &mut journal,
        &mut driver,
        0,
        &[NOW, NOW + 1_000],
    );
    for now in [NOW, NOW + 1_000] {
        let proposal = release(&mut controller, &mut journal, &driver, now).unwrap();
        let result = validate(&mut controller, &mut journal, &driver, proposal, now);
        accept(&mut controller, &mut journal, &mut driver, result);
    }
    // No release built an index snapshot to read its source.
    assert!(journal.held.as_ref().unwrap().index.is_none());
    assert_eq!(held(&journal), (0, 2, Offset::new(2)));
    assert_eq!(readable(&mut controller, &mut journal, &driver), [1, 2]);
}

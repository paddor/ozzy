use super::*;
use crate::replica_journal::{AppendAdmissionError, OwnerAppendError};
use ozzy_journal::operation::AppendRecord;
use ozzy_proto::{MessageId, append::Authority};

fn authority(driver: &Driver) -> Authority {
    let scope = driver.begin_validation().unwrap().scope();
    Authority {
        group_id: scope.group_id,
        config_epoch: scope.configuration_epoch,
        view: scope.view,
    }
}

/// An owner batch of `count` records whose IDs start at `first_id`.
fn owned(
    journal: &OwnedJournal,
    authority: Authority,
    first_offset: u64,
    first_id: u8,
    count: u8,
) -> ProposalBuffer {
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_owner_append(
            authority,
            partition(),
            Offset::new(first_offset),
            (first_id..first_id + count)
                .map(|id| AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([id; 16]),
                    parts: vec![b"owned".as_slice()].into(),
                })
                .collect(),
        )
        .unwrap();
    buffer
}

fn refused(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    buffer: ProposalBuffer,
) -> JournalError {
    let ticket = driver.begin_validation().unwrap();
    match drive(controller, journal.propose_append(ticket, buffer, 777)) {
        ProposalValidation::Rejected { reason, .. } => reason,
        other => panic!("owner append must be refused: {other:?}"),
    }
}

fn owner_journal(controller: &mut Controller, io: Local) -> (OwnedJournal, Driver) {
    let (mut journal, mut driver) = drive(
        controller,
        OwnedJournal::format_local(local_config(), io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize_with_writers(controller, &mut journal, &mut driver, &[]);
    (journal, driver)
}

#[test]
fn owner_batches_append_at_the_offsets_their_writer_expects() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = owner_journal(&mut controller, io);
    let authority = authority(&driver);
    let first = owned(&journal, authority, 0, 1, 2);
    let first = admit(&mut controller, &mut journal, &mut driver, first);
    // The next batch validates against the accepted image, before the
    // first one is written.
    let second = owned(&journal, authority, 2, 3, 1);
    let second = admit(&mut controller, &mut journal, &mut driver, second);
    write(&mut controller, &mut journal, &mut driver, first);
    write(&mut controller, &mut journal, &mut driver, second);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let committed = journal.images().unwrap().committed();
    let state = committed.partition(partition()).unwrap();
    assert_eq!(state.next_offset, Offset::new(3));
    assert_eq!(state.producers().len(), 0);
    read(&mut controller, &mut journal, &driver, 3);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owner_batches_off_the_next_offset_are_refused_with_it() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = owner_journal(&mut controller, io);
    let authority = authority(&driver);
    let first = owned(&journal, authority, 0, 1, 2);
    let receipt = admit(&mut controller, &mut journal, &mut driver, first);
    for offset in [0, 1, 3] {
        let late = owned(&journal, authority, offset, 9, 1);
        let reason = refused(&mut controller, &mut journal, &driver, late);
        assert!(
            matches!(
                reason,
                JournalError::OwnerAppend(OwnerAppendError::Offset { next })
                    if next == Offset::new(2)
            ),
            "{reason:?}"
        );
    }
    let stale = Authority {
        view: authority.view + 1,
        ..authority
    };
    let foreign = owned(&journal, stale, 2, 9, 1);
    let reason = refused(&mut controller, &mut journal, &driver, foreign);
    assert!(
        matches!(
            reason,
            JournalError::ProducerAppend(AppendAdmissionError::Authority)
        ),
        "{reason:?}"
    );
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    read(&mut controller, &mut journal, &driver, 2);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn owner_buffers_take_one_batch_and_no_other_operation() {
    let (mut controller, io) = setup();
    let (journal, driver) = owner_journal(&mut controller, io);
    let mut buffer = owned(&journal, authority(&driver), 0, 1, 1);
    let body = OperationBody::Trim(ozzy_journal::operation::Trim {
        partition: partition(),
        expected_floor: Offset::ZERO,
        new_floor: Offset::ZERO,
        expected_operation_floor: 0,
        new_operation_floor: 0,
        operation_id: OperationId::from_bytes([5; 16]),
    });
    let encoded = encode_operation_body(&body, journal.limits.operations).unwrap();
    assert!(matches!(
        buffer.push(body.kind(), &encoded),
        Err(JournalError::AppendMismatch)
    ));
    assert!(matches!(
        buffer.prepare_owner_append(authority(&driver), partition(), Offset::ZERO, Vec::new()),
        Err(JournalError::AppendMismatch)
    ));
    buffer.clear();
    assert!(matches!(
        buffer.prepare_owner_append(authority(&driver), partition(), Offset::ZERO, Vec::new()),
        Err(JournalError::AppendMismatch)
    ));
}

#[test]
fn owner_batches_survive_power_loss_and_replay_without_producers() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = owner_journal(&mut controller, io);
    let authority = authority(&driver);
    for (offset, id) in [(0, 1), (2, 3)] {
        let buffer = owned(&journal, authority, offset, id, 2);
        let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
        write(&mut controller, &mut journal, &mut driver, receipt);
    }
    sync_apply(&mut controller, &mut journal, &mut driver);
    let confirmed = driver.snapshot().committed;
    let (image, _) = controller.crash(true).unwrap();
    drop(journal);
    let (mut controller, io) = setup_image(image);
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(local_config(), io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(driver.snapshot().applied, confirmed);
    let images = journal.images().unwrap();
    assert_eq!(
        images
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::new(4)
    );
    let authority = self::authority(&driver);
    let stale = owned(&journal, authority, 2, 9, 1);
    let reason = refused(&mut controller, &mut journal, &driver, stale);
    assert!(
        matches!(
            reason,
            JournalError::OwnerAppend(OwnerAppendError::Offset { next })
                if next == Offset::new(4)
        ),
        "{reason:?}"
    );
    let next = owned(&journal, authority, 4, 9, 1);
    commit(&mut controller, &mut journal, &mut driver, next);
    read(&mut controller, &mut journal, &driver, 5);
    drive(&mut controller, journal.shutdown()).unwrap();
}

/// Commit the owner's trim from `expected` to `floor`.
fn trim(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    (expected, floor): (u64, u64),
) {
    let operation_floor = journal.images().unwrap().committed().operation_floor();
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_owner_trim(
            authority(driver),
            ozzy_journal::operation::Trim {
                partition: partition(),
                expected_floor: Offset::new(expected),
                new_floor: Offset::new(floor),
                expected_operation_floor: operation_floor,
                new_operation_floor: operation_floor,
                operation_id: OperationId::from_bytes([floor as u8 + 1; 16]),
            },
            local_config().limits.operations,
        )
        .unwrap();
    commit(controller, journal, driver, buffer);
}

/// Run retention turns, their proposals and their jobs until none is owed.
/// Turns take their operation IDs from `first_id` on.
fn retain(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    first_id: u8,
) {
    for turn in 0..16u8 {
        let ticket = driver.begin_validation().unwrap();
        let seed = OperationId::from_bytes([first_id + turn; 16]);
        let result = drive(
            controller,
            journal.retention_turn(ticket, 999.into(), seed, true),
        )
        .unwrap();
        if let Some(proposal) = result.proposal {
            commit(controller, journal, driver, proposal);
        }
        super::retention::run_retention_jobs(controller, journal);
        if !result.more_work {
            return;
        }
    }
    panic!("retention never settled");
}

fn segments(journal: &OwnedJournal) -> usize {
    journal.journal.ready().unwrap().manifest().segments.len()
}

/// A partition without a retention policy keeps every segment until its
/// owner trims. Then each sealed segment whose records all lie below the
/// floor retires; one that reaches past the floor stays, as does the
/// segment being written.
#[test]
fn an_owner_trim_retires_the_sealed_segments_below_its_floor() {
    let (mut controller, io) = setup();
    let (mut journal, mut driver) = owner_journal(&mut controller, io);
    for first in [0, 2, 4, 6] {
        let batch = owned(&journal, authority(&driver), first, first as u8 + 1, 2);
        commit(&mut controller, &mut journal, &mut driver, batch);
        if first < 6 {
            super::retention::roll(&mut controller, &mut journal);
        }
    }
    assert_eq!(segments(&journal), 4);
    retain(&mut controller, &mut journal, &mut driver, 100);
    assert_eq!((segments(&journal), journal.owner_floor()), (4, None));

    // Offset 3 lies in the second segment, so only the first one goes.
    trim(&mut controller, &mut journal, &mut driver, (0, 3));
    assert_eq!(journal.owner_floor(), Some(Offset::new(3)));
    retain(&mut controller, &mut journal, &mut driver, 120);
    assert_eq!(segments(&journal), 3);

    // The whole log is trimmed: the sealed segments go, the active stays.
    trim(&mut controller, &mut journal, &mut driver, (3, 8));
    retain(&mut controller, &mut journal, &mut driver, 140);
    assert_eq!(segments(&journal), 1);
    let committed = journal.images().unwrap().committed();
    let state = committed.partition(partition()).unwrap();
    assert_eq!(
        (state.retained_from, state.next_offset),
        (Offset::new(8), Offset::new(8))
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}

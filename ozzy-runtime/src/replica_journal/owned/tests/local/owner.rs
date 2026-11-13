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
    let receipt = admit(&mut controller, &mut journal, &mut driver, next);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    read(&mut controller, &mut journal, &driver, 5);
    drive(&mut controller, journal.shutdown()).unwrap();
}

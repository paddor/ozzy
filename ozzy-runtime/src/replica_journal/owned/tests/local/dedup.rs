use super::super::proposal::dedup::{copy, resolved};
use super::*;
use crate::replica_journal::AppendAdmissionError;
use crate::replica_journal::owned::StorageCleanup;
use ozzy_journal::operation::{DedupWatermark, OperationKind, ProducerResultFloor};
use ozzy_proto::{CheckpointId, ProducerSequence};

fn commit(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    body: &(OperationKind, Vec<u8>),
) {
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer.push(body.0, &body.1).unwrap();
    let receipt = admit(controller, journal, driver, buffer);
    write(controller, journal, driver, receipt);
    sync_apply(controller, journal, driver);
}

fn retire(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    id: u8,
) -> Vec<u64> {
    drive(
        controller,
        journal.retire_confirmed_history(
            driver.begin_validation().unwrap(),
            CheckpointId::from_bytes([id; 16]),
            ozzy_journal_segment::RetirementBudget {
                max_segments: 2,
                max_read_bytes: 65536,
            },
        ),
    )
    .unwrap()
    .unreferenced_segment_ids
}

fn check_retry(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    attempted: &crate::replica_journal::ProducerAppend<'static>,
    through: Prefix,
) {
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer.prepare_append(copy(attempted)).unwrap();
    let ticket = driver.begin_validation().unwrap();
    let result = drive(controller, journal.propose_append(ticket, buffer, 999));
    let ProposalValidation::Resolved {
        through: actual,
        buffer,
        ..
    } = result
    else {
        panic!("retained duplicate: {result:?}");
    };
    assert_eq!(actual, through);
    assert_eq!(coordinates(&buffer.0), [(0, 0, 777, 1)]);
}

#[test]
fn duplicate_retry_survives_payload_deletion_checkpoint_restart_and_time_advance() {
    let (mut controller, io) = setup();
    let config = local_config();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let buffer = append(&journal, 12, 0);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let work = journal.begin_roll(8).unwrap();
    let done = drive(&mut controller, work.publish());
    journal.complete_roll(done).unwrap();
    let mut attempted = request(30, 0, 1);
    attempted.records[0].parts = vec![b"copy".as_slice(), b"\0\xff".as_slice()].into();
    commit(
        &mut controller,
        &mut journal,
        &mut driver,
        &resolved(&attempted, Offset::new(1), &[Some(Offset::ZERO)], 101),
    );
    let proof = driver.begin_validation().unwrap().committed();
    let work = journal.begin_roll(8).unwrap();
    let done = drive(&mut controller, work.publish());
    journal.complete_roll(done).unwrap();
    let buffer = journal
        .retention_proposal(
            partition(),
            Offset::new(1),
            proof.op.0,
            OperationId::from_bytes([90; 16]),
        )
        .unwrap()
        .unwrap();
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let body = OperationBody::DedupWatermark(DedupWatermark {
        partition: partition(),
        owner_epoch: OwnerEpoch::INITIAL,
        logical_time: 600_001,
    });
    commit(
        &mut controller,
        &mut journal,
        &mut driver,
        &(
            body.kind(),
            encode_operation_body(&body, config.limits.operations).unwrap(),
        ),
    );
    assert_eq!(retire(&mut controller, &mut journal, &driver, 94), [1]);
    let cleanup = drive(
        &mut controller,
        journal.cleanup_storage(
            driver.begin_validation().unwrap(),
            StorageCleanup::Segments,
            16,
        ),
    )
    .unwrap();
    assert_eq!(cleanup.removed_objects, 1);
    assert!(
        !controller
            .image()
            .exists(&config.root.join("segments/1.log"), false)
    );
    assert!(
        controller
            .image()
            .exists(&config.root.join("segments/2.log"), false)
    );
    check_retry(&mut controller, &mut journal, &driver, &attempted, proof);
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config.clone(), io, JournalGeneration(2)),
    )
    .unwrap();
    let state = journal
        .images()
        .unwrap()
        .committed()
        .partition(partition())
        .unwrap();
    assert_eq!(state.retained_from, Offset::new(1));
    assert_eq!(state.dedup_time, 600_001);
    check_retry(&mut controller, &mut journal, &driver, &attempted, proof);
    expire_retry(
        &mut controller,
        &mut journal,
        &mut driver,
        &config,
        attempted,
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}

fn expire_retry(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    config: &OwnedConfig<Configuration>,
    attempted: crate::replica_journal::ProducerAppend<'static>,
) {
    let body = OperationBody::ProducerResultFloor(ProducerResultFloor {
        partition: partition(),
        producer_id: attempted.producer_id,
        producer_epoch: attempted.producer_epoch,
        expected_floor: ProducerSequence::ZERO,
        new_floor: ProducerSequence::new(1),
        operation_id: OperationId::from_bytes([95; 16]),
    });
    commit(
        controller,
        journal,
        driver,
        &(
            body.kind(),
            encode_operation_body(&body, config.limits.operations).unwrap(),
        ),
    );
    assert_eq!(retire(controller, journal, driver, 96), [2]);
    drive(
        controller,
        journal.cleanup_storage(
            driver.begin_validation().unwrap(),
            StorageCleanup::Segments,
            16,
        ),
    )
    .unwrap();
    assert!(
        !controller
            .image()
            .exists(&config.root.join("segments/2.log"), false)
    );
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer.prepare_append(attempted).unwrap();
    assert!(matches!(
        drive(
            controller,
            journal.propose_append(driver.begin_validation().unwrap(), buffer, 999)
        ),
        ProposalValidation::Rejected {
            reason: JournalError::ProducerAppend(AppendAdmissionError::RetryHistoryExpired),
            ..
        }
    ));
    assert!(!journal.is_faulted());
}

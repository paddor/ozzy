use super::*;
use crate::replica_journal::AppendAdmissionError;
use ozzy_proto::{
    append::{Authority, Policy},
    producer::{Mode, Open},
};

#[test]
fn local_identity_resume_survives_selected_canonical_checkpoint() {
    let (mut controller, io) = setup();
    let config = local_config();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let state = journal.images().unwrap().committed().clone();
    let limits = config.recovery;
    let id = ozzy_proto::CheckpointId::from_bytes([99; 16]);
    drive(
        &mut controller,
        journal.journal.ready_mut().unwrap().publish_progress(),
    )
    .unwrap();
    let files = drive(
        &mut controller,
        journal
            .journal
            .ready_mut()
            .unwrap()
            .build_canonical_checkpoint(id, 4096, &state, limits.snapshot),
    )
    .unwrap();
    drive(
        &mut controller,
        journal
            .journal
            .ready_mut()
            .unwrap()
            .install_canonical_checkpoint(id, limits.state, limits.snapshot, limits.checkpoint),
    )
    .unwrap();
    drop(files);
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    let buffer = request_for(&journal, &driver, 12, Mode::Resume, None, 100);
    resolved(&mut controller, &mut journal, &driver, buffer, 1);
    drive(&mut controller, journal.shutdown()).unwrap();
}

fn request(
    journal: &OwnedJournal,
    driver: &Driver,
    mode: Mode,
    old: Option<u64>,
    id: u8,
) -> ProposalBuffer {
    request_for(journal, driver, 40, mode, old, id)
}

fn request_for(
    journal: &OwnedJournal,
    driver: &Driver,
    writer: u8,
    mode: Mode,
    old: Option<u64>,
    id: u8,
) -> ProposalBuffer {
    let scope = driver.begin_validation().unwrap().scope();
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_producer_open(
            Open {
                authority: Authority {
                    group_id: scope.group_id,
                    config_epoch: scope.configuration_epoch,
                    view: scope.view,
                },
                partition: partition(),
                producer: ProducerId::from_bytes([writer; 16]),
                mode,
                expected_epoch: old,
                operation: OperationId::from_bytes([id; 16]),
            },
            Policy::LocalDurable,
        )
        .unwrap();
    buffer
}

fn resolved(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    buffer: ProposalBuffer,
    epoch: u64,
) {
    let result = drive(
        controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    );
    let ProposalValidation::Resolved {
        through, buffer, ..
    } = result
    else {
        panic!("producer open should resolve: {result:?}");
    };
    assert_eq!(through, driver.snapshot().accepted);
    let opened = buffer.producer_opened().unwrap();
    assert_eq!(opened.epoch, epoch);
    assert_eq!(opened.next_sequence, 0);
    assert_eq!(opened.retry_floor, 0);
    assert_eq!(opened.policy, Policy::LocalDurable);
}

#[test]
fn local_producer_open_fence_and_retry_survive_pending_write_and_restart() {
    let (mut controller, io) = setup();
    let config = local_config();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let buffer = request(&journal, &driver, Mode::Resume, None, 41);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    let buffer = request(&journal, &driver, Mode::Resume, None, 41);
    resolved(&mut controller, &mut journal, &driver, buffer, 1);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let before = driver.snapshot().accepted;
    let buffer = request(&journal, &driver, Mode::Resume, Some(1), 43);
    resolved(&mut controller, &mut journal, &driver, buffer, 1);
    assert_eq!(driver.snapshot().accepted, before);
    let buffer = request(&journal, &driver, Mode::Fence, Some(1), 42);
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    let buffer = request(&journal, &driver, Mode::Fence, Some(1), 42);
    resolved(&mut controller, &mut journal, &driver, buffer, 2);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let buffer = request(&journal, &driver, Mode::Resume, None, 41);
    assert!(matches!(
        drive(
            &mut controller,
            journal.propose_append(driver.begin_validation().unwrap(), buffer, 777)
        ),
        ProposalValidation::Rejected {
            reason: JournalError::ProducerAppend(AppendAdmissionError::Fenced),
            ..
        }
    ));
    let confirmed = driver.snapshot().accepted;
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    let buffer = request(&journal, &driver, Mode::Fence, Some(1), 42);
    resolved(&mut controller, &mut journal, &driver, buffer, 2);
    assert_eq!(driver.snapshot().accepted, confirmed);
    let buffer = request(&journal, &driver, Mode::Resume, None, 42);
    assert!(matches!(
        drive(
            &mut controller,
            journal.propose_append(driver.begin_validation().unwrap(), buffer, 777)
        ),
        ProposalValidation::Rejected {
            reason: JournalError::AppendMismatch,
            ..
        }
    ));
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn local_producer_resume_preserves_independent_next_sequences_after_restart() {
    let (mut controller, io) = setup();
    let config = local_config();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    for (writer, sequence) in [(12, 0), (30, 0), (12, 1)] {
        let buffer = append(&journal, writer, sequence);
        let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
        write(&mut controller, &mut journal, &mut driver, receipt);
    }
    sync_apply(&mut controller, &mut journal, &mut driver);
    let through = driver.snapshot().applied;
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    for (writer, sequence) in [(12, 2), (30, 1)] {
        let buffer = request_for(&journal, &driver, writer, Mode::Resume, None, 45);
        let result = drive(
            &mut controller,
            journal.propose_append(driver.begin_validation().unwrap(), buffer, 999),
        );
        let ProposalValidation::Resolved {
            through: required,
            buffer,
            ..
        } = result
        else {
            panic!("resume: {result:?}");
        };
        let opened = buffer.producer_opened().unwrap();
        assert_eq!(opened.producer, ProducerId::from_bytes([writer; 16]));
        assert_eq!(opened.epoch, 1);
        assert_eq!(opened.next_sequence, sequence);
        assert_eq!(opened.retry_floor, 0);
        assert_eq!(required, through);
    }
    assert_eq!(driver.snapshot().accepted, through);
    drive(&mut controller, journal.shutdown()).unwrap();
}

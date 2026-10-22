use super::*;
use crate::replica_journal::{AppendAdmissionError, RetentionTurn};
use ozzy_journal::operation::{OperationKind, RetireProducer, decode_operation_body};
use ozzy_proto::{
    ProducerSequence,
    append::{Authority, Policy},
    producer::{Mode, Open, Opened},
};
use std::time::Duration;

fn turn(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    now: u64,
) -> RetentionTurn {
    drive(
        controller,
        journal.retention_turn(
            driver.begin_validation().unwrap(),
            now,
            OperationId::from_bytes([93; 16]),
            true,
        ),
    )
    .unwrap()
}

fn resume(journal: &OwnedJournal, driver: &Driver, writer: u8, id: u8) -> ProposalBuffer {
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
                mode: Mode::Resume,
                expected_epoch: None,
                operation: OperationId::from_bytes([id; 16]),
            },
            Policy::LocalDurable,
        )
        .unwrap();
    buffer
}

fn opened(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    buffer: ProposalBuffer,
) -> Opened {
    let validation = drive(
        controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    );
    let ProposalValidation::Resolved { buffer, .. } = validation else {
        panic!("resume should resolve: {validation:?}");
    };
    buffer.producer_opened().unwrap()
}

fn commit(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    buffer: ProposalBuffer,
) {
    let receipt = admit(controller, journal, driver, buffer);
    write(controller, journal, driver, receipt);
    sync_apply(controller, journal, driver);
}

#[test]
fn an_idle_session_retires_and_resumes_above_every_retired_epoch() {
    let (mut controller, io) = setup();
    let config = OwnedConfig {
        producer_idle: Some(Duration::from_secs(16)),
        ..local_config()
    };
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let buffer = append(&journal, 12, 0);
    commit(&mut controller, &mut journal, &mut driver, buffer);
    let first = turn(&mut controller, &mut journal, &driver, 1_000);
    assert!(
        first.enabled,
        "idle expiry keeps an unlimited policy's turns"
    );
    assert!(first.proposal.is_none(), "every session starts fresh");
    // An open restarts its writer's idle clock without changing state.
    let buffer = resume(&journal, &driver, 30, 41);
    assert_eq!(
        opened(&mut controller, &mut journal, &driver, buffer).epoch,
        1
    );
    assert!(
        turn(&mut controller, &mut journal, &driver, 16_000)
            .proposal
            .is_none()
    );
    let proposal = turn(&mut controller, &mut journal, &driver, 17_000)
        .proposal
        .unwrap();
    let retired: Vec<_> = proposal
        .0
        .operations()
        .map(|operation| {
            assert_eq!(operation.kind, OperationKind::RetireProducer);
            let body =
                decode_operation_body(operation.kind, operation.body, journal.limits.operations);
            let Ok(OperationBody::RetireProducer(retired)) = body else {
                panic!("idle sweep proposes only retirements: {body:?}");
            };
            retired
        })
        .collect();
    assert_eq!(
        retired,
        [RetireProducer {
            partition: partition(),
            producer_id: ProducerId::from_bytes([12; 16]),
            producer_epoch: ProducerEpoch::INITIAL,
            next_sequence: ProducerSequence::new(1),
        }]
    );
    commit(&mut controller, &mut journal, &mut driver, proposal);
    let state = journal.images().unwrap().committed().clone();
    let partition_state = state.partition(partition()).unwrap();
    assert!(
        partition_state
            .producer(ProducerId::from_bytes([12; 16]))
            .is_none()
    );
    assert!(
        partition_state
            .producer(ProducerId::from_bytes([30; 16]))
            .is_some()
    );
    assert_eq!(partition_state.retired_epoch, 1);
    let late = append(&journal, 12, 1);
    assert!(matches!(
        drive(
            &mut controller,
            journal.propose_append(driver.begin_validation().unwrap(), late, 777)
        ),
        ProposalValidation::Rejected {
            reason: JournalError::ProducerAppend(AppendAdmissionError::Fenced),
            ..
        }
    ));
    // The resumed identity starts above the retired epoch, and its exact
    // retry resolves to the same session.
    let buffer = resume(&journal, &driver, 12, 42);
    commit(&mut controller, &mut journal, &mut driver, buffer);
    let buffer = resume(&journal, &driver, 12, 42);
    let reopened = opened(&mut controller, &mut journal, &driver, buffer);
    assert_eq!((reopened.epoch, reopened.next_sequence), (2, 0));
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn idle_observations_last_one_view_and_end_on_a_follower() {
    let (mut controller, io) = setup();
    let config = OwnedConfig {
        producer_idle: Some(Duration::from_secs(16)),
        ..local_config()
    };
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let view = driver.begin_validation().unwrap().scope().view;
    let mut sweep = |view, now, leader| {
        journal
            .idle_producer_proposal(view, now, leader)
            .unwrap()
            .0
            .map(|proposal| proposal.0.len())
    };
    assert_eq!(sweep(view, 1_000, true), None);
    // Another view starts every session fresh, and so does a follower turn.
    assert_eq!(sweep(view + 1, 17_000, true), None);
    assert_eq!(sweep(view + 1, 33_000, false), None);
    assert_eq!(sweep(view + 1, 34_000, true), None);
    assert_eq!(sweep(view + 1, 49_000, true), None);
    assert_eq!(sweep(view + 1, 50_000, true), Some(2));
    drive(&mut controller, journal.shutdown()).unwrap();
}

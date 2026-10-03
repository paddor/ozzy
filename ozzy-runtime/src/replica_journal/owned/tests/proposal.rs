use super::writeback::{
    Replica, admit_encoded, finish, initialize_writer, prepare, replica, settle,
};
use super::*;
use crate::replica_journal::{
    AppendAdmissionError, JournalCompletion, ProducerAppend, ProposalBuffer, ProposalValidation,
    ValidatedAppend,
};
use ozzy_journal::operation::{
    AppendRecord, OpenProducer, OperationBody, decode_operation_body, encode_operation_body,
};
use ozzy_proto::{
    MessageId, OperationId, OwnerEpoch, PartitionIncarnation, ProducerEpoch, ProducerId,
    ProducerSequence,
};
use ozzy_replication::{Admission, WriteTicket};

pub(super) fn request(writer: u8, first: u64, count: usize) -> ProducerAppend<'static> {
    ProducerAppend {
        partition: PartitionIncarnation::from_bytes([11; 16]),
        owner_epoch: OwnerEpoch::INITIAL,
        producer_id: ProducerId::from_bytes([writer; 16]),
        producer_epoch: ProducerEpoch::INITIAL,
        first_sequence: ProducerSequence::new(first),
        records: (0..count)
            .map(|i| AppendRecord {
                encoding: ozzy_proto::data::Encoding::Raw,
                message_id: MessageId::from_bytes([(first + i as u64 + 1) as u8; 16]),
                parts: vec![b"opaque".as_slice(), b"\0\xff".as_slice()].into(),
            })
            .collect(),
    }
}

fn proposal(
    replica: &Replica,
    requests: impl IntoIterator<Item = ProducerAppend<'static>>,
) -> ProposalBuffer {
    let mut buffer = replica.journal.lease_proposal_buffer().unwrap();
    let scope = replica.driver.scope();
    for request in requests {
        buffer
            .prepare_stream_append(
                ozzy_proto::append::Authority {
                    group_id: scope.group_id,
                    config_epoch: scope.configuration_epoch,
                    view: scope.view,
                },
                request,
            )
            .unwrap();
    }
    buffer
}

fn validate(
    controller: &mut Controller,
    replica: &mut Replica,
    buffer: ProposalBuffer,
    now: u64,
) -> ProposalValidation {
    let ticket = replica.driver.begin_validation().unwrap();
    drive(
        controller,
        replica.journal.propose_append(ticket, buffer, now),
    )
}

fn ready(result: ProposalValidation) -> ValidatedAppend {
    let ProposalValidation::Ready(validated) = result else {
        panic!("fresh proposal: {result:?}")
    };
    validated
}

fn accept(replica: &mut Replica, mut validated: ValidatedAppend) -> JournalCompletion<WriteTicket> {
    assert!(replica.journal.can_admit(&mut validated));
    let Admission::Write { ticket, .. } = replica
        .driver
        .prepare_validated(
            replica.config.identity.replica_node_id,
            validated.validation,
            validated.prepared(),
            Duration::ZERO,
        )
        .unwrap()
    else {
        panic!("write ticket")
    };
    let (_, _, receipt) = replica
        .journal
        .admit_append(ticket, validated)
        .unwrap()
        .into_parts();
    receipt
}

pub(super) fn coordinates(buffer: &AppendBuffer) -> Vec<(u64, u64, u64, usize)> {
    buffer
        .operations()
        .map(|operation| {
            let OperationBody::Append(body) = decode_operation_body(
                operation.kind,
                operation.body,
                ozzy_journal::operation::OperationLimits::default(),
            )
            .unwrap() else {
                panic!("APPEND")
            };
            let batch = &body.batches[0];
            (
                batch.first_sequence.get(),
                batch.first_offset.get(),
                batch.append_timestamp_millis,
                batch.records.len(),
            )
        })
        .collect()
}

fn open_second(
    controller: &mut Controller,
    replica: &mut Replica,
) -> JournalCompletion<WriteTicket> {
    let body = OperationBody::OpenProducer(OpenProducer {
        partition: PartitionIncarnation::from_bytes([11; 16]),
        producer_id: ProducerId::from_bytes([30; 16]),
        expected_epoch: None,
        new_epoch: ProducerEpoch::INITIAL,
        operation_id: OperationId::from_bytes([31; 16]),
    });
    let encoded = encode_operation_body(&body, replica.config.limits.operations).unwrap();
    admit_encoded(controller, replica, [(body.kind(), encoded)]).2
}

#[test]
fn owned_producer_open_retry_keeps_one_operation_under_both_group_policies() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut replica = replica(&mut controller, io, 0, policy, 8192);
        let initial = initialize_writer(&mut controller, &mut replica);
        let scope = replica.driver.scope();
        let request = ozzy_proto::producer::Open {
            authority: ozzy_proto::append::Authority {
                group_id: scope.group_id,
                config_epoch: scope.configuration_epoch,
                view: scope.view,
            },
            partition: PartitionIncarnation::from_bytes([11; 16]),
            producer: ProducerId::from_bytes([40; 16]),
            mode: ozzy_proto::producer::Mode::Resume,
            expected_epoch: None,
            operation: OperationId::from_bytes([41; 16]),
        };
        let expected_policy = replica.config.configuration.configuration().append_policy();
        let mut buffer = replica.journal.lease_proposal_buffer().unwrap();
        buffer
            .prepare_producer_open(request, expected_policy)
            .unwrap();
        let validated = ready(validate(&mut controller, &mut replica, buffer, 777));
        assert_eq!(
            validated
                .buffer
                .producer_session
                .as_ref()
                .unwrap()
                .opened
                .unwrap()
                .epoch,
            1
        );
        let receipt = accept(&mut replica, validated);
        let accepted = replica.driver.normal().unwrap().snapshot().accepted;
        for written in [false, true] {
            let mut buffer = replica.journal.lease_proposal_buffer().unwrap();
            buffer
                .prepare_producer_open(request, expected_policy)
                .unwrap();
            let ProposalValidation::Resolved {
                through, buffer, ..
            } = validate(&mut controller, &mut replica, buffer, 888)
            else {
                panic!("same producer open must resolve");
            };
            assert_eq!(through, accepted);
            let opened = buffer.producer_opened().unwrap();
            assert_eq!(opened.epoch, 1);
            assert_eq!(opened.policy, expected_policy);
            assert_eq!(
                replica.driver.normal().unwrap().snapshot().accepted,
                accepted
            );
            drop(buffer);
            if !written {
                let work = prepare(&mut replica);
                finish(&mut controller, &mut replica, work);
            }
        }
        settle(&mut replica, initial);
        settle(&mut replica, receipt);
        drive(&mut controller, replica.journal.shutdown()).unwrap();
    }
}

#[test]
fn owned_writers_share_offsets_and_retry_exactly_before_and_after_disk() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let initial = initialize_writer(&mut controller, &mut replica);
    let second = open_second(&mut controller, &mut replica);
    let buffer = proposal(
        &replica,
        [request(12, 0, 1), request(30, 0, 1), request(12, 1, 1)],
    );
    let validated = ready(validate(&mut controller, &mut replica, buffer, 777));
    assert_eq!(
        coordinates(&validated.buffer),
        [(0, 0, 777, 1), (0, 1, 777, 1), (1, 2, 777, 1)]
    );
    let receipt = accept(&mut replica, validated);
    let accepted = replica.driver.normal().unwrap().snapshot().accepted;
    for on_disk in [false, true] {
        for (writer, seq, offset) in [(12, 0, 0), (30, 0, 1), (12, 1, 2)] {
            let buffer = proposal(&replica, [request(writer, seq, 1)]);
            let mut jobs = 0;
            let ticket = replica.driver.begin_validation().unwrap();
            let result = drive_except(
                &mut controller,
                replica.journal.propose_append(ticket, buffer, 999),
                None,
                |_| {
                    jobs += 1;
                    Effect::Normal
                },
            );
            let ProposalValidation::Resolved { buffer, .. } = result else {
                panic!("exact retry: {result:?}")
            };
            assert_eq!(coordinates(&buffer.0), [(seq, offset, 777, 1)]);
            assert_eq!(jobs > 0, on_disk);
        }
        if !on_disk {
            let work = prepare(&mut replica);
            finish(&mut controller, &mut replica, work);
        }
    }
    assert_eq!(
        replica.driver.normal().unwrap().snapshot().accepted,
        accepted
    );
    settle(&mut replica, initial);
    settle(&mut replica, second);
    settle(&mut replica, receipt);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_rebatched_retry_keeps_gaps_and_assigns_only_fresh_suffix() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let initial = initialize_writer(&mut controller, &mut replica);
    let second = open_second(&mut controller, &mut replica);
    let buffer = proposal(
        &replica,
        [request(12, 0, 1), request(30, 0, 1), request(12, 1, 1)],
    );
    let validated = ready(validate(&mut controller, &mut replica, buffer, 777));
    let receipt = accept(&mut replica, validated);
    let buffer = proposal(&replica, [request(12, 0, 3)]);
    let validated = ready(validate(&mut controller, &mut replica, buffer, 999));
    assert_eq!(coordinates(&validated.buffer), [(2, 3, 999, 1)]);
    let results = &validated.buffer.retry_results.as_ref().unwrap().ranges;
    assert_eq!(
        results
            .iter()
            .map(|span| (
                span.first_sequence.get(),
                span.first_offset.get(),
                span.records
            ))
            .collect::<Vec<_>>(),
        [(0, 0, 1), (1, 2, 2)]
    );
    let suffix = accept(&mut replica, validated);
    let work = prepare(&mut replica);
    finish(&mut controller, &mut replica, work);
    for receipt in [initial, second, receipt, suffix] {
        settle(&mut replica, receipt);
    }
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_conflicting_retry_and_grouped_bad_sequence_leave_image_and_input_unchanged() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let initial = initialize_writer(&mut controller, &mut replica);
    let buffer = proposal(&replica, [request(12, 0, 1)]);
    let validated = ready(validate(&mut controller, &mut replica, buffer, 777));
    let receipt = accept(&mut replica, validated);
    let accepted = replica.driver.normal().unwrap().snapshot().accepted;
    let mut changed = request(12, 0, 1);
    changed.records[0].parts = vec![b"changed".as_slice()].into();
    let buffer = proposal(&replica, [changed]);
    assert!(matches!(
        validate(&mut controller, &mut replica, buffer, 999),
        ProposalValidation::Rejected {
            reason: JournalError::ProducerAppend(AppendAdmissionError::RetryConflict),
            ..
        }
    ));
    let buffer = proposal(&replica, [request(12, 1, 1), request(12, 3, 1)]);
    let before = buffer
        .bodies()
        .map(|(_, bytes)| bytes.to_vec())
        .collect::<Vec<_>>();
    let ProposalValidation::Rejected { buffer, .. } =
        validate(&mut controller, &mut replica, buffer, 999)
    else {
        panic!("bad grouped sequence")
    };
    assert_eq!(
        buffer
            .bodies()
            .map(|(_, bytes)| bytes.to_vec())
            .collect::<Vec<_>>(),
        before
    );
    assert_eq!(
        replica.driver.normal().unwrap().snapshot().accepted,
        accepted
    );
    assert!(!replica.journal.is_faulted());
    let work = prepare(&mut replica);
    finish(&mut controller, &mut replica, work);
    settle(&mut replica, initial);
    settle(&mut replica, receipt);
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_cold_retry_remains_valid_during_writes_and_detached_roll() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, QuorumPolicy::Durable, 8192);
    let initial = initialize_writer(&mut controller, &mut replica);
    let buffer = proposal(&replica, [request(12, 0, 1)]);
    let validated = ready(validate(&mut controller, &mut replica, buffer, 777));
    let first = accept(&mut replica, validated);
    let work = prepare(&mut replica);
    finish(&mut controller, &mut replica, work);
    settle(&mut replica, initial);
    settle(&mut replica, first);
    let buffer = proposal(&replica, [request(12, 1, 1)]);
    let validated = ready(validate(&mut controller, &mut replica, buffer, 888));
    let second = accept(&mut replica, validated);
    let writing = prepare(&mut replica);
    let buffer = proposal(&replica, [request(12, 0, 1)]);
    assert!(matches!(
        validate(&mut controller, &mut replica, buffer, 999),
        ProposalValidation::Resolved { .. }
    ));
    let buffer = proposal(&replica, [request(12, 0, 2)]);
    assert!(matches!(
        validate(&mut controller, &mut replica, buffer, 999),
        ProposalValidation::Resolved { .. }
    ));
    finish(&mut controller, &mut replica, writing);
    settle(&mut replica, second);
    let rolling = replica.journal.begin_roll(16).unwrap();
    let buffer = proposal(&replica, [request(12, 0, 1)]);
    assert!(matches!(
        validate(&mut controller, &mut replica, buffer, 999),
        ProposalValidation::Resolved { .. }
    ));
    let completed = drive(&mut controller, rolling.publish());
    replica.journal.complete_roll(completed).unwrap();
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn owned_cold_batch_retry_verifies_records_without_per_record_file_jobs() {
    for policy in [QuorumPolicy::Durable, QuorumPolicy::Replicated] {
        let (mut controller, io) = setup();
        let mut replica = replica(&mut controller, io, 0, policy, 8192);
        let initial = initialize_writer(&mut controller, &mut replica);
        let buffer = proposal(&replica, [request(12, 0, 64)]);
        let validated = ready(validate(&mut controller, &mut replica, buffer, 777));
        let receipt = accept(&mut replica, validated);
        let work = prepare(&mut replica);
        finish(&mut controller, &mut replica, work);
        settle(&mut replica, initial);
        settle(&mut replica, receipt);
        let accepted = replica.driver.normal().unwrap().snapshot().accepted;
        let buffer = proposal(&replica, [request(12, 0, 64)]);
        let ticket = replica.driver.begin_validation().unwrap();
        let mut jobs = 0;
        let result = drive_except(
            &mut controller,
            replica.journal.propose_append(ticket, buffer, 999),
            None,
            |_| {
                jobs += 1;
                Effect::Normal
            },
        );
        let ProposalValidation::Resolved {
            through, buffer, ..
        } = result
        else {
            panic!("cold batch retry must resolve");
        };
        assert_eq!(through, accepted);
        assert_eq!(coordinates(&buffer.0), [(0, 0, 777, 64)]);
        assert!(jobs <= 64, "cold batch retry issued {jobs} file jobs");
        let mut changed = request(12, 0, 64);
        changed.records[31].parts = vec![b"conflicting later record".as_slice()].into();
        let buffer = proposal(&replica, [changed]);
        assert!(matches!(
            validate(&mut controller, &mut replica, buffer, 999),
            ProposalValidation::Rejected {
                reason: JournalError::ProducerAppend(AppendAdmissionError::RetryConflict),
                ..
            }
        ));
        assert_eq!(
            replica.driver.normal().unwrap().snapshot().accepted,
            accepted
        );
        drive(&mut controller, replica.journal.shutdown()).unwrap();
    }
}

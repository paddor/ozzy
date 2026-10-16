use super::*;
use ozzy_core::state::ProducerResultSource;
use ozzy_journal::operation::{
    AppendBatch, AppendResults, RecordParts, RecordRef, ResolvedAppend, retry_fingerprint,
};
use ozzy_proto::{
    Offset,
    append::{AppendDisposition, AppendOutcome},
};

pub(in crate::replica_journal::owned::tests) fn resolved(
    request: &ProducerAppend<'_>,
    first_offset: Offset,
    duplicates: &[Option<Offset>],
    time: u64,
) -> (ozzy_journal::operation::OperationKind, Vec<u8>) {
    assert_eq!(duplicates.len(), request.records.len());
    let mut outcomes = Vec::new();
    let mut proofs = Vec::new();
    let mut stored = Vec::new();
    let mut next = first_offset.get();
    for (record, duplicate) in request.records.iter().zip(duplicates) {
        let (disposition, offset) = if let Some(offset) = duplicate {
            proofs.push(
                retry_fingerprint(RecordRef {
                    encoding: record.encoding,
                    message_id: record.message_id,
                    parts: RecordParts::Described(&record.parts),
                })
                .unwrap(),
            );
            (AppendDisposition::Duplicate, *offset)
        } else {
            stored.push(AppendRecord {
                encoding: record.encoding,
                message_id: record.message_id,
                parts: record.parts.iter().map(|part| &part[..]).collect(),
            });
            let offset = Offset::new(next);
            next += 1;
            (AppendDisposition::Stored, offset)
        };
        outcomes.push(AppendOutcome {
            disposition,
            offset,
        });
    }
    let limits = ozzy_journal::operation::OperationLimits::default();
    let body = OperationBody::ResolvedAppend(ResolvedAppend {
        batch: AppendBatch {
            partition: request.partition,
            owner_epoch: request.owner_epoch,
            producer_id: request.producer_id,
            producer_epoch: request.producer_epoch,
            first_sequence: request.first_sequence,
            first_offset,
            append_timestamp_millis: 777,
            records: stored.into(),
        },
        results: AppendResults::classified(time, &outcomes, &proofs, limits).unwrap(),
    });
    (body.kind(), encode_operation_body(&body, limits).unwrap())
}

pub(in crate::replica_journal::owned::tests) fn copy(
    request: &ProducerAppend<'static>,
) -> ProducerAppend<'static> {
    ProducerAppend {
        partition: request.partition,
        owner_epoch: request.owner_epoch,
        producer_id: request.producer_id,
        producer_epoch: request.producer_epoch,
        first_sequence: request.first_sequence,
        records: request.records.clone(),
    }
}

fn redundant(count: usize) -> ProducerAppend<'static> {
    let mut request = request(30, 0, count);
    for record in &mut request.records {
        record.message_id = MessageId::from_bytes([1; 16]);
        record.parts = vec![b"red".as_slice(), b"undant feed".as_slice()].into();
    }
    request
}

fn assert_duplicate(result: ProposalValidation, through: Prefix, count: usize) {
    let ProposalValidation::Resolved {
        through: actual,
        buffer,
        ..
    } = result
    else {
        panic!("duplicate retry: {result:?}");
    };
    assert_eq!(actual, through);
    assert_eq!(coordinates(&buffer.0), [(0, 0, 777, count)]);
    let results = &buffer.0.retry_results.as_ref().unwrap().ranges;
    assert_eq!(results.len(), count);
    for (sequence, span) in results.iter().enumerate() {
        assert_eq!(span.first_sequence.get(), sequence as u64);
        assert_eq!(span.first_offset, Offset::ZERO);
        assert_eq!(span.records, 1);
        assert_eq!(
            span.source,
            ProducerResultSource::Duplicate {
                operation: through.op.0
            }
        );
    }
}

#[test]
fn duplicate_retries_use_queued_and_cold_proofs() {
    for count in [64, 128] {
        let (mut controller, io) = setup();
        let mut replica = replica(&mut controller, io, 0, 8192);
        let initial = initialize_writer(&mut controller, &mut replica);
        let second = open_second(&mut controller, &mut replica);
        let buffer = proposal(&replica, [request(12, 0, 1)]);
        let validated = ready(validate(&mut controller, &mut replica, buffer, 777));
        let original = accept(&mut replica, validated);
        let attempted = redundant(count);
        let (_, _, duplicate) = admit_encoded(
            &mut controller,
            &mut replica,
            [resolved(
                &attempted,
                Offset::new(1),
                &vec![Some(Offset::ZERO); count],
                100,
            )],
        );
        let through = replica.driver.begin_validation().unwrap().accepted();
        let buffer = proposal(&replica, [copy(&attempted)]);
        assert_duplicate(
            validate(&mut controller, &mut replica, buffer, 999),
            through,
            count,
        );
        assert!(
            controller.jobs().is_empty(),
            "queued proof requires no physical read"
        );
        assert!(
            replica.driver.begin_validation().unwrap().committed().op < through.op,
            "retry evidence does not confirm the original operation"
        );
        let work = prepare(&mut replica);
        finish(&mut controller, &mut replica, work);
        for receipt in [initial, second, original, duplicate] {
            settle(&mut replica, receipt);
        }
        let buffer = proposal(&replica, [copy(&attempted)]);
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
        assert_duplicate(result, through, count);
        assert!(
            jobs <= 24,
            "one cold proof operation issued {jobs} jobs for {count} attempts"
        );
        let mut changed = copy(&attempted);
        changed.records[count / 2].parts =
            vec![b"redu".as_slice(), b"ndant feed".as_slice()].into();
        let buffer = proposal(&replica, [changed]);
        assert!(matches!(
            validate(&mut controller, &mut replica, buffer, 999),
            ProposalValidation::Rejected {
                reason: JournalError::ProducerAppend(AppendAdmissionError::RetryConflict),
                ..
            }
        ));
        assert_eq!(
            replica.driver.begin_validation().unwrap().accepted(),
            through
        );
        assert!(!replica.journal.is_faulted());
        drive(&mut controller, replica.journal.shutdown()).unwrap();
    }
}

#[test]
fn mixed_retry_verifies_duplicate_proofs_and_sparse_stored_sequences() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, 8192);
    let initial = initialize_writer(&mut controller, &mut replica);
    let second = open_second(&mut controller, &mut replica);
    let buffer = proposal(&replica, [request(12, 0, 1)]);
    let validated = ready(validate(&mut controller, &mut replica, buffer, 777));
    let original = accept(&mut replica, validated);
    let mut attempted = request(30, 0, 6);
    for index in [0, 2, 4] {
        attempted.records[index].message_id = MessageId::from_bytes([1; 16]);
    }
    let duplicates = [
        Some(Offset::ZERO),
        None,
        Some(Offset::ZERO),
        None,
        Some(Offset::ZERO),
        None,
    ];
    let (_, _, mixed) = admit_encoded(
        &mut controller,
        &mut replica,
        [resolved(&attempted, Offset::new(1), &duplicates, 101)],
    );
    let through = replica.driver.begin_validation().unwrap().accepted();
    for cold in [false, true] {
        if cold {
            let work = prepare(&mut replica);
            finish(&mut controller, &mut replica, work);
        }
        let buffer = proposal(&replica, [copy(&attempted)]);
        let ProposalValidation::Resolved {
            through: actual,
            buffer,
            ..
        } = validate(&mut controller, &mut replica, buffer, 999)
        else {
            panic!("mixed retry");
        };
        assert_eq!(actual, through);
        let ranges = &buffer.0.retry_results.as_ref().unwrap().ranges;
        assert_eq!(
            ranges
                .iter()
                .map(|span| span.first_offset.get())
                .collect::<Vec<_>>(),
            [0, 1, 0, 2, 0, 3]
        );
        for index in [1, 2] {
            let mut changed = copy(&attempted);
            changed.records[index].parts = vec![b"changed".as_slice()].into();
            let buffer = proposal(&replica, [changed]);
            assert!(matches!(
                validate(&mut controller, &mut replica, buffer, 999),
                ProposalValidation::Rejected {
                    reason: JournalError::ProducerAppend(AppendAdmissionError::RetryConflict),
                    ..
                }
            ));
        }
    }
    for receipt in [initial, second, original, mixed] {
        settle(&mut replica, receipt);
    }
    drive(&mut controller, replica.journal.shutdown()).unwrap();
}

#[test]
fn duplicate_subrange_retry_verifies_clipped_proof_and_assigns_only_fresh_suffix() {
    for cold in [false, true] {
        let (mut controller, io) = setup();
        let mut replica = replica(&mut controller, io, 0, 8192);
        let initial = initialize_writer(&mut controller, &mut replica);
        let second = open_second(&mut controller, &mut replica);
        let buffer = proposal(&replica, [request(12, 0, 1)]);
        let validated = ready(validate(&mut controller, &mut replica, buffer, 777));
        let original = accept(&mut replica, validated);
        let attempted = redundant(64);
        let (_, _, duplicate) = admit_encoded(
            &mut controller,
            &mut replica,
            [resolved(
                &attempted,
                Offset::new(1),
                &[Some(Offset::ZERO); 64],
                101,
            )],
        );
        let through = replica.driver.begin_validation().unwrap().accepted();
        if cold {
            let work = prepare(&mut replica);
            finish(&mut controller, &mut replica, work);
        }
        let mut partial = copy(&attempted);
        partial.first_sequence = ProducerSequence::new(16);
        partial.records = attempted.records[16..48].to_vec();
        let buffer = proposal(&replica, [partial]);
        let ProposalValidation::Resolved {
            through: actual,
            buffer,
            ..
        } = validate(&mut controller, &mut replica, buffer, 999)
        else {
            panic!("duplicate subrange retry");
        };
        assert_eq!(actual, through);
        assert_eq!(coordinates(&buffer.0), [(16, 0, 777, 32)]);
        let mut extended = copy(&attempted);
        extended.first_sequence = ProducerSequence::new(16);
        extended.records = attempted.records[16..].to_vec();
        extended.records.extend(request(30, 64, 2).records);
        let buffer = proposal(&replica, [extended]);
        let validated = ready(validate(&mut controller, &mut replica, buffer, 999));
        assert_eq!(coordinates(&validated.buffer), [(64, 1, 999, 2)]);
        let results = &validated.buffer.retry_results.as_ref().unwrap().ranges;
        assert_eq!(results.len(), 49);
        assert_eq!(results[0].first_sequence, ProducerSequence::new(16));
        assert!(results[..48].iter().all(|span| {
            span.first_offset == Offset::ZERO
                && span.records == 1
                && span.source
                    == ProducerResultSource::Duplicate {
                        operation: through.op.0,
                    }
        }));
        assert_eq!(
            results[48],
            ozzy_core::state::ProducerResultSpan {
                first_sequence: ProducerSequence::new(64),
                first_offset: Offset::new(1),
                records: 2,
                source: ProducerResultSource::Stored,
            }
        );
        let suffix = accept(&mut replica, validated);
        let work = prepare(&mut replica);
        finish(&mut controller, &mut replica, work);
        for receipt in [initial, second, original, duplicate, suffix] {
            settle(&mut replica, receipt);
        }
        drive(&mut controller, replica.journal.shutdown()).unwrap();
    }
}

#[test]
fn failed_cold_duplicate_proof_read_fences_owner_without_advancing_accepted_prefix() {
    let (mut controller, io) = setup();
    let mut replica = replica(&mut controller, io, 0, 8192);
    let initial = initialize_writer(&mut controller, &mut replica);
    let second = open_second(&mut controller, &mut replica);
    let buffer = proposal(&replica, [request(12, 0, 1)]);
    let validated = ready(validate(&mut controller, &mut replica, buffer, 777));
    let original = accept(&mut replica, validated);
    let attempted = redundant(1);
    let (_, _, duplicate) = admit_encoded(
        &mut controller,
        &mut replica,
        [resolved(
            &attempted,
            Offset::new(1),
            &[Some(Offset::ZERO)],
            101,
        )],
    );
    let through = replica.driver.begin_validation().unwrap().accepted();
    let work = prepare(&mut replica);
    finish(&mut controller, &mut replica, work);
    for receipt in [initial, second, original, duplicate] {
        settle(&mut replica, receipt);
    }
    let buffer = proposal(&replica, [attempted]);
    let ticket = replica.driver.begin_validation().unwrap();
    let mut injected = false;
    let result = drive_except(
        &mut controller,
        replica.journal.propose_append(ticket, buffer, 999),
        None,
        |operation| {
            if !injected && matches!(operation, Operation::Read { .. }) {
                injected = true;
                Effect::FailAfter(std::io::ErrorKind::Other)
            } else {
                Effect::Normal
            }
        },
    );
    assert!(injected);
    assert!(matches!(result, ProposalValidation::Rejected { .. }));
    assert!(replica.journal.is_faulted());
    assert_eq!(
        replica.driver.begin_validation().unwrap().accepted(),
        through
    );
    assert!(matches!(
        drive(&mut controller, replica.journal.shutdown()),
        Err(JournalError::Faulted)
    ));
}

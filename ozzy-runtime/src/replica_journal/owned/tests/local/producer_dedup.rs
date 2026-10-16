use super::super::dedup::limits;
use super::*;
use crate::replica_journal::AppendAdmissionError;
use ozzy_journal::operation::{OperationKind, decode_resolved_append};
use ozzy_proto::{
    MessageId,
    append::{AppendDisposition, AppendOutcome},
};

mod checkpoint;
mod expiry;
mod overlap;
mod timing;
mod wait;

fn produce(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    requests: impl IntoIterator<Item = crate::replica_journal::ProducerAppend<'static>>,
    timestamp: u64,
) -> ProposalValidation {
    let ticket = driver.begin_validation().unwrap();
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    for request in requests {
        buffer
            .prepare_stream_append(
                ozzy_proto::append::Authority {
                    group_id: ticket.scope().group_id,
                    config_epoch: ticket.scope().configuration_epoch,
                    view: ticket.scope().view,
                },
                request,
            )
            .unwrap();
    }
    drive(
        controller,
        journal.propose_append(ticket, buffer, timestamp),
    )
}

fn ready(result: ProposalValidation) -> crate::replica_journal::ValidatedAppend {
    let ProposalValidation::Ready(validated) = result else {
        panic!("ready: {result:?}")
    };
    validated
}

fn accept(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    mut validated: crate::replica_journal::ValidatedAppend,
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

fn outcomes(validated: &crate::replica_journal::ValidatedAppend) -> Vec<Vec<AppendOutcome>> {
    validated
        .buffer()
        .operations()
        .map(|operation| {
            assert_eq!(operation.kind, OperationKind::ResolvedAppend);
            decode_resolved_append(
                operation.body,
                ozzy_journal::operation::OperationLimits::default(),
            )
            .unwrap()
            .results
            .attempts_with_fingerprints()
            .map(|(outcome, _)| outcome)
            .collect()
        })
        .collect()
}

fn stored(offset: u64) -> AppendOutcome {
    AppendOutcome {
        disposition: AppendDisposition::Stored,
        offset: Offset::new(offset),
    }
}
fn duplicate(offset: u64) -> AppendOutcome {
    AppendOutcome {
        disposition: AppendDisposition::Duplicate,
        offset: Offset::new(offset),
    }
}

fn stored_end(journal: &OwnedJournal) -> Offset {
    journal
        .images()
        .unwrap()
        .committed()
        .partition(partition())
        .unwrap()
        .next_offset
}

fn retained_ids(journal: &OwnedJournal) -> usize {
    journal
        .dedup
        .image
        .as_ref()
        .unwrap()
        .index
        .retained_entries()
}

#[tokio::test(start_paused = true)]
async fn primary_resolves_same_batch_and_redundant_producers_without_losing_retry_identity() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let mut first = request(12, 0, 3);
    first.records[1].message_id = first.records[0].message_id;
    first.records[1].parts = vec![b"same ID, different payload".as_slice()].into();
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [first],
        u64::MAX,
    ));
    assert_eq!(
        outcomes(&validated),
        [vec![stored(0), duplicate(0), stored(1)]]
    );
    assert_eq!(retained_ids(&journal), 0);
    let body = validated.buffer().operations().next().unwrap();
    let resolved = decode_resolved_append(body.body, journal.limits.operations).unwrap();
    assert_eq!(
        resolved.results.logical_time(),
        0,
        "wall clock is not log time"
    );
    assert_eq!(
        resolved
            .batch
            .descriptors()
            .map(|row| row.message_id)
            .collect::<Vec<_>>(),
        [
            MessageId::from_bytes([1; 16]),
            MessageId::from_bytes([3; 16])
        ]
    );
    let original = validated.into_buffer();
    assert_eq!(
        original.operations().next().unwrap().kind,
        OperationKind::Append
    );
    let validated = ready(drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), original.into(), 0),
    ));
    assert_eq!(
        outcomes(&validated),
        [vec![stored(0), duplicate(0), stored(1)]]
    );
    accept(&mut controller, &mut journal, &mut driver, validated);

    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [redundant_feed()],
        0,
    ));
    assert_eq!(
        outcomes(&validated),
        [vec![duplicate(0), duplicate(1), stored(2)]]
    );
    accept(&mut controller, &mut journal, &mut driver, validated);
    let state = journal
        .images()
        .unwrap()
        .committed()
        .partition(partition())
        .unwrap();
    assert_eq!(state.next_offset, Offset::new(3));
    assert_eq!(
        state
            .producer(ProducerId::from_bytes([30; 16]))
            .unwrap()
            .next_producer_sequence
            .get(),
        3
    );
    verify_redundant_retry(&mut controller, &mut journal, &driver);
    drive(&mut controller, journal.shutdown()).unwrap();
    let (journal, _) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(stored_end(&journal), Offset::new(3));
    drive(&mut controller, journal.shutdown()).unwrap();
}

fn redundant_feed() -> crate::replica_journal::ProducerAppend<'static> {
    let mut request = request(30, 0, 3);
    for (record, id) in request.records.iter_mut().zip([1, 3, 4]) {
        record.message_id = MessageId::from_bytes([id; 16]);
        record.parts = vec![b"redundant feed can differ".as_slice()].into();
    }
    request
}

fn verify_redundant_retry(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
) {
    let ProposalValidation::Resolved { buffer, .. } =
        produce(controller, journal, driver, [redundant_feed()], 0)
    else {
        panic!("exact retry")
    };
    assert_eq!(
        buffer
            .producer_retry_results()
            .iter()
            .map(|span| span.first_offset.get())
            .collect::<Vec<_>>(),
        [0, 2]
    );
    let mut changed = request(30, 0, 1);
    changed.records[0].message_id = MessageId::from_bytes([1; 16]);
    assert!(matches!(
        produce(controller, journal, driver, [changed], 0),
        ProposalValidation::Rejected {
            reason: JournalError::ProducerAppend(AppendAdmissionError::RetryConflict),
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn grouped_producers_keep_attempt_sequences_independent_from_shared_offsets() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let first = request(12, 0, 2);
    let second = request(30, 0, 3);
    let mut third = request(12, 2, 2);
    third.records[0].message_id = MessageId::from_bytes([1; 16]);
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [first, second, third],
        777,
    ));
    assert_eq!(
        outcomes(&validated),
        [
            vec![stored(0), stored(1)],
            vec![duplicate(0), duplicate(1), stored(2)],
            vec![duplicate(0), stored(3)]
        ]
    );
    accept(&mut controller, &mut journal, &mut driver, validated);
    let state = journal
        .images()
        .unwrap()
        .committed()
        .partition(partition())
        .unwrap();
    assert_eq!(state.next_offset, Offset::new(4));
    assert_eq!(
        state
            .producer(ProducerId::from_bytes([12; 16]))
            .unwrap()
            .next_producer_sequence
            .get(),
        4
    );
    assert_eq!(
        state
            .producer(ProducerId::from_bytes([30; 16]))
            .unwrap()
            .next_producer_sequence
            .get(),
        3
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[tokio::test(start_paused = true)]
async fn all_unique_lz4_keeps_the_sdk_block_and_mixed_resolution_decodes_only_selected_records() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let (buffer, block) = compressed(&journal, &driver, request(12, 0, 8));
    let validated = ready(drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    ));
    let resolved = decode_resolved_append(
        validated.buffer().operations().next().unwrap().body,
        journal.limits.operations,
    )
    .unwrap();
    assert_eq!(resolved.results.stored(), 8);
    let payload = resolved.batch.prepared_payload.unwrap();
    assert_eq!(payload.encoding, ozzy_proto::append::PayloadEncoding::Lz4);
    assert_eq!(payload.encoded, block);
    accept(&mut controller, &mut journal, &mut driver, validated);

    let mut mixed = request(30, 0, 8);
    for (record, id) in mixed.records.iter_mut().zip([1, 2, 3, 4, 9, 10, 11, 12]) {
        record.message_id = MessageId::from_bytes([id; 16]);
    }
    let (buffer, _) = compressed(&journal, &driver, mixed);
    let validated = ready(drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    ));
    let resolved = decode_resolved_append(
        validated.buffer().operations().next().unwrap().body,
        journal.limits.operations,
    )
    .unwrap();
    assert_eq!(resolved.results.stored(), 4);
    assert_eq!(resolved.results.attempts() - resolved.results.stored(), 4);
    assert_eq!(
        resolved.batch.prepared_payload.unwrap().encoding,
        ozzy_proto::append::PayloadEncoding::Lz4
    );
    let batch = resolved.batch.materialize().unwrap();
    assert_eq!(
        batch
            .records
            .iter()
            .map(|row| row.message_id)
            .collect::<Vec<_>>(),
        [9, 10, 11, 12].map(|id| MessageId::from_bytes([id; 16]))
    );
    for record in &batch.records {
        assert!(
            record
                .parts
                .iter()
                .eq([b"".as_slice(), PAYLOAD, b"end"].into_iter())
        );
    }
    drop(batch);
    accept(&mut controller, &mut journal, &mut driver, validated);

    let mut repeated = request(12, 8, 8);
    for (record, id) in repeated.records.iter_mut().zip(1..=8) {
        record.message_id = MessageId::from_bytes([id; 16]);
    }
    let (buffer, _) = compressed(&journal, &driver, repeated);
    let validated = ready(drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 777),
    ));
    let resolved = decode_resolved_append(
        validated.buffer().operations().next().unwrap().body,
        journal.limits.operations,
    )
    .unwrap();
    assert_eq!(resolved.results.stored(), 0);
    assert_eq!(resolved.results.attempts() - resolved.results.stored(), 8);
    assert!(resolved.batch.prepared_payload.is_none());
    assert_eq!(resolved.batch.summary.record_count, 0);
    accept(&mut controller, &mut journal, &mut driver, validated);
    assert_eq!(stored_end(&journal), Offset::new(12));
    verify_stored_read(&mut controller, &mut journal, &driver);
    drive(&mut controller, journal.shutdown()).unwrap();
}

fn verify_stored_read(controller: &mut Controller, journal: &mut OwnedJournal, driver: &Driver) {
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
                max_records: 16,
                max_parts: 48,
                max_payload_bytes: 8192,
            },
            journal.lease_append_buffer().unwrap().into(),
        )
        .unwrap();
    let done = drive(controller, work.read());
    let read = journal.complete_read(done).unwrap();
    assert_eq!(read.buffer.records().len(), 12);
    assert_eq!(read.cursor.next_offset(), Offset::new(12));
}

const PAYLOAD: &[u8] = &[b'x'; 512];

fn compressed(
    journal: &OwnedJournal,
    driver: &Driver,
    mut request: crate::replica_journal::ProducerAppend<'static>,
) -> (ProposalBuffer, Vec<u8>) {
    use ozzy_proto::append::{
        Append, AppendKey, Authority, DataLimits, PayloadEncoding, Policy, Record,
        encode_prepared_append_metadata, validate_append,
    };
    for record in &mut request.records {
        record.parts = vec![b"".as_slice(), PAYLOAD, b"end".as_slice()].into();
    }
    let records: Vec<_> = request
        .records
        .iter()
        .map(|record| Record {
            encoding: record.encoding,
            message_id: record.message_id,
            parts: &record.parts,
        })
        .collect();
    let decoded: Vec<u8> = records
        .iter()
        .flat_map(|record| record.parts.iter().flat_map(|part| part.iter().copied()))
        .collect();
    let payload = lz4rip::block::compress(&decoded);
    let scope = driver.begin_validation().unwrap().scope();
    let envelope = ozzy_proto::Envelope {
        opcode: ozzy_proto::Opcode::Append,
        response: false,
        request_id: Some(ozzy_proto::RequestId::from_bytes([1; 16])),
        sender: ozzy_proto::NodeId::from_bytes([2; 16]),
        session: Some(ozzy_proto::LinkSessionId::from_bytes([3; 16])),
    };
    let mut metadata = Vec::with_capacity(4096);
    let limits = DataLimits::default();
    let header = encode_prepared_append_metadata(
        envelope,
        Append {
            authority: Authority {
                group_id: scope.group_id,
                config_epoch: scope.configuration_epoch,
                view: scope.view,
            },
            partition: request.partition,
            owner_epoch: request.owner_epoch.get(),
            key: AppendKey {
                producer_id: request.producer_id,
                producer_epoch: request.producer_epoch.get(),
                first_sequence: request.first_sequence.get(),
            },
            policy: Policy::LocalDurable,
            records: &records,
        },
        &mut metadata,
        PayloadEncoding::Lz4,
        payload.len(),
        limits,
    )
    .unwrap();
    let packet =
        ozzy_proto::decode_packet(&[&header, &metadata, &payload], limits.envelope).unwrap();
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_stream_wire_append(validate_append(packet, limits).unwrap())
        .unwrap();
    (buffer, payload)
}

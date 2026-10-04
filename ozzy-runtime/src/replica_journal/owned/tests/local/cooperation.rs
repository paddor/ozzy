use super::*;

#[test]
fn native_large_proposal_yields_before_admission_and_keeps_local_durability() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    let capacity = 256 * 1024;
    config.limits.io.max_segment_bytes = capacity;
    config.append_limits.max_body_bytes = 192 * 1024;
    config.writeback = config.append_limits;
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), capacity),
    )
    .unwrap();
    journal
        .bind_append_memory(&payload_owner(1024 * 1024))
        .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let payload = vec![91; 128 * 1024];
    let template = request(12, 0, 1);
    let request = crate::replica_journal::ProducerAppend {
        partition: template.partition,
        owner_epoch: template.owner_epoch,
        producer_id: template.producer_id,
        producer_epoch: template.producer_epoch,
        first_sequence: template.first_sequence,
        records: vec![ozzy_journal::operation::AppendRecord {
            encoding: ozzy_proto::data::Encoding::Raw,
            message_id: ozzy_proto::MessageId::from_bytes([1; 16]),
            parts: vec![payload.as_slice()].into(),
        }],
    };
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer.prepare_append(request).unwrap();
    let before = driver.snapshot();
    let validated = {
        let ticket = driver.begin_validation().unwrap();
        let mut proposal = std::pin::pin!(journal.propose_append(ticket, buffer, 777));
        // These are CPU yields. No file operation or state installation hides
        // behind them, so another partition can run between hashing slices.
        for _ in 0..2 {
            assert!(poll(proposal.as_mut()).is_pending());
            assert_eq!(controller.jobs().len(), 0);
            assert_eq!(driver.snapshot(), before);
        }
        let Poll::Ready(ProposalValidation::Ready(validated)) = poll(proposal.as_mut()) else {
            panic!("bounded hashing must finish without extra I/O");
        };
        validated
    };
    assert_eq!(driver.snapshot(), before, "validation is not admission");
    assert_eq!(
        journal.images().unwrap().speculative().revision(),
        before.accepted.op.0
    );
    let mut validated = validated;
    assert!(journal.can_admit(&mut validated));
    let write_ticket = driver
        .prepare_validated(validated.validation(), validated.prepared())
        .unwrap();
    let receipt = journal
        .admit_append(write_ticket, validated)
        .unwrap()
        .into_parts()
        .2;
    assert_eq!(driver.snapshot().accepted.op.0, before.accepted.op.0 + 1);
    assert_eq!(driver.snapshot().applied, before.applied);
    write(&mut controller, &mut journal, &mut driver, receipt);
    assert_eq!(driver.snapshot().applied, before.applied);
    sync_apply(&mut controller, &mut journal, &mut driver);
    assert_eq!(driver.snapshot().applied.op.0, before.applied.op.0 + 1);
    assert_eq!(driver.snapshot().committed, driver.snapshot().applied);
    drive(&mut controller, journal.shutdown()).unwrap();
}

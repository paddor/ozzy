use super::*;

#[tokio::test(start_paused = true)]
async fn overlap_retains_the_complete_attempt_across_pending_original_and_stale_validation() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let first = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [request(12, 0, 1)],
        0,
    ));
    accept(&mut controller, &mut journal, &mut driver, first);
    let mut second = request(30, 0, 1);
    second.records[0].message_id = MessageId::from_bytes([2; 16]);
    let mut second = ready(produce(&mut controller, &mut journal, &driver, [second], 0));
    assert!(journal.can_admit(&mut second));
    let ticket = driver
        .prepare_validated(second.validation(), second.prepared())
        .unwrap();
    let receipt = journal.admit_append(ticket, second).unwrap().into_parts().2;

    let ProposalValidation::Deferred { reason, buffer, .. } = produce(
        &mut controller,
        &mut journal,
        &driver,
        [request(12, 0, 2)],
        0,
    ) else {
        panic!("fresh suffix must wait for producer 30's original")
    };
    assert_eq!(
        reason,
        crate::replica_journal::ProposalDeferral::Original(ticket.through())
    );
    assert!(!buffer.has_trimmed_retry());
    let batch = ozzy_journal::operation::decode_append_batches(
        buffer.bodies().next().unwrap().1,
        journal.limits.operations,
    )
    .unwrap()
    .remove(0);
    assert_eq!(batch.summary.first_sequence.get(), 0);
    assert_eq!(batch.summary.record_count, 2);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let resolved = ready(drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 0),
    ));
    assert_eq!(outcomes(&resolved), [vec![duplicate(1)]]);
    let restored = resolved.into_buffer();
    let batch = ozzy_journal::operation::decode_append_batches(
        restored.operations().next().unwrap().body,
        journal.limits.operations,
    )
    .unwrap()
    .remove(0);
    assert_eq!(batch.summary.first_sequence.get(), 0);
    assert_eq!(batch.summary.record_count, 2);
    let resolved = ready(drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), restored.into(), 0),
    ));
    assert_eq!(outcomes(&resolved), [vec![duplicate(1)]]);
    let ranges = &resolved.buffer().retry_results.as_ref().unwrap().ranges;
    assert_eq!(ranges.len(), 2);
    assert_eq!(
        (ranges[0].first_sequence.get(), ranges[0].first_offset.get()),
        (0, 0)
    );
    assert_eq!(
        (ranges[1].first_sequence.get(), ranges[1].first_offset.get()),
        (1, 1)
    );
    accept(&mut controller, &mut journal, &mut driver, resolved);
    assert_eq!(stored_end(&journal), Offset::new(2));
    let retry = produce(
        &mut controller,
        &mut journal,
        &driver,
        [request(12, 0, 2)],
        0,
    );
    assert!(matches!(retry, ProposalValidation::Resolved { .. }));
    drive(&mut controller, journal.shutdown()).unwrap();
}

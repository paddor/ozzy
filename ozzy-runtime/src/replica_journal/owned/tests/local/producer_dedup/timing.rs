use super::*;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn partial_preparation_capacity_waits_for_the_whole_working_set() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    let memory = payload_owner(1024 * 1024);
    journal.bind_append_memory(&memory).unwrap();
    let mut driver = driver;
    initialize(&mut controller, &mut journal, &mut driver);
    let ticket = driver.begin_validation().unwrap();
    let mut proposal = journal.lease_proposal_buffer().unwrap();
    proposal.prepare_append(request(12, 0, 1)).unwrap();
    let held = memory
        .try_charge(1024 * 1024 - memory.allocated_bytes() - 3500)
        .unwrap();
    let ProposalValidation::Deferred { buffer, .. } =
        drive(&mut controller, journal.propose_append(ticket, proposal, 0))
    else {
        panic!("partial capacity must retain the proposal")
    };
    assert!(
        !journal.validation_memory_ready(),
        "released scratch must not cause a busy retry"
    );
    let mut capacity = Box::pin(journal.validation_memory_wait().unwrap());
    assert!(poll(capacity.as_mut()).is_pending());
    drop(held);
    assert!(poll(capacity.as_mut()).is_ready());
    drop(capacity);
    let validated = ready(drive(
        &mut controller,
        journal.propose_append(ticket, buffer, 0),
    ));
    assert_eq!(outcomes(&validated), [vec![stored(0)]]);
    drop(validated);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[tokio::test(start_paused = true)]
async fn preparation_pressure_retains_proposals_and_received_suffixes_until_backing_is_released() {
    use crate::replica_journal::owned::tests::proposal::dedup::resolved;
    use crate::replica_journal::{ProposalDeferral, ReceivedValidation};
    use ozzy_journal::operation::CanonicalOperation;
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    let memory = payload_owner(1024 * 1024);
    journal.bind_append_memory(&memory).unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let ticket = driver.begin_validation().unwrap();
    let mut proposal = journal.lease_proposal_buffer().unwrap();
    proposal.prepare_append(request(12, 0, 1)).unwrap();
    let held = memory
        .try_charge(1024 * 1024 - memory.allocated_bytes())
        .unwrap();
    let ProposalValidation::Deferred {
        reason: ProposalDeferral::MemoryCapacity,
        buffer,
        ..
    } = drive(&mut controller, journal.propose_append(ticket, proposal, 0))
    else {
        panic!("memory pressure must retain the unadmitted proposal")
    };
    assert!(!journal.validation_memory_ready());
    assert!(!journal.is_faulted());
    assert_eq!(retained_ids(&journal), 0);
    let mut capacity = Box::pin(journal.validation_memory_wait().unwrap());
    assert!(poll(capacity.as_mut()).is_pending());
    drop(held);
    assert!(poll(capacity.as_mut()).is_ready());
    drop(capacity);
    let validated = ready(drive(
        &mut controller,
        journal.propose_append(ticket, buffer, 0),
    ));
    assert_eq!(outcomes(&validated), [vec![stored(0)]]);
    drop(validated);
    memory.trim_cache();
    journal.clear_validation_memory_wait();
    let body = resolved(&request(12, 0, 1), Offset::ZERO, &[None], 0);
    let mut buffer = journal.lease_append_buffer().unwrap();
    buffer
        .push(CanonicalOperation {
            group_id: ticket.scope().group_id,
            configuration_epoch: ticket.scope().configuration_epoch,
            original_view: ticket.scope().view,
            op_number: ticket.accepted().op.0 + 1,
            previous_digest: ticket.accepted().digest,
            kind: body.0,
            body: &body.1,
        })
        .unwrap();
    let held = memory
        .try_charge(1024 * 1024 - memory.allocated_bytes())
        .unwrap();
    let ReceivedValidation::Deferred { buffer, .. } =
        drive(&mut controller, journal.validate_received(ticket, buffer))
    else {
        panic!("memory pressure must retain the received canonical suffix")
    };
    assert!(!journal.validation_memory_ready());
    assert!(!journal.is_faulted());
    assert_eq!(retained_ids(&journal), 0);
    drop(held);
    let ReceivedValidation::Ready(validated) =
        drive(&mut controller, journal.validate_received(ticket, buffer))
    else {
        panic!("released backing must let the same suffix validate")
    };
    assert_eq!(outcomes(&validated), [vec![stored(0)]]);
    accept(&mut controller, &mut journal, &mut driver, validated);
    assert_eq!(stored_end(&journal), Offset::new(1));
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[tokio::test(start_paused = true)]
async fn small_appends_and_expiry_fit_a_small_owner_with_a_large_identity_capacity() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(ozzy_core::dedup::Limits {
        entries: 8192,
        pending_records: 8192,
        bytes: 16 * 1024 * 1024,
        ..limits()
    });
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    let memory = payload_owner(32 * 1024);
    journal.bind_append_memory(&memory).unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let controls = journal.identity_capacity().unwrap();
    for sequence in 0..32 {
        let validated = ready(produce(
            &mut controller,
            &mut journal,
            &driver,
            [request(12, sequence, 1)],
            0,
        ));
        assert_eq!(outcomes(&validated), [vec![stored(sequence)]]);
        accept(&mut controller, &mut journal, &mut driver, validated);
        tokio::time::advance(Duration::from_millis(11)).await;
        let turn = drive(
            &mut controller,
            journal.retention_turn(
                driver.begin_validation().unwrap(),
                0,
                OperationId::new(),
                true,
            ),
        )
        .unwrap();
        if let Some(buffer) = turn.proposal {
            let validated = ready(drive(
                &mut controller,
                journal.propose_append(driver.begin_validation().unwrap(), buffer, 0),
            ));
            accept(&mut controller, &mut journal, &mut driver, validated);
        }
        assert_eq!(journal.identity_capacity().unwrap(), controls);
        assert!(memory.allocated_bytes() <= 32 * 1024);
    }
    assert_eq!(stored_end(&journal), Offset::new(32));
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[tokio::test(start_paused = true)]
async fn monotonic_window_never_refreshes_for_duplicates_and_restart_extends_it() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [request(12, 0, 1)],
        u64::MAX,
    ));
    assert_eq!(outcomes(&validated), [vec![stored(0)]]);
    accept(&mut controller, &mut journal, &mut driver, validated);
    tokio::time::advance(Duration::from_millis(9)).await;
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [request(30, 0, 1)],
        0,
    ));
    assert_eq!(outcomes(&validated), [vec![duplicate(0)]]);
    accept(&mut controller, &mut journal, &mut driver, validated);
    tokio::time::advance(Duration::from_millis(1)).await;
    let mut repeated = request(12, 1, 1);
    repeated.records[0].message_id = MessageId::from_bytes([1; 16]);
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [repeated],
        0,
    ));
    assert_eq!(outcomes(&validated), [vec![stored(1)]]);
    accept(&mut controller, &mut journal, &mut driver, validated);
    drive(&mut controller, journal.shutdown()).unwrap();
    tokio::time::advance(Duration::from_secs(60)).await;
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    let mut repeated = request(30, 1, 1);
    repeated.records[0].message_id = MessageId::from_bytes([1; 16]);
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [repeated],
        u64::MAX,
    ));
    assert_eq!(
        outcomes(&validated),
        [vec![duplicate(1)]],
        "restart downtime never expires live IDs"
    );
    accept(&mut controller, &mut journal, &mut driver, validated);
    let ProposalValidation::Resolved { buffer, .. } = produce(
        &mut controller,
        &mut journal,
        &driver,
        [request(30, 0, 1)],
        0,
    ) else {
        panic!("original duplicate retry must remain exact after expiry")
    };
    assert_eq!(
        buffer.producer_retry_results()[0].first_offset,
        Offset::ZERO
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[tokio::test(start_paused = true)]
async fn canceling_prepared_resolution_releases_all_metadata_and_reserves_no_ids() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    let memory = payload_owner(1024 * 1024);
    journal.bind_append_memory(&memory).unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let mut attempted = request(12, 0, 2);
    attempted.records[1].message_id = attempted.records[0].message_id;
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [attempted],
        0,
    ));
    assert_eq!(outcomes(&validated), [vec![stored(0), duplicate(0)]]);
    assert!(memory.allocated_bytes() > validated.buffer().retained_bytes());
    assert_eq!(retained_ids(&journal), 0);
    let original = validated.into_buffer();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), original.retained_bytes());
    assert_eq!(
        original.operations().next().unwrap().kind,
        OperationKind::Append
    );
    drop(original);
    drive(&mut controller, journal.shutdown()).unwrap();
    memory.trim_cache();
    assert_eq!(memory.allocated_bytes(), 0);
}

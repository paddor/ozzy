use super::*;
use crate::replica_journal::ProposalDeferral;
use std::time::Duration;

fn small_limits() -> ozzy_core::dedup::Limits {
    ozzy_core::dedup::Limits {
        entries: 2,
        pending_records: 2,
        batch_records: 2,
        ..limits()
    }
}

fn retention(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    leader: bool,
) -> crate::replica_journal::RetentionTurn {
    drive(
        controller,
        journal.retention_turn(
            driver.begin_validation().unwrap(),
            u64::MAX,
            OperationId::from_bytes([71; 16]),
            leader,
        ),
    )
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn idle_watermark_waits_for_payload_backing_without_faulting_or_busy_retry() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    let memory = payload_owner(32 * 1024);
    journal.bind_append_memory(&memory).unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [request(12, 0, 1)],
        0,
    ));
    accept(&mut controller, &mut journal, &mut driver, validated);
    tokio::time::advance(Duration::from_millis(11)).await;
    memory.trim_cache();
    let held = memory
        .try_charge(32 * 1024 - memory.allocated_bytes())
        .unwrap();
    let turn = retention(&mut controller, &mut journal, &driver, true);
    assert!(turn.enabled);
    assert!(!turn.more_work);
    assert!(turn.proposal.is_none());
    assert!(!journal.is_faulted());
    assert_eq!(retained_ids(&journal), 1);
    drop(held);
    let buffer = retention(&mut controller, &mut journal, &driver, true)
        .proposal
        .expect("released backing must allow the next retention turn");
    let validated = ready(drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 0),
    ));
    accept(&mut controller, &mut journal, &mut driver, validated);
    assert_eq!(
        retention(&mut controller, &mut journal, &driver, true).expired_identities,
        1
    );
    assert_eq!(stored_end(&journal), Offset::new(1));
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[tokio::test(start_paused = true)]
async fn full_index_defers_unique_records_until_an_idle_watermark_is_confirmed() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(small_limits());
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
        [request(12, 0, 2)],
        0,
    ));
    accept(&mut controller, &mut journal, &mut driver, validated);
    let before = driver.snapshot();
    for millis in [0, 9, 1] {
        tokio::time::advance(Duration::from_millis(millis)).await;
        assert!(matches!(
            produce(
                &mut controller,
                &mut journal,
                &driver,
                [request(12, 2, 1)],
                0
            ),
            ProposalValidation::Deferred {
                reason: ProposalDeferral::IdentityCapacity,
                ..
            }
        ));
        assert_eq!(driver.snapshot().accepted, before.accepted);
        assert_eq!(
            retained_ids(&journal),
            2,
            "live identities cannot be evicted"
        );
    }
    assert!(
        retention(&mut controller, &mut journal, &driver, false)
            .proposal
            .is_none()
    );
    confirm_idle_expiry(&mut controller, &mut journal, &mut driver);
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [request(12, 2, 1)],
        0,
    ));
    assert_eq!(outcomes(&validated), [vec![stored(2)]]);
    accept(&mut controller, &mut journal, &mut driver, validated);
    assert_eq!(stored_end(&journal), Offset::new(3));
    drive(&mut controller, journal.shutdown()).unwrap();
    tokio::time::advance(Duration::from_secs(60)).await;
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    let mut redundant = request(30, 0, 1);
    redundant.records[0].message_id = MessageId::from_bytes([3; 16]);
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [redundant],
        0,
    ));
    assert_eq!(outcomes(&validated), [vec![duplicate(2)]]);
    accept(&mut controller, &mut journal, &mut driver, validated);
    assert!(matches!(
        produce(
            &mut controller,
            &mut journal,
            &driver,
            [request(12, 0, 2)],
            0
        ),
        ProposalValidation::Resolved { .. }
    ));
    drive(&mut controller, journal.shutdown()).unwrap();
}

fn confirm_idle_expiry(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
) {
    let turn = retention(controller, journal, driver, true);
    assert_eq!(turn.expired_identities, 0);
    let buffer = turn
        .proposal
        .expect("idle time must progress under full admission");
    let validated = ready(drive(
        controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 0),
    ));
    assert_eq!(
        retained_ids(journal),
        2,
        "preparation cannot expire identities"
    );
    let ticket = driver
        .prepare_validated(validated.validation(), validated.prepared())
        .unwrap();
    let mut validated = validated;
    assert!(journal.can_admit(&mut validated));
    let receipt = journal
        .admit_append(ticket, validated)
        .unwrap()
        .into_parts()
        .2;
    write(controller, journal, driver, receipt);
    let turn = retention(controller, journal, driver, true);
    assert_eq!(
        turn.expired_identities, 0,
        "physical write is not confirmation"
    );
    assert_eq!(retained_ids(journal), 2);
    sync_apply(controller, journal, driver);
    let turn = retention(controller, journal, driver, true);
    assert!(
        turn.enabled,
        "identity expiry survives unlimited record retention"
    );
    assert_eq!(turn.expired_identities, 2);
    assert_eq!(retained_ids(journal), 0);
}

/// One request of `writer` at `first` with `count` records whose identities
/// stay distinct beyond 256 records.
fn request_wide(
    writer: u8,
    first: u64,
    count: usize,
) -> crate::replica_journal::ProducerAppend<'static> {
    crate::replica_journal::ProducerAppend {
        records: (0..count)
            .map(|index| {
                let id = first + index as u64 + 1;
                let mut bytes = [9; 16];
                bytes[0] = (id >> 8) as u8 + 1;
                bytes[1] = id as u8;
                ozzy_journal::operation::AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes(bytes),
                    parts: vec![b"opaque".as_slice(), b"\0\xff".as_slice()].into(),
                }
            })
            .collect(),
        ..request(writer, first, 0)
    }
}

#[tokio::test(start_paused = true)]
async fn identity_expiry_steps_run_without_a_settled_journal() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(ozzy_core::dedup::Limits {
        entries: 512,
        bytes: 4 * 1024 * 1024,
        ..limits()
    });
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    // More confirmed identities than one bounded expiry step releases.
    let mut first = 0;
    while first < 300 {
        let count = (300 - first).min(8);
        let validated = ready(produce(
            &mut controller,
            &mut journal,
            &driver,
            [request_wide(12, first as u64, count)],
            0,
        ));
        accept(&mut controller, &mut journal, &mut driver, validated);
        first += count;
    }
    assert_eq!(retained_ids(&journal), 300);
    tokio::time::advance(Duration::from_millis(11)).await;
    // The settled turn proposes the idle watermark; confirming it makes every
    // identity expirable, and the next settled turn releases one bounded step.
    let buffer = retention(&mut controller, &mut journal, &driver, true)
        .proposal
        .expect("idle watermark");
    let validated = ready(drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 0),
    ));
    accept(&mut controller, &mut journal, &mut driver, validated);
    let turn = retention(&mut controller, &mut journal, &driver, true);
    assert_eq!(turn.expired_identities, 256);
    assert!(turn.more_work);
    // An accepted-only append leaves the journal unsettled: a retention turn
    // defers, but the expiry step still releases the remaining identities.
    let mut validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [request_wide(12, 300, 1)],
        0,
    ));
    assert!(journal.can_admit(&mut validated));
    let ticket = driver
        .prepare_validated(validated.validation(), validated.prepared())
        .unwrap();
    let receipt = journal
        .admit_append(ticket, validated)
        .unwrap()
        .into_parts()
        .2;
    write(&mut controller, &mut journal, &mut driver, receipt);
    let snapshot = driver.snapshot();
    assert_ne!(snapshot.accepted, snapshot.applied);
    // Admission may already have released a few expirable identities; the
    // accepted-only record's own identity stays live.
    let remaining = retained_ids(&journal) - 1;
    assert!((1..=44).contains(&remaining));
    let deferred = retention(&mut controller, &mut journal, &driver, true);
    assert_eq!(deferred.expired_identities, 0);
    assert!(!deferred.more_work);
    let step = journal
        .retain_identities_turn(driver.begin_validation().unwrap(), true)
        .unwrap();
    assert_eq!(step.expired, remaining);
    assert!(!step.more_work);
    assert!(step.proposal.is_none());
    sync_apply(&mut controller, &mut journal, &mut driver);
    assert_eq!(retained_ids(&journal), 1);
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[tokio::test(start_paused = true)]
async fn record_retention_proceeds_while_identity_expiry_has_more_work() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(ozzy_core::dedup::Limits {
        entries: 1024,
        bytes: 4 * 1024 * 1024,
        ..limits()
    });
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io, JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    // Seal more identities than one expiry step releases below a retention
    // target that every sealed segment exceeds.
    let mut first = 0;
    while first < 400 {
        let count = (400 - first).min(8);
        let validated = ready(produce(
            &mut controller,
            &mut journal,
            &driver,
            [request_wide(12, first as u64, count)],
            0,
        ));
        accept(&mut controller, &mut journal, &mut driver, validated);
        first += count;
    }
    let work = journal.begin_roll(8).unwrap();
    let done = drive(&mut controller, work.publish());
    journal.complete_roll(done).unwrap();
    let body = OperationBody::PartitionPolicy(ozzy_journal::operation::PartitionPolicy {
        partition: partition(),
        expected_revision: 1,
        new_revision: 2,
        retention: RetentionPolicy {
            max_age_millis: None,
            max_bytes: std::num::NonZeroU64::new(1),
        },
        operation_id: OperationId::from_bytes([90; 16]),
    });
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .push(
            body.kind(),
            &encode_operation_body(&body, config.limits.operations).unwrap(),
        )
        .unwrap();
    let validated = ready(drive(
        &mut controller,
        journal.propose_append(driver.begin_validation().unwrap(), buffer, 0),
    ));
    accept(&mut controller, &mut journal, &mut driver, validated);
    // A later record advances logical time past every earlier identity.
    tokio::time::advance(Duration::from_millis(11)).await;
    let validated = ready(produce(
        &mut controller,
        &mut journal,
        &driver,
        [request_wide(12, 400, 1)],
        0,
    ));
    accept(&mut controller, &mut journal, &mut driver, validated);
    let turn = retention(&mut controller, &mut journal, &driver, true);
    assert_eq!(turn.expired_identities, 256);
    assert!(turn.more_work);
    let proposal = turn
        .proposal
        .expect("record retention must not wait for identity expiry");
    assert!(proposal.0.operations().any(|operation| matches!(
        operation.kind,
        OperationKind::ProducerResultFloor | OperationKind::Trim
    )));
    drive(&mut controller, journal.shutdown()).unwrap();
}

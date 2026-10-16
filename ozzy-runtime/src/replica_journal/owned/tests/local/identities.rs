use super::super::{dedup::limits, proposal::dedup::resolved};
use super::*;
use ozzy_core::dedup::Match;
use ozzy_io::simulation::Stage;
use ozzy_journal::operation::{DedupWatermark, OperationKind};
use ozzy_proto::MessageId;

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

fn found(journal: &OwnedJournal) -> Option<Match> {
    journal
        .dedup
        .image
        .as_ref()
        .unwrap()
        .index
        .lookup(MessageId::from_bytes([1; 16]))
}

#[test]
fn local_reopen_restores_original_id_time_and_duplicates_never_refresh_it() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    config.deduplication = Some(limits());
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    commit(
        &mut controller,
        &mut journal,
        &mut driver,
        &resolved(&request(12, 0, 1), Offset::ZERO, &[None], 0),
    );
    assert_eq!(found(&journal), Some(Match::Committed(Offset::ZERO)));
    commit(
        &mut controller,
        &mut journal,
        &mut driver,
        &resolved(&request(30, 0, 1), Offset::new(1), &[Some(Offset::ZERO)], 9),
    );
    let applied = driver.begin_validation().unwrap().committed();
    drive(&mut controller, journal.shutdown()).unwrap();
    let mut disabled = config.clone();
    disabled.deduplication = None;
    assert!(
        matches!(
            drive(
                &mut controller,
                OwnedJournal::open_local(disabled, io.clone(), JournalGeneration(2),)
            ),
            Err(JournalError::Configuration)
        ),
        "reopen cannot silently disable persisted dedup policy"
    );
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    assert_eq!(driver.begin_validation().unwrap().committed(), applied);
    assert_eq!(found(&journal), Some(Match::Committed(Offset::ZERO)));
    let watermark = OperationBody::DedupWatermark(DedupWatermark {
        partition: partition(),
        owner_epoch: OwnerEpoch::INITIAL,
        logical_time: 10,
    });
    let watermark = (
        watermark.kind(),
        encode_operation_body(&watermark, journal.limits.operations).unwrap(),
    );
    let identity_capacity = journal.identity_capacity().unwrap();
    commit(&mut controller, &mut journal, &mut driver, &watermark);
    assert_eq!(
        journal.identity_capacity().unwrap(),
        identity_capacity,
        "internal expiry must not trigger a control identity index rebuild"
    );
    assert_eq!(
        found(&journal),
        None,
        "time 9 duplicate did not refresh the birth time"
    );
    let mut repeated = request(30, 1, 1);
    repeated.records[0].message_id = MessageId::from_bytes([1; 16]);
    commit(
        &mut controller,
        &mut journal,
        &mut driver,
        &resolved(&repeated, Offset::new(1), &[None], 10),
    );
    assert_eq!(found(&journal), Some(Match::Committed(Offset::new(1))));
    let state = journal
        .images()
        .unwrap()
        .committed()
        .partition(partition())
        .unwrap();
    assert_eq!(state.next_offset, Offset::new(2));
    assert_eq!(
        state
            .producer(ProducerId::from_bytes([30; 16]))
            .unwrap()
            .next_producer_sequence
            .get(),
        2
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn invalid_identity_limits_fail_before_formatting_any_storage() {
    let (mut controller, io) = setup();
    let mut config = local_config();
    let mut invalid = limits();
    invalid.bytes = 32;
    config.deduplication = Some(invalid);
    let result = drive(
        &mut controller,
        OwnedJournal::format_local(config, io, JournalGeneration(1), 32768),
    );
    assert!(matches!(
        result,
        Err(JournalError::Dedup(ozzy_core::dedup::Error::Limits))
    ));
    assert_eq!(controller.jobs(), [] as [(JobId, Stage); 0]);
    assert!(
        !controller
            .image()
            .exists(std::path::Path::new("/local"), false)
    );
}

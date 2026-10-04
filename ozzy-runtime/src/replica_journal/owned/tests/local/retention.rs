use super::*;
use ozzy_journal::operation::PartitionPolicy;
use ozzy_proto::{CheckpointId, ProducerSequence};
use std::num::NonZeroU64;

mod repeated;

fn append(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    producer: u8,
    first: u64,
    records: usize,
) {
    let mut buffer = journal.lease_proposal_buffer().unwrap();
    buffer
        .prepare_append(request(producer, first, records))
        .unwrap();
    let receipt = admit(controller, journal, driver, buffer);
    write(controller, journal, driver, receipt);
    sync_apply(controller, journal, driver);
}

fn roll(controller: &mut Controller, journal: &mut OwnedJournal) {
    let work = journal.begin_roll(8).unwrap();
    let done = drive(controller, work.publish());
    journal.complete_roll(done).unwrap();
}

#[test]
fn retention_reclaims_bounded_orphans_without_advancing_the_record_floor() {
    let (mut controller, io) = setup();
    let config = local_config();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    append(&mut controller, &mut journal, &mut driver, 12, 0, 1);
    let body = OperationBody::PartitionPolicy(PartitionPolicy {
        partition: partition(),
        expected_revision: 1,
        new_revision: 2,
        retention: RetentionPolicy {
            max_age_millis: None,
            max_bytes: NonZeroU64::new(65536),
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
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let selected = controller
        .image()
        .bytes(&config.root.join("segments/1.log"), false)
        .unwrap()
        .to_vec();
    let paths = (999..1018)
        .map(|id| config.root.join(format!("segments/{id}.log")))
        .collect::<Vec<_>>();
    for path in &paths {
        super::super::maintenance::orphan(&mut controller, &io, path.clone());
    }
    let unknown = config.root.join("segments/unknown");
    super::super::maintenance::orphan(&mut controller, &io, unknown.clone());
    let ticket = driver.begin_validation().unwrap();
    for remaining in [11, 3, 0] {
        let turn = drive(
            &mut controller,
            journal.retention_turn(ticket, 999, OperationId::from_bytes([91; 16]), true),
        )
        .unwrap();
        assert!(turn.enabled);
        assert!(turn.proposal.is_none());
        assert!(turn.released.is_none());
        assert_eq!(
            paths
                .iter()
                .filter(|path| controller.image().exists(path, false))
                .count(),
            remaining
        );
        assert_eq!(
            controller
                .image()
                .bytes(&config.root.join("segments/1.log"), false)
                .unwrap(),
            selected
        );
        assert!(controller.image().exists(&unknown, false));
        assert_eq!(
            journal
                .images()
                .unwrap()
                .committed()
                .partition(partition())
                .unwrap()
                .retained_from,
            Offset::ZERO
        );
    }
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one controlled retirement and restart schedule"
)]
fn retention_confirms_retry_floors_before_checkpoint_retirement_and_identity_resume() {
    let (mut controller, io) = setup();
    let config = local_config();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(1), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    append(&mut controller, &mut journal, &mut driver, 12, 0, 2);
    roll(&mut controller, &mut journal);
    append(&mut controller, &mut journal, &mut driver, 12, 2, 1);
    roll(&mut controller, &mut journal);
    let body = OperationBody::PartitionPolicy(PartitionPolicy {
        partition: partition(),
        expected_revision: 1,
        new_revision: 2,
        retention: RetentionPolicy {
            max_age_millis: None,
            max_bytes: NonZeroU64::new(65536),
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
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let ticket = driver.begin_validation().unwrap();
    let (partition, plan) = drive(&mut controller, journal.plan_retention(ticket, 999, 1))
        .unwrap()
        .unwrap();
    assert_eq!(plan.retire, [1]);
    assert_eq!(plan.record_floor, Offset::new(2));
    let buffer = journal
        .retention_proposal(
            partition,
            plan.record_floor,
            OperationId::from_bytes([91; 16]),
        )
        .unwrap()
        .unwrap();
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    assert_eq!(
        journal
            .images()
            .unwrap()
            .committed()
            .partition(partition)
            .unwrap()
            .retained_from,
        Offset::ZERO,
        "accepted trim does not expire confirmed history"
    );
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    let retired = drive(
        &mut controller,
        journal.retire_confirmed_history(
            driver.begin_validation().unwrap(),
            CheckpointId::from_bytes([92; 16]),
            ozzy_journal_segment::AsyncRetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .unwrap();
    assert_eq!(retired.unreferenced_segment_ids, [1]);
    let state = journal
        .images()
        .unwrap()
        .committed()
        .partition(partition)
        .unwrap();
    let writer = state.producer(ProducerId::from_bytes([12; 16])).unwrap();
    assert_eq!(writer.next_producer_sequence, ProducerSequence::new(3));
    assert_eq!(writer.producer_result_floor, ProducerSequence::new(2));
    assert_eq!(writer.result_offset(ProducerSequence::ZERO), None);
    assert_eq!(
        writer.result_offset(ProducerSequence::new(2)),
        Some(Offset::new(2))
    );
    let ticket = driver.begin_validation().unwrap();
    let cleaned = drive(
        &mut controller,
        journal.cleanup_storage(ticket, super::super::StorageCleanup::Segments, 16),
    )
    .unwrap();
    assert!(cleaned.removed_objects > 0);
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(2)),
    )
    .unwrap();
    let cursor = journal
        .open_reader(
            driver.begin_validation().unwrap(),
            partition,
            Some(Offset::new(2)),
        )
        .unwrap();
    assert_eq!(cursor.next_offset(), Offset::new(2));
    assert!(
        journal
            .open_reader(
                driver.begin_validation().unwrap(),
                partition,
                Some(Offset::ZERO)
            )
            .is_err()
    );
    append(&mut controller, &mut journal, &mut driver, 12, 3, 1);
    let state = journal
        .images()
        .unwrap()
        .committed()
        .partition(partition)
        .unwrap();
    assert_eq!(state.next_offset, Offset::new(4));
    assert_eq!(state.retained_from, Offset::new(2));
    drive(&mut controller, journal.shutdown()).unwrap();
}

#[test]
fn retirement_restores_independent_retry_floors_for_interleaved_producers() {
    let (mut controller, io) = setup();
    let config = local_config();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::format_local(config.clone(), io.clone(), JournalGeneration(5), 32768),
    )
    .unwrap();
    initialize(&mut controller, &mut journal, &mut driver);
    append(&mut controller, &mut journal, &mut driver, 12, 0, 2);
    append(&mut controller, &mut journal, &mut driver, 30, 0, 1);
    roll(&mut controller, &mut journal);
    append(&mut controller, &mut journal, &mut driver, 12, 2, 1);
    append(&mut controller, &mut journal, &mut driver, 30, 1, 2);
    let buffer = journal
        .retention_proposal(
            partition(),
            Offset::new(3),
            OperationId::from_bytes([94; 16]),
        )
        .unwrap()
        .unwrap();
    let receipt = admit(&mut controller, &mut journal, &mut driver, buffer);
    write(&mut controller, &mut journal, &mut driver, receipt);
    sync_apply(&mut controller, &mut journal, &mut driver);
    drive(
        &mut controller,
        journal.retire_confirmed_history(
            driver.begin_validation().unwrap(),
            CheckpointId::from_bytes([95; 16]),
            ozzy_journal_segment::AsyncRetirementBudget {
                max_segments: 1,
                max_read_bytes: 32768,
            },
        ),
    )
    .unwrap();
    drive(&mut controller, journal.shutdown()).unwrap();
    let (mut journal, mut driver) = drive(
        &mut controller,
        OwnedJournal::open_local(config, io, JournalGeneration(6)),
    )
    .unwrap();
    let state = journal
        .images()
        .unwrap()
        .committed()
        .partition(partition())
        .unwrap();
    assert_eq!(state.next_offset, Offset::new(6));
    for (producer, floor, sequence, offset) in [(12, 2, 2, 3), (30, 1, 1, 4)] {
        let writer = state
            .producer(ProducerId::from_bytes([producer; 16]))
            .unwrap();
        assert_eq!(writer.next_producer_sequence, ProducerSequence::new(3));
        assert_eq!(writer.producer_result_floor, ProducerSequence::new(floor));
        assert_eq!(
            writer.result_offset(ProducerSequence::new(sequence)),
            Some(Offset::new(offset))
        );
        assert_eq!(writer.result_offset(ProducerSequence::ZERO), None);
    }
    append(&mut controller, &mut journal, &mut driver, 12, 3, 1);
    append(&mut controller, &mut journal, &mut driver, 30, 3, 1);
    assert_eq!(
        journal
            .images()
            .unwrap()
            .committed()
            .partition(partition())
            .unwrap()
            .next_offset,
        Offset::new(8)
    );
    drive(&mut controller, journal.shutdown()).unwrap();
}

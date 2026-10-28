use super::*;
use ozzy_core::state::{CanonicalImagesError, StateError};
use ozzy_journal::operation::{AppendBatch, AppendRecord, Carry, HeldPosition, RetentionPolicy};

const fn at(operation: u64, index: u32) -> HeldPosition {
    HeldPosition { operation, index }
}

const fn key(millis: u64, schedule: u64) -> ReleaseKey {
    ReleaseKey { millis, schedule }
}

/// The last accepted operation's number.
fn last(driver: &Driver) -> u64 {
    driver.begin_validation().unwrap().accepted().op.0
}

/// A `Carry` at `time` copying records `(id, key)` like `request` builds
/// them, from `from` up to `through`.
fn carry(
    time: u64,
    from: HeldPosition,
    through: HeldPosition,
    records: &[(u8, ReleaseKey)],
) -> Vec<u8> {
    let body = OperationBody::Carry(Carry {
        batch: AppendBatch {
            partition: partition(),
            owner_epoch: OwnerEpoch::INITIAL,
            producer_id: ProducerId::from_bytes([0; 16]),
            producer_epoch: ProducerEpoch::new(0),
            first_sequence: ProducerSequence::ZERO,
            first_offset: Offset::ZERO,
            append_timestamp_millis: time,
            records: records
                .iter()
                .map(|(id, _)| AppendRecord {
                    encoding: ozzy_proto::data::Encoding::Raw,
                    message_id: MessageId::from_bytes([*id; 16]),
                    parts: vec![b"opaque".as_slice(), b"\0\xff".as_slice()].into(),
                })
                .collect::<Vec<_>>()
                .into(),
        },
        from,
        through,
        keys: records.iter().map(|(_, key)| *key).collect(),
    });
    ozzy_journal::operation::encode_operation_body(&body, OperationLimits::default()).unwrap()
}

fn carried(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    body: &[u8],
) -> Result<(), JournalError> {
    replicate(controller, journal, driver, OperationKind::Carry, body)
}

fn refused(result: &Result<(), JournalError>) -> bool {
    matches!(
        result,
        Err(JournalError::Images(CanonicalImagesError::State(
            StateError::CarryMismatch
        )))
    )
}

/// The held index's records in release order: key and holding operation.
fn rows(journal: &OwnedJournal) -> Vec<(ReleaseKey, u64)> {
    journal
        .held
        .as_ref()
        .unwrap()
        .prefix()
        .map(|(key, row)| (key, row.operation))
        .collect()
}

/// Records 1 to 3, due out of order, and their keys.
const DUE: [u64; 3] = [NOW + 5_000, NOW + 1_000, NOW + 3_000];
const HELD: [(u8, ReleaseKey); 3] = [
    (1, key(NOW + 5_000, 0)),
    (2, key(NOW + 1_000, 1)),
    (3, key(NOW + 3_000, 2)),
];

fn release_at(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    now: u64,
) {
    let proposal = release(controller, journal, driver, now).unwrap();
    let result = validate(controller, journal, driver, proposal, now);
    accept(controller, journal, driver, result);
}

#[test]
fn carry_frees_the_schedule_segment_and_releases_read_the_carry() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    scheduled(&mut controller, &mut journal, &mut driver, 0, &DUE);
    let schedule = last(&driver);
    roll(&mut controller, &mut journal);
    limit(
        &mut controller,
        &mut journal,
        &mut driver,
        RetentionPolicy {
            max_age_millis: std::num::NonZeroU64::new(60_000),
            max_bytes: None,
        },
    );
    let plan = |controller: &mut Controller, journal: &mut OwnedJournal, driver: &Driver| {
        drive(
            controller,
            journal.plan_retention(driver.begin_validation().unwrap(), NOW + 60_000, 2),
        )
        .unwrap()
        .map(|(_, plan)| plan.retire)
    };
    assert_eq!(
        plan(&mut controller, &mut journal, &driver),
        Some(Vec::new())
    );
    let body = carry(NOW + 10, at(0, 0), at(schedule + 1, 0), &HELD);
    carried(&mut controller, &mut journal, &mut driver, &body).unwrap();
    let carry = last(&driver);
    assert_eq!(held(&journal).0, 3);
    assert_eq!(
        rows(&journal),
        [(HELD[1].1, carry), (HELD[2].1, carry), (HELD[0].1, carry)]
    );
    // Nothing before the floor holds a record now.
    assert_eq!(plan(&mut controller, &mut journal, &driver), Some(vec![1]));
    release_at(&mut controller, &mut journal, &mut driver, NOW + 5_000);
    assert_eq!(held(&journal).0, 0);
    assert_eq!(readable(&mut controller, &mut journal, &driver), [2, 3, 1]);
}

#[test]
fn restart_skips_rows_before_the_carry_floor() {
    let limits = DelayLimits::new(16);
    let (mut controller, mut journal, mut driver) = delayed_with(limits);
    scheduled(&mut controller, &mut journal, &mut driver, 0, &DUE);
    let schedule = last(&driver);
    roll(&mut controller, &mut journal);
    let body = carry(NOW + 10, at(0, 0), at(schedule + 1, 0), &HELD);
    carried(&mut controller, &mut journal, &mut driver, &body).unwrap();
    let carry = last(&driver);
    roll(&mut controller, &mut journal);
    let (mut controller, mut journal, mut driver) = restart(controller, journal, limits);
    // Both sealed indexes list each record; only the carried rows count.
    assert_eq!(
        rows(&journal),
        [(HELD[1].1, carry), (HELD[2].1, carry), (HELD[0].1, carry)]
    );
    release_at(&mut controller, &mut journal, &mut driver, NOW + 5_000);
    assert_eq!(readable(&mut controller, &mut journal, &driver), [2, 3, 1]);
}

#[test]
fn a_copy_checks_each_carry_against_its_held_records() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    scheduled(&mut controller, &mut journal, &mut driver, 0, &DUE);
    let schedule = last(&driver);
    roll(&mut controller, &mut journal);
    let end = at(schedule + 1, 0);
    let [first, second, third] = HELD;
    let wrong = [
        // A missing record, records out of position order, a changed key,
        // and another record.
        vec![first, second],
        vec![first, third, second],
        vec![first, second, (3, key(NOW + 3_001, 2))],
        vec![first, second, (4, third.1)],
    ];
    for records in wrong {
        let body = carry(NOW, at(0, 0), end, &records);
        assert!(refused(&carried(
            &mut controller,
            &mut journal,
            &mut driver,
            &body
        )));
    }
    // A carry starts at the floor.
    let body = carry(NOW, at(1, 0), end, &HELD);
    assert!(refused(&carried(
        &mut controller,
        &mut journal,
        &mut driver,
        &body
    )));
    // A floor inside the schedule splits its records.
    let body = carry(NOW, at(0, 0), at(schedule, 2), &[first, second]);
    carried(&mut controller, &mut journal, &mut driver, &body).unwrap();
    let body = carry(NOW, at(schedule, 2), end, &[first]);
    assert!(refused(&carried(
        &mut controller,
        &mut journal,
        &mut driver,
        &body
    )));
    let body = carry(NOW, at(schedule, 2), end, &[third]);
    carried(&mut controller, &mut journal, &mut driver, &body).unwrap();
    let carry_op = last(&driver);
    // A released record is not carried again.
    release_at(&mut controller, &mut journal, &mut driver, NOW + 1_000);
    let next = at(last(&driver) + 1, 0);
    let body = carry(NOW + 1_000, end, next, &[first, second, third]);
    assert!(refused(&carried(
        &mut controller,
        &mut journal,
        &mut driver,
        &body
    )));
    let body = carry(NOW + 1_000, end, next, &[first, third]);
    carried(&mut controller, &mut journal, &mut driver, &body).unwrap();
    let again = last(&driver);
    assert!(again > carry_op);
    assert_eq!(rows(&journal), [(third.1, again), (first.1, again)]);
    release_at(&mut controller, &mut journal, &mut driver, NOW + 5_000);
    assert_eq!(readable(&mut controller, &mut journal, &driver), [2, 3, 1]);
}

#[test]
fn a_copy_reads_unloaded_sealed_rows_to_check_a_carry() {
    let limits = DelayLimits {
        window: 1,
        ..DelayLimits::new(64)
    };
    let (mut controller, mut journal, mut driver) = delayed_with(limits);
    let mut records = Vec::new();
    for (batch, due) in [[50, 10, 30], [40, 20, 60]].into_iter().enumerate() {
        let first = batch as u64 * 3;
        let due = due.map(|due| NOW + due);
        scheduled(&mut controller, &mut journal, &mut driver, first, &due);
        for (at, due) in (0..).zip(due) {
            records.push(((first + at + 1) as u8, key(due, first + at)));
        }
        roll(&mut controller, &mut journal);
    }
    let end = at(last(&driver) + 1, 0);
    assert!(journal.held_rows() <= 1);
    // The fourth record is not loaded; leaving it out is still caught.
    let mut short = records.clone();
    short.remove(3);
    let body = carry(NOW, at(0, 0), end, &short);
    assert!(refused(&carried(
        &mut controller,
        &mut journal,
        &mut driver,
        &body
    )));
    let body = carry(NOW, at(0, 0), end, &records);
    carried(&mut controller, &mut journal, &mut driver, &body).unwrap();
    let carry = last(&driver);
    assert!(
        rows(&journal)
            .iter()
            .all(|(_, operation)| *operation == carry)
    );
    assert_eq!(rows(&journal).len(), 6);
    release_at(&mut controller, &mut journal, &mut driver, NOW + 60);
    assert_eq!(
        readable(&mut controller, &mut journal, &driver),
        [2, 5, 3, 4, 1, 6]
    );
}

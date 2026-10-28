use super::*;

/// Retention runs here, after the first segment aged out.
const AGED: u64 = NOW + 10_000;

/// Records 1 to 3, due long after retention runs, and their keys.
const LATE: [u64; 3] = [NOW + 50_000, NOW + 20_000, NOW + 40_000];
const LATE_HELD: [(u8, ReleaseKey); 3] = [
    (1, key(NOW + 50_000, 0)),
    (2, key(NOW + 20_000, 1)),
    (3, key(NOW + 40_000, 2)),
];

fn policy(max_age_millis: Option<u64>, max_bytes: Option<u64>) -> RetentionPolicy {
    RetentionPolicy {
        max_age_millis: max_age_millis.and_then(std::num::NonZeroU64::new),
        max_bytes: max_bytes.and_then(std::num::NonZeroU64::new),
    }
}

/// Seal the held records of `LATE` in the first segment, then limit
/// retention by `policy`. The `Schedule` operation's number.
fn sealed(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    policy: RetentionPolicy,
) -> u64 {
    scheduled(controller, journal, driver, 0, &LATE);
    let schedule = last(driver);
    roll(controller, journal);
    limit(controller, journal, driver, policy);
    schedule
}

/// Two records of 3000 bytes from `first`, due at `due`; three of them pass
/// one carry or release. The `Schedule` operation's number.
fn large(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &mut Driver,
    first: u64,
    due: [u64; 2],
) -> u64 {
    static PAYLOAD: [u8; 3000] = [7; 3000];
    let mut request = request(12, first, 2);
    for record in &mut request.records {
        record.parts = vec![PAYLOAD.as_slice()].into();
    }
    let result = produce(controller, journal, driver, request, Some(&due), NOW);
    accept(controller, journal, driver, result);
    last(driver)
}

/// Run retention turns as the leader at `now`, confirming each proposal
/// and finishing each job.
fn retain(controller: &mut Controller, journal: &mut OwnedJournal, driver: &mut Driver, now: u64) {
    for seed in 100..108 {
        let ticket = driver.begin_validation().unwrap();
        let turn = drive(
            controller,
            journal.retention_turn(ticket, now, OperationId::from_bytes([seed; 16]), true),
        )
        .unwrap();
        if let Some(buffer) = turn.proposal {
            let receipt = admit(controller, journal, driver, buffer);
            write(controller, journal, driver, receipt);
            sync_apply(controller, journal, driver);
        }
        run_retention_jobs(controller, journal);
    }
}

fn first_segment(journal: &OwnedJournal) -> u64 {
    journal.journal.readable().unwrap().manifest().segments[0].segment_id
}

/// The validated `Carry`: time, floor range, and records with their keys.
fn carried_by(
    result: &ProposalValidation,
) -> (u64, HeldPosition, HeldPosition, Vec<(u8, ReleaseKey)>) {
    let (kind, body) = body(result);
    assert_eq!(kind, OperationKind::Carry);
    let OperationBody::Carry(carry) =
        decode_operation_body(kind, &body, OperationLimits::default()).unwrap()
    else {
        unreachable!()
    };
    let records = carry
        .batch
        .records
        .iter()
        .map(|record| record.message_id.as_bytes()[0])
        .zip(carry.keys.iter().copied())
        .collect();
    (
        carry.batch.append_timestamp_millis,
        carry.from,
        carry.through,
        records,
    )
}

#[test]
fn the_delay_step_carries_an_aged_segment_so_retention_retires_it() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let schedule = sealed(
        &mut controller,
        &mut journal,
        &mut driver,
        policy(Some(10_000), None),
    );
    assert_eq!(journal.next_release(), Some(NOW + 20_000));
    retain(&mut controller, &mut journal, &mut driver, AGED);
    // Held records keep the aged segment; the delay step starts at once.
    assert_eq!(first_segment(&journal), 1);
    assert_eq!(journal.next_release(), Some(0));
    let proposal = release(&mut controller, &mut journal, &driver, AGED).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, AGED + 1);
    // Records keep their keys, in position order.
    assert_eq!(
        carried_by(&result),
        (AGED + 1, at(0, 0), at(schedule + 1, 0), LATE_HELD.to_vec())
    );
    accept(&mut controller, &mut journal, &mut driver, result);
    assert_eq!(journal.next_release(), Some(NOW + 20_000));
    assert!(release(&mut controller, &mut journal, &driver, AGED).is_none());
    retain(&mut controller, &mut journal, &mut driver, AGED);
    assert!(first_segment(&journal) > 1);
    assert_eq!(held(&journal).0, 3);
    release_at(&mut controller, &mut journal, &mut driver, NOW + 50_000);
    assert_eq!(readable(&mut controller, &mut journal, &driver), [2, 3, 1]);
}

#[test]
fn a_restart_between_carry_and_trim_retires_and_refuses_the_stale_carry() {
    let limits = DelayLimits::new(16);
    let (mut controller, mut journal, mut driver) = delayed_with(limits);
    sealed(
        &mut controller,
        &mut journal,
        &mut driver,
        policy(Some(10_000), None),
    );
    retain(&mut controller, &mut journal, &mut driver, AGED);
    let proposal = release(&mut controller, &mut journal, &driver, AGED).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, AGED);
    let (_, stale) = body(&result);
    accept(&mut controller, &mut journal, &mut driver, result);
    let (mut controller, mut journal, mut driver) = restart(controller, journal, limits);
    assert!(refused(&carried(
        &mut controller,
        &mut journal,
        &mut driver,
        &stale
    )));
    assert_eq!(journal.next_release(), Some(NOW + 20_000));
    retain(&mut controller, &mut journal, &mut driver, AGED);
    assert!(first_segment(&journal) > 1);
    release_at(&mut controller, &mut journal, &mut driver, NOW + 50_000);
    assert_eq!(readable(&mut controller, &mut journal, &driver), [2, 3, 1]);
}

#[test]
fn a_full_carry_ends_where_the_next_held_record_starts() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let schedules: Vec<u64> = (0..3)
        .map(|batch| {
            let due = [NOW + 50_000 + batch, NOW + 20_000 + batch];
            large(&mut controller, &mut journal, &mut driver, batch * 2, due)
        })
        .collect();
    roll(&mut controller, &mut journal);
    limit(
        &mut controller,
        &mut journal,
        &mut driver,
        policy(Some(10_000), None),
    );
    retain(&mut controller, &mut journal, &mut driver, AGED);
    let mut from = at(0, 0);
    for (batch, schedule) in (0_u8..).zip(&schedules) {
        let proposal = release(&mut controller, &mut journal, &driver, AGED).unwrap();
        let result = validate(&mut controller, &mut journal, &driver, proposal, AGED);
        let (_, start, through, records) = carried_by(&result);
        let ids: Vec<u8> = records.iter().map(|(id, _)| *id).collect();
        assert_eq!((start, ids), (from, vec![batch * 2 + 1, batch * 2 + 2]));
        let next = schedules
            .get(usize::from(batch) + 1)
            .map_or(*schedule + 1, |next| *next);
        assert_eq!(through, at(next, 0));
        accept(&mut controller, &mut journal, &mut driver, result);
        roll(&mut controller, &mut journal);
        from = through;
    }
    assert!(release(&mut controller, &mut journal, &driver, AGED).is_none());
    retain(&mut controller, &mut journal, &mut driver, AGED);
    assert!(first_segment(&journal) > 1);
    assert_eq!(held(&journal).0, 6);
}

#[test]
fn releases_and_carries_take_turns() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    let schedules: Vec<u64> = (0..3)
        .map(|batch| {
            let due = [NOW + 6 - batch, NOW + 1 + batch];
            large(&mut controller, &mut journal, &mut driver, batch * 2, due)
        })
        .collect();
    roll(&mut controller, &mut journal);
    limit(
        &mut controller,
        &mut journal,
        &mut driver,
        policy(Some(10_000), None),
    );
    retain(&mut controller, &mut journal, &mut driver, AGED);
    let mut steps = Vec::new();
    while let Some(proposal) = release(&mut controller, &mut journal, &driver, AGED) {
        let result = validate(&mut controller, &mut journal, &driver, proposal, AGED);
        let (kind, _) = body(&result);
        steps.push(match kind {
            OperationKind::Carry => {
                let (_, _, through, records) = carried_by(&result);
                let ids = records.iter().map(|(id, _)| *id).collect();
                (kind, ids, Some(through))
            }
            _ => (
                kind,
                released(&result)
                    .3
                    .iter()
                    .map(|id| id.as_bytes()[0])
                    .collect(),
                None,
            ),
        });
        accept(&mut controller, &mut journal, &mut driver, result);
        // Later steps read the sealed carries.
        roll(&mut controller, &mut journal);
    }
    let end = at(schedules[2] + 1, 0);
    assert_eq!(
        steps,
        [
            (OperationKind::Carry, vec![1, 2], Some(at(schedules[1], 0))),
            (OperationKind::Release, vec![2, 4], None),
            // The second record of the third batch is next.
            (OperationKind::Carry, vec![3, 5], Some(at(schedules[2], 1))),
            (OperationKind::Release, vec![6, 5], None),
            // Every record left in range was released.
            (OperationKind::Carry, vec![], Some(end)),
            (OperationKind::Release, vec![3, 1], None),
        ]
    );
    assert_eq!(held(&journal).0, 0);
    retain(&mut controller, &mut journal, &mut driver, AGED);
    assert!(first_segment(&journal) > 1);
}

#[test]
fn a_byte_target_carries_only_a_segment_its_held_records_fill_at_most_half() {
    let (mut controller, mut journal, mut driver) = delayed(16);
    for batch in 0..3 {
        let due = [NOW + 50_000 + batch, NOW + 20_000 + batch];
        large(&mut controller, &mut journal, &mut driver, batch * 2, due);
    }
    roll(&mut controller, &mut journal);
    limit(
        &mut controller,
        &mut journal,
        &mut driver,
        policy(None, Some(32768)),
    );
    retain(&mut controller, &mut journal, &mut driver, AGED);
    assert_eq!(journal.next_release(), Some(0));
    // 18 KB of held records in a 32 KiB segment: byte retention waits.
    assert!(release(&mut controller, &mut journal, &driver, AGED).is_none());
    assert_eq!(journal.next_release(), Some(NOW + 20_000));
    retain(&mut controller, &mut journal, &mut driver, AGED);
    assert_eq!(first_segment(&journal), 1);

    let (mut controller, mut journal, mut driver) = delayed(16);
    let schedule = sealed(
        &mut controller,
        &mut journal,
        &mut driver,
        policy(None, Some(32768)),
    );
    retain(&mut controller, &mut journal, &mut driver, AGED);
    let proposal = release(&mut controller, &mut journal, &driver, AGED).unwrap();
    let result = validate(&mut controller, &mut journal, &driver, proposal, AGED);
    assert_eq!(carried_by(&result).2, at(schedule + 1, 0));
    accept(&mut controller, &mut journal, &mut driver, result);
    retain(&mut controller, &mut journal, &mut driver, AGED);
    assert!(first_segment(&journal) > 1);
}

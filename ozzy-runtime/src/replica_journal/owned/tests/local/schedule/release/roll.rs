//! A copy whose active segment holds the bound of held rows rolls it at a
//! retention turn when no write rolled it.

use super::*;

fn retention_turn(
    controller: &mut Controller,
    journal: &mut OwnedJournal,
    driver: &Driver,
    seed: u8,
) -> crate::replica_journal::RetentionTurn {
    drive(
        controller,
        journal.retention_turn(
            driver.begin_validation().unwrap(),
            NOW.into(),
            ozzy_proto::OperationId::from_bytes([seed; 16]),
            true,
        ),
    )
    .unwrap()
}

#[test]
fn a_retention_turn_rolls_held_rows_that_no_write_rolled() {
    let limits = DelayLimits {
        window: 4,
        active: 7,
        ..DelayLimits::new(64)
    };
    let (mut controller, mut journal, mut driver) = delayed_with(limits);
    let before = segments(&journal);
    // A write checks the bound before its group, and one group holds all
    // eight rows, so no write rolls. An installed suffix ends the same way.
    scheduled(
        &mut controller,
        &mut journal,
        &mut driver,
        0,
        &[NOW + 30_000; 8],
    );
    assert_eq!(segments(&journal), before);
    assert_eq!(journal.held_memory(), 8);

    let turn = retention_turn(&mut controller, &mut journal, &driver, 95);
    assert!(turn.more_work);
    assert!(turn.proposal.is_none());
    assert_eq!(segments(&journal), before + 1);

    // The active segment holds none of them now, so the next turn rolls
    // nothing.
    retention_turn(&mut controller, &mut journal, &driver, 96);
    assert_eq!(segments(&journal), before + 1);
}

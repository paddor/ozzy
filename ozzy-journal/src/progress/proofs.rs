//! Kani proofs of journal progress transitions.

use super::*;

#[kani::proof]
fn transitions_preserve_prefix_order_or_leave_state_unchanged() {
    let before = JournalSnapshot {
        generation: JournalGeneration(kani::any()),
        accepted: OpNumber(kani::any()),
        written: OpNumber(kani::any()),
        durable: OpNumber(kani::any()),
        faulted: kani::any(),
    };
    // Inductive precondition: every public constructor/transition must
    // establish this ordering. Event fields themselves are unrestricted.
    kani::assume(before.durable <= before.written && before.written <= before.accepted);
    let mut progress = JournalProgress { state: before };
    let generation = JournalGeneration(kani::any());
    let through = OpNumber(kani::any());
    let result = match kani::any::<u8>() {
        0 => progress.admit(kani::any()).map(|_| ()),
        1 => progress.complete_write(WriteTicket {
            generation,
            first: OpNumber(kani::any()),
            through,
        }),
        2 => progress.complete_sync(SyncTicket {
            generation,
            through,
        }),
        3 => progress.complete_durable_write(WriteTicket {
            generation,
            first: OpNumber(kani::any()),
            through,
        }),
        _ => progress.fail(generation),
    };
    let after = progress.snapshot();
    assert!(after.durable <= after.written && after.written <= after.accepted);
    assert!(after.durable >= before.durable);
    assert!(after.written >= before.written);
    assert!(after.accepted >= before.accepted);
    if result.is_err() {
        assert_eq!(after, before);
    }
}

#[kani::proof]
fn admission_is_atomic_and_cannot_wrap() {
    let start: u64 = kani::any();
    let count: u64 = kani::any();
    let mut progress = JournalProgress::recover(JournalGeneration(1), OpNumber(start));
    let before = progress.snapshot();
    match progress.admit(count) {
        Ok(ticket) => {
            assert!(count > 0);
            assert!(ticket.first.0 > start);
            assert_eq!(Some(ticket.through.0), start.checked_add(count));
            assert_eq!(progress.snapshot().written, before.written);
            assert_eq!(progress.snapshot().durable, before.durable);
        }
        Err(_) => {
            assert!(count == 0 || start.checked_add(count).is_none());
            assert_eq!(progress.snapshot(), before);
        }
    }
}

#[kani::proof]
fn sync_cannot_acknowledge_later_writes() {
    let start: u64 = kani::any();
    let first_count = u64::from(kani::any::<u8>()) + 1;
    let second_count = u64::from(kani::any::<u8>()) + 1;
    let mut progress = JournalProgress::recover(JournalGeneration(1), OpNumber(start));
    let Ok(first) = progress.admit(first_count) else {
        return;
    };
    progress.complete_write(first).unwrap();
    let sync = progress.begin_sync().unwrap();
    let Ok(second) = progress.admit(second_count) else {
        return;
    };
    progress.complete_write(second).unwrap();
    progress.complete_sync(sync).unwrap();
    assert_eq!(progress.snapshot().durable, first.through);
    assert!(progress.snapshot().durable < progress.snapshot().written);
}

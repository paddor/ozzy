#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "test code"
)]

use ozzy_core::retention::{Carry, Kept, Plan, Segment};
use ozzy_journal::operation::RetentionPolicy;
use ozzy_proto::Offset;
use std::num::NonZeroU64;

const fn kept(committed_operation: u64, held_operation: Option<u64>) -> Kept {
    Kept {
        committed_operation,
        held_operation,
        trimmed: None,
    }
}

#[test]
fn either_limit_expires_only_a_sealed_prefix_using_newest_append_time() {
    let segments = [
        Segment {
            id: 1,
            capacity: 1024,
            sealed: true,
            last_operation: 4,
            record_end: Offset::new(2),
            newest_append_millis: Some(100),
        },
        Segment {
            id: 2,
            capacity: 1024,
            sealed: true,
            last_operation: 8,
            record_end: Offset::new(4),
            newest_append_millis: Some(900),
        },
        Segment {
            id: 3,
            capacity: 1024,
            sealed: false,
            last_operation: 12,
            record_end: Offset::new(6),
            newest_append_millis: Some(200),
        },
    ];
    let policy = RetentionPolicy {
        max_age_millis: NonZeroU64::new(500),
        max_bytes: None,
    };
    let plan = Plan::select(&segments, policy, Some(1000), kept(12, None), 8).unwrap();
    assert_eq!(plan.retire, vec![1]);
    assert_eq!(plan.record_floor, Offset::new(2));
    assert_eq!(plan.operation_floor, 5);
    assert!(plan.roll_active);
    let policy = RetentionPolicy {
        max_bytes: NonZeroU64::new(1024),
        ..policy
    };
    let plan = Plan::select(&segments, policy, Some(1000), kept(12, None), 8).unwrap();
    assert_eq!(plan.retire, vec![1, 2]);
    assert_eq!(plan.operation_floor, 9);
    assert_eq!(plan.selected_bytes, 1024);
    let plan = Plan::select(&segments, policy, Some(1000), kept(4, None), 8).unwrap();
    assert_eq!(
        plan.retire,
        vec![1],
        "uncommitted segments remain protected"
    );
    assert!(!plan.roll_active);
    let carry = |end, expired| {
        Some(Carry {
            end,
            expired,
            capacity: 1024,
        })
    };
    for (held, retire, target) in [
        (Some(5), vec![1], carry(9, false)),
        (Some(4), vec![], carry(5, true)),
        (Some(9), vec![1, 2], None),
    ] {
        let plan = Plan::select(&segments, policy, Some(1000), kept(12, held), 8).unwrap();
        assert_eq!(
            plan.retire, retire,
            "a held schedule at {held:?} stops retention"
        );
        assert_eq!(plan.carry, target, "the held segment is carried");
        assert!(
            plan.roll_active,
            "a held schedule does not stop the active roll"
        );
    }
    // A held segment no target would retire is not carried.
    let policy = RetentionPolicy {
        max_bytes: NonZeroU64::new(4096),
        ..policy
    };
    let plan = Plan::select(&segments, policy, Some(1000), kept(12, Some(5)), 8).unwrap();
    assert_eq!((plan.retire, plan.carry), (vec![1], None));
    let plan = Plan::select(&segments, policy, Some(1000), kept(4, Some(1)), 8).unwrap();
    assert_eq!(plan.carry, carry(5, true));
    let plan = Plan::select(&segments, policy, Some(1000), kept(3, Some(1)), 8).unwrap();
    assert_eq!(plan.carry, None, "an uncommitted segment is not carried");
}

#[test]
fn without_a_trusted_time_only_the_byte_target_retires() {
    let segment = |id: u64, sealed: bool, last_operation: u64, newest: u64| Segment {
        id,
        capacity: 1024,
        sealed,
        last_operation,
        record_end: Offset::new(id),
        newest_append_millis: Some(newest),
    };
    let segments = [
        segment(1, true, 4, 100),
        segment(2, true, 8, 200),
        segment(3, false, 12, 300),
    ];
    let aged = RetentionPolicy {
        max_age_millis: NonZeroU64::new(500),
        max_bytes: None,
    };
    let trusted = Plan::select(&segments, aged, Some(1000), kept(12, None), 8).unwrap();
    assert_eq!(trusted.retire, vec![1, 2]);
    assert!(trusted.roll_active);
    let paused = Plan::select(&segments, aged, None, kept(12, None), 8).unwrap();
    assert_eq!(paused.retire, Vec::<u64>::new());
    assert!(!paused.roll_active);
    assert_eq!(paused.carry, None);

    let sized = RetentionPolicy {
        max_bytes: NonZeroU64::new(2048),
        ..aged
    };
    let paused = Plan::select(&segments, sized, None, kept(12, None), 8).unwrap();
    assert_eq!(paused.retire, vec![1]);
    assert!(!paused.roll_active);
}

/// Without a target, a sealed committed segment retires once every record
/// in it lies below the floor its owner trimmed to. A segment that reaches
/// past the floor stays, with everything after it.
#[test]
fn an_owner_trim_retires_only_segments_wholly_below_its_floor() {
    let policy = RetentionPolicy {
        max_age_millis: None,
        max_bytes: None,
    };
    let segment = |id, sealed, last_operation, record_end| Segment {
        id,
        capacity: 1024,
        sealed,
        last_operation,
        record_end: Offset::new(record_end),
        newest_append_millis: Some(id),
    };
    let segments = [
        segment(1, true, 10, 4),
        segment(2, true, 20, 8),
        segment(3, true, 30, 12),
        segment(4, false, 40, 16),
    ];
    let plan = |floor: Option<u64>, committed| {
        let kept = Kept {
            trimmed: floor.map(Offset::new),
            ..kept(committed, None)
        };
        Plan::select(&segments, policy, Some(1_000_000), kept, 4).unwrap()
    };
    assert_eq!(plan(None, 40).retire, [] as [u64; 0]);
    assert_eq!(plan(Some(3), 40).retire, [] as [u64; 0]);
    let two = plan(Some(11), 40);
    assert_eq!(two.retire, [1, 2]);
    assert_eq!(
        (two.record_floor, two.operation_floor),
        (Offset::new(8), 21)
    );
    assert_eq!(plan(Some(16), 40).retire, [1, 2, 3]);
    // An uncommitted segment stays whatever the floor.
    assert_eq!(plan(Some(16), 15).retire, [1]);
}

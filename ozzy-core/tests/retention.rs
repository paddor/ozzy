use ozzy_core::retention::{Plan, Segment};
use ozzy_journal::operation::RetentionPolicy;
use ozzy_proto::Offset;
use std::num::NonZeroU64;

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
    let plan = Plan::select(&segments, policy, 1000, 12, 8).unwrap();
    assert_eq!(plan.retire, vec![1]);
    assert_eq!(plan.record_floor, Offset::new(2));
    assert!(plan.roll_active);
    let policy = RetentionPolicy {
        max_bytes: NonZeroU64::new(1024),
        ..policy
    };
    let plan = Plan::select(&segments, policy, 1000, 12, 8).unwrap();
    assert_eq!(plan.retire, vec![1, 2]);
    assert_eq!(plan.selected_bytes, 1024);
    let plan = Plan::select(&segments, policy, 1000, 4, 8).unwrap();
    assert_eq!(
        plan.retire,
        vec![1],
        "uncommitted segments remain protected"
    );
    assert!(!plan.roll_active);
}

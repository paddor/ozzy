use super::*;
use ozzy_core::state::{IdentityKey, StateSnapshotLimits};

fn barrier(id: u8) -> OperationBody<'static> {
    OperationBody::Barrier(Barrier {
        operation_id: operation(id),
    })
}

fn floor_trim(expected: u64, new: u64) -> OperationBody<'static> {
    OperationBody::Trim(Trim {
        partition: partition(),
        expected_floor: Offset::ZERO,
        new_floor: Offset::ZERO,
        expected_operation_floor: expected,
        new_operation_floor: new,
        operation_id: operation(0x70),
    })
}

/// Copies retire history on their own schedules, so one may still hold a
/// claim another forgot. Below the operation floor every copy treats the
/// claim as absent and a retry of it as new.
#[test]
fn a_claim_below_the_operation_floor_counts_as_absent_on_every_copy() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut index = MemoryIdentityIndex::new(32);
    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &barrier(0x71));
    assert_eq!(
        state.prepare(3, &barrier(0x71), &index).unwrap_err(),
        StateError::IdentityConflict
    );
    apply(&mut state, &mut index, 3, &floor_trim(0, 3));
    assert_eq!(state.operation_floor(), 3);
    // This index still holds the claim at operation 2, like a copy that
    // has not retired its segment yet.
    let key = IdentityKey::operation(operation(0x71));
    assert_eq!(index.get(key).unwrap().op_number, 2);
    apply(&mut state, &mut index, 4, &barrier(0x71));
    // The new claim binds the identity again.
    assert_eq!(
        state.prepare(5, &barrier(0x71), &index).unwrap_err(),
        StateError::IdentityConflict
    );
    let limits = StateSnapshotLimits::default();
    let bytes = state.encode_snapshot(limits).unwrap();
    let decoded = CanonicalState::decode_snapshot(&bytes, StateLimits::default(), limits).unwrap();
    assert_eq!(decoded, state);
    assert_eq!(decoded.operation_floor(), 3);
}

#[test]
fn a_trim_raises_the_operation_floor_by_exact_fence_and_never_past_itself() {
    let mut state = CanonicalState::new(StateLimits::default());
    let mut index = MemoryIdentityIndex::new(32);
    apply(&mut state, &mut index, 1, &create());
    apply(&mut state, &mut index, 2, &barrier(0x72));
    for (trim, error) in [
        (floor_trim(1, 3), StateError::TrimFloorMismatch),
        (floor_trim(0, 4), StateError::InvalidTrimFloor),
        // A trim must advance a floor.
        (floor_trim(0, 0), StateError::InvalidTrimFloor),
    ] {
        assert_eq!(state.prepare(3, &trim, &index).unwrap_err(), error);
    }
    apply(&mut state, &mut index, 3, &floor_trim(0, 2));
    assert_eq!(
        state.prepare(4, &floor_trim(2, 1), &index).unwrap_err(),
        StateError::InvalidTrimFloor
    );
    assert_eq!(state.operation_floor(), 2);
}

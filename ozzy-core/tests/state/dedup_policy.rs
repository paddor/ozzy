use super::*;
use ozzy_core::state::{StateSnapshotError, StateSnapshotLimits};

#[test]
fn dedup_policy_is_canonical_and_survives_checkpoint_independently_of_retention() {
    for window in [None, NonZeroU64::new(300_000), NonZeroU64::new(u64::MAX)] {
        let OperationBody::CreatePartition(mut definition) = create() else {
            unreachable!()
        };
        definition.dedup_window_millis = window;
        let mut state = CanonicalState::new(StateLimits::default());
        let mut identities = MemoryIdentityIndex::new(128);
        apply(
            &mut state,
            &mut identities,
            1,
            &OperationBody::CreatePartition(definition),
        );
        assert_eq!(
            state.partition(partition()).unwrap().dedup_window_millis,
            window
        );
        apply(
            &mut state,
            &mut identities,
            2,
            &OperationBody::PartitionPolicy(PartitionPolicy {
                partition: partition(),
                expected_revision: 1,
                new_revision: 2,
                retention: RetentionPolicy {
                    max_age_millis: NonZeroU64::new(1000),
                    max_bytes: None,
                },
                operation_id: operation(0x70),
            }),
        );
        let encoded = state
            .encode_snapshot(StateSnapshotLimits::default())
            .unwrap();
        let restored = CanonicalState::decode_snapshot(
            &encoded,
            StateLimits::default(),
            StateSnapshotLimits::default(),
        )
        .unwrap();
        assert_eq!(restored, state);
        assert_eq!(
            restored.partition(partition()).unwrap().dedup_window_millis,
            window
        );
        let mut old_version = encoded;
        old_version[8..10].copy_from_slice(&2_u16.to_be_bytes());
        assert!(matches!(
            CanonicalState::decode_snapshot(
                &old_version,
                StateLimits::default(),
                StateSnapshotLimits::default(),
            ),
            Err(StateSnapshotError::UnsupportedVersion(2))
        ));
    }
}

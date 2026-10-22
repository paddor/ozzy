//! Group preparation and installation on committed/speculative images.

use super::*;

#[test]
fn canonical_group_preparation_is_atomic_and_revision_bound() {
    let operations = vec![(1, create()), (2, open()), (3, append(0, 0x40))];
    let mut images = CanonicalImages::new(StateLimits::default(), 16, 16);
    let first = images.prepare_group(&operations).unwrap();
    let stale = images.prepare_group(&operations).unwrap();

    assert_eq!(images.committed().revision(), 0);
    assert_eq!(images.speculative().revision(), 0);
    assert_eq!(images.pending_len(), 0);

    images.install_prepared_group(first).unwrap();
    assert_eq!(images.committed().revision(), 0);
    assert_eq!(images.speculative().revision(), 3);
    assert_eq!(images.pending_len(), 3);
    assert_eq!(
        images.install_prepared_group(stale).unwrap_err(),
        CanonicalImagesError::StalePreparedGroup
    );

    images.commit_through(3).unwrap();
    assert_eq!(images.committed(), images.speculative());
    assert_eq!(images.pending_len(), 0);
}

#[test]
fn identity_index_replacement_requires_one_exact_settled_revision() {
    let mut images = CanonicalImages::new(StateLimits::default(), 16, 16);
    images.admit(1, &create()).unwrap();
    let identities = images.speculative_identities().clone();
    let before = images.clone();
    assert_eq!(
        images.replace_settled_identity_index(1, identities.clone()),
        Err(CanonicalImagesError::RecoveredStateMismatch)
    );
    assert_eq!(images, before);
    images.commit_through(1).unwrap();
    let before = images.clone();
    assert_eq!(
        images.replace_settled_identity_index(0, identities.clone()),
        Err(CanonicalImagesError::RecoveredStateMismatch)
    );
    assert_eq!(images, before);
    images
        .replace_settled_identity_index(1, identities)
        .unwrap();
    assert_eq!(images, before);
}

#[test]
fn compact_group_matches_typed_plans_and_rejects_bad_suffix_atomically() {
    use ozzy_journal::operation::{OperationLimits, decode_append_summary, encode_operation_body};
    let mut initial = CanonicalImages::new(StateLimits::default(), 16, 4);
    initial.admit(1, &create()).unwrap();
    initial.admit(2, &open()).unwrap();
    initial.commit_through(2).unwrap();
    for suffix in [append(1, 2), append(3, 2), append(1, 0)] {
        let operations = [(3, append(0, 1)), (4, suffix)];
        let summaries: Vec<_> = operations
            .iter()
            .map(|(number, body)| {
                let bytes = encode_operation_body(body, OperationLimits::default()).unwrap();
                (
                    *number,
                    decode_append_summary(&bytes, OperationLimits::default()).unwrap(),
                )
            })
            .collect();
        let mut compact = initial.clone();
        let mut typed = initial.clone();
        let result = compact.prepare_append_group(&summaries);
        assert_eq!(compact, initial);
        match (result, typed.prepare_group(&operations)) {
            (Ok(plan), Ok(expected)) => {
                let stale = compact.prepare_append_group(&summaries).unwrap();
                compact.install_prepared_group(plan).unwrap();
                typed.install_prepared_group(expected).unwrap();
                assert_eq!(compact, typed);
                assert_eq!(
                    compact.install_prepared_group(stale).unwrap_err(),
                    CanonicalImagesError::StalePreparedGroup
                );
                compact.commit_through(4).unwrap();
                typed.commit_through(4).unwrap();
                assert_eq!(compact, typed);
            }
            (Err(actual), Err(expected)) => assert_eq!(actual, expected),
            _ => panic!("compact and typed admission differ"),
        }
    }
}

#[test]
fn consecutive_singleton_preparation_is_read_only_and_revision_bound() {
    let mut images = CanonicalImages::new(StateLimits::default(), 16, 16);
    let empty = images.prepare_consecutive_group(1, &[]).unwrap();
    images.install_prepared_group(empty).unwrap();
    assert_eq!(images.speculative().revision(), 0);
    let first = images.prepare_consecutive_group(1, &[create()]).unwrap();
    let stale = images.prepare_consecutive_group(1, &[create()]).unwrap();
    assert_eq!(images.speculative().revision(), 0);
    assert_eq!(images.pending_len(), 0);
    images.install_prepared_group(first).unwrap();
    assert_eq!(images.speculative().revision(), 1);
    assert_eq!(images.committed().revision(), 0);
    assert_eq!(
        images.install_prepared_group(stale).unwrap_err(),
        CanonicalImagesError::StalePreparedGroup
    );
    images.commit_through(1).unwrap();
    assert_eq!(images.speculative(), images.committed());
}

#[test]
fn canonical_group_preparation_checks_aggregate_identity_capacity() {
    let operations = vec![
        (1, create()),
        (2, open()),
        (
            3,
            OperationBody::Barrier(Barrier {
                operation_id: operation(0x60),
            }),
        ),
    ];
    let images = CanonicalImages::new(StateLimits::default(), 1, 16);
    assert_eq!(
        images.prepare_group(&operations).unwrap_err(),
        CanonicalImagesError::State(StateError::IdentityIndex(IdentityIndexError::Capacity))
    );
    assert_eq!(images.committed().revision(), 0);
    assert_eq!(images.speculative().revision(), 0);
    assert_eq!(images.pending_len(), 0);
    let bodies = operations
        .into_iter()
        .map(|(_, body)| body)
        .collect::<Vec<_>>();
    assert_eq!(
        images.prepare_consecutive_group(1, &bodies).unwrap_err(),
        CanonicalImagesError::State(StateError::IdentityIndex(IdentityIndexError::Capacity))
    );
    assert_eq!(images.speculative().revision(), 0);
    assert_eq!(images.pending_len(), 0);
}

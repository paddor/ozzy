use super::*;
use ozzy_proto::Offset;

fn outcomes() -> [AppendOutcome; 4] {
    [
        AppendOutcome {
            disposition: AppendDisposition::Stored,
            offset: Offset::new(10),
        },
        AppendOutcome {
            disposition: AppendDisposition::Duplicate,
            offset: Offset::new(2),
        },
        AppendOutcome {
            disposition: AppendDisposition::Duplicate,
            offset: Offset::new(3),
        },
        AppendOutcome {
            disposition: AppendDisposition::Stored,
            offset: Offset::new(11),
        },
    ]
}

#[test]
fn mixed_outcomes_preserve_attempts_offsets_time_and_exact_proofs() {
    let outcomes = outcomes();
    let proofs = [
        RetryFingerprint::from_bytes([1; 32]),
        RetryFingerprint::from_bytes([2; 32]),
    ];
    let limits = OperationLimits::default();
    let results = AppendResults::classified(123, &outcomes, &proofs, limits).unwrap();
    let mut bytes = Vec::new();
    append_results(&mut bytes, results, limits).unwrap();
    assert_eq!(bytes.len(), 16 + 3 * 13 + 2 * 32);
    let decoded = decode_append_results(&bytes, limits).unwrap();
    assert_eq!(
        (decoded.logical_time(), decoded.attempts(), decoded.stored()),
        (123, 4, 2)
    );
    assert_eq!(
        decoded.runs().collect::<Vec<_>>(),
        results.runs().collect::<Vec<_>>()
    );
    assert_eq!(decoded.duplicate_fingerprints().collect::<Vec<_>>(), proofs);
    assert_eq!(
        decoded
            .validate_positions(ProducerSequence::new(100), Offset::new(10), 2)
            .unwrap(),
        (ProducerSequence::new(104), Offset::new(12))
    );
    let attempts: Vec<_> = decoded.attempts_with_fingerprints().collect();
    assert_eq!(
        attempts
            .iter()
            .map(|(outcome, _)| *outcome)
            .collect::<Vec<_>>(),
        outcomes
    );
    assert_eq!(
        attempts.iter().map(|(_, proof)| *proof).collect::<Vec<_>>(),
        [None, Some(proofs[0]), Some(proofs[1]), None]
    );
}

#[test]
fn all_duplicate_attempts_need_no_user_records_but_keep_their_proofs() {
    let outcomes = [AppendOutcome {
        disposition: AppendDisposition::Duplicate,
        offset: Offset::new(7),
    }; 3];
    let proofs = [RetryFingerprint::from_bytes([1; 32]); 3];
    let limits = OperationLimits::default();
    let results = AppendResults::classified(0, &outcomes, &proofs, limits).unwrap();
    let mut bytes = Vec::new();
    append_results(&mut bytes, results, limits).unwrap();
    let decoded = decode_append_results(&bytes, limits).unwrap();
    assert_eq!(decoded.attempts(), 3);
    assert_eq!(decoded.stored(), 0);
    assert_eq!(decoded.runs().count(), 3);
    assert_eq!(decoded.duplicate_fingerprints().count(), 3);
    assert_eq!(
        decoded
            .validate_positions(ProducerSequence::new(5), Offset::new(8), 0)
            .unwrap(),
        (ProducerSequence::new(8), Offset::new(8))
    );
}

#[test]
fn positions_reject_forward_references_gaps_mismatched_records_and_sequence_overflow() {
    let rows = outcomes();
    let proofs = [RetryFingerprint::from_bytes([1; 32]); 2];
    let limits = OperationLimits::default();
    let results = AppendResults::classified(0, &rows, &proofs, limits).unwrap();
    for (sequence, offset, count) in [(100, 9, 2), (100, 10, 3), (u64::MAX - 3, 10, 2)] {
        assert!(
            results
                .validate_positions(ProducerSequence::new(sequence), Offset::new(offset), count)
                .is_err()
        );
    }
    for changed in [
        [
            rows[0],
            AppendOutcome {
                offset: Offset::new(11),
                ..rows[1]
            },
            rows[2],
            rows[3],
        ],
        [
            rows[0],
            rows[1],
            rows[2],
            AppendOutcome {
                offset: Offset::new(12),
                ..rows[3]
            },
        ],
    ] {
        let invalid = AppendResults::classified(0, &changed, &proofs, limits).unwrap();
        assert!(
            invalid
                .validate_positions(ProducerSequence::ZERO, Offset::new(10), 2)
                .is_err()
        );
    }
}

#[test]
fn result_table_matches_independent_bytes_and_rejects_every_truncation() {
    let outcome = [AppendOutcome {
        disposition: AppendDisposition::Stored,
        offset: Offset::new(42),
    }];
    let limits = OperationLimits::default();
    let results = AppendResults::classified(5, &outcome, &[], limits).unwrap();
    let expected = [
        0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 1,
    ];
    let mut bytes = Vec::new();
    append_results(&mut bytes, results, limits).unwrap();
    assert_eq!(bytes, expected);
    for end in 0..bytes.len() {
        assert!(decode_append_results(&bytes[..end], limits).is_err());
    }
    bytes.push(0);
    assert!(decode_append_results(&bytes, limits).is_err());
}

#[test]
fn altered_counts_tags_proof_lengths_and_limits_fail_closed() {
    let rows = outcomes();
    let proofs = [RetryFingerprint::from_bytes([1; 32]); 2];
    let limits = OperationLimits::default();
    assert!(AppendResults::classified(0, &[], &[], limits).is_err());
    assert!(AppendResults::classified(0, &rows, &proofs[..1], limits).is_err());
    let results = AppendResults::classified(0, &rows, &proofs, limits).unwrap();
    let mut bytes = Vec::new();
    append_results(&mut bytes, results, limits).unwrap();
    for (field, value) in [
        (8, 0u32),
        (8, 3),
        (8, 5),
        (12, 0),
        (12, 5),
        (25, 0),
        (25, 5),
    ] {
        let mut bad = bytes.clone();
        bad[field..field + 4].copy_from_slice(&value.to_be_bytes());
        assert!(
            decode_append_results(&bad, limits).is_err(),
            "field {field} value {value}"
        );
    }
    let mut bad = bytes.clone();
    bad[16] = 2;
    assert!(decode_append_results(&bad, limits).is_err());
    let bounded = OperationLimits {
        max_records: 3,
        ..limits
    };
    assert!(decode_append_results(&bytes, bounded).is_err());
    assert!(AppendResults::classified(0, &rows, &proofs, bounded).is_err());
    let mut output = vec![9; 4];
    assert!(append_results(&mut output, results, bounded).is_err());
    assert_eq!(output, [9; 4]);
    let bounded = OperationLimits {
        max_body_bytes: bytes.len() - 1,
        ..limits
    };
    assert!(append_results(&mut output, results, bounded).is_err());
    assert_eq!(output, [9; 4]);
    assert!(decode_append_results(&bytes, bounded).is_err());
}

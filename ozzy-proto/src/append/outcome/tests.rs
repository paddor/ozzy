use super::*;

fn outcome(disposition: AppendDisposition, offset: u64) -> AppendOutcome {
    AppendOutcome {
        disposition,
        offset: Offset::new(offset),
    }
}

#[test]
fn runs_split_on_kind_gaps_repeated_offsets_and_reordered_duplicates() {
    use AppendDisposition::{Duplicate, Stored};
    let outcomes = [
        outcome(Stored, 10),
        outcome(Stored, 11),
        outcome(Duplicate, 3),
        outcome(Duplicate, 4),
        outcome(Duplicate, 4),
        outcome(Duplicate, 2),
        outcome(Stored, 12),
        outcome(Stored, 14),
    ];
    let runs: Vec<_> = OutcomeRuns::new(&outcomes).collect();
    assert_eq!(
        runs.iter().map(|run| run.records).collect::<Vec<_>>(),
        [2, 2, 1, 1, 1, 1]
    );
    let decoded: Vec<_> = runs
        .into_iter()
        .flat_map(|run| {
            let copy = OutcomeRun::decode(&run.encode().unwrap()).unwrap();
            (0..copy.records).map(move |index| copy.outcome(index).unwrap())
        })
        .collect();
    assert_eq!(decoded, outcomes);
}

#[test]
fn outcome_run_has_independent_fixed_bytes() {
    let run = OutcomeRun {
        disposition: AppendDisposition::Duplicate,
        first_offset: Offset::new(42),
        records: 3,
    };
    let bytes = [1, 0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 3];
    assert_eq!(run.encode().unwrap(), bytes);
    assert_eq!(OutcomeRun::decode(&bytes).unwrap(), run);
    assert_eq!(
        run.outcome(2),
        Some(outcome(AppendDisposition::Duplicate, 44))
    );
    assert_eq!(run.outcome(3), None);
}

#[test]
fn invalid_kind_empty_run_and_overflow_fail_closed() {
    let run = OutcomeRun {
        disposition: AppendDisposition::Stored,
        first_offset: Offset::ZERO,
        records: 1,
    };
    let bytes = run.encode().unwrap();
    for tag in 3..=255 {
        let mut bad = bytes;
        bad[0] = tag;
        assert_eq!(OutcomeRun::decode(&bad), Err(CodecError::Profile));
    }
    for bad in [
        OutcomeRun { records: 0, ..run },
        OutcomeRun {
            first_offset: Offset::new(u64::MAX),
            ..run
        },
    ] {
        assert_eq!(bad.encode(), Err(CodecError::Length));
    }
    let mut empty = bytes;
    empty[9..].fill(0);
    assert_eq!(OutcomeRun::decode(&empty), Err(CodecError::Length));
    let mut overflowing = bytes;
    overflowing[1..9].fill(255);
    assert_eq!(OutcomeRun::decode(&overflowing), Err(CodecError::Length));
    assert_eq!(OutcomeRuns::new(&[]).next(), None);
}

#[test]
fn run_encoding_preserves_every_generated_mapping() {
    for seed in 0..256u64 {
        let mut value = seed;
        let outcomes: Vec<_> = (0..128)
            .map(|_| {
                value = value
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1);
                outcome(
                    if value & 8 == 0 {
                        AppendDisposition::Stored
                    } else {
                        AppendDisposition::Duplicate
                    },
                    value % 32,
                )
            })
            .collect();
        let flattened: Vec<_> = OutcomeRuns::new(&outcomes)
            .flat_map(|run| {
                let decoded = OutcomeRun::decode(&run.encode().unwrap()).unwrap();
                (0..decoded.records).map(move |index| decoded.outcome(index).unwrap())
            })
            .collect();
        assert_eq!(flattened, outcomes);
    }
}

//! Bounded deletion-based reduction of replayable event schedules.

/// Reduced sequence and the number of replays used to establish it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reduction<T> {
    /// Remaining schedule known to reproduce the classified failure.
    pub events: Vec<T>,
    /// Replay predicate calls consumed, including the initial check.
    pub attempts: usize,
}

/// Remove chunks while preserving the caller's exact failure classification.
///
/// The predicate must replay from fresh initial state. It must reject different
/// failures and invalid schedules. Exhausting the budget returns the smallest
/// sequence verified so far; it does not claim a globally minimal counterexample.
pub fn minimize<T: Clone>(
    events: &[T],
    max_attempts: usize,
    mut same_failure: impl FnMut(&[T]) -> bool,
) -> Reduction<T> {
    assert!(max_attempts > 0);
    assert!(
        same_failure(events),
        "initial sequence must reproduce the failure"
    );
    let mut result = Reduction {
        events: events.to_vec(),
        attempts: 1,
    };
    let mut width = result.events.len().div_ceil(2).max(1);
    while result.attempts < max_attempts && !result.events.is_empty() {
        let mut start = 0;
        let mut changed = false;
        while start < result.events.len() && result.attempts < max_attempts {
            let end = (start + width).min(result.events.len());
            let candidate: Vec<_> = result.events[..start]
                .iter()
                .chain(&result.events[end..])
                .cloned()
                .collect();
            result.attempts += 1;
            if same_failure(&candidate) {
                result.events = candidate;
                changed = true;
            } else {
                start = end;
            }
        }
        if width == 1 {
            if !changed {
                break;
            }
        } else {
            width = width.div_ceil(2);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::minimize;

    #[test]
    fn preserves_interacting_causes_and_respects_replay_budget() {
        let original: Vec<_> = (0..64).collect();
        let fails = |events: &[i32]| events.contains(&7) && events.contains(&51);
        let reduced = minimize(&original, 128, fails);
        assert_eq!(reduced.events, [7, 51]);
        let bounded = minimize(&original, 3, fails);
        assert_eq!(bounded.attempts, 3);
        assert!(fails(&bounded.events));
    }
}

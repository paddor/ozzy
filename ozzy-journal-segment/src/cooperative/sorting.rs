use super::Budget;
use std::cmp::Ordering;

/// In-place heap sorting keeps scratch constant while yielding after one turn of
/// comparison/swap bytes. Single steps are far smaller than a turn, so they are
/// not counted as budget units. Cancellation leaves a permutation; callers keep
/// the partially ordered image private until sorting and validation both finish.
pub(crate) async fn sort_by<T>(values: &mut [T], compare: fn(&T, &T) -> Ordering) {
    let mut budget = Budget::default();
    let mut pending = 0usize;
    ozzy_journal::work::sort_by(values, compare, &mut async |bytes| {
        pending += bytes;
        if pending >= super::TURN_BYTES {
            budget.charge(std::mem::take(&mut pending)).await;
        }
    })
    .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_io::Scheduling;
    use std::{sync::Arc, task::Poll};

    #[test]
    fn cooperative_sort_preserves_order_and_permutation_across_cancellation() {
        for count in [0, 1, 2, 3, 63, 64, 65, 130, 1024, 1 << 16] {
            for mut input in [
                (0..count).collect::<Vec<_>>(),
                (0..count).rev().collect(),
                (0..count).map(|index| (index * 7919) % 31).collect(),
            ] {
                let mut expected = input.clone();
                expected.sort_unstable();
                let scheduling = Arc::new(Scheduling::default());
                let polls = {
                    let mut sorting = Box::pin(sort_by(&mut input, usize::cmp));
                    let mut polls = 0;
                    while scheduling.poll(sorting.as_mut()).is_pending() {
                        assert!(scheduling.woken());
                        polls += 1;
                        assert!(polls < 10_000);
                    }
                    polls
                };
                assert_eq!(input, expected);
                if count == 1 << 16 {
                    assert!(polls > 0);
                }
            }
        }
        let original: Vec<_> = (0..1 << 16).rev().collect();
        for cut in [1, 2, 10, 50] {
            let mut values = original.clone();
            let scheduling = Arc::new(Scheduling::default());
            let mut sorting = Box::pin(sort_by(&mut values, usize::cmp));
            for _ in 0..cut {
                assert_eq!(scheduling.poll(sorting.as_mut()), Poll::Pending);
            }
            drop(sorting);
            let mut observed = values.clone();
            observed.sort_unstable();
            assert_eq!(observed, (0..1 << 16).collect::<Vec<_>>());
            let mut sorting = Box::pin(sort_by(&mut values, usize::cmp));
            for _ in 0..10_000 {
                if scheduling.poll(sorting.as_mut()).is_ready() {
                    break;
                }
            }
            drop(sorting);
            assert_eq!(values, observed);
        }
    }
}

//! Bounded metadata computation shared by state and storage codecs.
//! Callers supply scheduling; this module performs no I/O or clock reads.

use std::cmp::Ordering;

/// Sort in place with constant scratch space. Each callback follows at most
/// two comparisons and one swap, reporting a conservative byte-work charge.
/// The callback may yield or remain ready. Cancellation leaves a permutation;
/// callers must keep partially sorted metadata private until validation ends.
pub async fn sort_by<T>(
    values: &mut [T],
    compare: fn(&T, &T) -> Ordering,
    step: &mut impl AsyncFnMut(usize),
) {
    for root in (0..values.len() / 2).rev() {
        sift_down(values, root, compare, step).await;
    }
    for end in (1..values.len()).rev() {
        values.swap(0, end);
        step(size_of::<T>().saturating_mul(2)).await;
        sift_down(&mut values[..end], 0, compare, step).await;
    }
}

async fn sift_down<T>(
    values: &mut [T],
    mut root: usize,
    compare: fn(&T, &T) -> Ordering,
    step: &mut impl AsyncFnMut(usize),
) {
    while let Some(left) = root.checked_mul(2).and_then(|value| value.checked_add(1)) {
        if left >= values.len() {
            break;
        }
        let child = if left + 1 < values.len()
            && compare(&values[left], &values[left + 1]) == Ordering::Less
        {
            left + 1
        } else {
            left
        };
        let ordered = compare(&values[root], &values[child]) != Ordering::Less;
        if !ordered {
            values.swap(root, child);
            root = child;
        }
        step(size_of::<T>().saturating_mul(4)).await;
        if ordered {
            break;
        }
    }
}

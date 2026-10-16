//! Fair round-robin turns over ordered maps.

use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Unbounded};

/// First entry after `last`, wrapping to the first entry.
pub(crate) fn after<'a, K: Ord, V>(
    map: &'a BTreeMap<K, V>,
    last: Option<&K>,
) -> Option<(&'a K, &'a V)> {
    last.and_then(|last| map.range((Excluded(last), Unbounded)).next())
        .or_else(|| map.first_key_value())
}

/// Mutable form of [`after`].
pub(crate) fn after_mut<K: Ord + Copy, V>(
    map: &mut BTreeMap<K, V>,
    last: Option<K>,
) -> Option<(K, &mut V)> {
    // The greatest key decides the wrap without a key search, so only the
    // range walk compares keys.
    let lower = match last {
        Some(last)
            if map
                .last_key_value()
                .is_some_and(|(greatest, _)| *greatest > last) =>
        {
            Excluded(last)
        }
        _ => Unbounded,
    };
    map.range_mut((lower, Unbounded))
        .next()
        .map(|(&key, value)| (key, value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn turns_advance_past_the_last_key_and_wrap_after_the_greatest() {
        let mut map = BTreeMap::from([(2, 'b'), (4, 'd'), (6, 'f')]);
        for (last, expected) in [
            (None, 2),
            (Some(1), 2),
            (Some(2), 4),
            (Some(5), 6),
            (Some(6), 2),
            (Some(9), 2),
        ] {
            assert_eq!(
                after(&map, last.as_ref()).map(|(&key, _)| key),
                Some(expected)
            );
            assert_eq!(
                after_mut(&mut map, last).map(|(key, _)| key),
                Some(expected)
            );
        }
        map.clear();
        assert!(after_mut(&mut map, Some(1)).is_none());
    }
}

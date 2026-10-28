//! Held rows of two maps with distinct keys, in release order.

use std::iter::Peekable;

use ozzy_core::state::PendingSchedule;
use ozzy_journal::operation::ReleaseKey;

use super::Rows;

pub(super) fn rows(rows: &Rows) -> impl Iterator<Item = (ReleaseKey, &PendingSchedule)> {
    rows.iter().map(|(key, row)| (*key, row))
}

pub(super) fn merge<'a, L, R>(left: L, right: R) -> Merge<L, R>
where
    L: Iterator<Item = (ReleaseKey, &'a PendingSchedule)>,
    R: Iterator<Item = (ReleaseKey, &'a PendingSchedule)>,
{
    Merge {
        left: left.peekable(),
        right: right.peekable(),
    }
}

/// Two row streams with distinct keys, in key order.
pub(super) struct Merge<L: Iterator, R: Iterator> {
    left: Peekable<L>,
    right: Peekable<R>,
}

impl<'a, L, R> Iterator for Merge<L, R>
where
    L: Iterator<Item = (ReleaseKey, &'a PendingSchedule)>,
    R: Iterator<Item = (ReleaseKey, &'a PendingSchedule)>,
{
    type Item = (ReleaseKey, &'a PendingSchedule);

    fn next(&mut self) -> Option<Self::Item> {
        let take_left = match (self.left.peek(), self.right.peek()) {
            (Some((left, _)), Some((right, _))) => left < right,
            (left, _) => left.is_some(),
        };
        if take_left {
            self.left.next()
        } else {
            self.right.next()
        }
    }
}

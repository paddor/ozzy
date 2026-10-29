//! Where held `Schedule` and `Carry` operations were written, so a release
//! step reads those past the kept bodies without an index snapshot.

use std::collections::{BTreeMap, BTreeSet};

use ozzy_journal::operation::ReleaseKey;
use ozzy_journal_segment::WrittenOperation;

/// Held operations one delay partition keeps the place of. Releases read
/// later ones through an index snapshot.
const HELD_LOCATIONS: usize = 65_536;

/// Places of held operations by number, within `HELD_LOCATIONS`.
#[derive(Debug, Default)]
pub(super) struct Locations {
    /// Release key of the last record of each admitted held operation not
    /// written yet.
    unwritten: BTreeMap<u64, ReleaseKey>,
    /// Written held operations and the release key of their last record.
    written: BTreeMap<u64, (WrittenOperation, ReleaseKey)>,
    /// Numbers of `written` by last release key.
    by_last: BTreeSet<(ReleaseKey, u64)>,
}

impl Locations {
    /// Note an admitted operation whose last record is `last`.
    pub(super) fn admit(&mut self, number: u64, last: ReleaseKey) {
        self.unwritten.insert(number, last);
    }

    /// Keep the places of the admitted operations of one written group.
    /// Groups complete in order, so earlier unwritten ones never will here.
    pub(super) fn written(&mut self, operations: impl Iterator<Item = WrittenOperation>) {
        let mut through = None;
        for operation in operations {
            let number = operation.location.op_number;
            through = through.max(Some(number));
            let Some(last) = self.unwritten.remove(&number) else {
                continue;
            };
            if self.written.len() < HELD_LOCATIONS {
                self.written.insert(number, (operation, last));
                self.by_last.insert((last, number));
            }
        }
        if let Some(through) = through {
            self.unwritten = self.unwritten.split_off(&through.saturating_add(1));
        }
    }

    pub(super) fn get(&self, number: u64) -> Option<WrittenOperation> {
        self.written.get(&number).map(|(operation, _)| *operation)
    }

    /// Drop the operations whose records are all released through `through`.
    pub(super) fn release(&mut self, through: ReleaseKey) {
        while let Some(&(last, number)) = self.by_last.first()
            && last <= through
        {
            self.by_last.pop_first();
            self.written.remove(&number);
        }
        self.unwritten.retain(|_, last| *last > through);
    }

    /// Drop the operations before `operation`, whose records a carry moved.
    pub(super) fn carried(&mut self, operation: u64) {
        let kept = self.written.split_off(&operation);
        for (number, (_, last)) in std::mem::replace(&mut self.written, kept) {
            self.by_last.remove(&(last, number));
        }
        self.unwritten = self.unwritten.split_off(&operation);
    }
}

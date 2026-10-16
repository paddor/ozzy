//! Accepted-only rows are few in steady state but bounded by a much larger
//! worst case. They live in a small table that stays cache resident and move
//! into the full reservation only while they outgrow it. Both are reserved
//! when the index is created.

use super::{Error, HashTable, MessageId, Pending, memory::table};

/// Rows the small table holds before spilling into the full reservation.
pub(super) const NEAR_ROWS: usize = if cfg!(test) { 4 } else { 4096 };

#[derive(Debug)]
pub(super) struct Spill {
    near: HashTable<Pending>,
    far: HashTable<Pending>,
    spilled: bool,
}

impl Spill {
    pub(super) fn new(capacity: usize, blank: Pending) -> Result<Self, Error> {
        let near = capacity.min(NEAR_ROWS);
        Ok(Self {
            near: table(near, blank)?,
            far: if capacity > near {
                table(capacity, blank)?
            } else {
                HashTable::new()
            },
            spilled: false,
        })
    }

    const fn active(&self) -> &HashTable<Pending> {
        if self.spilled { &self.far } else { &self.near }
    }

    const fn active_mut(&mut self) -> &mut HashTable<Pending> {
        if self.spilled {
            &mut self.far
        } else {
            &mut self.near
        }
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.active().len()
    }

    #[cfg(test)]
    pub(super) const fn spilled(&self) -> bool {
        self.spilled
    }

    pub(super) fn is_empty(&self) -> bool {
        self.active().is_empty()
    }

    pub(super) fn allocation_size(&self) -> usize {
        self.near.allocation_size() + self.far.allocation_size()
    }

    pub(super) fn find(&self, hash: u64, id: MessageId) -> Option<&Pending> {
        self.active().find(hash, |row| row.entry.id == id)
    }

    /// Remove the matching row, if any.
    pub(super) fn remove(&mut self, hash: u64, eq: impl FnMut(&Pending) -> bool) {
        if let Ok(entry) = self.active_mut().find_entry(hash, eq) {
            entry.remove();
        }
    }

    /// Insert a row whose ID is absent. Filling the small table moves its
    /// rows into the full reservation first.
    pub(super) fn insert(&mut self, hash: u64, row: Pending, hasher: impl Fn(&Pending) -> u64) {
        if !self.spilled && self.near.len() == NEAR_ROWS {
            for moved in self.near.drain() {
                self.far.insert_unique(hasher(&moved), moved, &hasher);
            }
            self.spilled = true;
        }
        self.active_mut().insert_unique(hash, row, hasher);
    }

    /// Return to the small table once at most a quarter of it would be used.
    /// `rows` holds every stored row, possibly with removed ones. The
    /// hysteresis bounds moves to one per row inserted since the spill.
    pub(super) fn settle<'a>(
        &mut self,
        rows: impl IntoIterator<Item = &'a Pending>,
        hasher: impl Fn(&Pending) -> u64,
    ) {
        if !self.spilled || self.far.len() > NEAR_ROWS / 4 {
            return;
        }
        for row in rows {
            let hash = hasher(row);
            if let Ok(entry) = self.far.find_entry(hash, |stored| {
                stored.entry.id == row.entry.id && stored.entry.offset == row.entry.offset
            }) {
                let (moved, _) = entry.remove();
                self.near.insert_unique(hash, moved, &hasher);
            }
        }
        // Every stored row is in `rows`; anything left would be a bug, so
        // move it too rather than lose an identity.
        debug_assert!(self.far.is_empty());
        if !self.far.is_empty() {
            for moved in self.far.drain() {
                self.near.insert_unique(hasher(&moved), moved, &hasher);
            }
        }
        self.spilled = false;
    }

    pub(super) fn clear(&mut self) {
        self.near.clear();
        if self.spilled {
            self.far.clear();
            self.spilled = false;
        }
    }
}

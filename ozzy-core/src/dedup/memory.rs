use super::{
    Boundary, Entry, Error, HashTable, Index, Limits, MessageId, Offset, Pending, RandomState,
    VecDeque, size_of,
    spill::{NEAR_ROWS, Spill},
};

// Every reservation is committed when the index is created: one fill writes
// each page, so admission never takes first-touch page faults or allocates.
pub(super) fn table<T: Copy>(capacity: usize, blank: T) -> Result<HashTable<T>, Error> {
    let mut table = HashTable::new();
    // Headroom keeps delete/insert churn on hashbrown's in-place rehash path.
    let capacity = capacity.checked_mul(2).ok_or(Error::Limits)?;
    table
        .try_reserve(capacity, |_| 0)
        .map_err(|_| Error::Allocation)?;
    // An odd multiplier maps slots to distinct buckets across every page.
    for slot in 0..table.capacity() as u64 {
        table.insert_unique(slot.wrapping_mul(0x9e37_79b9_7f4a_7c15), blank, |_| 0);
    }
    table.clear();
    Ok(table)
}

fn queue<T: Copy>(capacity: usize, blank: T) -> Result<VecDeque<T>, Error> {
    let mut queue = VecDeque::new();
    queue
        .try_reserve_exact(capacity)
        .map_err(|_| Error::Allocation)?;
    queue.resize(queue.capacity(), blank);
    queue.clear();
    Ok(queue)
}

fn table_bound<T>(capacity: usize) -> Option<usize> {
    // Includes power-of-two rounding, control bytes, alignment, and headroom.
    capacity
        .checked_mul(8)?
        .checked_mul(size_of::<T>() + 1)?
        .checked_add(256)
}

fn upper_bound(limits: Limits) -> Option<usize> {
    // Accepted-only and scratch rows each have a small and a full table.
    let near = table_bound::<Pending>(limits.pending_records.min(NEAR_ROWS))?;
    table_bound::<Entry>(limits.entries)?
        .checked_add(table_bound::<Pending>(limits.pending_records)?)?
        .checked_add(table_bound::<Pending>(limits.pending_records)?)?
        .checked_add(near.checked_mul(2)?)?
        .checked_add(limits.entries.checked_mul(size_of::<Entry>())?)?
        .checked_add(limits.pending_records.checked_mul(size_of::<Pending>())?)?
        .checked_add(
            limits
                .pending_operations
                .checked_mul(size_of::<Boundary>())?,
        )?
        .checked_add(limits.pending_records.checked_mul(size_of::<Pending>())?)
}

impl Index {
    /// Empty the preparation table in proportion to its last use. Clearing
    /// would rewrite every control byte of the preallocated capacity. Each
    /// inserted row is also recorded in `scratch_rows`.
    pub(super) fn reset_scratch(&mut self) {
        for row in self.scratch_rows.drain(..) {
            let hash = self.hash.hash_one(row.entry.id);
            self.scratch
                .remove(hash, |found| found.entry.id == row.entry.id);
        }
        debug_assert!(self.scratch.is_empty());
        self.scratch
            .settle([], |row| self.hash.hash_one(row.entry.id));
    }
}

pub(super) fn resident(index: &Index) -> usize {
    index.committed.allocation_size()
        + index.pending.allocation_size()
        + index.scratch.allocation_size()
        + index.expiry.capacity() * size_of::<Entry>()
        + index.pending_rows.capacity() * size_of::<Pending>()
        + index.boundaries.capacity() * size_of::<Boundary>()
        + index.scratch_rows.capacity() * size_of::<Pending>()
}

pub(super) fn create(limits: Limits, first_offset: Offset, time: u64) -> Result<Index, Error> {
    limits.validate()?;
    let entry = Entry {
        id: MessageId::from_bytes([0; 16]),
        offset: first_offset,
        time,
    };
    let pending = Pending {
        operation: 0,
        entry,
    };
    let boundary = Boundary {
        operation: 0,
        time,
        end: first_offset,
        attempt_end: 0,
    };
    let mut scratch_rows = Vec::new();
    scratch_rows
        .try_reserve_exact(limits.pending_records)
        .map_err(|_| Error::Allocation)?;
    scratch_rows.resize(scratch_rows.capacity(), pending);
    scratch_rows.clear();
    let mut index = Index {
        owner: std::rc::Rc::new(()),
        limits,
        hash: RandomState::new(),
        committed: table(limits.entries, entry)?,
        pending: Spill::new(limits.pending_records, pending)?,
        expiry: queue(limits.entries, entry)?,
        pending_rows: queue(limits.pending_records, pending)?,
        boundaries: queue(limits.pending_operations, boundary)?,
        scratch: Spill::new(limits.pending_records, pending)?,
        scratch_rows,
        accepted_operation: 0,
        committed_operation: 0,
        accepted_time: time,
        committed_time: time,
        accepted_end: first_offset,
        committed_end: first_offset,
        reserved_bytes: 0,
        snapshot: None,
    };
    index.reserved_bytes = resident(&index);
    if index.reserved_bytes > limits.bytes {
        return Err(Error::Limits);
    }
    Ok(index)
}

impl Limits {
    /// Check every bound before formatting storage or allocating an index.
    pub fn validate(self) -> Result<(), Error> {
        let limits = self;
        if limits.window_millis == 0
            || limits.entries == 0
            || limits.pending_records == 0
            || limits.pending_operations == 0
            || limits.batch_records == 0
            || limits.pending_records > limits.entries
            || limits.batch_records > limits.pending_records
            || upper_bound(limits).is_none_or(|bound| bound > limits.bytes)
        {
            return Err(Error::Limits);
        }
        Ok(())
    }
}

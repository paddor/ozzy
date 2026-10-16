//! Partition-local event admission. Outcomes remain unconfirmed until the
//! containing canonical operation commits. No index is cloned with core state.

use std::{collections::VecDeque, mem::size_of, rc::Rc};

use ahash::RandomState;
use hashbrown::HashTable;
use ozzy_proto::{
    MessageId, Offset,
    append::{AppendDisposition, AppendOutcome},
};

mod admission;
mod canonical;
pub use canonical::StoredOperation;
mod memory;
mod restore;
mod rows;
mod snapshot;
mod spill;
pub use restore::RestoringIndex;
pub use rows::{SnapshotRow, SnapshotRows};
pub use snapshot::{SNAPSHOT_HEADER_BYTES, SNAPSHOT_ROW_BYTES, Snapshot, SnapshotHeader};
#[cfg(test)]
mod canonical_tests;
#[cfg(test)]
mod group_tests;
#[cfg(test)]
mod preparation_tests;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod tests;

/// Hard bounds for one partition's retained deduplication state.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Replicated logical time window; duplicates never refresh it.
    pub window_millis: u64,
    /// Maximum retained identities, including accepted-only reservations.
    pub entries: usize,
    /// Maximum accepted-only unique identities.
    pub pending_records: usize,
    /// Maximum accepted-only operations, including expiry operations.
    pub pending_operations: usize,
    /// Maximum attempted records in one admission.
    pub batch_records: usize,
    /// Budget for tables, queues, and reusable classification scratch.
    pub bytes: usize,
}

/// Invalid admission, exhausted resources, or stale completion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Limits are empty, inconsistent, or do not fit the byte budget.
    #[error("invalid deduplication limits")]
    Limits,
    /// A fallible allocation could not reserve bounded storage.
    #[error("deduplication allocation failed")]
    Allocation,
    /// Live identities cannot be evicted to admit new ones.
    #[error("deduplication identity capacity exhausted")]
    Capacity,
    /// Accepted-only record or operation capacity is full.
    #[error("deduplication pending capacity exhausted")]
    PendingCapacity,
    /// An original occurrence is still accepted-only; retry after its commit.
    #[error("deduplication original operation {0} is not confirmed")]
    PendingOriginal(u64),
    /// Empty, oversized, or zero-identity input.
    #[error("invalid deduplication batch")]
    Batch,
    /// Logical time moved backward.
    #[error("deduplication time regressed")]
    TimeRegression,
    /// An operation was reused, regressed, or confirmed before admission.
    #[error("deduplication operation boundary mismatch")]
    Operation,
    /// Admission does not start at the partition's accepted record end.
    #[error("deduplication record offset mismatch")]
    Offset,
    /// A record end cannot be represented.
    #[error("deduplication record offset exhausted")]
    PositionExhausted,
    /// Snapshot bytes, bounds, cursor, or restore image are invalid.
    #[error("invalid deduplication snapshot")]
    Snapshot,
    /// A captured snapshot still holds the confirmed expiry prefix.
    #[error("deduplication snapshot already active")]
    SnapshotBusy,
    /// New identities exhausted the bounded snapshot delta allowance.
    #[error("deduplication snapshot delta capacity exhausted")]
    SnapshotCapacity,
}

/// An identity in the accepted image; pending matches are not confirmations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Match {
    /// An earlier operation has already crossed the confirmation boundary.
    Committed(Offset),
    /// The original operation must commit before a duplicate can succeed.
    Pending {
        /// Original event offset.
        offset: Offset,
        /// Original accepted-only operation.
        operation: u64,
    },
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    id: MessageId,
    offset: Offset,
    time: u64,
}

#[derive(Debug, Clone, Copy)]
struct Pending {
    operation: u64,
    entry: Entry,
}

#[derive(Debug, Clone, Copy)]
struct Boundary {
    operation: u64,
    time: u64,
    end: Offset,
    attempt_end: usize,
}

/// One producer operation in a bounded, atomic owner admission.
#[derive(Debug, Clone, Copy)]
pub struct AttemptOperation<'a> {
    /// Exact proposed canonical operation number.
    pub operation: u64,
    /// Replicated logical time for this operation.
    pub logical_time: u64,
    /// Attempted identities in producer-sequence order; empty for a watermark.
    pub ids: &'a [MessageId],
}

/// One operation's classifications borrowed from a private preparation.
#[derive(Debug, Clone, Copy)]
pub struct OperationAdmission<'a> {
    /// Partition end before this operation's newly stored records.
    pub first_offset: Offset,
    /// Logical time to persist alongside the outcomes.
    pub logical_time: u64,
    /// One stored or duplicate outcome per attempted sequence.
    pub outcomes: &'a [AppendOutcome],
}

/// One accepted operation's outcomes. Persist all attempts, including duplicates.
/// The caller separately charges this metadata until journal completion.
#[derive(Debug)]
pub struct Admission {
    /// Containing canonical operation; not evidence of confirmation.
    pub operation: u64,
    /// Record end after newly stored occurrences only.
    pub next_offset: Offset,
    /// One disposition for each producer sequence, in attempt order.
    pub outcomes: Vec<AppendOutcome>,
}

/// Classification at one exact owner image. Dropping it reserves nothing.
/// The caller charges its rows and outcomes until installation or rejection.
#[derive(Debug)]
pub struct PreparedAdmission {
    owner: Rc<()>,
    accepted_operation: u64,
    committed_operation: u64,
    /// Canonical identities were independently verified; later confirmation of
    /// the same accepted image cannot change their stored outcomes.
    allow_commit_progress: bool,
    operation: u64,
    time: u64,
    next_offset: Offset,
    first_offset: Offset,
    outcomes: Vec<AppendOutcome>,
    entries: Vec<Pending>,
    boundaries: smallvec::SmallVec<[Boundary; 1]>,
    pending_original: Option<u64>,
}

impl PreparedAdmission {
    /// Proposed canonical operation, without confirmation authority.
    pub const fn operation(&self) -> u64 {
        self.operation
    }

    /// Logical acceptance time to persist with the operation.
    pub const fn logical_time(&self) -> u64 {
        self.time
    }

    /// Record end after storing only first occurrences.
    pub const fn next_offset(&self) -> Offset {
        self.next_offset
    }

    /// One outcome per attempted producer sequence.
    pub fn outcomes(&self) -> &[AppendOutcome] {
        &self.outcomes
    }

    /// Exact metadata for one operation within this bounded preparation.
    pub fn operation_results(&self, operation: u64) -> Option<OperationAdmission<'_>> {
        let index = self
            .boundaries
            .iter()
            .position(|row| row.operation == operation)?;
        let previous = index.checked_sub(1).map(|index| self.boundaries[index]);
        let row = self.boundaries[index];
        Some(OperationAdmission {
            first_offset: previous.map_or(self.first_offset, |row| row.end),
            logical_time: row.time,
            outcomes: &self.outcomes[previous.map_or(0, |row| row.attempt_end)..row.attempt_end],
        })
    }

    /// Latest original operation that must commit before this can be admitted.
    /// A new classification is required after that boundary changes.
    pub const fn pending_original(&self) -> Option<u64> {
        self.pending_original
    }

    /// Physical prepared metadata, excluding the separately reserved index.
    pub fn retained_bytes(&self) -> usize {
        self.entries.capacity() * size_of::<Pending>()
            + self.outcomes.capacity() * size_of::<AppendOutcome>()
            + if self.boundaries.spilled() {
                self.boundaries.capacity() * size_of::<Boundary>()
            } else {
                0
            }
            + size_of::<Self>()
    }
}

/// One shard-owned index. Tables and queues are reserved once; admission never
/// grows them or evicts a live identity. Confirmed expiry uses bounded maintenance.
#[derive(Debug)]
pub struct Index {
    owner: Rc<()>,
    limits: Limits,
    hash: RandomState,
    committed: HashTable<Entry>,
    pending: spill::Spill,
    expiry: VecDeque<Entry>,
    pending_rows: VecDeque<Pending>,
    boundaries: VecDeque<Boundary>,
    scratch: spill::Spill,
    scratch_rows: Vec<Pending>,
    accepted_operation: u64,
    committed_operation: u64,
    accepted_time: u64,
    committed_time: u64,
    accepted_end: Offset,
    committed_end: Offset,
    reserved_bytes: usize,
    snapshot: Option<snapshot::ActiveSnapshot>,
}

impl Index {
    /// Start a fresh, empty index at a confirmed record/time boundary.
    pub fn new(limits: Limits, first_offset: Offset, time: u64) -> Result<Self, Error> {
        memory::create(limits, first_offset, time)
    }

    /// Physical table/queue/scratch reservation; excludes returned outcome vectors.
    pub const fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }

    /// Number of retained physical rows, including pending reservations and rows
    /// awaiting bounded expiry maintenance.
    pub fn retained_entries(&self) -> usize {
        self.expiry.len() + self.pending_rows.len()
    }

    /// Confirmed logical time. Wall-clock timestamps never enter this index.
    pub const fn committed_time(&self) -> u64 {
        self.committed_time
    }

    /// Earliest retained confirmed row's logical expiration. Pending originals
    /// cannot expire before their operation reaches the confirmation boundary.
    pub fn next_expiration_time(&self) -> Option<u64> {
        self.expiry
            .front()
            .map(|entry| entry.time.saturating_add(self.limits.window_millis))
    }

    /// Resolve an accepted identity at the current accepted logical time.
    pub fn lookup(&self, id: MessageId) -> Option<Match> {
        self.lookup_at(id, self.accepted_time)
    }

    fn lookup_at(&self, id: MessageId, time: u64) -> Option<Match> {
        let hash = self.hash.hash_one(id);
        if let Some(pending) = self.pending.find(hash, id) {
            return Some(Match::Pending {
                offset: pending.entry.offset,
                operation: pending.operation,
            });
        }
        self.committed
            .find(hash, |row| row.id == id)
            .filter(|row| time.saturating_sub(row.time) < self.limits.window_millis)
            .map(|row| Match::Committed(row.offset))
    }

    /// Move only admitted rows through one verified canonical commit prefix.
    /// Work is bounded by the configured pending record/operation capacities.
    pub fn commit_through(&mut self, operation: u64) -> Result<(), Error> {
        if operation < self.committed_operation || operation > self.accepted_operation {
            return Err(Error::Operation);
        }
        while self
            .pending_rows
            .front()
            .is_some_and(|row| row.operation <= operation)
        {
            let row = self.pending_rows.pop_front().expect("checked pending row");
            let hash = self.hash.hash_one(row.entry.id);
            // A verified canonical group can contain several expired versions
            // of one ID. Keep the latest accepted version until its own commit.
            self.pending.remove(hash, |item| {
                item.entry.id == row.entry.id && item.entry.offset == row.entry.offset
            });
            if let Ok(old) = self
                .committed
                .find_entry(hash, |item| item.id == row.entry.id)
            {
                old.remove();
            }
            self.committed
                .insert_unique(hash, row.entry, |item| self.hash.hash_one(item.id));
            self.expiry.push_back(row.entry);
        }
        self.pending
            .settle(&self.pending_rows, |row| self.hash.hash_one(row.entry.id));
        while self
            .boundaries
            .front()
            .is_some_and(|boundary| boundary.operation <= operation)
        {
            let boundary = self.boundaries.pop_front().expect("checked boundary");
            self.committed_time = boundary.time;
            self.committed_end = boundary.end;
        }
        self.committed_operation = operation;
        debug_assert_eq!(memory::resident(self), self.reserved_bytes);
        Ok(())
    }

    /// Retire only confirmed, expired rows within both caller work budgets.
    /// Admission may remain pressured until subsequent maintenance turns finish.
    pub fn expire(&mut self, maximum_records: usize, maximum_bytes: usize) -> usize {
        if self.snapshot_active().is_some() {
            return 0;
        }
        self.snapshot = None;
        let maximum = maximum_records.min(maximum_bytes / size_of::<Entry>());
        let mut removed = 0;
        while removed < maximum
            && self.expiry.front().is_some_and(|row| {
                self.committed_time.saturating_sub(row.time) >= self.limits.window_millis
            })
        {
            let row = self.expiry.pop_front().expect("checked expiry row");
            if let Ok(entry) = self
                .committed
                .find_entry(self.hash.hash_one(row.id), |entry| {
                    entry.id == row.id && entry.offset == row.offset
                })
            {
                entry.remove();
            }
            removed += 1;
        }
        removed
    }

    /// Discard an unconfirmed suffix without touching the confirmed index.
    /// All cleared work is bounded by the configured pending capacities.
    pub fn discard_unconfirmed(&mut self) {
        // An old prepared proposal cannot regain validity when a different
        // branch reuses the same operation, time, and record coordinates.
        self.owner = Rc::new(());
        self.pending.clear();
        self.pending_rows.clear();
        self.boundaries.clear();
        self.scratch.clear();
        self.scratch_rows.clear();
        self.accepted_operation = self.committed_operation;
        self.accepted_time = self.committed_time;
        self.accepted_end = self.committed_end;
    }
}

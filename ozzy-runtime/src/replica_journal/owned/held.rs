//! Held scheduled records each copy keeps beside canonical state, which
//! holds only the last released key, the carry floor, and held totals. Rows
//! of sealed segments stay in their index files; only the next ones after
//! the last release are loaded. Rows of later operations stay in memory.
//! Rows before the carry floor were released or carried, so loads skip
//! them. Each copy checks a `Release` or `Carry` against these rows before
//! canonical validation.

use std::collections::BTreeMap;

use ozzy_core::state::PendingSchedule;
use ozzy_journal::operation::{
    CanonicalOperation, HeldPosition, OperationBody, OperationKind, OperationLimits, ReleaseKey,
    decode_operation_body,
};
use ozzy_journal_segment::{JournalIndexSnapshot, SegmentReference};
use ozzy_proto::PartitionIncarnation;

use super::JournalError;

mod carry;
mod check;
mod merge;
pub(super) use carry::{Between, boundary};
use merge::{merge, rows};

/// Sealed rows a copy loads past the last release by default. Rows are
/// fixed size, so the count also bounds their memory.
pub(super) const HELD_WINDOW: usize = 65_536;

/// Held rows of the active segment and later operations at which a copy
/// rolls the segment early by default, so they move to its index file.
pub(super) const HELD_ACTIVE: usize = 262_144;

type Rows = BTreeMap<ReleaseKey, PendingSchedule>;

/// A sealed operation range holding scheduled records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sealed {
    first: u64,
    last: u64,
    /// Release key of its last scheduled record, released or not.
    key: ReleaseKey,
}

/// Held records of the journal's delay partition.
#[derive(Debug)]
pub(super) struct Held {
    partition: PartitionIncarnation,
    window: usize,
    /// Release key of the last released record.
    released: Option<ReleaseKey>,
    /// Every held record before this position was released or carried.
    carried: HeldPosition,
    /// First operation of the active segment. Index files hold the rows of
    /// earlier operations.
    active: u64,
    /// Held rows of operations from `active` on.
    recent: Rows,
    /// Last release key of each `Schedule` from `active` on.
    recent_operations: BTreeMap<u64, ReleaseKey>,
    /// Held rows of sealed segments, through `loaded`.
    sealed: Rows,
    /// Every held sealed row through this key is in `sealed`; `None` when
    /// every one is.
    loaded: Option<ReleaseKey>,
    /// Sealed ranges with scheduled records, oldest first.
    segments: Vec<Sealed>,
    /// The segment retention would free by carrying, on the primary.
    target: Option<ozzy_core::retention::Carry>,
    /// The last delay step carried, so the next one releases first.
    carried_last: bool,
}

/// One bounded read of the sealed rows after the loaded ones.
#[derive(Debug)]
pub(super) struct Refill {
    load: Load,
    snapshot: JournalIndexSnapshot,
}

/// Sealed rows read after a key, for the copy that asked.
#[derive(Debug)]
pub(super) struct Load {
    partition: PartitionIncarnation,
    after: Option<ReleaseKey>,
    from: HeldPosition,
    active: u64,
    window: usize,
    rows: Vec<ozzy_journal_segment::ScheduleIndexEntry>,
}

impl Held {
    /// A delay partition with no records yet.
    pub(super) fn new(
        partition: PartitionIncarnation,
        window: usize,
        references: &[SegmentReference],
    ) -> Self {
        Self {
            partition,
            window,
            released: None,
            carried: HeldPosition::default(),
            active: active_first(references),
            recent: Rows::new(),
            recent_operations: BTreeMap::new(),
            sealed: Rows::new(),
            loaded: None,
            segments: Vec::new(),
            target: None,
            carried_last: false,
        }
    }

    /// Rebuild from sealed indexes and the active prefix of `snapshot`.
    pub(super) async fn restore(
        partition: PartitionIncarnation,
        window: usize,
        (released, carried): (Option<ReleaseKey>, HeldPosition),
        snapshot: &JournalIndexSnapshot,
    ) -> Result<Self, JournalError> {
        let mut held = Self::new(partition, window, snapshot.segment_references());
        held.released = released;
        held.carried = carried;
        held.segments = snapshot
            .sealed_schedules(partition)
            .filter(|(source, _)| source.last_op_number >= carried.operation)
            .map(|(source, key)| Sealed {
                first: source.first_op_number,
                last: source.last_op_number,
                key,
            })
            .collect();
        for row in snapshot.active_schedules(partition) {
            if row.record.position() >= carried {
                held.schedule(row.key, row.record);
            }
        }
        let load = Load {
            partition,
            after: released,
            from: carried,
            active: held.active,
            window,
            rows: snapshot
                .sealed_schedules_after(partition, released, carried, window)
                .await?,
        };
        held.loaded = released;
        held.load(load);
        Ok(held)
    }

    pub(super) const fn partition(&self) -> PartitionIncarnation {
        self.partition
    }

    /// Install the `Schedule`, `Release`, and `Carry` operations of one
    /// admitted group.
    pub(super) fn admit<'a>(
        &mut self,
        operations: impl Iterator<Item = CanonicalOperation<'a>>,
        limits: OperationLimits,
    ) -> Result<(), JournalError> {
        for operation in operations {
            match operation.kind {
                OperationKind::Schedule => {
                    let OperationBody::Schedule(schedule) =
                        decode_operation_body(operation.kind, operation.body, limits)?
                    else {
                        return Err(JournalError::AppendMismatch);
                    };
                    if schedule.batch.partition != self.partition {
                        return Err(JournalError::AppendMismatch);
                    }
                    let entries =
                        ozzy_core::state::schedule_entries(operation.op_number, &schedule)
                            .map_err(|_| JournalError::AppendMismatch)?;
                    for (key, record) in entries {
                        self.schedule(key, record);
                    }
                }
                OperationKind::Release => {
                    let OperationBody::Release(release) =
                        decode_operation_body(operation.kind, operation.body, limits)?
                    else {
                        return Err(JournalError::AppendMismatch);
                    };
                    self.release_through(release.through);
                }
                OperationKind::Carry => {
                    let OperationBody::Carry(carry) =
                        decode_operation_body(operation.kind, operation.body, limits)?
                    else {
                        return Err(JournalError::AppendMismatch);
                    };
                    if carry.batch.partition != self.partition {
                        return Err(JournalError::AppendMismatch);
                    }
                    self.carry(operation.op_number, &carry)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn schedule(&mut self, key: ReleaseKey, record: PendingSchedule) {
        let last = self
            .recent_operations
            .entry(record.operation)
            .or_insert(key);
        *last = (*last).max(key);
        if self.released.is_none_or(|released| key > released) {
            self.recent.insert(key, record);
        }
    }

    fn release_through(&mut self, through: ReleaseKey) {
        self.released = Some(through);
        for rows in [&mut self.recent, &mut self.sealed] {
            while rows
                .first_key_value()
                .is_some_and(|(key, _)| *key <= through)
            {
                rows.pop_first();
            }
        }
        // Sealed rows through the release are gone, loaded or not.
        self.loaded = self.loaded.map(|loaded| loaded.max(through));
        self.settle();
    }

    /// Follow segment rolls and retirement. Rows of newly sealed segments
    /// move to `sealed` when they are within the loaded range; the rest stay
    /// in their index files.
    pub(super) fn observe(&mut self, references: &[SegmentReference]) {
        let active = active_first(references);
        if active > self.active {
            for pair in references.windows(2) {
                let first = pair[0].first_chain.next_op_number();
                if first < self.active || first >= active {
                    continue;
                }
                let last = pair[1].first_chain.next_op_number() - 1;
                if let Some(key) = self
                    .recent_operations
                    .range(first..=last)
                    .map(|(_, key)| *key)
                    .max()
                {
                    self.segments.push(Sealed { first, last, key });
                }
            }
            self.recent_operations = self.recent_operations.split_off(&active);
            let loaded = self.loaded;
            let (kept, sealed): (Rows, Rows) = std::mem::take(&mut self.recent)
                .into_iter()
                .partition(|(_, record)| record.operation >= active);
            self.recent = kept;
            self.sealed.extend(
                sealed
                    .into_iter()
                    .filter(|(key, _)| loaded.is_none_or(|loaded| *key <= loaded)),
            );
            self.active = active;
            self.settle();
        }
        let retained = references
            .first()
            .map_or(0, |first| first.first_chain.next_op_number());
        self.segments.retain(|segment| segment.last >= retained);
    }

    /// Whether `active` held rows are in memory and the active segment
    /// holds a `Schedule` before `next`, the first operation not yet
    /// written. Rolling then moves that segment's rows to its index file.
    pub(super) fn crowded(&self, next: u64, active: usize) -> bool {
        self.recent.len() >= active && self.recent_operations.range(..next).next().is_some()
    }

    /// Bound `sealed` by the window, and mark every sealed row loaded once
    /// no segment has one past `loaded`.
    fn settle(&mut self) {
        self.settle_within(self.window);
    }

    fn settle_within(&mut self, window: usize) {
        let mut trimmed = false;
        while self.sealed.len() > window {
            self.sealed.pop_last();
            trimmed = true;
        }
        if trimmed {
            self.loaded = self.sealed.last_key_value().map(|(key, _)| *key);
        }
        if let Some(loaded) = self.loaded
            && self.segments.iter().all(|segment| segment.key <= loaded)
        {
            self.loaded = None;
        }
    }

    /// Held records in release order whose place is certain: every held
    /// record before them is loaded.
    pub(super) fn prefix(&self) -> impl Iterator<Item = (ReleaseKey, &PendingSchedule)> {
        let loaded = self.loaded;
        merge(rows(&self.recent), rows(&self.sealed))
            .take_while(move |(key, _)| loaded.is_none_or(|loaded| *key <= loaded))
    }

    /// Release key millis to wake at: the first held record's, or, when no
    /// loaded record is left but sealed ones are, the loaded bound's, so a
    /// step loads more.
    pub(super) fn next(&self, committed: u64) -> Option<u64> {
        match self.prefix().next() {
            Some((key, record)) => (record.operation <= committed).then_some(key.millis),
            None => self.loaded.map(|loaded| loaded.millis),
        }
    }

    /// Whether held sealed records exist but none is loaded.
    pub(super) fn needs_refill(&self) -> bool {
        self.loaded.is_some() && self.prefix().next().is_none()
    }

    /// Read the next sealed rows from `snapshot`.
    pub(super) fn refill(&self, snapshot: JournalIndexSnapshot) -> Refill {
        Refill {
            load: Load {
                partition: self.partition,
                after: self.loaded,
                from: self.carried,
                active: self.active,
                window: self.window,
                rows: Vec::new(),
            },
            snapshot,
        }
    }

    /// Install rows read after the loaded bound. Rows read before a segment
    /// rolled may miss its rows, so they are dropped.
    pub(super) fn load(&mut self, load: Load) {
        self.load_within(load, self.window);
    }

    /// Install rows a release needs checked. Another copy's segments may
    /// hold in memory rows that are sealed here, so one release can pass
    /// more than the window; its rows stay loaded until it applies.
    fn extend(&mut self, load: Load) {
        self.load_within(load, usize::MAX);
    }

    fn load_within(&mut self, load: Load, window: usize) {
        if load.active != self.active || load.partition != self.partition {
            return;
        }
        let full = load.rows.len() == load.window;
        let last = load.rows.last().map(|row| row.key);
        let (released, carried) = (self.released, self.carried);
        // Rows read before a release or carry applied may be gone now.
        self.sealed.extend(
            load.rows
                .into_iter()
                .filter(|row| {
                    released.is_none_or(|released| row.key > released)
                        && row.record.position() >= carried
                })
                .map(|row| (row.key, row.record)),
        );
        self.loaded = if full {
            last.max(self.loaded).max(load.after)
        } else {
            None
        };
        self.settle_within(window);
    }

    /// First operation retention keeps for held records, given the last
    /// committed release and carry floor. A segment wholly before the floor
    /// holds none; one it splits stays from the floor on.
    pub(super) fn held_operation(
        &self,
        released: Option<ReleaseKey>,
        carried: HeldPosition,
    ) -> Option<u64> {
        let held = |last: u64, key: &ReleaseKey| {
            last >= carried.operation && released.is_none_or(|released| *key > released)
        };
        self.segments
            .iter()
            .find(|segment| held(segment.last, &segment.key))
            .map(|segment| segment.first.max(carried.operation))
            .or_else(|| {
                self.recent_operations
                    .iter()
                    .find(|(operation, key)| held(**operation, key))
                    .map(|(operation, _)| *operation)
            })
    }

    #[cfg(test)]
    pub(super) fn loaded_rows(&self) -> usize {
        self.sealed.len()
    }

    /// Whether the index agrees with canonical held totals. Only a fully
    /// loaded index can count them.
    #[cfg(debug_assertions)]
    pub(super) fn matches(&self, delay: &ozzy_core::state::DelayState) -> bool {
        let (records, bytes) = self
            .prefix()
            .fold((0_u64, 0_u64), |(records, bytes), (_, row)| {
                (records + 1, bytes + u64::from(row.bytes))
            });
        self.released == delay.released()
            && self.carried == delay.carried()
            && (self.loaded.is_some()
                || (records, bytes) == (delay.held_records(), delay.held_bytes()))
    }
}

impl Refill {
    pub(super) async fn read(mut self) -> Result<Load, JournalError> {
        let load = &mut self.load;
        load.rows = self
            .snapshot
            .sealed_schedules_after(load.partition, load.after, load.from, load.window)
            .await?;
        Ok(self.load)
    }
}

fn active_first(references: &[SegmentReference]) -> u64 {
    references
        .last()
        .map_or(1, |active| active.first_chain.next_op_number())
}

impl super::OwnedJournal {
    fn held_window(&self) -> usize {
        self.delay.map_or(HELD_WINDOW, |limits| limits.window)
    }

    /// Whether to roll before writing operations from `next` on, to bound
    /// the held rows in memory.
    pub(super) fn held_crowded(&mut self, next: u64) -> Result<bool, JournalError> {
        self.observe_held()?;
        let active = self.delay.map_or(HELD_ACTIVE, |limits| limits.active);
        Ok(matches!(&self.held, Some(held) if held.crowded(next, active)))
    }

    /// Rebuild the held index from indexes and the active segment before
    /// admission. Accepted operations are all written here.
    pub(in crate::replica_journal) async fn restore_held(&mut self) -> Result<(), JournalError> {
        let window = self.held_window();
        // Activation holds the owner faulted until it completes.
        let Some((partition, floors)) = self
            .images
            .as_ref()
            .ok_or(JournalError::InstallationMismatch)?
            .speculative()
            .partitions()
            .find_map(|(partition, state)| {
                let delay = state.delay.as_ref()?;
                Some((partition, (delay.released(), delay.carried())))
            })
        else {
            self.held = None;
            return Ok(());
        };
        let snapshot = self
            .journal
            .ready_mut()?
            .build_index_snapshot(
                ozzy_journal_segment::JournalIndexBoundary::Accepted,
                self.recovery.index,
            )
            .await?;
        let held = Held::restore(partition, window, floors, &snapshot).await?;
        self.held = Some(held);
        self.check_held()
    }

    /// Install an admitted group's schedules and releases. A delay
    /// partition it creates starts with no held records.
    pub(super) fn admit_held(
        &mut self,
        append: &crate::replica_journal::append::RetainedAppend,
    ) -> Result<(), JournalError> {
        if self.held.is_none() {
            let window = self.held_window();
            // Admission holds the owner faulted until it completes.
            let Some(partition) = self
                .images
                .as_ref()
                .ok_or(JournalError::AppendMismatch)?
                .speculative()
                .partitions()
                .find_map(|(partition, state)| state.delay.is_some().then_some(partition))
            else {
                return Ok(());
            };
            let references = &self.journal.readable()?.manifest().segments;
            self.held = Some(Held::new(partition, window, references));
        }
        let references = &self.journal.readable()?.manifest().segments;
        let held = self.held.as_mut().ok_or(JournalError::AppendMismatch)?;
        held.admit(append.operations(), self.limits.operations)?;
        held.observe(references);
        self.check_held()
    }

    /// Sealed held records loaded now.
    #[cfg(test)]
    pub(in crate::replica_journal) fn held_rows(&self) -> usize {
        self.held.as_ref().map_or(0, Held::loaded_rows)
    }

    /// Held rows in memory, loaded or of the active segment on.
    #[cfg(test)]
    pub(in crate::replica_journal) fn held_memory(&self) -> usize {
        let Some(held) = &self.held else { return 0 };
        held.recent.len() + held.sealed.len()
    }

    /// First operation of the oldest segment holding a record the last
    /// committed release and carry left, for retention.
    pub(super) fn held_operation(&self) -> Result<Option<u64>, JournalError> {
        let images = self.images()?;
        let Some((_, delay)) = images
            .committed()
            .partitions()
            .find_map(|(partition, state)| Some((partition, state.delay.as_ref()?)))
        else {
            return Ok(None);
        };
        Ok(self
            .held
            .as_ref()
            .ok_or(JournalError::AppendMismatch)?
            .held_operation(delay.released(), delay.carried()))
    }

    /// Follow segment rolls and retirement since the last admission.
    pub(super) fn observe_held(&mut self) -> Result<(), JournalError> {
        if let Some(held) = self.held.as_mut() {
            held.observe(&self.journal.readable()?.manifest().segments);
        }
        Ok(())
    }

    /// Debug builds compare the held index with the canonical cursor and
    /// held totals.
    pub(super) fn check_held(&self) -> Result<(), JournalError> {
        #[cfg(debug_assertions)]
        if let Some(held) = &self.held {
            let delay = self
                .images
                .as_ref()
                .ok_or(JournalError::AppendMismatch)?
                .speculative()
                .partition(held.partition())
                .and_then(|state| state.delay.as_ref())
                .ok_or(JournalError::AppendMismatch)?;
            assert!(held.matches(delay), "held index diverged: {held:?}");
        }
        Ok(())
    }
}

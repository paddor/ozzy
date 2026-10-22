use super::{
    Admission, AppendDisposition, AppendOutcome, AttemptOperation, Boundary, Entry, Error, Index,
    Match, MessageId, Offset, Pending, PreparedAdmission, memory, try_vec,
};

impl Index {
    /// Atomically reserve first occurrences and return one outcome per attempt.
    /// The caller must persist these outcomes and confirm the containing operation
    /// before replying successfully. Matches of another unconfirmed operation
    /// defer admission until that original commits and classification runs again.
    pub fn admit(
        &mut self,
        operation: u64,
        time: u64,
        first_offset: Offset,
        ids: &[MessageId],
    ) -> Result<Admission, Error> {
        if ids.is_empty() {
            return Err(Error::Batch);
        }
        let prepared = self.prepare(operation, time, first_offset, ids)?;
        self.install(prepared)
    }

    /// Propose a replicated idle-time watermark without admitting user records.
    /// Confirmation and bounded expiry can release unique-event capacity.
    pub fn advance_time(&mut self, operation: u64, time: u64) -> Result<(), Error> {
        let prepared = self.prepare_inner(operation, time, self.accepted_end, &[])?;
        self.install(prepared)?;
        Ok(())
    }

    /// Classify at one exact image without reserving identities, advancing time,
    /// or changing accepted offsets. Installation belongs to actual admission.
    pub fn prepare(
        &mut self,
        operation: u64,
        time: u64,
        first_offset: Offset,
        ids: &[MessageId],
    ) -> Result<PreparedAdmission, Error> {
        if ids.is_empty() {
            return Err(Error::Batch);
        }
        self.prepare_inner(operation, time, first_offset, ids)
    }

    fn prepare_inner(
        &mut self,
        operation: u64,
        time: u64,
        first_offset: Offset,
        ids: &[MessageId],
    ) -> Result<PreparedAdmission, Error> {
        self.prepare_group(
            &[AttemptOperation {
                operation,
                logical_time: time,
                ids,
            }],
            first_offset,
        )
    }

    /// Classify a bounded canonical group without admitting any of its records.
    /// Earlier new occurrences within this same atomic group win. External
    /// accepted-only originals still require confirmation and reclassification.
    pub fn prepare_group(
        &mut self,
        operations: &[AttemptOperation<'_>],
        first_offset: Offset,
    ) -> Result<PreparedAdmission, Error> {
        if operations.is_empty() {
            return Err(Error::Operation);
        }
        if first_offset != self.accepted_end {
            return Err(Error::Offset);
        }
        let attempts = operations.iter().try_fold(0_usize, |count, operation| {
            if operation.ids.len() > self.limits.batch_records {
                return Err(Error::Batch);
            }
            count.checked_add(operation.ids.len()).ok_or(Error::Batch)
        })?;
        let mut boundaries = self.begin_preparation(operations.len())?;
        let mut outcomes = try_vec(attempts)?;
        let mut next = first_offset;
        let mut pending_original = None;
        let mut operation = self.accepted_operation;
        let mut time = self.accepted_time;
        for incoming in operations {
            if incoming.operation <= operation {
                return Err(Error::Operation);
            }
            if incoming.logical_time < time {
                return Err(Error::TimeRegression);
            }
            operation = incoming.operation;
            time = incoming.logical_time;
            next = self.classify(*incoming, next, &mut outcomes, &mut pending_original)?;
            boundaries.push(Boundary {
                operation,
                time,
                end: next,
                attempt_end: outcomes.len(),
            });
        }
        let entries = self.take_scratch_rows()?;
        Ok(PreparedAdmission {
            owner: self.owner.clone(),
            accepted_operation: self.accepted_operation,
            committed_operation: self.committed_operation,
            allow_commit_progress: false,
            operation,
            time,
            next_offset: next,
            first_offset,
            outcomes,
            entries,
            boundaries,
            pending_original,
        })
    }

    fn classify(
        &mut self,
        incoming: AttemptOperation<'_>,
        mut next: Offset,
        outcomes: &mut Vec<AppendOutcome>,
        pending_original: &mut Option<u64>,
    ) -> Result<Offset, Error> {
        for &id in incoming.ids {
            if *id.as_bytes() == [0; 16] {
                return Err(Error::Batch);
            }
            let hash = self.hash.hash_one(id);
            let previous = match self.scratch.find(hash, id) {
                Some(row) => Some(row.entry.offset),
                None => match self.lookup_at(id, incoming.logical_time) {
                    Some(Match::Committed(offset)) => Some(offset),
                    Some(Match::Pending { offset, operation }) => {
                        *pending_original = Some(pending_original.unwrap_or(0).max(operation));
                        Some(offset)
                    }
                    None => None,
                },
            };
            let outcome = if let Some(offset) = previous {
                AppendOutcome {
                    disposition: AppendDisposition::Duplicate,
                    offset,
                }
            } else {
                self.check_capacity(self.scratch_rows.len() + 1)?;
                let row = Entry {
                    id,
                    offset: next,
                    time: incoming.logical_time,
                };
                next = next
                    .get()
                    .checked_add(1)
                    .map(Offset::new)
                    .ok_or(Error::PositionExhausted)?;
                let pending = Pending {
                    operation: incoming.operation,
                    entry: row,
                };
                self.scratch
                    .insert(hash, pending, |row| self.hash.hash_one(row.entry.id));
                self.scratch_rows.push(pending);
                AppendOutcome {
                    disposition: AppendDisposition::Stored,
                    offset: row.offset,
                }
            };
            outcomes.push(outcome);
        }
        Ok(next)
    }

    /// Start preparing `operations` canonical operations: they must fit the
    /// pending boundaries, and the scratch table starts empty.
    pub(super) fn begin_preparation(
        &mut self,
        operations: usize,
    ) -> Result<smallvec::SmallVec<[Boundary; 1]>, Error> {
        if operations > self.limits.pending_operations - self.boundaries.len() {
            return Err(Error::PendingCapacity);
        }
        self.reset_scratch();
        let mut boundaries = smallvec::SmallVec::new();
        boundaries
            .try_reserve_exact(operations)
            .map_err(|_| Error::Allocation)?;
        Ok(boundaries)
    }

    /// Copy the prepared scratch rows once the image can hold them.
    pub(super) fn take_scratch_rows(&mut self) -> Result<Vec<Pending>, Error> {
        self.check_capacity(self.scratch_rows.len())?;
        let mut entries = try_vec(self.scratch_rows.len())?;
        entries.extend_from_slice(&self.scratch_rows);
        Ok(entries)
    }

    pub(super) fn check_capacity(&self, unique: usize) -> Result<(), Error> {
        if unique > self.limits.entries - self.retained_entries() {
            return Err(Error::Capacity);
        }
        if unique > self.limits.pending_records - self.pending_rows.len() {
            return Err(Error::PendingCapacity);
        }
        if self.snapshot_active().is_some_and(|snapshot| {
            unique
                > self.limits.pending_records
                    - (self.expiry.len() - snapshot.baseline + self.pending_rows.len())
        }) {
            return Err(Error::SnapshotCapacity);
        }
        Ok(())
    }

    /// Install against the same accepted owner image. Producer classification
    /// also fences confirmation; verified canonical rows may survive its advance.
    /// An unconfirmed match must be classified again after its original commits.
    pub fn install(&mut self, prepared: PreparedAdmission) -> Result<Admission, Error> {
        if !std::rc::Rc::ptr_eq(&self.owner, &prepared.owner)
            || self.accepted_operation != prepared.accepted_operation
            || (self.committed_operation != prepared.committed_operation
                && !prepared.allow_commit_progress)
        {
            return Err(Error::Operation);
        }
        if let Some(operation) = prepared.pending_original {
            return Err(Error::PendingOriginal(operation));
        }
        if prepared.boundaries.len() > self.limits.pending_operations - self.boundaries.len() {
            return Err(Error::PendingCapacity);
        }
        self.check_capacity(prepared.entries.len())?;
        let PreparedAdmission {
            operation,
            time,
            next_offset,
            outcomes,
            entries,
            boundaries,
            ..
        } = prepared;
        for row in entries {
            let hash = self.hash.hash_one(row.entry.id);
            self.pending
                .remove(hash, |old| old.entry.id == row.entry.id);
            self.pending
                .insert(hash, row, |row| self.hash.hash_one(row.entry.id));
            self.pending_rows.push_back(row);
        }
        self.boundaries.extend(boundaries);
        self.accepted_operation = operation;
        self.accepted_time = time;
        self.accepted_end = next_offset;
        debug_assert_eq!(memory::resident(self), self.reserved_bytes);
        Ok(Admission {
            operation,
            next_offset,
            outcomes,
        })
    }
}

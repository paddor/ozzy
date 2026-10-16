use super::{
    AppendDisposition, AppendOutcome, Boundary, Entry, Error, Index, MessageId, Offset, Pending,
    PreparedAdmission,
};

/// Stored identities declared by an independently verified canonical operation.
/// Discarded attempts are absent and never refresh the original event's time.
#[derive(Debug, Clone, Copy)]
pub struct StoredOperation<'a> {
    /// Exact canonical operation number; controls supply an empty identity list.
    pub operation: u64,
    /// Replicated logical log time, never a client or wall-clock timestamp.
    pub logical_time: u64,
    /// Partition record end before this operation's newly stored records.
    pub first_offset: Offset,
    /// Newly stored records only, in assigned offset order.
    pub ids: &'a [MessageId],
}

impl Index {
    /// Prepare one bounded canonical group without changing the accepted image.
    /// The enclosing log must independently prove its authority and contents.
    /// `confirmed` is its verified confirmation prefix, including any original
    /// operation whose expired ID is reused before local application catches up.
    /// Partial failure or cancellation reserves no identities or boundaries.
    /// Confirmation may advance while the accepted image stays unchanged.
    pub fn prepare_stored(
        &mut self,
        operations: &[StoredOperation<'_>],
        confirmed: u64,
    ) -> Result<PreparedAdmission, Error> {
        if confirmed < self.committed_operation || operations.is_empty() {
            return Err(Error::Operation);
        }
        if operations.len() > self.limits.pending_operations - self.boundaries.len() {
            return Err(Error::PendingCapacity);
        }
        self.reset_scratch();
        let mut boundaries = smallvec::SmallVec::new();
        boundaries
            .try_reserve_exact(operations.len())
            .map_err(|_| Error::Allocation)?;
        let mut last = Boundary {
            operation: self.accepted_operation,
            time: self.accepted_time,
            end: self.accepted_end,
            attempt_end: 0,
        };
        for incoming in operations {
            self.prepare_stored_operation(*incoming, confirmed, last)?;
            let end = incoming
                .first_offset
                .get()
                .checked_add(incoming.ids.len() as u64)
                .map(Offset::new)
                .ok_or(Error::PositionExhausted)?;
            last = Boundary {
                operation: incoming.operation,
                time: incoming.logical_time,
                end,
                attempt_end: self.scratch_rows.len(),
            };
            boundaries.push(last);
        }
        self.check_capacity(self.scratch_rows.len())?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(self.scratch_rows.len())
            .map_err(|_| Error::Allocation)?;
        entries.extend_from_slice(&self.scratch_rows);
        let mut outcomes = Vec::new();
        outcomes
            .try_reserve_exact(entries.len())
            .map_err(|_| Error::Allocation)?;
        outcomes.extend(entries.iter().map(|row| AppendOutcome {
            disposition: AppendDisposition::Stored,
            offset: row.entry.offset,
        }));
        Ok(PreparedAdmission {
            owner: self.owner.clone(),
            accepted_operation: self.accepted_operation,
            committed_operation: self.committed_operation,
            allow_commit_progress: true,
            operation: last.operation,
            time: last.time,
            next_offset: last.end,
            first_offset: self.accepted_end,
            entries,
            boundaries,
            outcomes,
            pending_original: None,
        })
    }

    fn prepare_stored_operation(
        &mut self,
        incoming: StoredOperation<'_>,
        confirmed: u64,
        before: Boundary,
    ) -> Result<(), Error> {
        if incoming.operation <= before.operation {
            return Err(Error::Operation);
        }
        if incoming.logical_time < before.time {
            return Err(Error::TimeRegression);
        }
        if incoming.first_offset != before.end {
            return Err(Error::Offset);
        }
        if incoming.ids.len() > self.limits.batch_records {
            return Err(Error::Batch);
        }
        let available = self.limits.pending_records - self.pending_rows.len();
        if incoming.ids.len() > available - self.scratch_rows.len() {
            return Err(Error::PendingCapacity);
        }
        incoming
            .first_offset
            .get()
            .checked_add(incoming.ids.len() as u64)
            .ok_or(Error::PositionExhausted)?;
        for (delta, &id) in incoming.ids.iter().enumerate() {
            if *id.as_bytes() == [0; 16] {
                return Err(Error::Batch);
            }
            let hash = self.hash.hash_one(id);
            let pending = self
                .scratch
                .find(hash, id)
                .or_else(|| self.pending.find(hash, id));
            let previous = pending
                .map(|row| &row.entry)
                .or_else(|| self.committed.find(hash, |row| row.id == id));
            if let Some(previous) = previous {
                if incoming.logical_time - previous.time < self.limits.window_millis {
                    return Err(Error::Batch);
                }
                if let Some(pending) = pending
                    && pending.operation > confirmed
                {
                    return Err(Error::PendingOriginal(pending.operation));
                }
            }
            let row = Pending {
                operation: incoming.operation,
                entry: Entry {
                    id,
                    offset: Offset::new(incoming.first_offset.get() + delta as u64),
                    time: incoming.logical_time,
                },
            };
            self.scratch.remove(hash, |row| row.entry.id == id);
            self.scratch
                .insert(hash, row, |row| self.hash.hash_one(row.entry.id));
            self.scratch_rows.push(row);
        }
        Ok(())
    }
}

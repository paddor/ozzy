//! Explicit single-broker authority and local-durable confirmation.
//!
//! This driver has no votes, followers, elections, or network events. It shares
//! canonical validation fences and journal completion tickets with replicated
//! partitions. A failed replicated group cannot become a local driver.

mod configuration;
pub use configuration::{CONFIGURATION_BYTES, Configuration};
#[cfg(test)]
mod tests;

use crate::driver::ValidationTicket;
use crate::{
    JournalGeneration, PipelineLimits, Prefix, PreparedOperation, SyncTicket, WriteTicket,
};
use ozzy_journal::progress::{JournalProgress, JournalSnapshot, ProgressError};
use std::collections::VecDeque;

/// Independent observation of acceptance, persistence, and applied visibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    /// Exact local written and durable journal progress.
    pub journal: JournalSnapshot,
    /// Last admitted canonical operation prefix.
    pub accepted: Prefix,
    /// Contiguous locally durable confirmation prefix.
    pub committed: Prefix,
    /// Last prefix installed in application state.
    pub applied: Prefix,
    /// Accepted operations still awaiting application.
    pub pending_operations: usize,
    /// Canonical body bytes retained by those operations.
    pub pending_body_bytes: usize,
}

/// Bounded single-broker state. Payloads remain owned by the journal adapter.
#[derive(Debug)]
pub struct Driver {
    configuration: Configuration,
    journal: JournalProgress,
    accepted: Prefix,
    committed: Prefix,
    applied: Prefix,
    operations: VecDeque<PreparedOperation>,
    body_bytes: usize,
    limits: PipelineLimits,
}

impl Driver {
    /// Persisted single-broker identity, independent of current write readiness.
    pub const fn configuration(&self) -> Configuration {
        self.configuration
    }

    /// Count/byte capacity shared by all admitted operations on this partition.
    pub const fn limits(&self) -> PipelineLimits {
        self.limits
    }

    /// Activate only after exact-identity journal recovery has validated and
    /// synchronized this prefix. A fresh, explicitly formatted store uses genesis.
    /// Unlike replicated restart, no remote agreement or election exists here.
    pub fn recover(
        configuration: Configuration,
        generation: JournalGeneration,
        recovered: Prefix,
        limits: PipelineLimits,
    ) -> Result<Self, Error> {
        if generation.0 == 0 || (recovered.op.0 == 0) != (recovered.digest == crate::Digest::ZERO) {
            return Err(Error::Lineage);
        }
        if limits.max_operations == 0
            || limits.max_operations > u32::MAX as usize
            || limits.max_body_bytes == 0
            || limits.max_body_bytes > isize::MAX as usize
        {
            return Err(Error::Limits);
        }
        Ok(Self {
            configuration,
            journal: JournalProgress::recover(generation, recovered.op),
            accepted: recovered,
            committed: recovered,
            applied: recovered,
            operations: VecDeque::with_capacity(limits.max_operations),
            body_bytes: 0,
            limits,
        })
    }

    /// Observe local progress without changing authority.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            journal: self.journal.snapshot(),
            accepted: self.accepted,
            committed: self.committed,
            applied: self.applied,
            pending_operations: self.operations.len(),
            pending_body_bytes: self.body_bytes,
        }
    }

    /// Capture validation authority without accepting or confirming records.
    pub fn begin_validation(&self) -> Result<ValidationTicket, Error> {
        let journal = self.journal.snapshot();
        if journal.faulted {
            return Err(ProgressError::Faulted.into());
        }
        Ok(ValidationTicket::from_local_image(
            self.configuration.broker(),
            self.configuration.scope(),
            journal.generation,
            self.accepted,
            self.committed,
            self.applied,
        ))
    }

    /// Admit only a fresh contiguous suffix validated on this exact image.
    /// Retries resolve in the shared canonical journal before this boundary.
    pub fn prepare_validated(
        &mut self,
        ticket: ValidationTicket,
        operations: &[PreparedOperation],
    ) -> Result<WriteTicket, Error> {
        if !ticket.same_images_as(self.begin_validation()?) {
            return Err(Error::StaleValidation);
        }
        if operations.is_empty() {
            return Err(ProgressError::EmptyGroup.into());
        }
        if operations.len() > self.limits.max_operations - self.operations.len() {
            return Err(Error::Capacity);
        }
        let mut end = self.accepted;
        let mut bytes = 0usize;
        let scope = self.configuration.scope();
        for operation in operations {
            if operation.group_id != scope.group_id
                || operation.configuration_epoch != scope.configuration_epoch
                || operation.original_view != 0
                || end.op.0.checked_add(1) != Some(operation.prefix.op.0)
                || operation.previous_digest != end.digest
            {
                return Err(Error::Lineage);
            }
            bytes = bytes
                .checked_add(operation.body_bytes)
                .ok_or(Error::Capacity)?;
            end = operation.prefix;
        }
        if bytes > self.limits.max_body_bytes - self.body_bytes {
            return Err(Error::Capacity);
        }
        let write = self.journal.admit(operations.len() as u64)?;
        self.operations.extend(operations);
        self.body_bytes += bytes;
        self.accepted = end;
        Ok(write)
    }

    /// Physical write completion alone never confirms or exposes records.
    pub fn complete_write(&mut self, ticket: WriteTicket) -> Result<(), Error> {
        Ok(self.journal.complete_write(ticket)?)
    }

    /// Capture a generation-fenced durability ticket for the written prefix.
    pub fn begin_sync(&self) -> Result<SyncTicket, Error> {
        Ok(self.journal.begin_sync()?)
    }

    /// Called after both the data barrier and exact durable-prefix evidence
    /// complete. Later writes cannot extend this captured confirmation boundary.
    pub fn complete_sync(&mut self, ticket: SyncTicket) -> Result<(), Error> {
        self.journal.complete_sync(ticket)?;
        let through = self.journal.snapshot().durable;
        if through > self.committed.op {
            self.committed = self
                .operations
                .iter()
                .find(|operation| operation.prefix.op == through)
                .ok_or(Error::Lineage)?
                .prefix;
        }
        Ok(())
    }

    /// Release live credit only after the shared journal applies this prefix.
    /// Application visibility and confirmation replies require this boundary.
    pub fn apply_through(&mut self, through: Prefix) -> Result<(), Error> {
        self.begin_validation()?;
        if through == self.applied {
            return Ok(());
        }
        if through.op <= self.applied.op
            || through.op > self.committed.op
            || !self
                .operations
                .iter()
                .any(|operation| operation.prefix == through)
        {
            return Err(Error::ApplyBeyondDurable);
        }
        while self
            .operations
            .front()
            .is_some_and(|operation| operation.prefix.op <= through.op)
        {
            let operation = self.operations.pop_front().expect("checked prefix");
            self.body_bytes -= operation.body_bytes;
        }
        self.applied = through;
        Ok(())
    }

    /// Fence uncertain I/O until a new exact-identity recovery and generation.
    pub fn fail(&mut self, generation: JournalGeneration) -> Result<(), Error> {
        Ok(self.journal.fail(generation)?)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
/// Rejected local authority, capacity, lineage, or journal progress.
pub enum Error {
    #[error("invalid single-broker configuration")]
    /// Identity, principal, or configuration epoch is invalid.
    Configuration,
    #[error("invalid single-broker configuration encoding or checksum")]
    /// Persistent configuration bytes or checksum are invalid.
    Encoding,
    #[error("invalid local partition pipeline limits")]
    /// Pipeline operation or body bounds are invalid.
    Limits,
    #[error("local partition capacity exhausted")]
    /// Local pending operation or byte capacity is full.
    Capacity,
    #[error("local partition history mismatch")]
    /// Canonical history does not extend the accepted prefix.
    Lineage,
    #[error("stale local partition validation")]
    /// Validation belongs to obsolete local authority or history.
    StaleValidation,
    #[error("local application exceeds its durable prefix")]
    /// Application would advance beyond synchronized history.
    ApplyBeyondDurable,
    #[error(transparent)]
    /// The journal progress contract rejected the transition.
    Journal(#[from] ProgressError),
}

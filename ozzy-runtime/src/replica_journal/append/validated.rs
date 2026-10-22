//! The arena after worker validation, and after admission with its write.

use ozzy_core::state::PreparedCanonicalGroup;
use ozzy_replication::driver::ValidationTicket;
use ozzy_replication::{PreparedOperation, WriteTicket};

use super::AppendBuffer;
use crate::replica_journal::JournalCompletion;

/// Worker-validated application plans, not admission, persistence, or quorum evidence.
///
/// Call `ReplicaDriver::prepare_validated` with this ticket and metadata before
/// submitting a write or transmitting PREPARE. Changed authority/image requires
/// discarding these plans and validating again. This initial command accepts
/// only a fresh contiguous suffix; duplicate/overlap handling remains actor work.
#[derive(Debug)]
pub struct ValidatedAppend {
    pub(in crate::replica_journal) validation: ValidationTicket,
    pub(in crate::replica_journal) buffer: AppendBuffer,
    pub(in crate::replica_journal) plan: PreparedCanonicalGroup,
    pub(in crate::replica_journal) records: Vec<ozzy_journal_segment::PreparedOperationRecords>,
    pub(in crate::replica_journal) dedup:
        Option<Box<crate::replica_journal::owned::dedup::Prepared>>,
}

impl ValidatedAppend {
    pub(crate) fn shared_bodies(&mut self) -> bytes::Bytes {
        self.buffer.shared_bodies()
    }

    /// Exact voter/writer/view and image against which application validation ran.
    pub const fn validation(&self) -> ValidationTicket {
        self.validation
    }

    /// Verified metadata for core admission. Payload remains owned by this value.
    pub fn prepared(&self) -> &[PreparedOperation] {
        &self.buffer.prepared
    }

    /// Immutable payload retained until written, transmitted, or explicitly discarded.
    pub const fn buffer(&self) -> &AppendBuffer {
        &self.buffer
    }

    /// Use worker-computed body digests for wire encoding without hashing payload on the actor.
    pub fn wire_operations(
        &self,
    ) -> impl ExactSizeIterator<Item = ozzy_replication::wire::Operation<'_>> {
        self.buffer.wire_operations()
    }

    /// Discard unadmitted plans and recover the reusable payload arena.
    pub fn into_buffer(mut self) -> AppendBuffer {
        self.buffer.restore_dedup_original();
        self.buffer
    }
}

/// Installed canonical state with its queued physical write. The write
/// completion follows independently; returning the caller arena does not
/// release the configured write backlog.
#[derive(Debug)]
pub struct AdmittedAppend {
    pub(in crate::replica_journal) ticket: WriteTicket,
    pub(in crate::replica_journal) buffer: AppendBuffer,
    pub(in crate::replica_journal) persisted: JournalCompletion<WriteTicket>,
}

impl AdmittedAppend {
    /// Return the exact admission, reusable arena, and independent write completion.
    pub fn into_parts(self) -> (WriteTicket, AppendBuffer, JournalCompletion<WriteTicket>) {
        (self.ticket, self.buffer, self.persisted)
    }
}

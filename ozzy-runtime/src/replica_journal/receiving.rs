//! Recovery-only ownership of a marker-backed replacement journal.

pub(super) mod driver;
pub use driver::{RecoveryStorage, ShardRecoveringJournal};

use super::{AppendBuffer, InstallationConfig, JournalError, completion};
use ozzy_proto::{NodeId, RequestId};
use ozzy_replication::recovery::{Recovery, RecoveryError, RecoveryTicket};
use ozzy_replication::{
    Configuration, JournalGeneration, PipelineLimits, Prefix, PreparedOperation,
};

#[derive(Debug)]
#[expect(
    clippy::large_enum_variant,
    reason = "bounded command slots retain the arena inline without another per-chunk allocation"
)]
pub(super) enum ReceiveAction {
    Begin {
        ticket: RecoveryTicket,
        config: InstallationConfig,
        done: completion::Sender<Result<RecoveryPlan, JournalError>>,
    },
    Chunk {
        ticket: RecoveryTicket,
        buffer: AppendBuffer,
        done: completion::Sender<Result<ReceivedChunk, JournalError>>,
    },
    Finish {
        ticket: RecoveryTicket,
        done: completion::Sender<Result<PublishedRecovery, JournalError>>,
    },
    Abort {
        ticket: RecoveryTicket,
        done: completion::Sender<Result<RecoveryTicket, JournalError>>,
    },
}

/// Worker-selected transfer: full replacement or missing sealed-file ranges.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryPlan {
    /// Fetch and replace the full accepted history from genesis.
    Full,
    /// Restore exact sealed ranges. `None` means all ranges have arrived.
    Repair(Option<ozzy_journal_segment::RepairRange>),
    /// Donor history disagrees with the old range. Abort, reopen full recovery,
    /// and obtain fresh authority; this chunk grants no publication evidence.
    RetryFull,
}

/// Worker-verified staged bytes, not a durable publication or vote.
#[derive(Debug)]
pub struct ReceivedChunk {
    pub(super) ticket: RecoveryTicket,
    pub(super) end: Prefix,
    pub(super) buffer: AppendBuffer,
    pub(super) plan: RecoveryPlan,
}

impl ReceivedChunk {
    /// Remaining transfer, including the next missing range after a repaired file.
    pub const fn plan(&self) -> RecoveryPlan {
        self.plan
    }
    /// Exact recovery attempt to which this chunk belongs.
    pub const fn ticket(&self) -> RecoveryTicket {
        self.ticket
    }
    /// Contiguous staged prefix after this chunk.
    pub const fn end(&self) -> Prefix {
        self.end
    }
    /// All verified descriptors, including committed and current-view operations.
    /// For full replacement, feed to the live recovery core before accepting
    /// completion. Physical repair validates original manifest anchors instead.
    pub fn prepared(&self) -> &[PreparedOperation] {
        &self.buffer.prepared
    }
    /// Return the same leased arena for clearing and reuse.
    pub fn into_buffer(self) -> AppendBuffer {
        self.buffer
    }
}

/// Exact durable replacement plus private semantic replay. Grants no voting authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishedRecovery {
    pub(super) ticket: RecoveryTicket,
    pub(super) applied: Prefix,
    pub(super) repaired: Option<ozzy_replication::RecoveredState>,
}

impl PublishedRecovery {
    /// Core-issued attempt authorizing the replacement or physical repair.
    pub const fn ticket(self) -> RecoveryTicket {
        self.ticket
    }
    /// Privately rebuilt committed state, never accepted-only state or consumer processing.
    pub const fn applied(self) -> Prefix {
        self.applied
    }
}

/// Nonvoting startup evidence. Never supplies bootstrap or normal-voter authority.
#[derive(Debug)]
pub struct RecoveryStartup {
    pub(super) configuration: Configuration,
    pub(super) local: NodeId,
    pub(super) generation: JournalGeneration,
}

impl RecoveryStartup {
    /// Exact configuration bound to the on-disk recovery marker.
    pub const fn configuration(&self) -> Configuration {
        self.configuration
    }
    /// Configured voter whose missing history is being replaced.
    pub const fn local(&self) -> NodeId {
        self.local
    }
    /// Fresh identity for this worker, its arenas, and the recovery attempt.
    pub const fn generation(&self) -> JournalGeneration {
        self.generation
    }
    /// Start nonvoting recovery with a fresh, nonzero attempt nonce.
    pub fn into_recovery(
        self,
        nonce: RequestId,
        limits: PipelineLimits,
    ) -> Result<Recovery, RecoveryError> {
        Recovery::new(
            self.configuration,
            self.local,
            self.generation,
            nonce,
            limits,
        )
    }
}

//! Fresh-quorum admission for a fixed voter that lost its previous state.
//!
//! Recovery never contributes its own vote. Only normal replicas answer; the
//! highest observed view's primary supplies the complete accepted history.
//! Descriptors are not proof of retained bytes, completed I/O, or application
//! recovery. The adapter authenticates peers and pins the exact selected source.

mod transfer;

use ozzy_proto::{NodeId, RequestId};

use crate::{
    Configuration, JournalGeneration, LogSource, NormalReplica, PipelineLimits, Prefix,
    ReplicationError, Scope, Status,
};

/// Primary's immutable full-log snapshot for one recovery nonce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryLog {
    /// Pinned source writer incarnation, never mixed with a replacement writer.
    pub generation: JournalGeneration,
    /// Complete accepted prefix, including unsynced or uncommitted prepares.
    pub accepted: Prefix,
    /// Known commit floor in this snapshot, not a truncation boundary.
    pub committed: Prefix,
}

/// Fresh response from an authenticated normal replica, not a durable vote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryResponse {
    /// Exact configuration and responder's normal view.
    pub scope: Scope,
    /// Globally fresh request nonce echoed unchanged across retransmissions.
    pub nonce: RequestId,
    /// Present exactly when the responder is primary in `scope.view`.
    pub primary: Option<RecoveryLog>,
}

impl NormalReplica {
    /// Capture a recovery response without adding I/O to normal commit.
    ///
    /// The adapter pins this exact accepted prefix, including in-flight writes,
    /// before offering its bytes. It must retain/retransmit the same response for
    /// this nonce, not recapture a moving tail. Only a new recovery nonce starts
    /// a new snapshot. A durable-prefix-only snapshot is unsafe: an earlier ACK
    /// from the lost voter may still commit an unsynced accepted operation.
    pub fn recovery_response(&self, nonce: RequestId) -> Result<RecoveryResponse, RecoveryError> {
        let snapshot = self.snapshot();
        if snapshot.status != Status::Normal || !snapshot.ready_for_appends {
            return Err(RecoveryError::NotReady);
        }
        if nonce.as_bytes() == &[0; 16] {
            return Err(RecoveryError::InvalidIdentity);
        }
        let (configuration, local) = self.driver_identity();
        Ok(RecoveryResponse {
            scope: snapshot.scope,
            nonce,
            primary: (local == configuration.primary(snapshot.scope.view)).then_some(RecoveryLog {
                generation: snapshot.journal.generation,
                accepted: snapshot.accepted,
                committed: snapshot.committed,
            }),
        })
    }
}

/// Quorum-authorized full-history transfer. No voting or storage-completion proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryTicket {
    local: NodeId,
    generation: JournalGeneration,
    nonce: RequestId,
    scope: Scope,
    source: LogSource,
    committed: Prefix,
    voters: u8,
}

impl RecoveryTicket {
    /// Existing fixed voter being recovered, never a new membership entry.
    pub const fn local(self) -> NodeId {
        self.local
    }
    /// Fresh replacement writer incarnation; binds every completion.
    pub const fn generation(self) -> JournalGeneration {
        self.generation
    }
    /// Recovery attempt identity, independent of transient transport sessions.
    pub const fn nonce(self) -> RequestId {
        self.nonce
    }
    /// Highest observed view, with a matching normal primary response.
    pub const fn scope(self) -> Scope {
        self.scope
    }
    /// Complete immutable accepted lineage to transfer, not only committed data.
    pub const fn source(self) -> LogSource {
        self.source
    }
    /// Application state must be rebuilt through this exact snapshot commit floor.
    pub const fn committed(self) -> Prefix {
        self.committed
    }
    /// Configured response participants. Never contains the recovering voter.
    pub const fn voter_mask(self) -> u8 {
        self.voters
    }
}

/// Bounded nonvoting recovery role. Owns no normal or view-change voting state.
#[derive(Debug)]
pub struct Recovery {
    configuration: Configuration,
    local: usize,
    generation: JournalGeneration,
    nonce: RequestId,
    view: u64,
    limits: PipelineLimits,
    responses: [Option<RecoveryResponse>; 3],
    pending: Option<Transfer>,
    completed: bool,
    faulted: bool,
}

#[derive(Debug, Clone, Copy)]
struct Transfer {
    ticket: RecoveryTicket,
    through: Prefix,
}

impl Recovery {
    /// Start explicit lost-state recovery under an externally trusted configuration.
    /// `nonce` and `generation` must be globally fresh, including after process
    /// restart. The caller must not have another live incarnation of this voter.
    pub fn new(
        configuration: Configuration,
        local: NodeId,
        generation: JournalGeneration,
        nonce: RequestId,
        limits: PipelineLimits,
    ) -> Result<Self, RecoveryError> {
        let local = configuration.voter_index(local)?;
        if generation.0 == 0 || nonce.as_bytes() == &[0; 16] {
            return Err(RecoveryError::InvalidIdentity);
        }
        if limits.max_operations == 0 || limits.max_body_bytes == 0 {
            return Err(ReplicationError::InvalidLimits.into());
        }
        Ok(Self {
            configuration,
            local,
            generation,
            nonce,
            view: 0,
            limits,
            responses: [None; 3],
            pending: None,
            completed: false,
            faulted: false,
        })
    }

    /// Record one authenticated normal response. Duplicate replies count once.
    pub fn receive(
        &mut self,
        from: NodeId,
        response: RecoveryResponse,
    ) -> Result<(), RecoveryError> {
        self.require_active()?;
        let voter = self.configuration.voter_index(from)?;
        if voter == self.local {
            return Err(RecoveryError::SelfResponse);
        }
        if response.nonce != self.nonce {
            return Err(RecoveryError::StaleNonce);
        }
        if response.scope
            != (Scope {
                view: response.scope.view,
                ..self.configuration.scope()
            })
        {
            return Err(ReplicationError::ScopeMismatch.into());
        }
        if response.primary.is_some() != (from == self.configuration.primary(response.scope.view)) {
            return Err(RecoveryError::InvalidResponse);
        }
        if let Some(log) = response.primary
            && (log.generation.0 == 0
                || !crate::view_change::valid_prefix(log.accepted)
                || !crate::view_change::valid_prefix(log.committed)
                || log.committed.op > log.accepted.op
                || (log.committed.op == log.accepted.op && log.committed != log.accepted))
        {
            return Err(RecoveryError::InvalidResponse);
        }
        if let Some(previous) = self.responses[voter] {
            if response.scope.view < previous.scope.view {
                return Ok(());
            }
            if response.scope.view == previous.scope.view && response != previous {
                self.faulted = true;
                return Err(RecoveryError::ConflictingResponse);
            }
        }
        self.view = self.view.max(response.scope.view);
        self.responses[voter] = Some(response);
        Ok(())
    }

    /// Observe configured peer authority outside the recovery response exchange.
    /// A higher election/normal view supersedes pending recovery but grants no
    /// vote. Drain/settle its worker action, then start a fresh nonce-scoped
    /// recovery attempt. A timeout alone must not invent higher-view evidence.
    pub fn observe_view(&mut self, from: NodeId, scope: Scope) -> Result<(), RecoveryError> {
        self.require_active()?;
        if self.configuration.voter_index(from)? == self.local {
            return Err(RecoveryError::SelfResponse);
        }
        if scope
            != (Scope {
                view: scope.view,
                ..self.configuration.scope()
            })
        {
            return Err(ReplicationError::ScopeMismatch.into());
        }
        self.view = self.view.max(scope.view);
        Ok(())
    }

    /// Freeze the highest-view primary's source after two distinct other responses.
    /// Older-view normal responses with this fresh nonce may witness the quorum.
    pub fn begin_transfer(&mut self) -> Result<RecoveryTicket, RecoveryError> {
        self.require_active()?;
        if self.pending.is_some() {
            return Err(RecoveryError::TransferPending);
        }
        let voters = self
            .responses
            .iter()
            .enumerate()
            .fold(0_u8, |mask, (index, response)| {
                mask | (u8::from(response.is_some()) << index)
            });
        if voters.count_ones() < 2 {
            return Err(RecoveryError::QuorumMissing);
        }
        let primary = self.configuration.primary(self.view);
        let response = self.responses[self.configuration.voter_index(primary)?]
            .filter(|response| response.scope.view == self.view)
            .ok_or(RecoveryError::PrimaryMissing)?;
        let log = response.primary.ok_or(RecoveryError::PrimaryMissing)?;
        if log.generation == self.generation {
            return Err(RecoveryError::InvalidIdentity);
        }
        let ticket = RecoveryTicket {
            local: self.configuration.voters()[self.local],
            generation: self.generation,
            nonce: self.nonce,
            scope: response.scope,
            source: LogSource {
                voter: primary,
                generation: log.generation,
                accepted: log.accepted,
            },
            committed: log.committed,
            voters,
        };
        self.pending = Some(Transfer {
            ticket,
            through: Prefix::GENESIS,
        });
        Ok(ticket)
    }

    fn require_active(&self) -> Result<(), RecoveryError> {
        if self.faulted {
            Err(RecoveryError::Faulted)
        } else if self.completed {
            Err(RecoveryError::Completed)
        } else {
            Ok(())
        }
    }
}

/// Rejected recovery evidence. No error grants a vote or replaces valid history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecoveryError {
    /// Existing configuration, membership, or bounds validation failed.
    #[error(transparent)]
    Replication(#[from] ReplicationError),
    /// Nonces and replacement writer incarnations must be nonzero and fresh.
    #[error("invalid recovery identity")]
    InvalidIdentity,
    /// Only an activated normal replica may respond to recovery.
    #[error("replica is not ready to answer recovery")]
    NotReady,
    /// Lost state cannot contribute evidence about its own past votes.
    #[error("recovering replica cannot count itself")]
    SelfResponse,
    /// Response belongs to another recovery attempt.
    #[error("stale recovery nonce")]
    StaleNonce,
    /// Invalid role, source incarnation, or prefix coordinates.
    #[error("invalid recovery response")]
    InvalidResponse,
    /// Same responder changed a frozen response within one view and nonce.
    #[error("conflicting recovery response")]
    ConflictingResponse,
    /// Two distinct other normal replicas have not responded.
    #[error("recovery quorum missing")]
    QuorumMissing,
    /// No response from the primary of the highest observed view.
    #[error("recovery primary missing")]
    PrimaryMissing,
    /// Previous transfer must settle before another can be admitted.
    #[error("recovery transfer already pending")]
    TransferPending,
    /// Callback or chunk belongs to another writer, nonce, source, or attempt.
    #[error("stale recovery transfer")]
    StaleTransfer,
    /// A newer configured view superseded this transfer's recovery authority.
    #[error("recovery view superseded")]
    StaleView,
    /// The entire selected full-WAL chain has not been validated.
    #[error("recovery history missing")]
    HistoryMissing,
    /// Actual durable publication does not cover the selected accepted prefix.
    #[error("recovery storage publication pending")]
    StoragePending,
    /// Canonical committed state has not been rebuilt at the selected commit floor.
    #[error("recovery application validation pending")]
    ApplicationPending,
    /// Recovery evidence has already been consumed by a fenced restart handoff.
    #[error("recovery already completed")]
    Completed,
    /// Conflicting evidence or uncertain publication requires a new recovery attempt.
    #[error("recovery faulted")]
    Faulted,
}

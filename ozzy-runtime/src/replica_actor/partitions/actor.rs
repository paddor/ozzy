//! One partition's actor in its explicit mode, and the observations it
//! offers without granting authority.

use crate::replica_actor::{
    ActorError, ActorStatus, LocalActor, ProposalSubmitter, ScheduledRecovery, ScheduledReplica,
};
use ozzy_proto::{GroupId, LinkSessionId, NodeId, PartitionIncarnation, directory::RouteState};
use ozzy_replication::driver::CleanStop;

/// Explicit partition mode. A failed group can never become a local actor.
#[derive(Debug)]
pub enum PartitionActor {
    /// Explicit single-broker local durability.
    Local(Box<LocalActor>),
    /// Fixed-three authority, including election after restart.
    Replicated(ScheduledReplica),
    /// Explicit nonvoting replacement. Full publication precedes normal election.
    Recovering(Box<ScheduledRecovery>),
}

/// One link fence: the session last applied to a peer and the newly
/// established one that replaces it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionReplacement {
    /// Last applied session; zero when none was.
    pub expected: LinkSessionId,
    /// Newly established nonzero session.
    pub replacement: LinkSessionId,
}

/// Coalesced observation, never a substitute for partition authority.
#[derive(Debug, Clone, Copy)]
#[expect(
    clippy::large_enum_variant,
    reason = "bounded copied status does not allocate on each observation"
)]
pub enum PartitionStatus {
    /// Local accepted, durable, and applied positions.
    Local(ozzy_replication::local::Snapshot),
    /// Per-partition replicated role and positions.
    Replicated(ActorStatus),
}

impl PartitionActor {
    pub(in crate::replica_actor) fn read_access(
        &mut self,
    ) -> Option<(
        Option<ozzy_replication::driver::ValidationTicket>,
        &mut crate::replica_journal::ReplicaJournal,
    )> {
        match self {
            Self::Local(actor) => Some(actor.read_access()),
            Self::Replicated(actor) => Some(actor.read_access()),
            Self::Recovering(_) => None,
        }
    }

    /// Lease fixed startup arenas before transferring the partition to its scheduler.
    pub fn lease_proposal_buffer_with_limits(
        &self,
        limits: ozzy_replication::PipelineLimits,
    ) -> Result<crate::replica_journal::ProposalBuffer, ActorError> {
        match self {
            Self::Local(actor) => actor.lease_proposal_buffer_with_limits(limits),
            Self::Replicated(actor) => actor.lease_proposal_buffer_with_limits(limits),
            Self::Recovering(_) => Err(ActorError::StartupMismatch),
        }
    }

    /// Take one remaining fixed submission lane for shard-local services.
    pub fn take_submitter(&mut self) -> Option<ProposalSubmitter> {
        match self {
            Self::Local(actor) => actor.take_submitter(),
            Self::Replicated(actor) => actor.take_submitter(),
            Self::Recovering(_) => None,
        }
    }

    /// Use shared shard memory for normal follower data, without receive grants.
    /// Nonvoting recovery retains its separately fenced transfer allowance.
    pub fn bind_receive_owner(&mut self, owner: &crate::memory::Owner) -> Result<(), ActorError> {
        match self {
            Self::Replicated(actor) => actor.bind_receive_owner(owner),
            Self::Recovering(actor) => actor.bind_receive_owner(owner),
            Self::Local(_) => Ok(()),
        }
    }

    /// Give a replicated actor's reserved but unused capacity back. See
    /// [`ReplicaActor::trim`](crate::replica_actor::ReplicaActor::trim).
    pub fn trim(&mut self) {
        if let Self::Replicated(actor) = self {
            actor.trim();
        }
    }

    /// Select the shared frontend's follower publication transport.
    pub fn enable_publication(&mut self) {
        if let Self::Replicated(actor) = self {
            actor.enable_publication();
        }
    }

    /// Immutable group identity, independent of current role.
    pub fn group(&self) -> GroupId {
        match self {
            Self::Local(actor) => actor.group(),
            Self::Replicated(actor) => actor.group(),
            Self::Recovering(actor) => actor.status().scope.group_id,
        }
    }

    /// Inspect progress without granting authority.
    pub fn status(&self) -> PartitionStatus {
        match self {
            Self::Local(actor) => PartitionStatus::Local(actor.snapshot()),
            Self::Replicated(actor) => PartitionStatus::Replicated(actor.status()),
            Self::Recovering(actor) => PartitionStatus::Replicated(actor.status()),
        }
    }

    /// Committed facts of `partition` for diagnostics. `None` while the
    /// journal is busy or recovering.
    pub fn facts(
        &self,
        partition: PartitionIncarnation,
    ) -> Option<crate::replica_journal::PartitionFacts> {
        match self {
            Self::Local(actor) => actor.facts(partition),
            Self::Replicated(actor) => actor.facts(partition),
            Self::Recovering(_) => None,
        }
    }

    /// Whether a local partition has no write or durable evidence outstanding.
    #[cfg(test)]
    pub(crate) fn storage_idle(&self) -> Option<bool> {
        match self {
            Self::Local(actor) => Some(actor.storage_idle()),
            Self::Replicated(_) | Self::Recovering(_) => None,
        }
    }

    /// Whether this broker leads a replicated partition but heard no
    /// follower for the primary timeout, so nothing it admits can commit.
    pub(in crate::replica_actor) fn leader_doubt(&self) -> bool {
        match self {
            Self::Replicated(actor) => actor.leader_doubt(),
            Self::Local(_) | Self::Recovering(_) => false,
        }
    }

    /// Hand a replicated partition this broker leads to `target`. `None`
    /// while accepted work is uncommitted.
    pub(in crate::replica_actor) fn step_down(
        &mut self,
        target: NodeId,
    ) -> Result<Option<u64>, ozzy_replication::driver::DriverError> {
        match self {
            Self::Replicated(actor) => actor.step_down(target),
            Self::Local(_) | Self::Recovering(_) => {
                Err(ozzy_replication::ReplicationError::WrongRole.into())
            }
        }
    }

    /// Current leader hint. It neither activates authority nor confirms history.
    pub fn authority_hint(&self) -> ozzy_proto::nack::AuthorityHint {
        match self {
            Self::Local(actor) => actor.authority_hint(),
            Self::Replicated(actor) => actor.authority_hint(),
            Self::Recovering(actor) => actor.authority_hint(),
        }
    }

    /// Allocate one initial watch identity from this actor's actual configuration.
    /// The incarnation comes from its checked partition journal placement.
    pub fn route_state(&self, partition: PartitionIncarnation) -> RouteState {
        match self {
            Self::Local(actor) => actor.route_state(partition),
            Self::Replicated(actor) => actor.route_state(partition),
            Self::Recovering(actor) => actor.route_state(partition),
        }
    }

    pub(super) fn observe_route(&self, route: &mut RouteState) -> bool {
        match self {
            Self::Local(actor) => actor.observe_route(route),
            Self::Replicated(actor) => actor.observe_route(route),
            Self::Recovering(actor) => actor.observe_route(route),
        }
    }

    pub(in crate::replica_actor) fn native_identity(
        &self,
    ) -> (GroupId, NodeId, ozzy_proto::append::Policy) {
        let (local, policy) = match self {
            Self::Local(actor) => (
                actor.authority_hint().primary,
                ozzy_proto::append::Policy::LocalDurable,
            ),
            Self::Replicated(actor) => actor.native_identity(),
            Self::Recovering(actor) => actor.native_identity(),
        };
        (self.group(), local, policy)
    }

    /// Close intake and drain the existing storage owner asynchronously.
    pub async fn shutdown(self) -> Result<(), ActorError> {
        match self {
            Self::Local(actor) => actor.shutdown().await,
            Self::Replicated(actor) => actor.shutdown().await,
            Self::Recovering(actor) => Box::pin(actor.shutdown()).await,
        }
    }

    /// Shut down like [`Self::shutdown`], and return a replicated primary's
    /// evidence of a clean stop.
    pub async fn stop(self) -> Result<Option<CleanStop>, ActorError> {
        match self {
            Self::Replicated(actor) => actor.stop().await,
            other => other.shutdown().await.map(|()| None),
        }
    }
}

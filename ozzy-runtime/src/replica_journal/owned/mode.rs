use super::JournalError;
use ozzy_proto::{NodeId, append::Policy};
use ozzy_replication::{Configuration, QuorumPolicy, Scope, local};

/// Storage's immutable confirmation and identity binding. Local durability
/// never supplies group membership, a vote, or election authority.
#[derive(Debug, Clone, Copy)]
pub(super) enum Mode {
    Replicated(Configuration),
    Local(local::Configuration),
}

impl Mode {
    pub(super) fn scope(self) -> Scope {
        match self {
            Self::Replicated(group) => group.scope(),
            Self::Local(local) => local.scope(),
        }
    }

    pub(super) fn primary(self, view: u64) -> NodeId {
        match self {
            Self::Replicated(group) => group.primary(view),
            Self::Local(local) => local.broker(),
        }
    }

    pub(super) fn append_policy(self) -> Policy {
        match self {
            Self::Replicated(group) => group.append_policy(),
            Self::Local(_) => Policy::LocalDurable,
        }
    }

    pub(super) fn memory_voting(self) -> bool {
        matches!(self, Self::Replicated(group) if group.policy() == QuorumPolicy::Replicated)
    }

    pub(super) fn replicated(self) -> Result<Configuration, JournalError> {
        match self {
            Self::Replicated(group) => Ok(group),
            Self::Local(_) => Err(JournalError::Configuration),
        }
    }
}

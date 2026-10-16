//! Consumer-group coordination on the partition that owns a group's plans.
//!
//! The service answers membership requests from the committed image, proposes
//! plan revisions through the actor's ordinary journal pipeline, and tracks
//! heartbeats, owned sources, and drain deadlines in volatile state. Members
//! relay committed targets to sources themselves; nothing here contacts another
//! partition. Losing leadership discards every observation.

mod proposals;
mod requests;
mod state;

use super::deadline::Earliest;
use super::{NativeIntakeError, PendingProposal, ProposalSubmitter};
use crate::replica_journal::ProposalBuffer;
use omq_tokio::Message;
use ozzy_proto::{
    ConsumerGroupId, EnvelopeLimits, GroupId, LinkSessionId, NodeId, Opcode, PartitionIncarnation,
    group,
};
use ozzy_replication::Scope;
use state::GroupState;
use std::{
    collections::{BTreeMap, VecDeque},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

/// Immutable topic shape every coordinator of that topic shares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicTopology {
    /// Persisted partitioner seed; group names select coordinators with it.
    pub seed: u64,
    /// Every partition in immutable numeric order.
    pub partitions: Vec<(u32, PartitionIncarnation)>,
}

impl TopicTopology {
    fn number(&self, partition: PartitionIncarnation) -> Option<u32> {
        self.partitions
            .iter()
            .find(|(_, incarnation)| *incarnation == partition)
            .map(|(number, _)| *number)
    }

    fn incarnations(&self) -> Vec<PartitionIncarnation> {
        self.partitions
            .iter()
            .map(|(_, incarnation)| *incarnation)
            .collect()
    }
}

/// Fixed per-partition coordinator resources. Every partition can coordinate
/// the groups whose names select it; the SDK and broker agree on that choice.
#[derive(Debug, Clone)]
pub struct GroupCoordinatorConfig {
    /// Broker process identity.
    pub local: NodeId,
    /// Immutable partition group.
    pub group: GroupId,
    /// Immutable partition incarnation.
    pub partition: PartitionIncarnation,
    /// Numeric partition within the topic.
    pub number: u32,
    /// Shared topic shape.
    pub topology: Arc<TopicTopology>,
    /// Negotiated frame bounds for decoding requests.
    pub limits: EnvelopeLimits,
    /// Replies queued for the shared reply path; further requests wait.
    pub requests: usize,
    /// Plan proposals in flight across groups; each holds one arena.
    pub plans: usize,
    /// Members one group may hold; later joins wait for capacity.
    pub max_members: usize,
    /// Members without a heartbeat for this long leave the next plan.
    pub membership_timeout: Duration,
    /// A moved source becomes activatable this long after its plan committed,
    /// even if the prior instance never reported releasing it.
    pub drain_deadline: Duration,
}

impl GroupCoordinatorConfig {
    /// Canonical body bytes of the largest plan this coordinator may propose.
    pub fn plan_bytes(&self) -> usize {
        341 + 48 * self.max_members + 122 * self.topology.partitions.len()
    }

    fn validate(&self) -> Result<(), NativeIntakeError> {
        let count = self.topology.partitions.len();
        if self.local.as_bytes() == &[0; 16]
            || self.group.as_bytes() == &[0; 16]
            || self.partition.as_bytes() == &[0; 16]
            || count == 0
            || count > group::MAX_PARTITIONS
            || self.topology.number(self.partition) != Some(self.number)
            || self.requests == 0
            || self.plans == 0
            || self.max_members == 0
            || self.max_members > group::MAX_MEMBERS
            || self.membership_timeout.is_zero()
            || self.drain_deadline.is_zero()
            || self.limits.max_metadata_bytes < group::JOINED_FIXED_BYTES
        {
            return Err(NativeIntakeError::Configuration);
        }
        Ok(())
    }
}

struct PlanProposal {
    name: String,
    pending: Pin<Box<PendingProposal>>,
}

struct DeleteProposal {
    name: String,
    group: ConsumerGroupId,
    requester: requests::Requester,
    pending: Pin<Box<PendingProposal>>,
}

struct Reply {
    peer: NodeId,
    session: LinkSessionId,
    frame: Message,
}

/// One partition's coordinator adapter. It owns no socket, timer, or
/// authority; poll it beside the partition actor on its shard.
pub struct GroupCoordinator {
    config: GroupCoordinatorConfig,
    submitter: ProposalSubmitter,
    arenas: Vec<ProposalBuffer>,
    proposals: Vec<PlanProposal>,
    deletions: Vec<DeleteProposal>,
    groups: BTreeMap<String, GroupState>,
    replies: VecDeque<Reply>,
    metadata: Vec<u8>,
    scope: Option<Scope>,
}

impl std::fmt::Debug for GroupCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupCoordinator")
            .field("partition", &self.config.partition)
            .field("groups", &self.groups.len())
            .field("proposals", &self.proposals.len())
            .finish_non_exhaustive()
    }
}

impl GroupCoordinator {
    /// Take the proposal lane and exactly `config.plans` empty arenas of at
    /// least `config.plan_bytes()` before transferring the actor.
    pub fn new(
        config: GroupCoordinatorConfig,
        submitter: ProposalSubmitter,
        arenas: Vec<ProposalBuffer>,
    ) -> Result<Self, NativeIntakeError> {
        config.validate()?;
        if arenas.len() != config.plans
            || arenas.iter().any(|arena| {
                !arena.is_empty() || arena.limits().max_body_bytes < config.plan_bytes()
            })
        {
            return Err(NativeIntakeError::Configuration);
        }
        Ok(Self {
            metadata: Vec::with_capacity(config.limits.max_metadata_bytes),
            config,
            submitter,
            arenas,
            proposals: Vec::new(),
            deletions: Vec::new(),
            groups: BTreeMap::new(),
            replies: VecDeque::new(),
            scope: None,
        })
    }

    /// Whether this command belongs to the coordinator rather than native intake.
    pub fn supports(message: &Message) -> bool {
        message.len() == 4
            && message.part_slice(1).is_some_and(|header| {
                header.get(5).is_some_and(|&opcode| {
                    [Opcode::GroupJoin, Opcode::GroupInspect, Opcode::GroupDelete]
                        .iter()
                        .any(|candidate| *candidate as u8 == opcode)
                })
            })
    }

    /// The earliest member expiry or drain deadline among this leader's groups.
    pub(super) fn next_deadline(&self, now: Duration) -> Option<Duration> {
        self.groups
            .values()
            .fold(Earliest::after(now), |next, group| {
                group.deadlines(
                    next,
                    self.config.membership_timeout,
                    self.config.drain_deadline,
                )
            })
            .get()
    }

    pub(super) fn identity(&self) -> (GroupId, NodeId) {
        (self.config.group, self.config.local)
    }

    /// Queued replies for one client; they retain no proposal resources.
    pub fn client_work(&self, node: NodeId) -> usize {
        self.replies
            .iter()
            .filter(|reply| reply.peer == node)
            .count()
    }

    /// Replies, plan proposals, or deletions still owned by this service.
    pub fn has_work(&self) -> bool {
        !self.replies.is_empty() || !self.proposals.is_empty() || !self.deletions.is_empty()
    }

    fn observe_scope(&mut self, scope: Scope) {
        if self.scope != Some(scope) {
            self.scope = Some(scope);
            self.groups.clear();
        }
    }
}

//! Durable coordinator membership and source assignment intent.

use ozzy_proto::{
    ConsumerGroupId, ConsumerMemberId, NodeId, OperationId, PartitionIncarnation, group::Start,
};

/// Absolute member bound for one coordinator image.
pub const GROUP_PLAN_MAX_MEMBERS: usize = ozzy_proto::group::MAX_MEMBERS;
/// Absolute source bound for one coordinator image. One member's targets must
/// fit one bounded control reply.
pub const GROUP_PLAN_MAX_PARTITIONS: usize = ozzy_proto::group::MAX_PARTITIONS;

/// One processing instance. A new join identity fences a restarted instance,
/// even when the application retains its member ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupMember {
    /// Application member identity.
    pub id: ConsumerMemberId,
    /// Physical client identity used for addressed control traffic.
    pub node: NodeId,
    /// Stable retry identity of this processing instance's join.
    pub join: OperationId,
}

/// Committed intent for one source partition; it does not prove activation.
/// Sources allocate their own assignment epochs when an intent reaches them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupTarget {
    /// Source incarnation in immutable numeric topic order.
    pub partition: PartitionIncarnation,
    /// Last coordinator revision that changed this source's owner.
    pub plan_revision: u64,
    /// Desired processing instance, or no owner.
    pub member: Option<GroupMember>,
    /// Instance intended by the previous plan, drained before activation.
    pub prior_member: Option<GroupMember>,
}

/// Complete, bounded coordinator intent. Source changes follow its confirmation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupPlan<'a> {
    /// Deterministic coordinator partition for the topic and group name.
    pub coordinator: PartitionIncarnation,
    /// Group incarnation preserved across ordinary member churn.
    pub group: ConsumerGroupId,
    /// Topic-scoped group name.
    pub name: &'a str,
    /// Expected durable coordinator revision; zero creates a group.
    pub expected_revision: u64,
    /// Next coordinator revision.
    pub revision: u64,
    /// First position of sources without a cursor; immutable after creation.
    pub initial: Start,
    /// Current processing instances, strictly sorted by member ID.
    pub members: Vec<GroupMember>,
    /// Sticky balanced ownership in immutable numeric topic order.
    pub targets: Vec<GroupTarget>,
    /// Retry identity for this membership transition.
    pub operation_id: OperationId,
}

/// Retire one coordinator group incarnation. Its name becomes free, so a
/// later join creates a new incarnation with no cursors of its own. Sources
/// fence the retired incarnation separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupDelete {
    /// Coordinator partition that journals the group.
    pub coordinator: PartitionIncarnation,
    /// Retired group incarnation.
    pub group: ConsumerGroupId,
    /// Committed coordinator revision being retired.
    pub revision: u64,
    /// Retry identity for this deletion.
    pub operation_id: OperationId,
}

//! Durable coordinator images; source activation remains a separate boundary.

use super::{
    CanonicalState, IdentityClaim, IdentityIndex, Mutation, StateError, operation_claim, require_id,
};
use ozzy_journal::operation::{GroupDelete, GroupMember, GroupPlan, GroupTarget};
use ozzy_proto::{ConsumerGroupId, OperationId, PartitionIncarnation, group::Start};
use std::{collections::BTreeSet, sync::Arc};

mod planner;
pub use planner::group_targets;

/// Immutable coordinator image shared by committed and speculative state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorGroup {
    /// Partition that journals this group's membership and plans.
    pub coordinator: PartitionIncarnation,
    /// Group incarnation, independent of its human-readable name.
    pub group: ConsumerGroupId,
    /// Topic-scoped group name.
    pub name: String,
    /// Committed or speculative coordinator plan revision.
    pub revision: u64,
    /// First position of sources without a cursor.
    pub initial: Start,
    /// Processing instances in strictly increasing member ID order.
    pub members: Vec<GroupMember>,
    /// Intended source ownership; source replies prove activation separately.
    pub targets: Vec<GroupTarget>,
    /// Last membership transition's retry identity.
    pub operation: OperationId,
}

impl CoordinatorGroup {
    /// Canonical image used for checkpoint encoding and inspection.
    pub fn image(&self) -> GroupPlan<'_> {
        GroupPlan {
            coordinator: self.coordinator,
            group: self.group,
            name: &self.name,
            expected_revision: self.revision - 1,
            revision: self.revision,
            initial: self.initial,
            members: self.members.clone(),
            targets: self.targets.clone(),
            operation_id: self.operation,
        }
    }

    /// Current intent for one source, if the topology contains it.
    pub fn target(&self, partition: PartitionIncarnation) -> Option<&GroupTarget> {
        self.targets
            .iter()
            .find(|target| target.partition == partition)
    }

    pub(super) fn from_plan(plan: &GroupPlan<'_>) -> Self {
        Self {
            coordinator: plan.coordinator,
            group: plan.group,
            name: plan.name.to_owned(),
            revision: plan.revision,
            initial: plan.initial,
            members: plan.members.clone(),
            targets: plan.targets.clone(),
            operation: plan.operation_id,
        }
    }
}

impl CanonicalState {
    /// Inspect one group incarnation on its coordinator.
    pub fn coordinator_group(&self, group: ConsumerGroupId) -> Option<&Arc<CoordinatorGroup>> {
        self.groups.get(&group)
    }

    /// Resolve a topic-scoped group name on its deterministic coordinator.
    pub fn coordinator_group_by_name(
        &self,
        coordinator: PartitionIncarnation,
        name: &str,
    ) -> Option<&Arc<CoordinatorGroup>> {
        self.groups
            .values()
            .find(|g| g.coordinator == coordinator && g.name == name)
    }

    pub(super) fn prepare_group_plan(
        &self,
        plan: &GroupPlan<'_>,
        op: u64,
        identities: &impl IdentityIndex,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        self.require_partition(plan.coordinator)?;
        validate_image(plan)?;
        let prior = self.coordinator_group(plan.group).map(Arc::as_ref);
        if prior.map_or(0, |g| g.revision) != plan.expected_revision {
            return Err(StateError::GroupRevisionMismatch);
        }
        if prior.is_some_and(|g| {
            g.coordinator != plan.coordinator || g.name != plan.name || g.initial != plan.initial
        }) || self
            .coordinator_group_by_name(plan.coordinator, plan.name)
            .is_some_and(|g| g.group != plan.group)
        {
            return Err(StateError::GroupIdentityMismatch);
        }
        if prior.is_none() && self.groups.len() >= self.limits.max_groups {
            return Err(StateError::LimitExceeded("consumer groups"));
        }
        let members = self
            .group_member_count
            .checked_sub(prior.map_or(0, |g| g.members.len()))
            .and_then(|count| count.checked_add(plan.members.len()))
            .ok_or(StateError::InvalidGroupPlan)?;
        let sources = self
            .group_target_count
            .checked_sub(prior.map_or(0, |g| g.targets.len()))
            .and_then(|count| count.checked_add(plan.targets.len()))
            .ok_or(StateError::InvalidGroupPlan)?;
        if members > self.limits.max_group_members {
            return Err(StateError::LimitExceeded("group members"));
        }
        if sources > self.limits.max_group_partitions {
            return Err(StateError::LimitExceeded("group source partitions"));
        }
        let partitions: Vec<_> = plan.targets.iter().map(|target| target.partition).collect();
        if group_targets(prior, &plan.members, &partitions, plan.revision)? != plan.targets {
            return Err(StateError::InvalidGroupPlan);
        }
        let claims = operation_claim(plan.operation_id, op, identities)?;
        Ok((
            Mutation::GroupPlan {
                value: Arc::new(CoordinatorGroup::from_plan(plan)),
                members,
                sources,
            },
            claims,
        ))
    }
}

impl CanonicalState {
    /// Retire the committed incarnation at exactly the revision the
    /// coordinator observed. Its name becomes free for a new incarnation.
    pub(super) fn prepare_group_delete(
        &self,
        value: GroupDelete,
        op: u64,
        identities: &impl IdentityIndex,
    ) -> Result<(Mutation, Vec<IdentityClaim>), StateError> {
        self.require_partition(value.coordinator)?;
        for (kind, bytes) in [
            ("coordinator", value.coordinator.as_bytes()),
            ("consumer group", value.group.as_bytes()),
            ("group operation", value.operation_id.as_bytes()),
        ] {
            require_id(kind, bytes)?;
        }
        let group = self
            .coordinator_group(value.group)
            .ok_or(StateError::GroupIdentityMismatch)?;
        if group.coordinator != value.coordinator {
            return Err(StateError::GroupIdentityMismatch);
        }
        if group.revision != value.revision {
            return Err(StateError::GroupRevisionMismatch);
        }
        let members = self
            .group_member_count
            .checked_sub(group.members.len())
            .ok_or(StateError::InvalidGroupPlan)?;
        let sources = self
            .group_target_count
            .checked_sub(group.targets.len())
            .ok_or(StateError::InvalidGroupPlan)?;
        let claims = operation_claim(value.operation_id, op, identities)?;
        Ok((
            Mutation::GroupDelete {
                group: value.group,
                members,
                sources,
            },
            claims,
        ))
    }
}

pub(super) fn validate_image(plan: &GroupPlan<'_>) -> Result<(), StateError> {
    for (kind, bytes) in [
        ("coordinator", plan.coordinator.as_bytes()),
        ("consumer group", plan.group.as_bytes()),
        ("group operation", plan.operation_id.as_bytes()),
    ] {
        require_id(kind, bytes)?;
    }
    if plan.name.is_empty()
        || plan.name.len() > 255
        || plan.expected_revision.checked_add(1) != Some(plan.revision)
        || plan.targets.is_empty()
        || plan.targets.len() > ozzy_journal::operation::GROUP_PLAN_MAX_PARTITIONS
        || plan.members.len() > ozzy_journal::operation::GROUP_PLAN_MAX_MEMBERS
    {
        return Err(StateError::InvalidGroupPlan);
    }
    let mut previous = None;
    for member in &plan.members {
        validate_member(*member)?;
        if previous.is_some_and(|id| id >= member.id) {
            return Err(StateError::InvalidGroupPlan);
        }
        previous = Some(member.id);
    }
    let mut partitions = BTreeSet::new();
    let mut counts = vec![0usize; plan.members.len()];
    for target in &plan.targets {
        require_id("group source partition", target.partition.as_bytes())?;
        if !partitions.insert(target.partition)
            || target.plan_revision == 0
            || target.plan_revision > plan.revision
            || target
                .prior_member
                .is_some_and(|member| Some(member) == target.member)
        {
            return Err(StateError::InvalidGroupPlan);
        }
        if let Some(member) = target.member {
            let index = plan
                .members
                .binary_search_by_key(&member.id, |m| m.id)
                .map_err(|_| StateError::InvalidGroupPlan)?;
            if plan.members.get(index) != Some(&member) {
                return Err(StateError::InvalidGroupPlan);
            }
            *counts.get_mut(index).ok_or(StateError::InvalidGroupPlan)? += 1;
        } else if !plan.members.is_empty() {
            return Err(StateError::InvalidGroupPlan);
        }
        if let Some(prior) = target.prior_member {
            validate_member(prior)?;
        }
    }
    if !partitions.contains(&plan.coordinator)
        || counts.iter().max().copied().unwrap_or(0) - counts.iter().min().copied().unwrap_or(0) > 1
    {
        return Err(StateError::InvalidGroupPlan);
    }
    Ok(())
}

fn validate_member(member: GroupMember) -> Result<(), StateError> {
    require_id("consumer member", member.id.as_bytes())?;
    require_id("consumer node", member.node.as_bytes())?;
    require_id("consumer join", member.join.as_bytes())
}

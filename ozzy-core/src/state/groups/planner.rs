use super::{CoordinatorGroup, GroupMember, GroupTarget, StateError};
use ozzy_proto::PartitionIncarnation;
use std::{cmp::Reverse, collections::BTreeSet};

/// Deterministic balanced assignment that retains the maximum existing owners.
/// A restarted instance has a new join identity, so every source it owned moves.
pub fn group_targets(
    prior: Option<&CoordinatorGroup>,
    members: &[GroupMember],
    partitions: &[PartitionIncarnation],
    revision: u64,
) -> Result<Vec<GroupTarget>, StateError> {
    validate_inputs(prior, members, partitions, revision)?;
    let owner = |target: &GroupTarget| {
        target.member.and_then(|member| {
            members
                .binary_search_by_key(&member.id, |m| m.id)
                .ok()
                .filter(|&i| members.get(i) == Some(&member))
        })
    };
    let prior_targets = prior.map_or(&[][..], |prior| prior.targets.as_slice());
    let owners: Vec<_> = prior_targets.iter().map(owner).collect();
    let mut remaining = quotas(members, partitions.len(), &owners);
    let mut desired = vec![None; partitions.len()];
    // Incumbents keep what their quota allows.
    for (slot, &owned) in desired.iter_mut().zip(&owners) {
        if let Some(i) = owned
            && let (Some(quota), Some(&member)) = (remaining.get_mut(i), members.get(i))
            && *quota > 0
        {
            *slot = Some(member);
            *quota -= 1;
        }
    }
    // Free slots go to members with quota left, in member order. The quotas
    // sum to the slot count, so every slot gets a member.
    let mut quotas = remaining.iter_mut().zip(members);
    let mut current = quotas.next();
    for slot in desired.iter_mut().filter(|slot| slot.is_none()) {
        while let Some((quota, _)) = &current
            && **quota == 0
        {
            current = quotas.next();
        }
        let Some((quota, member)) = current.as_mut() else {
            break;
        };
        *slot = Some(**member);
        **quota -= 1;
    }
    Ok(partitions
        .iter()
        .zip(desired)
        .enumerate()
        .map(|(i, (&partition, member))| {
            let old = prior_targets.get(i).copied();
            match old.filter(|old| old.member == member) {
                Some(old) => old,
                None => GroupTarget {
                    partition,
                    plan_revision: revision,
                    member,
                    prior_member: old.and_then(|target| target.member),
                },
            }
        })
        .collect())
}

fn validate_inputs(
    prior: Option<&CoordinatorGroup>,
    members: &[GroupMember],
    partitions: &[PartitionIncarnation],
    revision: u64,
) -> Result<(), StateError> {
    if partitions.is_empty()
        || partitions.len() > ozzy_journal::operation::GROUP_PLAN_MAX_PARTITIONS
        || members.len() > ozzy_journal::operation::GROUP_PLAN_MAX_MEMBERS
        || revision == 0
        || prior.is_some_and(|g| g.revision.checked_add(1) != Some(revision))
        || partitions.iter().any(|p| p.as_bytes() == &[0; 16])
        || partitions.iter().copied().collect::<BTreeSet<_>>().len() != partitions.len()
        || members
            .windows(2)
            .any(|pair| matches!(pair, [left, right] if left.id >= right.id))
    {
        return Err(StateError::InvalidGroupPlan);
    }
    for &member in members {
        super::validate_member(member)?;
    }
    if prior.is_some_and(|g| {
        g.targets.len() != partitions.len()
            || g.targets
                .iter()
                .zip(partitions)
                .any(|(target, partition)| target.partition != *partition)
    }) {
        return Err(StateError::GroupIdentityMismatch);
    }
    Ok(())
}

/// How many partitions each member owns: an equal share, with the extra
/// slots awarded to incumbents first. That avoids a needless move when a
/// newly joined member sorts before an existing one.
fn quotas(members: &[GroupMember], partitions: usize, owners: &[Option<usize>]) -> Vec<usize> {
    let mut remaining = vec![0usize; members.len()];
    if members.is_empty() {
        return remaining;
    }
    let mut counts = vec![0usize; members.len()];
    for &i in owners.iter().flatten() {
        if let Some(count) = counts.get_mut(i) {
            *count += 1;
        }
    }
    remaining.fill(partitions / members.len());
    // Member IDs are unique, so the index never breaks a tie.
    let mut order: Vec<_> = counts
        .iter()
        .zip(members)
        .enumerate()
        .map(|(i, (&count, member))| (Reverse(count), member.id, i))
        .collect();
    order.sort_unstable();
    for &(_, _, i) in order.iter().take(partitions % members.len()) {
        if let Some(quota) = remaining.get_mut(i) {
            *quota += 1;
        }
    }
    remaining
}

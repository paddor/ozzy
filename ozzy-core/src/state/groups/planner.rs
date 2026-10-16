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
    if partitions.is_empty()
        || partitions.len() > ozzy_journal::operation::GROUP_PLAN_MAX_PARTITIONS
        || members.len() > ozzy_journal::operation::GROUP_PLAN_MAX_MEMBERS
        || revision == 0
        || prior.is_some_and(|g| g.revision.checked_add(1) != Some(revision))
        || partitions.iter().any(|p| p.as_bytes() == &[0; 16])
        || partitions.iter().copied().collect::<BTreeSet<_>>().len() != partitions.len()
        || members.windows(2).any(|pair| pair[0].id >= pair[1].id)
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
    let mut counts = vec![0usize; members.len()];
    let owner = |target: &GroupTarget| {
        target.member.and_then(|member| {
            members
                .binary_search_by_key(&member.id, |m| m.id)
                .ok()
                .filter(|&i| members[i] == member)
        })
    };
    if let Some(prior) = prior {
        for target in &prior.targets {
            if let Some(i) = owner(target) {
                counts[i] += 1;
            }
        }
    }
    let mut remaining = vec![0usize; members.len()];
    if !members.is_empty() {
        remaining.fill(partitions.len() / members.len());
        // Award extra slots to incumbents first, avoiding an otherwise needless
        // move when a newly joined member sorts before an existing one.
        let mut order: Vec<_> = (0..members.len()).collect();
        order.sort_by_key(|&i| (Reverse(counts[i]), members[i].id));
        for &i in order.iter().take(partitions.len() % members.len()) {
            remaining[i] += 1;
        }
    }
    let mut desired = vec![None; partitions.len()];
    if let Some(prior) = prior {
        for (slot, target) in prior.targets.iter().enumerate() {
            if let Some(i) = owner(target).filter(|&i| remaining[i] > 0) {
                desired[slot] = Some(members[i]);
                remaining[i] -= 1;
            }
        }
    }
    let mut next = 0;
    for member in &mut desired {
        if member.is_some() || members.is_empty() {
            continue;
        }
        while remaining[next] == 0 {
            next += 1;
        }
        *member = Some(members[next]);
        remaining[next] -= 1;
    }
    Ok(partitions
        .iter()
        .zip(desired)
        .enumerate()
        .map(|(i, (&partition, member))| {
            let old = prior.map(|g| g.targets[i]);
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

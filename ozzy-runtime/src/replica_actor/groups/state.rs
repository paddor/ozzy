//! Volatile membership observations beside one committed coordinator image.
//! Nothing here is authority: plans commit through the journal and sources
//! fence stale revisions themselves.

use ozzy_core::state::CoordinatorGroup;
use ozzy_journal::operation::{GroupMember, GroupTarget};
use ozzy_proto::{
    ConsumerMemberId, NodeId, OperationId, PartitionIncarnation,
    group::{Mode, Owned, Start, Target},
};
use std::{collections::BTreeMap, time::Duration};

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub(super) struct LiveMember {
    pub join: OperationId,
    pub node: NodeId,
    pub seen: Duration,
    pub owned: Vec<Owned>,
    pub ownership_observed: bool,
    pub leaving: bool,
}

/// One group name on this coordinator while this actor serves as leader.
#[derive(Debug, Default)]
pub(super) struct GroupState {
    pub members: BTreeMap<ConsumerMemberId, LiveMember>,
    /// First observation of each moved target, for the drain deadline.
    pub drains: BTreeMap<(PartitionIncarnation, u64), Duration>,
    /// Plan revision proposed and not yet settled.
    pub pending: Option<u64>,
    /// Deletion of the committed incarnation proposed and not yet settled.
    /// No plan is proposed meanwhile.
    pub deleting: bool,
    /// Creation selector requested by the first joiner.
    pub initial: Start,
    /// Members adopted from the committed image under the current leadership.
    pub adopted_revision: Option<u64>,
}

/// Outcome of one membership request before encoding.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Disposition {
    /// Another instance of the same member ID joined later.
    Fenced,
    /// Reply with the committed view: status and this instance's targets.
    Reply(ozzy_proto::group::Status, Vec<Target>),
}

impl GroupState {
    /// Every committed member starts with a full membership timeout under a
    /// new leader; their last heartbeat is unknown.
    pub(super) fn adopt(&mut self, image: &CoordinatorGroup, now: Duration) {
        if self.adopted_revision == Some(image.revision) {
            return;
        }
        self.adopted_revision = Some(image.revision);
        self.initial = image.initial;
        for member in &image.members {
            let live = self.members.entry(member.id).or_insert_with(|| LiveMember {
                join: member.join,
                node: member.node,
                seen: now,
                owned: Vec::new(),
                ownership_observed: false,
                leaving: false,
            });
            if live.join == member.join {
                live.node = member.node;
            }
        }
        // A committed removal retires the leaver's volatile entry.
        self.members.retain(|id, live| {
            !live.leaving
                || image
                    .members
                    .iter()
                    .any(|member| member.id == *id && member.join == live.join)
        });
    }

    pub(super) fn expire(&mut self, now: Duration, timeout: Duration) -> bool {
        let before = self.members.len();
        self.members
            .retain(|_, live| now.saturating_sub(live.seen) < timeout);
        self.members.len() != before
    }

    /// Record one request and decide its reply. Joins supersede earlier
    /// instances of the same member ID; heartbeats of unknown instances are fenced.
    pub(super) fn observe(
        &mut self,
        image: Option<&CoordinatorGroup>,
        request: ozzy_proto::group::JoinRequest,
        node: NodeId,
        now: Duration,
        drain_deadline: Duration,
        number: &impl Fn(PartitionIncarnation) -> Option<u32>,
    ) -> Disposition {
        let ozzy_proto::group::JoinRequest {
            member,
            join,
            mode,
            initial,
            owned,
            ..
        } = request;
        match (mode, self.members.get_mut(&member)) {
            (Mode::Join, Some(live)) if live.join != join => {
                *live = LiveMember {
                    join,
                    node,
                    seen: now,
                    owned,
                    ownership_observed: true,
                    leaving: false,
                };
            }
            (Mode::Join, Some(live)) => {
                live.seen = now;
                live.node = node;
                live.owned = owned;
                live.ownership_observed = true;
            }
            (Mode::Join, None) => {
                if image.is_none() && self.members.is_empty() {
                    self.initial = initial;
                }
                self.members.insert(
                    member,
                    LiveMember {
                        join,
                        node,
                        seen: now,
                        owned,
                        ownership_observed: true,
                        leaving: false,
                    },
                );
            }
            (Mode::Heartbeat | Mode::Leave, Some(live)) if live.join == join => {
                live.seen = now;
                live.node = node;
                live.owned = owned;
                live.ownership_observed = true;
                live.leaving |= mode == Mode::Leave;
            }
            (Mode::Heartbeat | Mode::Leave, _) => return Disposition::Fenced,
        }
        let Some(image) = image else {
            return Disposition::Reply(
                if mode == Mode::Join {
                    ozzy_proto::group::Status::Joining
                } else {
                    ozzy_proto::group::Status::Unknown
                },
                Vec::new(),
            );
        };
        let instance = GroupMember {
            id: member,
            node,
            join,
        };
        if !image.members.contains(&instance) {
            return Disposition::Reply(ozzy_proto::group::Status::Joining, Vec::new());
        }
        let targets = image
            .targets
            .iter()
            .filter(|target| target.member == Some(instance))
            .filter_map(|target| {
                Some(Target {
                    partition: target.partition,
                    number: number(target.partition)?,
                    plan_revision: target.plan_revision,
                    activatable: self.activatable(target, now, drain_deadline),
                    prior_member: target.prior_member.map(|prior| prior.id),
                })
            })
            .collect();
        Disposition::Reply(ozzy_proto::group::Status::Member, targets)
    }

    fn activatable(&mut self, target: &GroupTarget, now: Duration, deadline: Duration) -> bool {
        let Some(prior) = target.prior_member else {
            return true;
        };
        let first = *self
            .drains
            .entry((target.partition, target.plan_revision))
            .or_insert(now);
        if now.saturating_sub(first) >= deadline {
            return true;
        }
        match self.members.get(&prior.id) {
            // Left, timed out, or replaced by a newer instance: nothing to drain.
            None => true,
            Some(live) if live.join != prior.join || live.leaving => true,
            Some(live) => {
                live.ownership_observed
                    && !live.owned.iter().any(|owned| {
                        owned.partition == target.partition
                            && owned.plan_revision < target.plan_revision
                    })
            }
        }
    }

    /// Members the next plan should contain, or `None` when the committed
    /// image already matches the live observations.
    pub(super) fn desired(&self, image: Option<&CoordinatorGroup>) -> Option<Vec<GroupMember>> {
        let desired: Vec<GroupMember> = self
            .members
            .iter()
            .filter(|(_, live)| !live.leaving)
            .map(|(&id, live)| GroupMember {
                id,
                node: live.node,
                join: live.join,
            })
            .collect();
        let current = image.map_or(&[][..], |image| image.members.as_slice());
        if desired == current && (image.is_some() || desired.is_empty()) {
            return None;
        }
        if image.is_none() && desired.is_empty() {
            return None;
        }
        Some(desired)
    }
}

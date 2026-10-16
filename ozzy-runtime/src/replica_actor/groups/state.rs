//! Volatile membership observations beside one committed coordinator image.
//! Nothing here is authority: plans commit through the journal and sources
//! fence stale revisions themselves.

use crate::replica_actor::deadline::Earliest;
use ozzy_core::state::CoordinatorGroup;
use ozzy_journal::operation::{GroupMember, GroupTarget};
use ozzy_proto::{
    ConsumerMemberId, NodeId, OperationId, PartitionIncarnation,
    group::{JoinRequest, Mode, Owned, Start, Status, Target},
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

impl LiveMember {
    /// A member instance that just reported what it owns.
    fn new(join: OperationId, node: NodeId, now: Duration, owned: Vec<Owned>) -> Self {
        Self {
            join,
            node,
            seen: now,
            owned,
            ownership_observed: true,
            leaving: false,
        }
    }

    fn refresh(&mut self, node: NodeId, now: Duration, owned: Vec<Owned>) {
        self.seen = now;
        self.node = node;
        self.owned = owned;
        self.ownership_observed = true;
    }
}

/// One membership request as the coordinator received it.
pub(super) struct Observation {
    pub request: JoinRequest,
    pub node: NodeId,
    pub now: Duration,
}

/// How reply targets are numbered and when moved ones may activate.
pub(super) struct Targeting<'a> {
    pub number: &'a dyn Fn(PartitionIncarnation) -> Option<u32>,
    pub drain_deadline: Duration,
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
    Reply(Status, Vec<Target>),
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
                ownership_observed: false,
                ..LiveMember::new(member.join, member.node, now, Vec::new())
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

    /// Add each member's expiry and each moved target's drain deadline.
    pub(super) fn deadlines(&self, next: Earliest, timeout: Duration, drain: Duration) -> Earliest {
        next.each(
            self.members
                .values()
                .map(|live| live.seen.saturating_add(timeout)),
        )
        .each(
            self.drains
                .values()
                .map(|first| first.saturating_add(drain)),
        )
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
        seen: Observation,
        targeting: &Targeting<'_>,
    ) -> Disposition {
        let Observation { request, node, now } = seen;
        let instance = GroupMember {
            id: request.member,
            node,
            join: request.join,
        };
        let mode = request.mode;
        if !self.record(image, request, node, now) {
            return Disposition::Fenced;
        }
        let Some(image) = image else {
            let status = if mode == Mode::Join {
                Status::Joining
            } else {
                Status::Unknown
            };
            return Disposition::Reply(status, Vec::new());
        };
        if !image.members.contains(&instance) {
            return Disposition::Reply(Status::Joining, Vec::new());
        }
        let targets = image
            .targets
            .iter()
            .filter(|target| target.member == Some(instance))
            .filter_map(|target| {
                Some(Target {
                    partition: target.partition,
                    number: (targeting.number)(target.partition)?,
                    plan_revision: target.plan_revision,
                    activatable: self.activatable(target, now, targeting.drain_deadline),
                    prior_member: target.prior_member.map(|prior| prior.id),
                })
            })
            .collect();
        Disposition::Reply(Status::Member, targets)
    }

    /// Update the live member a request names. `false` fences a heartbeat or
    /// leave from an instance this coordinator does not hold.
    fn record(
        &mut self,
        image: Option<&CoordinatorGroup>,
        request: JoinRequest,
        node: NodeId,
        now: Duration,
    ) -> bool {
        let JoinRequest {
            member,
            join,
            mode,
            initial,
            owned,
            ..
        } = request;
        match (mode, self.members.get_mut(&member)) {
            (Mode::Join, Some(live)) if live.join != join => {
                *live = LiveMember::new(join, node, now, owned);
            }
            (Mode::Join, Some(live)) => live.refresh(node, now, owned),
            (Mode::Join, None) => {
                if image.is_none() && self.members.is_empty() {
                    self.initial = initial;
                }
                self.members
                    .insert(member, LiveMember::new(join, node, now, owned));
            }
            (Mode::Heartbeat | Mode::Leave, Some(live)) if live.join == join => {
                live.refresh(node, now, owned);
                live.leaving |= mode == Mode::Leave;
            }
            (Mode::Heartbeat | Mode::Leave, _) => return false,
        }
        true
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

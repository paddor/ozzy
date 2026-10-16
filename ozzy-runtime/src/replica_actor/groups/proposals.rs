//! Plan and deletion proposals, reply delivery, and membership expiry.

use super::{GroupCoordinator, Reply};
use crate::{
    frontend::{Kind, Link},
    replica_actor::{
        NativeIntakeError, PartitionActor, PendingProposal, ProposalOutcome, response,
    },
};
use omq_tokio::{Message, TrySendError};
use ozzy_core::state::{CoordinatorGroup, group_targets};
use ozzy_journal::operation::{GroupPlan, OperationBody, OperationKind, encode_operation_body};
use ozzy_proto::{ConsumerGroupId, NodeId, OperationId, nack::RetryClass};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

/// Remove and return the next item whose proposal settled.
fn next_settled<T>(
    items: &mut Vec<T>,
    cx: &mut Context<'_>,
    pending: impl Fn(&mut T) -> &mut Pin<Box<PendingProposal>>,
) -> Option<(T, <PendingProposal as Future>::Output)> {
    let index =
        (0..items.len()).find_map(|index| match pending(&mut items[index]).as_mut().poll(cx) {
            Poll::Ready(result) => Some((index, result)),
            Poll::Pending => None,
        })?;
    Some((items.swap_remove(index.0), index.1))
}

impl GroupCoordinator {
    /// Settle plan and deletion proposals, and expire silent members. Timers
    /// come from the supplied observation; plans use the actor's lane.
    /// Send replies first with [`Self::flush_replies`].
    pub fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
        now: Duration,
        actor: &mut PartitionActor,
    ) -> Result<bool, NativeIntakeError> {
        let mut changed = false;
        while let Some((proposal, result)) =
            next_settled(&mut self.proposals, cx, |proposal| &mut proposal.pending)
        {
            // A stale revision loses to a committed plan; the next request
            // or timer reads the committed image again.
            let mut reply = result?;
            if let Some(state) = self.groups.get_mut(&proposal.name) {
                state.pending = None;
            }
            reply.buffer.clear();
            self.arenas.push(reply.buffer);
            changed = true;
        }
        changed |= self.settle_deletions(cx)?;
        Ok(self.refresh(now, actor) | changed)
    }

    /// Send queued replies while their clients' links stay current. `current`
    /// returns a peer's latest link.
    pub fn flush_replies(
        &mut self,
        cx: &mut Context<'_>,
        mut current: impl FnMut(NodeId) -> Option<Link>,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, NativeIntakeError> {
        let mut changed = false;
        while let Some(reply) = self.replies.pop_front() {
            if !current(reply.peer).is_some_and(|link| {
                link.binding.kind == Kind::Client && link.binding.session == reply.session
            }) {
                changed = true;
                continue;
            }
            match try_send(cx, reply.frame) {
                Ok(()) => changed = true,
                Err(TrySendError::Full(frame)) => {
                    self.replies.push_front(Reply { frame, ..reply });
                    break;
                }
                Err(TrySendError::Closed) => return Err(omq_tokio::Error::Closed.into()),
                Err(TrySendError::Error(error)) => return Err(error.into()),
            }
        }
        Ok(changed)
    }

    /// Answer settled deletion proposals. A committed deletion drops the
    /// volatile group state; anything else lets planning resume.
    fn settle_deletions(&mut self, cx: &mut Context<'_>) -> Result<bool, NativeIntakeError> {
        let mut changed = false;
        while let Some((deletion, result)) =
            next_settled(&mut self.deletions, cx, |deletion| &mut deletion.pending)
        {
            let mut reply = result?;
            let requester = &deletion.requester;
            if let ProposalOutcome::Committed { scope, .. } = reply.outcome {
                // Old members' observations must not recreate the group.
                self.groups.remove(&deletion.name);
                self.deleted(requester, response::authority(scope), deletion.group, true)?;
            } else {
                if let Some(state) = self.groups.get_mut(&deletion.name) {
                    state.deleting = false;
                }
                let rejection = match &reply.outcome {
                    ProposalOutcome::NotAdmitted => (12, RetryClass::AfterAuthorityRefresh),
                    ProposalOutcome::Invalid(error) => response::rejection(error),
                    ProposalOutcome::Unknown | ProposalOutcome::Committed { .. } => {
                        (13, RetryClass::UnknownOutcome)
                    }
                };
                self.reject(requester, rejection)?;
            }
            reply.buffer.clear();
            self.arenas.push(reply.buffer);
            changed = true;
        }
        Ok(changed)
    }

    /// Reread each observed group's committed image, expire silent members,
    /// and plan where observations differ. Losing leadership drops them all.
    fn refresh(&mut self, now: Duration, actor: &mut PartitionActor) -> bool {
        let Some((Some(ticket), journal)) = actor.read_access() else {
            let lost = self.scope.take().is_some();
            if lost {
                self.groups.clear();
            }
            return lost;
        };
        self.observe_scope(ticket.scope());
        let mut changed = false;
        let names: Vec<String> = self.groups.keys().cloned().collect();
        for name in names {
            // Busy or stopped. This visit's actor poll registered the slot for
            // the owner's return, which retries the remaining groups.
            let Ok(image) = journal.coordinator_group(ticket, self.config.partition, &name) else {
                break;
            };
            let Some(state) = self.groups.get_mut(&name) else {
                continue;
            };
            if let Some(image) = &image {
                state.adopt(image, now);
            }
            changed |= state.expire(now, self.config.membership_timeout);
            if image.is_none() && state.members.is_empty() {
                self.groups.remove(&name);
                continue;
            }
            changed |= self.plan(&name, image.as_deref());
        }
        changed
    }

    /// Propose the next revision when live observations differ from the
    /// committed image. One proposal per group is outstanding at a time.
    pub(super) fn plan(&mut self, name: &str, image: Option<&CoordinatorGroup>) -> bool {
        let Some(state) = self.groups.get_mut(name) else {
            return false;
        };
        if state.pending.is_some() || state.deleting || self.arenas.is_empty() {
            return false;
        }
        let Some(members) = state.desired(image) else {
            return false;
        };
        let revision = image.map_or(1, |image| image.revision + 1);
        let Ok(targets) = group_targets(
            image,
            &members,
            &self.config.topology.incarnations(),
            revision,
        ) else {
            return false;
        };
        let plan = GroupPlan {
            coordinator: self.config.partition,
            group: image.map_or_else(ConsumerGroupId::new, |image| image.group),
            name,
            expected_revision: revision - 1,
            revision,
            initial: image.map_or(state.initial, |image| image.initial),
            members,
            targets,
            operation_id: OperationId::new(),
        };
        let Ok(body) = encode_operation_body(
            &OperationBody::GroupPlan(plan),
            ozzy_journal::operation::OperationLimits {
                max_group_members: self.config.max_members,
                ..Default::default()
            },
        ) else {
            return false;
        };
        let Some(pending) = self.submit_body(OperationKind::GroupPlan, &body) else {
            return false;
        };
        if let Some(state) = self.groups.get_mut(name) {
            state.pending = Some(revision);
        }
        self.proposals.push(super::PlanProposal {
            name: name.to_owned(),
            pending,
        });
        true
    }

    /// Submit one encoded coordinator operation in a free arena. `None` when
    /// no arena or proposal lane is free; the arena returns to the pool.
    pub(super) fn submit_body(
        &mut self,
        kind: OperationKind,
        body: &[u8],
    ) -> Option<Pin<Box<PendingProposal>>> {
        let mut arena = self.arenas.pop()?;
        arena.clear();
        if arena.push(kind, body).is_err() {
            arena.clear();
            self.arenas.push(arena);
            return None;
        }
        match self.submitter.try_submit(arena) {
            Ok(pending) => Some(Box::pin(pending)),
            Err(mut unsubmitted) => {
                unsubmitted.buffer.clear();
                self.arenas.push(unsubmitted.buffer);
                None
            }
        }
    }
}

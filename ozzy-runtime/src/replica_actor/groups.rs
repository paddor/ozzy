//! Consumer-group coordination on the partition that owns a group's plans.
//!
//! The service answers membership requests from the committed image, proposes
//! plan revisions through the actor's ordinary journal pipeline, and tracks
//! heartbeats, owned sources, and drain deadlines in volatile state. Members
//! relay committed targets to sources themselves; nothing here contacts another
//! partition. Losing leadership discards every observation.

mod state;

use super::{
    NativeIntakeError, NativeReceive, PartitionActor, PendingProposal, ProposalOutcome,
    ProposalSubmitter, response,
};
use crate::{
    frontend::{Kind, Link},
    replica_journal::{JournalError, ProposalBuffer},
};
use bytes::Bytes;
use omq_tokio::{Message, TrySendError};
use ozzy_core::state::{CoordinatorGroup, group_targets};
use ozzy_journal::operation::{
    GroupDelete, GroupPlan, OperationBody, OperationKind, encode_operation_body,
};
use ozzy_proto::{
    ConsumerGroupId, Envelope, EnvelopeLimits, GroupId, LinkSessionId, NodeId, Opcode, OperationId,
    Packet, PartitionIncarnation,
    append::Authority,
    decode_packet,
    group::{self, Start, Status},
    handshake,
    nack::{AuthorityHint, RetryClass},
};
use ozzy_replication::Scope;
use state::{Disposition, GroupState};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
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

/// Requester of a deletion, answered once its proposal settles.
struct Requester {
    envelope: Envelope,
    session: LinkSessionId,
    send: EnvelopeLimits,
    hint: AuthorityHint,
}

struct DeleteProposal {
    name: String,
    group: ConsumerGroupId,
    requester: Requester,
    pending: Pin<Box<PendingProposal>>,
}

/// Common request checks either produce the committed image or an answer.
enum Committed {
    Image(Option<Arc<CoordinatorGroup>>),
    Answered(NativeReceive),
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

    /// Answer one membership, inspection, or deletion request. Membership
    /// and inspection replies come from the committed image and never wait
    /// for a proposal. A deletion reply follows its proposal's confirmation.
    pub fn receive(
        &mut self,
        message: &Message,
        link: Link,
        hint: AuthorityHint,
        actor: &mut PartitionActor,
        now: Duration,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let Some(packet) = self.packet(message, link) else {
            return Ok(NativeReceive::Ignored);
        };
        if self.replies.len() >= self.config.requests {
            return Ok(NativeReceive::Busy);
        }
        if link.send.envelope.max_metadata_bytes < group::JOINED_FIXED_BYTES {
            return Ok(NativeReceive::Ignored);
        }
        let envelope = packet.envelope;
        if link.remote.roles & handshake::CONSUMER == 0
            || link.remote.capabilities & handshake::OWNER_GROUPS == 0
        {
            return self.reject(envelope, link, 3, RetryClass::Permanent, hint);
        }
        match envelope.opcode {
            Opcode::GroupInspect => return self.inspect(packet, link, hint, actor),
            Opcode::GroupDelete => return self.delete(packet, link, hint, actor),
            _ => {}
        }
        let Ok(request) = group::decode_join(packet, self.config.limits) else {
            return self.reject(envelope, link, 1, RetryClass::Permanent, hint);
        };
        let image = match self.committed(
            envelope,
            link,
            hint,
            actor,
            (request.authority, request.coordinator, &request.name),
        )? {
            Committed::Image(image) => image,
            Committed::Answered(receive) => return Ok(receive),
        };
        // An instance of a retired incarnation stops instead of creating a
        // new one; a fresh reader names no incarnation.
        if request
            .group
            .is_some_and(|group| image.as_ref().is_none_or(|image| image.group != group))
        {
            let reply = group::JoinReply {
                authority: hint.authority,
                coordinator: self.config.partition,
                group: ConsumerGroupId::from_bytes([0; 16]),
                revision: 0,
                status: Status::Deleted,
                targets: Vec::new(),
            };
            return self.reply(envelope, link, &reply);
        }
        let topology = self.config.topology.clone();
        let drain = self.config.drain_deadline;
        let state = self.groups.entry(request.name.clone()).or_default();
        if let Some(image) = &image {
            state.adopt(image, now);
        }
        if request.mode == group::Mode::Join
            && !state.members.contains_key(&request.member)
            && state.members.len() >= self.config.max_members
        {
            return self.reject(envelope, link, 10, RetryClass::AfterBackoff, hint);
        }
        let name = request.name.clone();
        let disposition = state.observe(
            image.as_deref(),
            request,
            envelope.sender,
            now,
            drain,
            &|partition| topology.number(partition),
        );
        let (status, targets) = match disposition {
            Disposition::Fenced => {
                return self.reject(envelope, link, 6, RetryClass::Permanent, hint);
            }
            Disposition::Reply(status, targets) => (status, targets),
        };
        self.plan(&name, image.as_deref());
        let reply = group::JoinReply {
            authority: hint.authority,
            coordinator: self.config.partition,
            group: image
                .as_ref()
                .map_or(ConsumerGroupId::from_bytes([0; 16]), |image| image.group),
            revision: image.as_ref().map_or(0, |image| image.revision),
            status,
            targets,
        };
        debug_assert!(status != Status::Member || reply.revision != 0);
        self.reply(envelope, link, &reply)
    }

    /// Coordinator placement, authority, and read access shared by every
    /// request, then the committed image of the named group.
    fn committed(
        &mut self,
        envelope: Envelope,
        link: Link,
        hint: AuthorityHint,
        actor: &mut PartitionActor,
        (authority, coordinator, name): (Authority, PartitionIncarnation, &str),
    ) -> Result<Committed, NativeIntakeError> {
        let reject = |this: &mut Self, code, retry| {
            this.reject(envelope, link, code, retry, hint)
                .map(Committed::Answered)
        };
        if coordinator != self.config.partition
            || authority.group_id != self.config.group
            || authority != hint.authority
            || hint.primary != self.config.local
        {
            return reject(self, 5, RetryClass::AfterAuthorityRefresh);
        }
        if crate::topic_metadata::coordinator_number(
            name,
            self.config.topology.seed,
            self.config.topology.partitions.len(),
        ) != self.config.number
        {
            return reject(self, 6, RetryClass::Permanent);
        }
        let Some((Some(ticket), journal)) = actor.read_access() else {
            return reject(self, 12, RetryClass::AfterAuthorityRefresh);
        };
        let image = match journal.coordinator_group(ticket, self.config.partition, name) {
            Ok(image) => image,
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(Committed::Answered(NativeReceive::Busy));
            }
            Err(_) => return reject(self, 13, RetryClass::AfterAuthorityRefresh),
        };
        self.observe_scope(ticket.scope());
        Ok(Committed::Image(image))
    }

    /// Describe the committed image; an absent name has no incarnation.
    fn inspect(
        &mut self,
        packet: Packet<'_>,
        link: Link,
        hint: AuthorityHint,
        actor: &mut PartitionActor,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let envelope = packet.envelope;
        let Ok(request) = group::decode_inspect(packet, self.config.limits) else {
            return self.reject(envelope, link, 1, RetryClass::Permanent, hint);
        };
        let image = match self.committed(
            envelope,
            link,
            hint,
            actor,
            (request.authority, request.coordinator, &request.name),
        )? {
            Committed::Image(image) => image,
            Committed::Answered(receive) => return Ok(receive),
        };
        let topology = &self.config.topology;
        let targets = image.as_ref().map_or(Some(Vec::new()), |image| {
            image
                .targets
                .iter()
                .map(|target| {
                    Some(group::InspectedTarget {
                        partition: target.partition,
                        number: topology.number(target.partition)?,
                        plan_revision: target.plan_revision,
                        member: target.member.map(|member| member.id),
                    })
                })
                .collect()
        });
        let Some(targets) = targets else {
            return self.reject(envelope, link, 13, RetryClass::AfterAuthorityRefresh, hint);
        };
        let reply = group::Inspection {
            authority: hint.authority,
            coordinator: self.config.partition,
            group: image.as_ref().map(|image| image.group),
            revision: image.as_ref().map_or(0, |image| image.revision),
            initial: image
                .as_ref()
                .map_or(Start::Earliest, |image| image.initial),
            members: image
                .iter()
                .flat_map(|image| image.members.iter())
                .map(|member| group::InspectedMember {
                    id: member.id,
                    node: member.node,
                    join: member.join,
                })
                .collect(),
            targets,
        };
        if group::inspected_bytes(reply.members.len(), reply.targets.len())?
            > link.send.envelope.max_metadata_bytes
        {
            return self.reject(envelope, link, 1, RetryClass::Permanent, hint);
        }
        let header = group::encode_inspected(
            Envelope {
                opcode: Opcode::GroupInspected,
                response: true,
                sender: self.config.local,
                ..envelope
            },
            &reply,
            &mut self.metadata,
            link.send.envelope,
        )?;
        self.queue(envelope.sender, link.binding.session, header);
        Ok(NativeReceive::Accepted)
    }

    /// Retire the committed incarnation the request names. Sources must have
    /// been revoked first; this only frees the name. A name that no longer
    /// holds the incarnation answers at once.
    fn delete(
        &mut self,
        packet: Packet<'_>,
        link: Link,
        hint: AuthorityHint,
        actor: &mut PartitionActor,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let envelope = packet.envelope;
        let Ok(request) = group::decode_delete(packet, self.config.limits) else {
            return self.reject(envelope, link, 1, RetryClass::Permanent, hint);
        };
        let image = match self.committed(
            envelope,
            link,
            hint,
            actor,
            (request.authority, request.coordinator, &request.name),
        )? {
            Committed::Image(image) => image,
            Committed::Answered(receive) => return Ok(receive),
        };
        let requester = Requester {
            envelope,
            session: link.binding.session,
            send: link.send.envelope,
            hint,
        };
        let Some(image) = image.filter(|image| image.group == request.group) else {
            self.deleted(&requester, hint.authority, request.group, false)?;
            return Ok(NativeReceive::Accepted);
        };
        let state = self.groups.entry(request.name.clone()).or_default();
        // A plan or deletion in flight settles first; the caller retries.
        if state.pending.is_some() || state.deleting || self.arenas.is_empty() {
            return self.reject(envelope, link, 10, RetryClass::AfterBackoff, hint);
        }
        let body = encode_operation_body(
            &OperationBody::GroupDelete(GroupDelete {
                coordinator: self.config.partition,
                group: image.group,
                revision: image.revision,
                operation_id: OperationId::new(),
            }),
            ozzy_journal::operation::OperationLimits::default(),
        )
        .map_err(|_| NativeIntakeError::Configuration)?;
        let mut arena = self.arenas.pop().expect("free plan arena");
        arena.clear();
        if arena.push(OperationKind::GroupDelete, &body).is_err() {
            arena.clear();
            self.arenas.push(arena);
            return self.reject(envelope, link, 10, RetryClass::AfterBackoff, hint);
        }
        match self.submitter.try_submit(arena) {
            Ok(pending) => {
                state.deleting = true;
                self.deletions.push(DeleteProposal {
                    name: request.name,
                    group: image.group,
                    requester,
                    pending: Box::pin(pending),
                });
                Ok(NativeReceive::Accepted)
            }
            Err(mut unsubmitted) => {
                unsubmitted.buffer.clear();
                self.arenas.push(unsubmitted.buffer);
                self.reject(envelope, link, 10, RetryClass::AfterBackoff, hint)
            }
        }
    }

    fn deleted(
        &mut self,
        requester: &Requester,
        authority: Authority,
        group: ConsumerGroupId,
        deleted: bool,
    ) -> Result<(), NativeIntakeError> {
        let header = group::encode_deleted(
            Envelope {
                opcode: Opcode::GroupDeleted,
                response: true,
                sender: self.config.local,
                ..requester.envelope
            },
            group::DeleteReply {
                authority,
                coordinator: self.config.partition,
                group,
                deleted,
            },
            &mut self.metadata,
            requester.send,
        )?;
        self.queue(requester.envelope.sender, requester.session, header);
        Ok(())
    }

    fn queue(
        &mut self,
        peer: NodeId,
        session: LinkSessionId,
        header: [u8; ozzy_proto::ENVELOPE_BYTES],
    ) {
        self.replies.push_back(Reply {
            peer,
            session,
            frame: crate::native_frames::message(
                peer.as_bytes(),
                header,
                &self.metadata,
                Bytes::new(),
            ),
        });
    }

    fn reply(
        &mut self,
        envelope: Envelope,
        link: Link,
        reply: &group::JoinReply,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let header = group::encode_joined(
            Envelope {
                opcode: Opcode::GroupJoined,
                response: true,
                sender: self.config.local,
                ..envelope
            },
            reply,
            &mut self.metadata,
            link.send.envelope,
        )?;
        self.replies.push_back(Reply {
            peer: envelope.sender,
            session: link.binding.session,
            frame: crate::native_frames::message(
                envelope.sender.as_bytes(),
                header,
                &self.metadata,
                Bytes::new(),
            ),
        });
        Ok(NativeReceive::Accepted)
    }

    /// Answer settled deletion proposals. A committed deletion drops the
    /// volatile group state; anything else lets planning resume.
    fn settle_deletions(&mut self, cx: &mut Context<'_>) -> Result<bool, NativeIntakeError> {
        let mut changed = false;
        let mut index = 0;
        while index < self.deletions.len() {
            let Poll::Ready(result) = self.deletions[index].pending.as_mut().poll(cx) else {
                index += 1;
                continue;
            };
            let deletion = self.deletions.swap_remove(index);
            let mut reply = result?;
            let requester = &deletion.requester;
            if let ProposalOutcome::Committed { scope, .. } = reply.outcome {
                // Old members' observations must not recreate the group.
                self.groups.remove(&deletion.name);
                let authority = Authority {
                    group_id: scope.group_id,
                    config_epoch: scope.configuration_epoch,
                    view: scope.view,
                };
                self.deleted(requester, authority, deletion.group, true)?;
            } else {
                if let Some(state) = self.groups.get_mut(&deletion.name) {
                    state.deleting = false;
                }
                let (code, retry) = match &reply.outcome {
                    ProposalOutcome::NotAdmitted => (12, RetryClass::AfterAuthorityRefresh),
                    ProposalOutcome::Invalid(error) => response::rejection(error),
                    ProposalOutcome::Unknown | ProposalOutcome::Committed { .. } => {
                        (13, RetryClass::UnknownOutcome)
                    }
                };
                let frame = response::nack_frame(
                    self.config.local,
                    requester.envelope,
                    requester.send,
                    (code, retry),
                    requester.hint,
                    &[],
                    &mut self.metadata,
                )?;
                self.replies.push_back(Reply {
                    peer: requester.envelope.sender,
                    session: requester.session,
                    frame,
                });
            }
            reply.buffer.clear();
            self.arenas.push(reply.buffer);
            changed = true;
        }
        Ok(changed)
    }

    /// Send queued replies, settle plan proposals, and expire silent members.
    /// Timers come from the supplied observation; plans use the actor's lane.
    pub fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
        now: Duration,
        actor: &mut PartitionActor,
        mut current: impl FnMut(NodeId) -> Option<Link>,
        try_send: &mut impl FnMut(&mut Context<'_>, Message) -> Result<(), TrySendError>,
    ) -> Result<bool, NativeIntakeError> {
        let mut changed = false;
        while let Some(reply) = self.replies.front() {
            if !current(reply.peer).is_some_and(|link| {
                link.binding.kind == Kind::Client && link.binding.session == reply.session
            }) {
                self.replies.pop_front();
                changed = true;
                continue;
            }
            let reply = self.replies.pop_front().expect("queued reply");
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
        let mut index = 0;
        while index < self.proposals.len() {
            let Poll::Ready(result) = self.proposals[index].pending.as_mut().poll(cx) else {
                index += 1;
                continue;
            };
            let proposal = self.proposals.swap_remove(index);
            let mut reply = result?;
            if let Some(state) = self.groups.get_mut(&proposal.name) {
                state.pending = None;
            }
            if let ProposalOutcome::Invalid(JournalError::Images(_)) = reply.outcome {
                // A stale revision loses to a committed plan; the next request
                // or timer reads the committed image again.
            }
            reply.buffer.clear();
            self.arenas.push(reply.buffer);
            changed = true;
        }
        changed |= self.settle_deletions(cx)?;
        let Some((Some(ticket), journal)) = actor.read_access() else {
            if self.scope.take().is_some() {
                self.groups.clear();
                changed = true;
            }
            return Ok(changed);
        };
        self.observe_scope(ticket.scope());
        let names: Vec<String> = self.groups.keys().cloned().collect();
        for name in names {
            let image = match journal.coordinator_group(ticket, self.config.partition, &name) {
                Ok(image) => image,
                Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    cx.waker().wake_by_ref();
                    break;
                }
                Err(_) => break,
            };
            let state = self.groups.get_mut(&name).expect("listed group");
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
        Ok(changed)
    }

    fn observe_scope(&mut self, scope: Scope) {
        if self.scope != Some(scope) {
            self.scope = Some(scope);
            self.groups.clear();
        }
    }

    /// Propose the next revision when live observations differ from the
    /// committed image. One proposal per group is outstanding at a time.
    fn plan(&mut self, name: &str, image: Option<&CoordinatorGroup>) -> bool {
        let state = self.groups.get_mut(name).expect("observed group");
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
        let mut arena = self.arenas.pop().expect("free plan arena");
        arena.clear();
        if arena
            .push(ozzy_journal::operation::OperationKind::GroupPlan, &body)
            .is_err()
        {
            arena.clear();
            self.arenas.push(arena);
            return false;
        }
        match self.submitter.try_submit(arena) {
            Ok(pending) => {
                state.pending = Some(revision);
                self.proposals.push(PlanProposal {
                    name: name.to_owned(),
                    pending: Box::pin(pending),
                });
                true
            }
            Err(mut unsubmitted) => {
                unsubmitted.buffer.clear();
                self.arenas.push(unsubmitted.buffer);
                false
            }
        }
    }

    fn packet<'a>(&self, message: &'a Message, link: Link) -> Option<Packet<'a>> {
        if link.binding.kind != Kind::Client
            || link.binding.peer == self.config.local
            || link.binding.peer.as_bytes() == &[0; 16]
            || message.len() != 4
            || message.part_slice(0) != Some(link.binding.peer.as_bytes().as_slice())
        {
            return None;
        }
        let frames = std::array::from_fn::<_, 3, _>(|index| {
            message.part_slice(index + 1).expect("checked frames")
        });
        let packet = decode_packet(&frames, self.config.limits).ok()?;
        if packet.envelope.sender != link.binding.peer
            || packet.envelope.session != Some(link.binding.session)
            || packet.envelope.response
            || packet.envelope.request_id.is_none()
            || !matches!(
                packet.envelope.opcode,
                Opcode::GroupJoin | Opcode::GroupInspect | Opcode::GroupDelete
            )
        {
            return None;
        }
        Some(packet)
    }

    fn reject(
        &mut self,
        envelope: Envelope,
        link: Link,
        code: u16,
        retry: RetryClass,
        hint: AuthorityHint,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let frame = response::nack_frame(
            self.config.local,
            envelope,
            link.send.envelope,
            (code, retry),
            hint,
            &[],
            &mut self.metadata,
        )?;
        self.replies.push_back(Reply {
            peer: envelope.sender,
            session: link.binding.session,
            frame,
        });
        Ok(NativeReceive::Accepted)
    }
}

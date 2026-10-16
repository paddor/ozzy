//! Membership, inspection, and deletion requests answered from the committed
//! image, and the replies they queue.

use super::state::{Disposition, Observation, Targeting};
use super::{DeleteProposal, GroupCoordinator, Reply};
use crate::{
    frontend::Link,
    replica_actor::{NativeIntakeError, NativeReceive, PartitionActor, response},
    replica_journal::JournalError,
};
use omq_tokio::Message;
use ozzy_core::state::CoordinatorGroup;
use ozzy_journal::operation::{GroupDelete, OperationBody, OperationKind, encode_operation_body};
use ozzy_proto::{
    ConsumerGroupId, Envelope, EnvelopeLimits, LinkSessionId, Opcode, OperationId, Packet,
    PartitionIncarnation,
    append::Authority,
    group::{self, Start, Status},
    handshake,
    nack::{AuthorityHint, RetryClass},
};
use std::{sync::Arc, time::Duration};

/// The client a coordinator reply goes to, and how to reach it.
pub(super) struct Requester {
    pub(super) envelope: Envelope,
    pub(super) session: LinkSessionId,
    pub(super) send: EnvelopeLimits,
    pub(super) hint: AuthorityHint,
}

impl Requester {
    fn new(envelope: Envelope, link: Link, hint: AuthorityHint) -> Self {
        Self {
            envelope,
            session: link.binding.session,
            send: link.send.envelope,
            hint,
        }
    }
}

/// Common request checks either produce the committed image or an answer.
enum Committed {
    Image(Option<Arc<CoordinatorGroup>>),
    Answered(NativeReceive),
}

/// The coordinator and group a request names.
struct Named<'a> {
    authority: Authority,
    coordinator: PartitionIncarnation,
    name: &'a str,
}

impl GroupCoordinator {
    /// Answer one membership, inspection, or deletion request. Membership
    /// and inspection replies come from the committed image and never wait
    /// for a proposal. A deletion reply follows its proposal's confirmation.
    pub fn receive(
        &mut self,
        message: &Message,
        link: Link,
        actor: &mut PartitionActor,
        now: Duration,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let hint = actor.authority_hint();
        let Some(packet) = response::client_packet(
            message,
            link,
            self.config.local,
            self.config.limits,
            &[Opcode::GroupJoin, Opcode::GroupInspect, Opcode::GroupDelete],
        ) else {
            return Ok(NativeReceive::Ignored);
        };
        if self.replies.len() >= self.config.requests {
            return Ok(NativeReceive::Busy);
        }
        if link.send.envelope.max_metadata_bytes < group::JOINED_FIXED_BYTES {
            return Ok(NativeReceive::Ignored);
        }
        let requester = Requester::new(packet.envelope, link, hint);
        if link.remote.roles & handshake::CONSUMER == 0
            || link.remote.capabilities & handshake::OWNER_GROUPS == 0
        {
            return self.reject(&requester, (3, RetryClass::Permanent));
        }
        match packet.envelope.opcode {
            Opcode::GroupInspect => self.inspect(packet, &requester, actor),
            Opcode::GroupDelete => self.delete(packet, requester, actor),
            _ => self.join(packet, &requester, actor, now),
        }
    }

    fn join(
        &mut self,
        packet: Packet<'_>,
        requester: &Requester,
        actor: &mut PartitionActor,
        now: Duration,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let Ok(request) = group::decode_join(packet, self.config.limits) else {
            return self.reject(requester, (1, RetryClass::Permanent));
        };
        let named = Named {
            authority: request.authority,
            coordinator: request.coordinator,
            name: &request.name,
        };
        let image = match self.committed(requester, actor, &named)? {
            Committed::Image(image) => image,
            Committed::Answered(receive) => return Ok(receive),
        };
        // An instance of a retired incarnation stops instead of creating a
        // new one; a fresh reader names no incarnation.
        if request
            .group
            .is_some_and(|group| image.as_ref().is_none_or(|image| image.group != group))
        {
            let reply = self.joined(requester.hint, None, Status::Deleted, Vec::new());
            return self.reply_joined(requester, &reply);
        }
        let topology = self.config.topology.clone();
        let targeting = Targeting {
            number: &|partition| topology.number(partition),
            drain_deadline: self.config.drain_deadline,
        };
        let state = self.groups.entry(request.name.clone()).or_default();
        if let Some(image) = &image {
            state.adopt(image, now);
        }
        if request.mode == group::Mode::Join
            && !state.members.contains_key(&request.member)
            && state.members.len() >= self.config.max_members
        {
            return self.reject(requester, (10, RetryClass::AfterBackoff));
        }
        let name = request.name.clone();
        let seen = Observation {
            request,
            node: requester.envelope.sender,
            now,
        };
        let disposition = state.observe(image.as_deref(), seen, &targeting);
        let (status, targets) = match disposition {
            Disposition::Fenced => return self.reject(requester, (6, RetryClass::Permanent)),
            Disposition::Reply(status, targets) => (status, targets),
        };
        self.plan(&name, image.as_deref());
        let reply = self.joined(requester.hint, image.as_deref(), status, targets);
        debug_assert!(status != Status::Member || reply.revision != 0);
        self.reply_joined(requester, &reply)
    }

    fn joined(
        &self,
        hint: AuthorityHint,
        image: Option<&CoordinatorGroup>,
        status: Status,
        targets: Vec<group::Target>,
    ) -> group::JoinReply {
        group::JoinReply {
            authority: hint.authority,
            coordinator: self.config.partition,
            group: image.map_or(ConsumerGroupId::from_bytes([0; 16]), |image| image.group),
            revision: image.map_or(0, |image| image.revision),
            status,
            targets,
        }
    }

    /// Coordinator placement, authority, and read access shared by every
    /// request, then the committed image of the named group.
    fn committed(
        &mut self,
        requester: &Requester,
        actor: &mut PartitionActor,
        named: &Named<'_>,
    ) -> Result<Committed, NativeIntakeError> {
        let reject =
            |this: &mut Self, rejection| this.reject(requester, rejection).map(Committed::Answered);
        if named.coordinator != self.config.partition
            || !response::leads(
                self.config.group,
                self.config.local,
                named.authority,
                requester.hint,
            )
        {
            return reject(self, (5, RetryClass::AfterAuthorityRefresh));
        }
        if crate::topic_metadata::coordinator_number(
            named.name,
            self.config.topology.seed,
            self.config.topology.partitions.len(),
        ) != self.config.number
        {
            return reject(self, (6, RetryClass::Permanent));
        }
        let Some((Some(ticket), journal)) = actor.read_access() else {
            return reject(self, (12, RetryClass::AfterAuthorityRefresh));
        };
        let image = match journal.coordinator_group(ticket, self.config.partition, named.name) {
            Ok(image) => image,
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Ok(Committed::Answered(NativeReceive::Busy));
            }
            Err(_) => return reject(self, (13, RetryClass::AfterAuthorityRefresh)),
        };
        self.observe_scope(ticket.scope());
        Ok(Committed::Image(image))
    }

    /// Describe the committed image; an absent name has no incarnation.
    fn inspect(
        &mut self,
        packet: Packet<'_>,
        requester: &Requester,
        actor: &mut PartitionActor,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let Ok(request) = group::decode_inspect(packet, self.config.limits) else {
            return self.reject(requester, (1, RetryClass::Permanent));
        };
        let named = Named {
            authority: request.authority,
            coordinator: request.coordinator,
            name: &request.name,
        };
        let image = match self.committed(requester, actor, &named)? {
            Committed::Image(image) => image,
            Committed::Answered(receive) => return Ok(receive),
        };
        let Some(reply) = self.inspection(requester.hint, image.as_deref()) else {
            return self.reject(requester, (13, RetryClass::AfterAuthorityRefresh));
        };
        if group::inspected_bytes(reply.members.len(), reply.targets.len())?
            > requester.send.max_metadata_bytes
        {
            return self.reject(requester, (1, RetryClass::Permanent));
        }
        let header = group::encode_inspected(
            response::reply_envelope(
                requester.envelope,
                self.config.local,
                Opcode::GroupInspected,
            ),
            &reply,
            &mut self.metadata,
            requester.send,
        )?;
        self.queue(requester, header);
        Ok(NativeReceive::Accepted)
    }

    /// The inspection reply, or `None` when a target names an unknown partition.
    fn inspection(
        &self,
        hint: AuthorityHint,
        image: Option<&CoordinatorGroup>,
    ) -> Option<group::Inspection> {
        let topology = &self.config.topology;
        let targets = image.map_or(Some(Vec::new()), |image| {
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
        })?;
        Some(group::Inspection {
            authority: hint.authority,
            coordinator: self.config.partition,
            group: image.map(|image| image.group),
            revision: image.map_or(0, |image| image.revision),
            initial: image.map_or(Start::Earliest, |image| image.initial),
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
        })
    }

    /// Retire the committed incarnation the request names. Sources must have
    /// been revoked first; this only frees the name. A name that no longer
    /// holds the incarnation answers at once.
    fn delete(
        &mut self,
        packet: Packet<'_>,
        requester: Requester,
        actor: &mut PartitionActor,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let Ok(request) = group::decode_delete(packet, self.config.limits) else {
            return self.reject(&requester, (1, RetryClass::Permanent));
        };
        let named = Named {
            authority: request.authority,
            coordinator: request.coordinator,
            name: &request.name,
        };
        let image = match self.committed(&requester, actor, &named)? {
            Committed::Image(image) => image,
            Committed::Answered(receive) => return Ok(receive),
        };
        let Some(image) = image.filter(|image| image.group == request.group) else {
            self.deleted(&requester, requester.hint.authority, request.group, false)?;
            return Ok(NativeReceive::Accepted);
        };
        let state = self.groups.entry(request.name.clone()).or_default();
        // A plan or deletion in flight settles first; the caller retries.
        if state.pending.is_some() || state.deleting {
            return self.reject(&requester, (10, RetryClass::AfterBackoff));
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
        let Some(pending) = self.submit_body(OperationKind::GroupDelete, &body) else {
            return self.reject(&requester, (10, RetryClass::AfterBackoff));
        };
        if let Some(state) = self.groups.get_mut(&request.name) {
            state.deleting = true;
        }
        self.deletions.push(DeleteProposal {
            name: request.name,
            group: image.group,
            requester,
            pending,
        });
        Ok(NativeReceive::Accepted)
    }

    pub(super) fn deleted(
        &mut self,
        requester: &Requester,
        authority: Authority,
        group: ConsumerGroupId,
        deleted: bool,
    ) -> Result<(), NativeIntakeError> {
        let reply = group::DeleteReply {
            authority,
            coordinator: self.config.partition,
            group,
            deleted,
        };
        let header = group::encode_deleted(
            response::reply_envelope(requester.envelope, self.config.local, Opcode::GroupDeleted),
            reply,
            &mut self.metadata,
            requester.send,
        )?;
        self.queue(requester, header);
        Ok(())
    }

    fn reply_joined(
        &mut self,
        requester: &Requester,
        reply: &group::JoinReply,
    ) -> Result<NativeReceive, NativeIntakeError> {
        let header = group::encode_joined(
            response::reply_envelope(requester.envelope, self.config.local, Opcode::GroupJoined),
            reply,
            &mut self.metadata,
            requester.send,
        )?;
        self.queue(requester, header);
        Ok(NativeReceive::Accepted)
    }

    fn queue(&mut self, requester: &Requester, header: [u8; ozzy_proto::ENVELOPE_BYTES]) {
        let peer = requester.envelope.sender;
        self.replies.push_back(Reply {
            peer,
            session: requester.session,
            frame: crate::native_frames::message(
                peer.as_bytes(),
                header,
                &self.metadata,
                bytes::Bytes::new(),
            ),
        });
    }

    pub(super) fn reject(
        &mut self,
        requester: &Requester,
        rejection: (u16, RetryClass),
    ) -> Result<NativeReceive, NativeIntakeError> {
        let nack = response::Nack {
            request: requester.envelope,
            send: requester.send,
            rejection,
            hint: requester.hint,
            detail: &[],
        };
        let frame = response::nack_frame(self.config.local, &nack, &mut self.metadata)?;
        self.replies.push_back(Reply {
            peer: requester.envelope.sender,
            session: requester.session,
            frame,
        });
        Ok(NativeReceive::Accepted)
    }
}

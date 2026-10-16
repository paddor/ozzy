//! Request bodies, their reply validation, and typed rejections.

use std::sync::{Arc, Weak};

use omq_tokio::Message;
use ozzy_proto::layout::Fixed;
use ozzy_proto::{
    Envelope, EnvelopeLimits, LinkSessionId, Opcode, Packet, directory, group, nack, producer,
    progress, reader,
};

use super::super::{BrokerLinkError, readers::Inbox};
use super::{Lease, track};

#[derive(Debug, Clone)]
pub(in crate::replicated::broker_links) enum Body {
    Topic(directory::TopicRequest),
    Open(producer::Open),
    Subscribe {
        subscribe: reader::Subscribe,
        /// Takes the records once the request is sent.
        inbox: Weak<Inbox>,
    },
    Ack(reader::Ack),
    Progress(progress::Request),
    Assign(group::AssignRequest),
    Join(Box<group::JoinRequest>),
    Inspect(Box<group::InspectRequest>),
    Delete(Box<group::DeleteRequest>),
    Unsubscribe {
        session: LinkSessionId,
        selected: reader::Subscribed,
    },
}

impl Body {
    pub(super) fn opcodes(&self) -> (Opcode, Opcode) {
        match self {
            Self::Topic(_) => (Opcode::StateSnapshotRequest, Opcode::StateSnapshot),
            Self::Open(_) => (Opcode::OpenProducer, Opcode::ProducerOpened),
            Self::Subscribe { .. } => (Opcode::Subscribe, Opcode::Subscribed),
            Self::Ack(_) => (Opcode::Ack, Opcode::Ack),
            Self::Progress(_) => (Opcode::ProgressCommit, Opcode::ProgressCommitted),
            Self::Assign(_) => (Opcode::GroupAssign, Opcode::GroupAssigned),
            Self::Join(_) => (Opcode::GroupJoin, Opcode::GroupJoined),
            Self::Inspect(_) => (Opcode::GroupInspect, Opcode::GroupInspected),
            Self::Delete(_) => (Opcode::GroupDelete, Opcode::GroupDeleted),
            Self::Unsubscribe { .. } => (Opcode::Unsubscribe, Opcode::Unsubscribed),
        }
    }

    pub(super) fn encode(
        &self,
        envelope: Envelope,
        metadata: &mut Vec<u8>,
        limits: EnvelopeLimits,
    ) -> Result<[u8; 64], BrokerLinkError> {
        Ok(match self {
            Self::Topic(request) => directory::encode_topic_request(
                envelope,
                request,
                metadata,
                limits,
                directory::Limits::default(),
            )?,
            Self::Open(open) => producer::encode_open(envelope, *open, metadata, limits)?,
            Self::Subscribe { subscribe, .. } => {
                reader::encode_subscribe(envelope, subscribe, metadata, limits)?
            }
            Self::Ack(ack) => reader::encode_ack(envelope, *ack, metadata, limits)?,
            Self::Progress(request) => {
                progress::encode_request(envelope, *request, metadata, limits)?
            }
            Self::Assign(request) => group::encode_assign(envelope, *request, metadata, limits)?,
            Self::Join(request) => group::encode_join(envelope, request, metadata, limits)?,
            Self::Inspect(request) => group::encode_inspect(envelope, request, metadata, limits)?,
            Self::Delete(request) => group::encode_delete(envelope, request, metadata, limits)?,
            Self::Unsubscribe { selected, .. } => {
                reader::encode_unsubscribe(envelope, *selected, metadata, limits)?
            }
        })
    }

    pub(super) fn reply(
        &self,
        packet: Packet<'_>,
        message: &Message,
        lease: &Arc<Lease>,
        limits: EnvelopeLimits,
    ) -> Result<Reply, BrokerLinkError> {
        match self {
            Self::Subscribe { subscribe, .. } => decode_subscription(packet, subscribe, limits),
            Self::Progress(request) => {
                let confirmed = progress::decode_confirmed(packet, limits)?;
                if !confirms(request, &confirmed) {
                    return Err(BrokerLinkError::Response);
                }
                Ok(Reply::Progress(confirmed))
            }
            Self::Assign(request) => {
                let reply = group::decode_assigned(packet, limits)?;
                if !assigns(request, &reply) {
                    return Err(BrokerLinkError::Response);
                }
                Ok(Reply::Assigned(reply))
            }
            Self::Join(request) => {
                let reply = group::decode_joined(packet, limits)?;
                if reply.authority != request.authority || reply.coordinator != request.coordinator
                {
                    return Err(BrokerLinkError::Response);
                }
                Ok(Reply::Joined(reply))
            }
            Self::Inspect(request) => {
                let reply = group::decode_inspected(packet, limits)?;
                if reply.authority != request.authority || reply.coordinator != request.coordinator
                {
                    return Err(BrokerLinkError::Response);
                }
                Ok(Reply::Inspected(Box::new(reply)))
            }
            Self::Delete(request) => {
                let reply = group::decode_deleted(packet, limits)?;
                if !deletes(request, &reply) {
                    return Err(BrokerLinkError::Response);
                }
                Ok(Reply::Deleted(reply))
            }
            Self::Unsubscribe { selected, .. } => {
                if reader::decode_unsubscribed(packet, limits)? != *selected {
                    return Err(BrokerLinkError::Response);
                }
                Ok(Reply::Unsubscribed)
            }
            _ => Ok(Reply::Message(track(message, lease, true))),
        }
    }
}

/// Reader and processing cursors reserve their completion in owner storage.
/// Decode here so an idle application does not retain control-frame admission.
#[derive(Debug)]
pub(in crate::replicated::broker_links) enum Reply {
    Message(Message),
    Subscribed(reader::Subscribed),
    Progress(progress::Confirmed),
    Assigned(group::AssignReply),
    Joined(group::JoinReply),
    Inspected(Box<group::Inspection>),
    Deleted(group::DeleteReply),
    Unsubscribed,
}

impl Reply {
    pub(in crate::replicated::broker_links) fn message(self) -> Result<Message, BrokerLinkError> {
        match self {
            Self::Message(message) => Ok(message),
            _ => Err(BrokerLinkError::Response),
        }
    }
}

/// Whether `confirmed` answers `request`: same authority, assignment, and
/// policy, at or past the requested cursor, else at or past the retained floor.
fn confirms(request: &progress::Request, confirmed: &progress::Confirmed) -> bool {
    confirmed.authority == request.authority
        && confirmed.assignment == request.assignment
        && confirmed.policy == request.policy
        && request
            .cursor
            .is_none_or(|cursor| confirmed.cursor >= cursor)
        && (request.cursor.is_some() || confirmed.cursor >= confirmed.retained_from)
}

/// Whether `reply` assigns exactly the requested target and plan.
fn assigns(request: &group::AssignRequest, reply: &group::AssignReply) -> bool {
    reply.authority == request.authority
        && reply.partition == request.partition
        && reply.group == request.group
        && reply.member == request.member
        && reply.plan_revision == request.plan_revision
        && reply.policy == request.policy
}

/// Whether `reply` deletes the requested group, under the requested
/// configuration at the requested view or a later one.
fn deletes(request: &group::DeleteRequest, reply: &group::DeleteReply) -> bool {
    reply.authority.group_id == request.authority.group_id
        && reply.authority.config_epoch == request.authority.config_epoch
        && reply.authority.view >= request.authority.view
        && reply.coordinator == request.coordinator
        && reply.group == request.group
}

pub(super) fn decode_subscription(
    packet: Packet<'_>,
    subscribe: &reader::Subscribe,
    limits: EnvelopeLimits,
) -> Result<Reply, BrokerLinkError> {
    let selected = reader::decode_subscribed(packet, limits)?;
    if selected.subscription != subscribe.subscription
        || Some(selected.source) != subscribe.target.group_source()
        || matches!(subscribe.start, reader::Start::Offset(offset) if offset != selected.resolved_offset)
    {
        return Err(BrokerLinkError::Response);
    }
    Ok(Reply::Subscribed(selected))
}

pub(super) fn reader_rejection(
    reply: nack::Nack<'_>,
    reader_request: bool,
    hint: Option<nack::AuthorityHint>,
) -> BrokerLinkError {
    let offset = <[u8; 8]>::try_from(reply.detail)
        .ok()
        .map(|bytes| ozzy_proto::Offset::new(u64::from_be_bytes(bytes)));
    let range = <[u8; 16]>::try_from(reply.detail).ok();
    if reader_request
        && reply.code == 14
        && let Some(earliest) = offset
    {
        BrokerLinkError::RetentionGap { earliest }
    } else if reader_request
        && reply.code == 19
        && let Some(earliest) = offset
    {
        BrokerLinkError::Seek(ozzy_core::reader::seek::SeekError::NotFound { earliest })
    } else if reader_request
        && reply.code == 20
        && let Some(range) = range
    {
        BrokerLinkError::Seek(ozzy_core::reader::seek::SeekError::Ambiguous {
            first: ozzy_proto::Offset::new(range.u64_at::<0>()),
            last: ozzy_proto::Offset::new(range.u64_at::<8>()),
        })
    } else if reader_request && matches!(reply.code, 14 | 19 | 20) {
        BrokerLinkError::Response
    } else {
        BrokerLinkError::Rejected {
            code: reply.code,
            retry: reply.retry,
            hint,
        }
    }
}

pub(super) fn group_rejection(
    reply: nack::Nack<'_>,
    hint: Option<nack::AuthorityHint>,
) -> BrokerLinkError {
    match reply.code {
        6 if reply.retry == nack::RetryClass::Permanent && reply.detail.is_empty() => {
            BrokerLinkError::Progress(super::super::super::ProgressError::Fenced)
        }
        7 if reply.retry == nack::RetryClass::Permanent => {
            match group::InitialPosition::decode(reply.detail) {
                Ok(detail) => BrokerLinkError::InitialPosition(detail),
                Err(_) => BrokerLinkError::Response,
            }
        }
        6 | 7 => BrokerLinkError::Response,
        _ => reader_rejection(reply, false, hint),
    }
}

pub(super) fn progress_rejection(
    request: progress::Request,
    reply: nack::Nack<'_>,
    hint: Option<nack::AuthorityHint>,
) -> BrokerLinkError {
    if !matches!(reply.code, 6 | 9) {
        return reader_rejection(reply, false, hint);
    }
    if reply.retry != nack::RetryClass::Permanent {
        return BrokerLinkError::Response;
    }
    let error = if reply.code == 6 && reply.detail.is_empty() {
        super::super::super::ProgressError::Fenced
    } else if reply.code == 9 {
        let Ok(expired) = progress::HistoryExpired::decode(reply.detail) else {
            return BrokerLinkError::Response;
        };
        if request
            .cursor
            .is_some_and(|cursor| cursor != expired.cursor)
        {
            return BrokerLinkError::Response;
        }
        super::super::super::ProgressError::HistoryExpired {
            cursor: expired.cursor,
            retained_from: expired.retained_from,
        }
    } else {
        return BrokerLinkError::Response;
    };
    BrokerLinkError::Progress(error)
}

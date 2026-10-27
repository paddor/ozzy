//! Reply mapping shared by shard intake and recovery routing.

use crate::transport::Frames;
use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::append::Authority;
use ozzy_proto::nack::{self, RetryClass};
use ozzy_proto::{Envelope, EnvelopeLimits, GroupId, NodeId, Opcode, Packet, decode_packet};

use crate::frontend::{Kind, Link};

use crate::replica_journal::{
    AppendAdmissionError, GroupAssignError, GroupProgressError, JournalError,
};

pub(super) fn rejection(error: &JournalError) -> (u16, RetryClass) {
    match error {
        JournalError::ProducerAppend(error) => match error {
            AppendAdmissionError::Authority => (5, RetryClass::AfterAuthorityRefresh),
            AppendAdmissionError::Policy
            | AppendAdmissionError::DelayDisabled
            | AppendAdmissionError::CompressedSchedule => (2, RetryClass::Permanent),
            AppendAdmissionError::UnknownPartition | AppendAdmissionError::Fenced => {
                (6, RetryClass::Permanent)
            }
            AppendAdmissionError::Sequence => (7, RetryClass::Permanent),
            AppendAdmissionError::SequenceGap | AppendAdmissionError::ScheduleCapacity => {
                (10, RetryClass::AfterBackoff)
            }
            AppendAdmissionError::RetryConflict => (8, RetryClass::Permanent),
            AppendAdmissionError::RetryHistoryExpired => (9, RetryClass::Permanent),
            AppendAdmissionError::DueOutOfRange => (21, RetryClass::Permanent),
            AppendAdmissionError::ScheduleTooLarge => (1, RetryClass::Permanent),
        },
        JournalError::GroupProgress(error) => match error {
            GroupProgressError::Authority => (5, RetryClass::AfterAuthorityRefresh),
            GroupProgressError::Policy | GroupProgressError::InvalidBinding => {
                (2, RetryClass::Permanent)
            }
            GroupProgressError::UnknownPartition | GroupProgressError::Fenced => {
                (6, RetryClass::Permanent)
            }
            GroupProgressError::HistoryExpired { .. } => (9, RetryClass::Permanent),
            GroupProgressError::BeyondCommit { .. } => (7, RetryClass::Permanent),
            GroupProgressError::Batch => (10, RetryClass::AfterBackoff),
        },
        JournalError::GroupAssign(error) => match error {
            GroupAssignError::Authority => (5, RetryClass::AfterAuthorityRefresh),
            GroupAssignError::Policy => (2, RetryClass::Permanent),
            GroupAssignError::Invalid => (1, RetryClass::Permanent),
            GroupAssignError::UnknownPartition | GroupAssignError::Fenced => {
                (6, RetryClass::Permanent)
            }
            GroupAssignError::Conflict => (8, RetryClass::Permanent),
            GroupAssignError::InitialPosition { .. } => (7, RetryClass::Permanent),
        },
        JournalError::Dedup(error) => match error {
            ozzy_core::dedup::Error::Batch => (1, RetryClass::Permanent),
            // Admission normally defers these; a reply still means retry later.
            ozzy_core::dedup::Error::Capacity
            | ozzy_core::dedup::Error::PendingCapacity
            | ozzy_core::dedup::Error::PendingOriginal(_)
            | ozzy_core::dedup::Error::SnapshotBusy
            | ozzy_core::dedup::Error::SnapshotCapacity
            | ozzy_core::dedup::Error::Allocation => (10, RetryClass::AfterBackoff),
            _ => (11, RetryClass::UnknownOutcome),
        },
        // A nearly full journal refuses new work until retention frees room.
        JournalError::RollCapacity => (10, RetryClass::AfterBackoff),
        JournalError::AppendCapacity | JournalError::AppendMismatch => (1, RetryClass::Permanent),
        _ => (11, RetryClass::UnknownOutcome),
    }
}

/// One bounded typed rejection of a correlated client request.
pub(super) struct Nack<'a> {
    /// The request it answers.
    pub request: Envelope,
    /// The requester's negotiated reply bounds.
    pub send: EnvelopeLimits,
    pub rejection: (u16, RetryClass),
    pub hint: nack::AuthorityHint,
    /// Code-specific detail. Authority codes carry the routing hint instead.
    pub detail: &'a [u8],
}

/// Encode one rejection frame addressed to its requester.
pub(super) fn nack_frame(
    local: NodeId,
    rejected: &Nack<'_>,
    metadata: &mut Vec<u8>,
) -> Result<Message, nack::NackError> {
    let (code, retry) = rejected.rejection;
    let hint = if matches!(code, 5 | 12 | 13) {
        Some(rejected.hint.encode()?)
    } else {
        None
    };
    let header = nack::encode(
        reply_envelope(rejected.request, local, Opcode::Nack),
        nack::Nack {
            code,
            retry,
            detail: hint
                .as_ref()
                .map_or(rejected.detail, |bytes| bytes.as_slice()),
            diagnostic: "",
        },
        metadata,
        rejected.send,
    )?;
    Ok(crate::native_frames::message(
        rejected.request.sender.as_bytes(),
        header,
        metadata,
        Bytes::new(),
    ))
}

/// The header envelope of a reply from this broker to a client request.
pub(super) fn reply_envelope(request: Envelope, local: NodeId, opcode: Opcode) -> Envelope {
    Envelope {
        opcode,
        response: true,
        sender: local,
        ..request
    }
}

/// The authority a committed or current scope grants.
pub(super) const fn authority(scope: ozzy_replication::Scope) -> Authority {
    Authority {
        group_id: scope.group_id,
        config_epoch: scope.configuration_epoch,
        view: scope.view,
    }
}

pub(super) fn authority_hint(
    configuration: ozzy_replication::Configuration,
    scope: ozzy_replication::Scope,
) -> nack::AuthorityHint {
    nack::AuthorityHint {
        authority: authority(scope),
        primary: configuration.primary(scope.view),
    }
}

/// Whether `local` leads `group` under the authority a request names.
pub(super) fn leads(
    group: GroupId,
    local: NodeId,
    authority: Authority,
    hint: nack::AuthorityHint,
) -> bool {
    authority.group_id == group && authority == hint.authority && hint.primary == local
}

/// Whether `link` is still the client session that sent `request`.
pub(super) fn current_client(link: Option<Link>, request: &Envelope) -> bool {
    link.is_some_and(|link| {
        link.binding.kind == Kind::Client
            && Some(link.binding.session) == request.session
            && link.binding.peer == request.sender
    })
}

/// Decode a correlated client request from its own live session. Requests
/// with any other opcode are ignored.
pub(super) fn client_packet<'a>(
    message: &'a Message,
    link: Link,
    local: NodeId,
    limits: EnvelopeLimits,
    opcodes: &[Opcode],
) -> Option<Packet<'a>> {
    let (route, frames) = message.routed_packet()?;
    if link.binding.kind != Kind::Client
        || link.binding.peer == local
        || link.binding.peer.as_bytes() == &[0; 16]
        || route != link.binding.peer.as_bytes()
    {
        return None;
    }
    let packet = decode_packet(&frames, limits).ok()?;
    let envelope = packet.envelope;
    (envelope.sender == link.binding.peer
        && envelope.session == Some(link.binding.session)
        && !envelope.response
        && envelope.request_id.is_some()
        && opcodes.contains(&envelope.opcode))
    .then_some(packet)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_core::dedup::Error;

    #[test]
    fn full_journal_retries_after_backoff() {
        assert_eq!(
            rejection(&JournalError::RollCapacity),
            (10, RetryClass::AfterBackoff)
        );
    }

    #[test]
    fn dedup_capacity_retries_after_backoff_and_bad_batches_are_permanent() {
        for error in [
            Error::Capacity,
            Error::PendingCapacity,
            Error::PendingOriginal(7),
            Error::SnapshotBusy,
            Error::SnapshotCapacity,
            Error::Allocation,
        ] {
            assert_eq!(
                rejection(&JournalError::Dedup(error)),
                (10, RetryClass::AfterBackoff)
            );
        }
        assert_eq!(
            rejection(&JournalError::Dedup(Error::Batch)),
            (1, RetryClass::Permanent)
        );
        assert_eq!(
            rejection(&JournalError::Dedup(Error::TimeRegression)),
            (11, RetryClass::UnknownOutcome)
        );
    }
}

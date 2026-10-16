//! Reply mapping shared by shard intake and recovery routing.

use bytes::Bytes;
use omq_tokio::Message;
use ozzy_proto::append::Authority;
use ozzy_proto::nack::{self, RetryClass};
use ozzy_proto::{Envelope, EnvelopeLimits, NodeId, Opcode};

use crate::replica_journal::{
    AppendAdmissionError, GroupAssignError, GroupProgressError, JournalError,
};

pub(super) fn rejection(error: &JournalError) -> (u16, RetryClass) {
    match error {
        JournalError::ProducerAppend(error) => match error {
            AppendAdmissionError::Authority => (5, RetryClass::AfterAuthorityRefresh),
            AppendAdmissionError::Policy => (2, RetryClass::Permanent),
            AppendAdmissionError::UnknownPartition | AppendAdmissionError::Fenced => {
                (6, RetryClass::Permanent)
            }
            AppendAdmissionError::Sequence => (7, RetryClass::Permanent),
            AppendAdmissionError::SequenceGap => (10, RetryClass::AfterBackoff),
            AppendAdmissionError::RetryConflict => (8, RetryClass::Permanent),
            AppendAdmissionError::RetryHistoryExpired => (9, RetryClass::Permanent),
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
        JournalError::AppendCapacity | JournalError::AppendMismatch => (1, RetryClass::Permanent),
        _ => (11, RetryClass::UnknownOutcome),
    }
}

/// One bounded typed rejection for a correlated client request. Authority
/// codes carry the routing hint; other codes carry the supplied detail.
pub(super) fn nack_frame(
    local: NodeId,
    request: Envelope,
    send: EnvelopeLimits,
    rejection: (u16, RetryClass),
    hint: nack::AuthorityHint,
    detail: &[u8],
    metadata: &mut Vec<u8>,
) -> Result<Message, nack::NackError> {
    let (code, retry) = rejection;
    let hint = if matches!(code, 5 | 12 | 13) {
        Some(hint.encode()?)
    } else {
        None
    };
    let header = nack::encode(
        Envelope {
            opcode: Opcode::Nack,
            response: true,
            sender: local,
            ..request
        },
        nack::Nack {
            code,
            retry,
            detail: hint.as_ref().map_or(detail, |bytes| bytes.as_slice()),
            diagnostic: "",
        },
        metadata,
        send,
    )?;
    Ok(crate::native_frames::message(
        request.sender.as_bytes(),
        header,
        metadata,
        Bytes::new(),
    ))
}

pub(super) fn authority_hint(
    configuration: ozzy_replication::Configuration,
    scope: ozzy_replication::Scope,
) -> nack::AuthorityHint {
    nack::AuthorityHint {
        authority: Authority {
            group_id: scope.group_id,
            config_epoch: scope.configuration_epoch,
            view: scope.view,
        },
        primary: configuration.primary(scope.view),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_core::dedup::Error;

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

//! Reply mapping shared by shard intake and recovery routing.

use ozzy_proto::append::Authority;
use ozzy_proto::nack::{self, RetryClass};

use crate::replica_journal::{AppendAdmissionError, JournalError};

pub(super) fn rejection(error: &JournalError) -> (u16, RetryClass) {
    match error {
        JournalError::ProducerAppend(error) => match error {
            AppendAdmissionError::Authority => (5, RetryClass::AfterAuthorityRefresh),
            AppendAdmissionError::Policy => (2, RetryClass::Permanent),
            AppendAdmissionError::UnknownPartition | AppendAdmissionError::Fenced => {
                (6, RetryClass::Permanent)
            }
            AppendAdmissionError::Sequence => (7, RetryClass::Permanent),
            AppendAdmissionError::SequenceGap => (10, RetryClass::AfterCredit),
            AppendAdmissionError::RetryConflict => (8, RetryClass::Permanent),
            AppendAdmissionError::RetryHistoryExpired => (9, RetryClass::Permanent),
        },
        JournalError::AppendCapacity | JournalError::AppendMismatch => (1, RetryClass::Permanent),
        _ => (11, RetryClass::UnknownOutcome),
    }
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

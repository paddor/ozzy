//! Restart-safe initialization through ordinary canonical proposal authority.

use super::{AppendBuffer, JournalError, ProposalBuffer};
use ozzy_core::state::{CanonicalImages, IdentityIndex};
use ozzy_journal::operation::{
    CreatePartition, OperationBody, OperationLimits, decode_operation_body,
};
use ozzy_replication::{JournalGeneration, Prefix, driver::ValidationTicket};

impl ProposalBuffer {
    /// Ensure this partition exists through the ordinary durable proposal path.
    /// Prepare in an empty lease. Matching accepted creation waits for application;
    /// matching applied creation resolves without another canonical operation.
    /// Incarnation, address, and owner epoch must agree. A changed retention policy
    /// becomes a separate confirmed policy operation on coordinated restart.
    /// This service request supplies no authority or confirmation by itself.
    pub fn prepare_partition(
        &mut self,
        partition: CreatePartition<'_>,
    ) -> Result<(), JournalError> {
        self.0.prepare_partition(partition)
    }
}

/// Caller first validates the exact driver/image ticket and local proposal role.
pub(in crate::replica_journal) fn partition_retry<I: IdentityIndex + Clone>(
    images: &CanonicalImages<I>,
    buffer: &mut AppendBuffer,
    ticket: ValidationTicket,
    generation: JournalGeneration,
    limits: OperationLimits,
) -> Result<Option<Prefix>, JournalError> {
    if !buffer.is_partition_creation() {
        return Ok(None);
    }
    if buffer.len() != 1 || buffer.is_producer() || buffer.owner_generation() != generation {
        return Err(JournalError::AppendMismatch);
    }
    let operation = buffer.operations().next().expect("one creation operation");
    let OperationBody::CreatePartition(request) =
        decode_operation_body(operation.kind, operation.body, limits)?
    else {
        return Err(JournalError::AppendMismatch);
    };
    let Some(existing) = images.speculative().partition(request.partition) else {
        return Ok(None); // Fresh creation still needs full canonical validation.
    };
    if existing.address.stream != request.stream
        || existing.address.topic != request.topic
        || existing.address.partition_id != request.partition_id
        || existing.owner_epoch != request.owner_epoch
    {
        return Err(JournalError::AppendMismatch);
    }
    if existing.retention != request.retention {
        let policy = ozzy_journal::operation::PartitionPolicy {
            partition: request.partition,
            expected_revision: existing.policy_revision,
            new_revision: existing
                .policy_revision
                .checked_add(1)
                .ok_or(JournalError::Configuration)?,
            retention: request.retention,
            operation_id: ozzy_proto::OperationId::new(),
        };
        buffer.clear();
        buffer.push_typed_body(&OperationBody::PartitionPolicy(policy))?;
        return Ok(None);
    }
    // These image revisions were checked against the ticket. Waiting through
    // the accepted tail is conservative and never confirms accepted-only state.
    Ok(Some(
        if images
            .committed()
            .partition(request.partition)
            .is_some_and(|committed| committed.retention == request.retention)
        {
            ticket.applied()
        } else {
            ticket.accepted()
        },
    ))
}

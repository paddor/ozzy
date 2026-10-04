//! Pure storage-profile and promise checks shared across execution placements.

use ozzy_journal::operation::ChainPosition;
use ozzy_journal_segment::{CommitMode, LogPosition, Manifest, WriterPosition};
use ozzy_replication::{Prefix, PromiseTicket, Scope};

use super::JournalError;

pub(super) fn position(prefix: Prefix) -> LogPosition {
    LogPosition {
        op_number: prefix.op.0,
        digest: prefix.digest,
    }
}

pub(super) fn recovery_ticket(
    configuration: ozzy_replication::Configuration,
    local: ozzy_proto::NodeId,
    generation: ozzy_replication::JournalGeneration,
    ticket: &ozzy_replication::recovery::RecoveryTicket,
) -> Result<(), JournalError> {
    let others = configuration
        .voters()
        .iter()
        .enumerate()
        .fold(0u8, |mask, (index, voter)| {
            mask | (u8::from(*voter != local) << index)
        });
    if ticket.generation() != generation
        || ticket.local() != local
        || ticket.scope()
            != (Scope {
                view: ticket.scope().view,
                ..configuration.scope()
            })
        || ticket.source().voter != configuration.primary(ticket.scope().view)
        || ticket.source().voter == local
        || ticket.voter_mask() != others
    {
        return Err(ozzy_replication::recovery::RecoveryError::StaleTransfer.into());
    }
    Ok(())
}

pub(super) fn validate_manifest(
    current: &Manifest,
    configuration_epoch: u64,
    segment_bytes: u64,
) -> Result<(), JournalError> {
    validate_storage_profile(
        current,
        CommitMode::External,
        configuration_epoch,
        segment_bytes,
    )
}

pub(super) fn validate_storage_profile(
    current: &Manifest,
    mode: CommitMode,
    configuration_epoch: u64,
    segment_bytes: u64,
) -> Result<(), JournalError> {
    if !current.durable_evidence
        || current.commit_mode != mode
        || current.configuration_epoch != configuration_epoch
        || current
            .segments
            .iter()
            .any(|segment| segment.capacity > segment_bytes)
        || current
            .segments
            .first()
            .is_none_or(|segment| !supported_anchor(current, segment.first_chain))
    {
        return Err(JournalError::UnsupportedHistory);
    }
    Ok(())
}

fn supported_anchor(manifest: &Manifest, first: ChainPosition) -> bool {
    if first == ChainPosition::GENESIS {
        return true;
    }
    let Some(checkpoint) = manifest.checkpoint else {
        return false;
    };
    let Some(previous) = first.next_op_number().checked_sub(1) else {
        return false;
    };
    previous < checkpoint.position.op_number
        || (previous == checkpoint.position.op_number
            && first.previous_digest() == checkpoint.position.digest)
}

pub(super) fn promise_manifest(
    scope: Scope,
    current: &Manifest,
    written: WriterPosition,
    durable: WriterPosition,
    accepted: Prefix,
    ticket: PromiseTicket,
) -> Result<Option<Manifest>, JournalError> {
    if ticket.scope()
        != (Scope {
            view: ticket.scope().view,
            ..scope
        })
        || ticket.generation() != durable.generation()
        || written != durable
        || ticket.scope().view < current.promised_view
        || ticket.log().last_normal_view != current.last_normal_view
        || ticket.log().accepted != accepted
        || ticket.log().committed.op.0 < current.committed.op_number
    {
        return Err(JournalError::PromiseMismatch);
    }
    if ticket.scope().view == current.promised_view {
        // A duplicate must name exactly the previously published history.
        if position(ticket.log().committed) != current.committed
            || position(ticket.log().accepted) != current.accepted
        {
            return Err(JournalError::PromiseMismatch);
        }
        return Ok(None);
    }
    let mut next = current.clone();
    next.parent_generation = next.generation;
    next.generation = next
        .generation
        .checked_add(1)
        .ok_or(JournalError::PromiseMismatch)?;
    next.promised_view = ticket.scope().view;
    next.accepted = position(ticket.log().accepted);
    next.committed = position(ticket.log().committed);
    Ok(Some(next))
}

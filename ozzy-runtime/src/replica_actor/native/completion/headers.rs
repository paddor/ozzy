//! Committed reply headers, each checked against its proposal's state.

use crate::{
    replica_actor::{native::NativeIntakeConfig, response},
    replica_journal::ProposalBuffer,
};
use ozzy_journal::operation::{OperationKind, OperationLimits, decode_append_summary};
use ozzy_proto::{
    Envelope, EnvelopeLimits, NodeId, Opcode, ProducerId, append, data::DataLimits, group,
    producer, progress,
};

use super::NativeIntakeError;

/// The producer result span of a committed APPEND's `index`th reply.
pub(super) fn result_range(
    buffer: &ProposalBuffer,
    index: usize,
    limits: DataLimits,
) -> Result<Option<ozzy_core::state::ProducerResultSpan>, NativeIntakeError> {
    let retained = buffer.producer_retry_results();
    if !retained.is_empty() {
        return Ok(retained.get(index).copied());
    }
    if index != 0 {
        return Ok(None);
    }
    let batch = if let Some(batch) = buffer.validated_producer_summary(0) {
        batch
    } else {
        let Some((OperationKind::Append, body)) = buffer.bodies().next() else {
            return Err(NativeIntakeError::History);
        };
        let summary = decode_append_summary(
            body,
            OperationLimits {
                max_body_bytes: buffer.limits().max_body_bytes,
                max_append_batches: 1,
                max_records: limits.max_records,
                max_parts: limits.max_parts,
                max_payload_bytes: limits.envelope.max_payload_bytes,
                ..OperationLimits::default()
            },
        )
        .map_err(|_| NativeIntakeError::History)?;
        let [batch] = summary.batches() else {
            return Err(NativeIntakeError::History);
        };
        *batch
    };
    Ok(Some(ozzy_core::state::ProducerResultSpan {
        first_sequence: batch.first_sequence,
        first_offset: batch.first_offset,
        records: batch.record_count as u64,
        source: ozzy_core::state::ProducerResultSource::Stored,
    }))
}

/// The request, authority, and bounds a committed reply header answers.
#[derive(Clone, Copy)]
pub(super) struct ReplyHeader {
    pub(super) request: Envelope,
    pub(super) local: NodeId,
    pub(super) authority: append::Authority,
    pub(super) send: EnvelopeLimits,
}

type Header = [u8; ozzy_proto::ENVELOPE_BYTES];

impl ReplyHeader {
    fn envelope(self, opcode: Opcode) -> Envelope {
        response::reply_envelope(self.request, self.local, opcode)
    }
}

pub(super) fn opened_header(
    buffer: &ProposalBuffer,
    config: &NativeIntakeConfig,
    writer: Option<ProducerId>,
    header: ReplyHeader,
    metadata: &mut Vec<u8>,
) -> Result<Header, NativeIntakeError> {
    let opened = buffer.producer_opened().ok_or(NativeIntakeError::History)?;
    if opened.partition != config.partition
        || opened.policy != config.policy
        || Some(opened.producer) != writer
        || opened.authority.group_id != header.authority.group_id
        || opened.authority.config_epoch != header.authority.config_epoch
    {
        return Err(NativeIntakeError::History);
    }
    let opened = producer::Opened {
        authority: header.authority,
        ..opened
    };
    Ok(producer::encode_opened(
        header.envelope(Opcode::ProducerOpened),
        opened,
        metadata,
        header.send,
    )?)
}

pub(super) fn appended_header(
    range: ozzy_core::state::ProducerResultSpan,
    mut confirmed: append::stream::Confirmed,
    header: ReplyHeader,
    metadata: &mut Vec<u8>,
) -> Result<Header, NativeIntakeError> {
    let sequence = range.first_sequence.get();
    let count = range.records;
    if sequence < confirmed.key.first_sequence
        || sequence
            .checked_add(count)
            .is_none_or(|end| end > confirmed.end_sequence)
    {
        return Err(NativeIntakeError::History);
    }
    confirmed.authority = header.authority;
    confirmed.key.first_sequence = sequence;
    confirmed.end_sequence = sequence + count;
    confirmed.first_offset = range.first_offset.get();
    confirmed.disposition = range.disposition();
    Ok(append::stream::encode_confirmed(
        header.envelope(Opcode::Appended),
        confirmed,
        metadata,
        header.send,
    )?)
}

pub(super) fn assignment_header(
    buffer: &ProposalBuffer,
    config: &NativeIntakeConfig,
    header: ReplyHeader,
    metadata: &mut Vec<u8>,
) -> Result<Header, NativeIntakeError> {
    let state = buffer
        .group_assignment_state()
        .ok_or(NativeIntakeError::History)?;
    if state.partition != config.partition || state.policy != config.policy {
        return Err(NativeIntakeError::History);
    }
    let assigned = group::AssignReply {
        authority: header.authority,
        partition: state.partition,
        group: state.group,
        member: state.member,
        epoch: state.epoch,
        plan_revision: state.plan_revision,
        cursor: state.cursor,
        retained_from: state.retained_from,
        end: state.end,
        policy: state.policy,
    };
    Ok(group::encode_assigned(
        header.envelope(Opcode::GroupAssigned),
        assigned,
        metadata,
        header.send,
    )?)
}

pub(super) fn progress_header(
    buffer: &ProposalBuffer,
    config: &NativeIntakeConfig,
    header: ReplyHeader,
    metadata: &mut Vec<u8>,
) -> Result<Header, NativeIntakeError> {
    let state = buffer
        .group_progress_state()
        .ok_or(NativeIntakeError::History)?;
    if state.assignment.partition != config.partition || state.policy != config.policy {
        return Err(NativeIntakeError::History);
    }
    let confirmed = progress::Confirmed {
        authority: header.authority,
        assignment: state.assignment,
        policy: state.policy,
        cursor: state.cursor,
        retained_from: state.retained_from,
        end: state.end,
    };
    Ok(progress::encode_confirmed(
        header.envelope(Opcode::ProgressCommitted),
        confirmed,
        metadata,
        header.send,
    )?)
}

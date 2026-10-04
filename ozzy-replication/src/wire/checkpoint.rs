//! Nonvoting checkpoint bytes over bounded, source/nonce-bound PEER exchanges.

use super::{
    COMMON_BYTES, ControlEncoding, PeerBinding, Reader, WireError, WireLimits, Writer,
    transfer::{read_source, validate_source, write_source},
    validate_scope,
};
use crate::{LogSource, Scope};
use ozzy_proto::{Envelope, LinkSessionId, NodeId, Opcode, RequestId};

const METADATA_BYTES: usize = COMMON_BYTES + 16 + 72 + 8 + 4;

/// One bounded byte range of a previously pinned recovery checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointRequest {
    /// Exact configuration/view of the recovery attempt.
    pub scope: Scope,
    /// Outstanding exchange, checked again against the current link session.
    pub request_id: RequestId,
    /// Fresh recovery nonce, independent of transient link identities.
    pub nonce: RequestId,
    /// Exact donor incarnation and accepted tail retained by its pin.
    pub source: LogSource,
    /// Next logical checkpoint-state byte, beginning at zero.
    pub offset: u64,
    /// Positive maximum returned bytes; a source chunk boundary may return less.
    pub max_bytes: u32,
}

impl CheckpointRequest {
    fn validate(self, limits: WireLimits) -> Result<(), WireError> {
        validate_scope(self.scope)?;
        validate_source(self.source)?;
        if self.request_id.as_bytes() == &[0; 16] || self.nonce.as_bytes() == &[0; 16] {
            return Err(WireError::Correlation);
        }
        if self.max_bytes == 0 || self.max_bytes as usize > limits.envelope.max_payload_bytes {
            return Err(WireError::Limit);
        }
        Ok(())
    }
}

/// Source-bound checkpoint exchange, supplying no normal quorum evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointMessage<'a> {
    /// Request bytes only after the donor has offered its immutable descriptor.
    Request(CheckpointRequest),
    /// Echo the exact request alongside one positive bounded state slice.
    Chunk {
        /// Exact nonce/source-bound request being answered.
        request: CheckpointRequest,
        /// Positive state slice within the requested limit.
        bytes: &'a [u8],
    },
}

/// Encode checkpoint request/chunk metadata without copying state bytes.
pub fn encode_checkpoint(
    sender: NodeId,
    session: LinkSessionId,
    message: CheckpointMessage<'_>,
    output: &mut [u8],
    limits: WireLimits,
) -> Result<ControlEncoding, WireError> {
    let (request, payload_bytes, response) = match message {
        CheckpointMessage::Request(request) => (request, 0, false),
        CheckpointMessage::Chunk { request, bytes } => {
            if bytes.is_empty()
                || bytes.len() > request.max_bytes as usize
                || request.source.voter != sender
            {
                return Err(WireError::History);
            }
            (request, bytes.len(), true)
        }
    };
    request.validate(limits)?;
    let header = Envelope {
        opcode: if response {
            Opcode::SnapshotChunk
        } else {
            Opcode::SnapshotBegin
        },
        response,
        request_id: Some(request.request_id),
        sender,
        session: Some(session),
    }
    .encode_header(METADATA_BYTES, payload_bytes, limits.envelope)?;
    if output.len() < METADATA_BYTES {
        return Err(WireError::Capacity);
    }
    let mut writer = Writer::new(&mut output[..METADATA_BYTES]);
    writer.scope(request.scope, sender);
    writer.bytes(request.nonce.as_bytes());
    write_source(&mut writer, request.source);
    writer.bytes(&request.offset.to_be_bytes());
    writer.bytes(&request.max_bytes.to_be_bytes());
    Ok(ControlEncoding {
        header,
        metadata_bytes: METADATA_BYTES,
    })
}

pub(super) fn decode<'a>(
    scope: Scope,
    envelope: Envelope,
    mut reader: Reader<'_>,
    payload: &'a [u8],
    binding: PeerBinding,
    limits: WireLimits,
) -> Result<CheckpointMessage<'a>, WireError> {
    let request = CheckpointRequest {
        scope,
        request_id: envelope.request_id.ok_or(WireError::Correlation)?,
        nonce: RequestId::from_bytes(reader.bytes()?),
        source: read_source(&mut reader)?,
        offset: u64::from_be_bytes(reader.bytes()?),
        max_bytes: reader.u32()?,
    };
    reader.finish()?;
    request.validate(limits)?;
    binding.configuration.voter_index(request.source.voter)?;
    match envelope.opcode {
        Opcode::SnapshotBegin if !envelope.response && payload.is_empty() => {
            Ok(CheckpointMessage::Request(request))
        }
        Opcode::SnapshotChunk
            if envelope.response
                && request.source.voter == binding.peer
                && !payload.is_empty()
                && payload.len() <= request.max_bytes as usize =>
        {
            Ok(CheckpointMessage::Chunk {
                request,
                bytes: payload,
            })
        }
        _ => Err(WireError::Correlation),
    }
}

/// Exact old receiver or history request being withdrawn. Neither is authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryFence {
    /// Volatile normal receive incarnation.
    Receive(crate::flow::ReceiveEpoch),
    /// Outstanding election history request.
    Fetch(RequestId),
}

/// Primary hint to withdraw a lagging receiver before fresh quorum recovery.
/// This carries no vote, commit proof, or checkpoint installation authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryRetired {
    /// Exact normal authority view.
    pub scope: Scope,
    /// Receiver incarnation whose independently checked history is unavailable.
    pub fence: HistoryFence,
    /// Last original operation preceding all required retained operations.
    pub before: crate::Prefix,
}

/// Encode a receiver-fenced withdrawal hint on the control plane.
pub fn encode_history_retired(
    sender: NodeId,
    session: LinkSessionId,
    notice: HistoryRetired,
    output: &mut [u8],
    limits: WireLimits,
) -> Result<ControlEncoding, WireError> {
    validate_scope(notice.scope)?;
    super::validate_prefix(notice.before)?;
    if notice.before.op.0 == 0
        || matches!(notice.fence, HistoryFence::Fetch(id) if id.as_bytes() == &[0; 16])
    {
        return Err(WireError::History);
    }
    let length = COMMON_BYTES + 1 + 16 + 40;
    let header = Envelope {
        opcode: Opcode::HistoryRetired,
        response: false,
        request_id: None,
        sender,
        session: Some(session),
    }
    .encode_header(length, 0, limits.envelope)?;
    if output.len() < length {
        return Err(WireError::Capacity);
    }
    let mut writer = Writer::new(&mut output[..length]);
    writer.scope(notice.scope, sender);
    match notice.fence {
        HistoryFence::Receive(epoch) => {
            writer.bytes(&[0]);
            writer.bytes(&epoch.get().to_be_bytes());
        }
        HistoryFence::Fetch(id) => {
            writer.bytes(&[1]);
            writer.bytes(id.as_bytes());
        }
    }
    writer.prefix(notice.before);
    Ok(ControlEncoding {
        header,
        metadata_bytes: length,
    })
}

pub(super) fn decode_retired(
    scope: Scope,
    envelope: Envelope,
    mut reader: Reader<'_>,
    payload: &[u8],
) -> Result<HistoryRetired, WireError> {
    if envelope.response || envelope.request_id.is_some() || !payload.is_empty() {
        return Err(WireError::Correlation);
    }
    let tag: [u8; 1] = reader.bytes()?;
    let id: [u8; 16] = reader.bytes()?;
    let fence = match tag[0] {
        0 => HistoryFence::Receive(crate::flow::ReceiveEpoch::new(u128::from_be_bytes(id))?),
        1 if id != [0; 16] => HistoryFence::Fetch(RequestId::from_bytes(id)),
        _ => return Err(WireError::Correlation),
    };
    let before = reader.prefix()?;
    reader.finish()?;
    super::validate_prefix(before)?;
    if before.op.0 == 0 {
        return Err(WireError::History);
    }
    Ok(HistoryRetired {
        scope,
        fence,
        before,
    })
}

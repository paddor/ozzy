//! Nonvoting, nonce-scoped recovery exchanges. Full WAL only, no checkpoints.

use ozzy_proto::{Envelope, Opcode};
use ozzy_proto::{LinkSessionId, NodeId, RequestId};

use super::{
    COMMON_BYTES, ControlEncoding, PeerBinding, Reader, WireError, WireLimits, Writer,
    validate_prefix, validate_scope,
};
use crate::recovery::{RecoveryLog, RecoveryResponse};
use crate::{JournalGeneration, Scope};

const REQUEST_BYTES: usize = COMMON_BYTES + 16;
const BACKUP_BYTES: usize = REQUEST_BYTES + 1;
const PRIMARY_BYTES: usize = BACKUP_BYTES + 16 + 40 + 40;

/// Fresh recovery attempt carried by one independently correlated link exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryRequest {
    /// Trusted group/configuration and a view hint, never a promise or vote.
    pub scope: Scope,
    /// Outstanding request in the authenticated link session.
    pub request_id: RequestId,
    /// Globally fresh recovery attempt, stable across retries and link replacement.
    pub nonce: RequestId,
}

impl RecoveryRequest {
    fn validate(self) -> Result<(), WireError> {
        validate_scope(self.scope)?;
        if self.request_id.as_bytes() == &[0; 16] || self.nonce.as_bytes() == &[0; 16] {
            return Err(WireError::Correlation);
        }
        Ok(())
    }
}

/// Correlated reply retaining the core's nonce-scoped normal-state evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveryState {
    /// Echoed link exchange ID, independently checked against the current request.
    pub request_id: RequestId,
    /// Immutable response for this recovery nonce and normal view, not a vote.
    pub response: RecoveryResponse,
}

impl RecoveryState {
    /// Match both the outstanding link request and the logical recovery attempt.
    ///
    /// Call after authenticated peer/session decoding. The responder's actual
    /// normal view can differ from the request hint; the recovery core selects
    /// the highest observed view. Matching alone grants no voting authority.
    pub fn validate_response(self, request: RecoveryRequest) -> Result<(), WireError> {
        self.validate()?;
        request.validate()?;
        if self.request_id != request.request_id || self.response.nonce != request.nonce {
            return Err(WireError::Correlation);
        }
        if self.response.scope
            != (Scope {
                view: self.response.scope.view,
                ..request.scope
            })
        {
            return Err(WireError::Scope);
        }
        Ok(())
    }

    fn validate(self) -> Result<(), WireError> {
        RecoveryRequest {
            scope: self.response.scope,
            request_id: self.request_id,
            nonce: self.response.nonce,
        }
        .validate()?;
        if let Some(log) = self.response.primary {
            validate_prefix(log.accepted)?;
            validate_prefix(log.committed)?;
            if log.generation.0 == 0
                || log.committed.op > log.accepted.op
                || (log.committed.op == log.accepted.op && log.committed != log.accepted)
            {
                return Err(WireError::History);
            }
        }
        Ok(())
    }
}

/// Separate recovery family. These messages never supply normal durable votes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMessage {
    /// Ask activated normal voters for fresh evidence under an existing membership.
    Request(RecoveryRequest),
    /// A normal responder's evidence, still subject to the recovery core's checks.
    State(RecoveryState),
}

/// Encode `RECOVERY_STATE` into reusable storage without mutation on error.
///
/// Obtain the response from an activated normal core and retain it unchanged per
/// nonce/view. Encoding proves neither that role nor retention of source bytes.
pub fn encode_recovery_state(
    sender: NodeId,
    session: LinkSessionId,
    state: RecoveryState,
    output: &mut [u8],
    limits: WireLimits,
) -> Result<ControlEncoding, WireError> {
    state.validate()?;
    let metadata_bytes = if state.response.primary.is_some() {
        PRIMARY_BYTES
    } else {
        BACKUP_BYTES
    };
    let header = Envelope {
        opcode: Opcode::RecoveryState,
        response: true,
        request_id: Some(state.request_id),
        sender,
        session: Some(session),
    }
    .encode_header(metadata_bytes, 0, limits.envelope)?;
    if output.len() < metadata_bytes {
        return Err(WireError::Capacity);
    }
    let mut writer = Writer::new(&mut output[..metadata_bytes]);
    writer.scope(state.response.scope, sender);
    writer.bytes(state.response.nonce.as_bytes());
    writer.bytes(&[u8::from(state.response.primary.is_some())]);
    if let Some(log) = state.response.primary {
        writer.bytes(&log.generation.0.to_be_bytes());
        writer.prefix(log.accepted);
        writer.prefix(log.committed);
    }
    Ok(ControlEncoding {
        header,
        metadata_bytes,
    })
}

/// Encode full-WAL `RECOVERY` without allocations or output mutation on error.
pub fn encode_recovery(
    sender: NodeId,
    session: LinkSessionId,
    request: RecoveryRequest,
    output: &mut [u8],
    limits: WireLimits,
) -> Result<ControlEncoding, WireError> {
    request.validate()?;
    let header = Envelope {
        opcode: Opcode::Recovery,
        response: false,
        request_id: Some(request.request_id),
        sender,
        session: Some(session),
    }
    .encode_header(REQUEST_BYTES, 0, limits.envelope)?;
    if output.len() < REQUEST_BYTES {
        return Err(WireError::Capacity);
    }
    let mut writer = Writer::new(&mut output[..REQUEST_BYTES]);
    writer.scope(request.scope, sender);
    writer.bytes(request.nonce.as_bytes());
    Ok(ControlEncoding {
        header,
        metadata_bytes: REQUEST_BYTES,
    })
}

pub(super) fn decode_request(
    scope: Scope,
    envelope: Envelope,
    mut reader: Reader<'_>,
    payload: &[u8],
) -> Result<RecoveryRequest, WireError> {
    if !payload.is_empty() {
        return Err(WireError::Payload);
    }
    if envelope.response {
        return Err(WireError::Correlation);
    }
    let request = RecoveryRequest {
        scope,
        request_id: envelope.request_id.ok_or(WireError::Correlation)?,
        nonce: RequestId::from_bytes(reader.bytes()?),
    };
    reader.finish()?;
    request.validate()?;
    Ok(request)
}

pub(super) fn decode_state(
    scope: Scope,
    envelope: Envelope,
    mut reader: Reader<'_>,
    payload: &[u8],
    binding: PeerBinding,
) -> Result<RecoveryState, WireError> {
    if !payload.is_empty() {
        return Err(WireError::Payload);
    }
    if !envelope.response {
        return Err(WireError::Correlation);
    }
    let request_id = envelope.request_id.ok_or(WireError::Correlation)?;
    let nonce = RequestId::from_bytes(reader.bytes()?);
    let primary = match reader.bytes::<1>()? {
        [0] => None,
        [1] => Some(RecoveryLog {
            generation: JournalGeneration(u128::from_be_bytes(reader.bytes()?)),
            accepted: reader.prefix()?,
            committed: reader.prefix()?,
        }),
        _ => return Err(WireError::History),
    };
    reader.finish()?;
    if primary.is_some() != (binding.peer == binding.configuration.primary(scope.view)) {
        return Err(WireError::History);
    }
    let state = RecoveryState {
        request_id,
        response: RecoveryResponse {
            scope,
            nonce,
            primary,
        },
    };
    state.validate()?;
    Ok(state)
}

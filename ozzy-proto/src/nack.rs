//! Bounded negative responses. A timeout is not a negative response.

use super::append::Authority;
use super::{ENVELOPE_BYTES, Envelope, EnvelopeError, EnvelopeLimits, Opcode, Packet};
use crate::{GroupId, NodeId};

/// Routing information, never evidence of primary activation or quorum commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthorityHint {
    /// Group/configuration and observed proposed view.
    pub authority: Authority,
    /// Configured primary for that view; clients verify against fixed membership.
    pub primary: NodeId,
}

impl AuthorityHint {
    /// Encode the 48-byte detail carried by authority-related native NACKs.
    pub fn encode(self) -> Result<[u8; 48], NackError> {
        self.validate()?;
        let mut bytes = [0; 48];
        bytes[..16].copy_from_slice(self.authority.group_id.as_bytes());
        bytes[16..24].copy_from_slice(&self.authority.config_epoch.to_be_bytes());
        bytes[24..32].copy_from_slice(&self.authority.view.to_be_bytes());
        bytes[32..].copy_from_slice(self.primary.as_bytes());
        Ok(bytes)
    }

    /// Decode structural fields only. Membership and monotonic-view checks remain
    /// client responsibilities, after verifying the response's session/correlation.
    pub fn decode(bytes: &[u8]) -> Result<Self, NackError> {
        let bytes: &[u8; 48] = bytes.try_into().map_err(|_| NackError::Length)?;
        let hint = Self {
            authority: Authority {
                group_id: GroupId::from_bytes(bytes[..16].try_into().expect("field size")),
                config_epoch: u64::from_be_bytes(bytes[16..24].try_into().expect("field size")),
                view: u64::from_be_bytes(bytes[24..32].try_into().expect("field size")),
            },
            primary: NodeId::from_bytes(bytes[32..].try_into().expect("field size")),
        };
        hint.validate()?;
        Ok(hint)
    }

    fn validate(self) -> Result<(), NackError> {
        if self.authority.group_id.as_bytes() == &[0; 16]
            || self.authority.config_epoch == 0
            || self.primary.as_bytes() == &[0; 16]
        {
            return Err(NackError::Authority);
        }
        Ok(())
    }
}

/// Machine-readable next action. Does not describe the outcome of prior attempts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RetryClass {
    /// Correct the request; repeating unchanged bytes cannot resolve this rejection.
    Permanent = 1,
    /// Retry after local/remote capacity becomes available.
    AfterCredit = 2,
    /// Negotiate a fresh link session before retrying unchanged append identity.
    AfterReconnect = 3,
    /// Refresh group/owner authority without changing append identity.
    AfterAuthorityRefresh = 4,
    /// An admitted attempt may have committed. Reconcile the same identity.
    UnknownOutcome = 5,
}

/// Typed rejection with opaque code-specific detail; unknown codes remain readable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Nack<'a> {
    /// Stable error code, independent of diagnostic text.
    pub code: u16,
    /// Protocol retry classification, never parsed from text.
    pub retry: RetryClass,
    /// Bounded code-specific detail bytes.
    pub detail: &'a [u8],
    /// Optional human-readable UTF-8; never used for decisions.
    pub diagnostic: &'a str,
}

/// Encode a correlated NACK into reserved metadata; payload must be empty.
pub fn encode(
    envelope: Envelope,
    nack: Nack<'_>,
    metadata: &mut Vec<u8>,
    limits: EnvelopeLimits,
) -> Result<[u8; ENVELOPE_BYTES], NackError> {
    command(envelope)?;
    let size = 11_usize
        .checked_add(nack.detail.len())
        .and_then(|n| n.checked_add(nack.diagnostic.len()))
        .ok_or(NackError::Length)?;
    let header = envelope.encode_header(size, 0, limits)?;
    let detail = u32::try_from(nack.detail.len()).map_err(|_| NackError::Length)?;
    let diagnostic = u32::try_from(nack.diagnostic.len()).map_err(|_| NackError::Length)?;
    if metadata.capacity() < size {
        return Err(NackError::Capacity);
    }
    metadata.clear();
    metadata.extend_from_slice(&nack.code.to_be_bytes());
    metadata.push(nack.retry as u8);
    metadata.extend_from_slice(&detail.to_be_bytes());
    metadata.extend_from_slice(nack.detail);
    metadata.extend_from_slice(&diagnostic.to_be_bytes());
    metadata.extend_from_slice(nack.diagnostic.as_bytes());
    Ok(header)
}

/// Decode exact frame lengths without allocating, retaining unknown codes/detail.
pub fn decode(packet: Packet<'_>, limits: EnvelopeLimits) -> Result<Nack<'_>, NackError> {
    command(packet.envelope)?;
    packet
        .envelope
        .validate_frames(packet.metadata.len(), packet.payload.len(), limits)?;
    if !packet.payload.is_empty() {
        return Err(NackError::Command);
    }
    let mut bytes = packet.metadata;
    let code = u16::from_be_bytes(take(&mut bytes, 2)?.try_into().expect("field size"));
    let retry = match take(&mut bytes, 1)?[0] {
        1 => RetryClass::Permanent,
        2 => RetryClass::AfterCredit,
        3 => RetryClass::AfterReconnect,
        4 => RetryClass::AfterAuthorityRefresh,
        5 => RetryClass::UnknownOutcome,
        _ => return Err(NackError::Command),
    };
    let size = u32::from_be_bytes(take(&mut bytes, 4)?.try_into().expect("field size")) as usize;
    let detail = take(&mut bytes, size)?;
    let size = u32::from_be_bytes(take(&mut bytes, 4)?.try_into().expect("field size")) as usize;
    let diagnostic = std::str::from_utf8(take(&mut bytes, size)?).map_err(|_| NackError::Length)?;
    if !bytes.is_empty() {
        return Err(NackError::Length);
    }
    Ok(Nack {
        code,
        retry,
        detail,
        diagnostic,
    })
}

fn command(envelope: Envelope) -> Result<(), NackError> {
    if envelope.opcode != Opcode::Nack || !envelope.response || envelope.request_id.is_none() {
        return Err(NackError::Command);
    }
    Ok(())
}

fn take<'a>(bytes: &mut &'a [u8], size: usize) -> Result<&'a [u8], NackError> {
    let (value, rest) = bytes.split_at_checked(size).ok_or(NackError::Length)?;
    *bytes = rest;
    Ok(value)
}

/// Malformed or unencodable negative response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NackError {
    /// Zero group, configuration epoch, or primary identity in a routing hint.
    #[error("invalid NACK authority hint")]
    Authority,
    /// Invalid outer command/session/limits.
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
    /// Wrong direction, opcode, payload, or retry class.
    #[error("invalid NACK command")]
    Command,
    /// Truncated, trailing, oversized, or invalid text bytes.
    #[error("invalid NACK field length")]
    Length,
    /// Caller must reserve sufficient metadata capacity.
    #[error("NACK encode buffer capacity exhausted")]
    Capacity,
}

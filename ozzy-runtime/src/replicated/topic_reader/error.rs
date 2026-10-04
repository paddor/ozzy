//! Typed read failures; no failure silently skips unavailable records.

use super::super::Error;
use ozzy_proto::{Offset, nack};

/// Read failure. No variant silently skips expired or unavailable records.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReaderError {
    /// Transport, negotiation, or local connection configuration failed.
    #[error(transparent)]
    Connection(#[from] Error),
    /// Malformed or oversized local request or remote data.
    #[error(transparent)]
    Codec(#[from] ozzy_proto::data::CodecError),
    /// Response identities or positions do not match the live subscription.
    #[error("read response does not match request")]
    Response,
    /// A canceled/failed request must be fenced before another subscription.
    #[error("reader must reconnect before another subscription")]
    ReconnectRequired,
    /// Requested records expired. Caller must explicitly select another offset.
    #[error("records expired; earliest retained offset is {earliest:?}")]
    RetentionGap {
        /// First still-retained offset.
        earliest: Offset,
    },
    /// Requested offset is beyond this broker's confirmed end.
    #[error("offset exceeds confirmed end {committed_end:?}")]
    Ahead {
        /// Exclusive locally applied end.
        committed_end: Offset,
    },
    /// Next record cannot fit this request's output bounds.
    #[error("record exceeds read bounds: {bytes} bytes, {parts} parts")]
    RecordTooLarge {
        /// Payload bytes in the indivisible record.
        bytes: u64,
        /// Parts in the indivisible record.
        parts: u64,
    },
    /// Explicit remote rejection, with uninterpreted bounded detail preserved.
    #[error("read rejected: code {code}, retry {retry:?}")]
    Rejected {
        /// Native error code.
        code: u16,
        /// Native retry class.
        retry: nack::RetryClass,
        /// Exact typed detail, never parsed from text.
        detail: Vec<u8>,
    },
}

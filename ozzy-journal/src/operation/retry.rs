//! Exact retry evidence for attempted records whose duplicate bytes are discarded.

use super::{OperationCodecError, RecordRef};
use ozzy_proto::data::Encoding;

const DOMAIN: &[u8] = b"ozzy producer retry fingerprint\0";

/// BLAKE3 over one attempted record, distinct from noncryptographic log checksums.
/// Retire this evidence only with the producer's confirmed result floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryFingerprint([u8; 32]);

impl RetryFingerprint {
    /// Restore exact evidence from a validated canonical operation.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Original 256-bit BLAKE3 output.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Hash without concatenating or retaining payload parts. Include the message ID,
/// encoding, part count, every part length, and every byte in record order.
/// Whole-batch packing must be removed before calling; per-record encoding stays
/// part of exact retry identity, as in ordinary byte-for-byte retry validation.
pub fn retry_fingerprint(record: RecordRef<'_>) -> Result<RetryFingerprint, OperationCodecError> {
    if record.message_id.as_bytes() == &[0; 16] {
        return Err(OperationCodecError::EmptyValue("message identity"));
    }
    if record.parts.is_empty() {
        return Err(OperationCodecError::EmptyRecordParts);
    }
    let mut hash = blake3::Hasher::new();
    hash.update(DOMAIN);
    hash.update(record.message_id.as_bytes());
    match record.encoding {
        Encoding::Raw => {
            hash.update(&[0]);
        }
        Encoding::Lz4 { decoded_bytes } => {
            hash.update(&[1]);
            hash.update(&decoded_bytes.to_be_bytes());
        }
    }
    let parts =
        u64::try_from(record.parts.len()).map_err(|_| OperationCodecError::LengthOverflow)?;
    hash.update(&parts.to_be_bytes());
    for part in record.parts.iter() {
        let length = u64::try_from(part.len()).map_err(|_| OperationCodecError::LengthOverflow)?;
        hash.update(&length.to_be_bytes());
        hash.update(part);
    }
    Ok(RetryFingerprint(*hash.finalize().as_bytes()))
}

#[cfg(test)]
mod tests;

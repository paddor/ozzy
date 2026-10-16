//! Decode bounds for one segment and their validation.

use super::{CodecError, DecodedOperation, ENTRY_HEADER_BYTES, entry::ParsedEntry, shared};

/// Bounds enforced before accepting declared entry lengths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeLimits {
    /// Maximum physical write groups decoded in one segment.
    pub max_groups: usize,
    /// Maximum canonical operation entries decoded in one physical group.
    pub max_entries: usize,
    /// Maximum encoded bytes per physical operation entry.
    pub max_entry_bytes: usize,
    /// Maximum decoded canonical bytes per operation body.
    pub max_decoded_body_bytes: usize,
    /// Maximum combined decoded canonical bytes per physical group.
    pub max_group_decoded_body_bytes: usize,
    /// Maximum combined decoded canonical bytes per segment.
    pub max_segment_decoded_body_bytes: usize,
}

impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_groups: 65_536,
            max_entries: 65_536,
            max_entry_bytes: 64 * 1024 * 1024,
            max_decoded_body_bytes: 64 * 1024 * 1024,
            max_group_decoded_body_bytes: 256 * 1024 * 1024,
            max_segment_decoded_body_bytes: 1024 * 1024 * 1024,
        }
    }
}

impl DecodeLimits {
    pub(crate) fn group_metadata_bytes(self) -> Option<usize> {
        // Parsing grows a Vec geometrically; decoded operations coexist with
        // its consuming iterator until the final entry has been checked.
        self.max_entries
            .checked_mul(2 * size_of::<ParsedEntry>() + size_of::<DecodedOperation<'_>>())
    }

    /// Maximum indexed extent, including a shared compressed body and its
    /// uncompressed operation headers. Individual operations keep their limits.
    pub fn max_indexed_entry_bytes(self) -> usize {
        self.max_entry_bytes.max(
            self.max_entries
                .saturating_mul(ENTRY_HEADER_BYTES)
                .saturating_add(shared::HEADER_BYTES)
                .saturating_add(self.max_group_decoded_body_bytes)
                .saturating_add(7),
        )
    }
}

pub(crate) fn validate_decode_limits(limits: DecodeLimits) -> Result<(), CodecError> {
    if limits.max_groups == 0
        || limits.max_entries == 0
        || limits.max_entry_bytes < ENTRY_HEADER_BYTES
        || limits.max_decoded_body_bytes == 0
        || limits.max_group_decoded_body_bytes < limits.max_decoded_body_bytes
        || limits.max_segment_decoded_body_bytes < limits.max_group_decoded_body_bytes
    {
        return Err(CodecError::LimitExceeded {
            kind: "decoder configuration",
            actual: 0,
            limit: 0,
        });
    }
    Ok(())
}

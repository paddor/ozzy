//! The current-reference file that selects one manifest.

use super::{
    CURRENT_BYTES, CurrentReference, METADATA_VERSION, MetadataError, all_zero, require_len,
    require_nonzero_id, require_version, verify_hash,
};
use ozzy_journal::integrity::hash_without_digest;
use ozzy_journal::operation::Digest;
use ozzy_proto::layout::{self, Fixed};
use ozzy_proto::{GroupId, StoreId};
use std::ops::Range;

const CURRENT_MAGIC: &[u8; 8] = b"OZYCUR\0\0";
const CURRENT_HASH_CONTEXT: &str = "ozzy journal current reference v1";
const CURRENT_DIGEST_RANGE: Range<usize> = 88..120;

/// Validate and encode an exact manifest-selection reference.
pub fn encode_current(current: CurrentReference) -> Result<[u8; CURRENT_BYTES], MetadataError> {
    validate_current(current)?;
    let mut output = [0_u8; CURRENT_BYTES];
    output[0..8].copy_from_slice(CURRENT_MAGIC);
    output.put_u16::<8>(METADATA_VERSION);
    output.put_u16::<10>(CURRENT_BYTES as u16);
    output.put::<16, 16>(*current.group_id.as_bytes());
    output.put::<32, 16>(*current.store_id.as_bytes());
    output.put_u64::<48>(current.generation);
    output.put::<56, 32>(*current.manifest_digest.as_bytes());
    let digest = hash_without_digest(CURRENT_HASH_CONTEXT, &output, CURRENT_DIGEST_RANGE.start);
    output[CURRENT_DIGEST_RANGE].copy_from_slice(digest.as_bytes());
    Ok(output)
}

/// Validate and decode an exact manifest-selection reference.
pub fn decode_current(input: &[u8]) -> Result<CurrentReference, MetadataError> {
    require_len(input, CURRENT_BYTES, "CURRENT")?;
    if input.len() != CURRENT_BYTES {
        return Err(MetadataError::TrailingBytes(input.len() - CURRENT_BYTES));
    }
    let input: &[u8; CURRENT_BYTES] = layout::record(input, 0)?;
    if input.array::<0, 8>() != *CURRENT_MAGIC {
        return Err(MetadataError::WrongMagic("CURRENT"));
    }
    require_version(input.u16_at::<8>())?;
    if input.u16_at::<10>() != CURRENT_BYTES as u16
        || input.u32_at::<12>() != 0
        || !all_zero(input.slice::<120, { CURRENT_BYTES - 120 }>())
    {
        return Err(MetadataError::UnsupportedFields("CURRENT"));
    }
    verify_hash(CURRENT_HASH_CONTEXT, input, CURRENT_DIGEST_RANGE, "CURRENT")?;
    let current = CurrentReference {
        group_id: GroupId::from_bytes(input.array::<16, 16>()),
        store_id: StoreId::from_bytes(input.array::<32, 16>()),
        generation: input.u64_at::<48>(),
        manifest_digest: Digest::from_bytes(input.array::<56, 32>()),
    };
    validate_current(current)?;
    Ok(current)
}

fn validate_current(current: CurrentReference) -> Result<(), MetadataError> {
    require_nonzero_id("group", current.group_id.as_bytes())?;
    require_nonzero_id("store", current.store_id.as_bytes())?;
    if current.generation == 0 || current.manifest_digest == Digest::ZERO {
        return Err(MetadataError::InvalidCurrent);
    }
    Ok(())
}

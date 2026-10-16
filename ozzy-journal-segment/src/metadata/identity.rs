//! The group identity file: encoding and validated decoding.

use super::{
    GROUP_IDENTITY_BYTES, GroupIdentity, METADATA_VERSION, MetadataError, all_zero, require_len,
    require_version, verify_hash,
};
use ozzy_journal::integrity::hash_without_digest;
use ozzy_proto::layout::{self, Fixed};
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};
use std::ops::Range;

const IDENTITY_MAGIC: &[u8; 8] = b"OZYIDENT";
const IDENTITY_HASH_CONTEXT: &str = "ozzy journal group identity v1";
const IDENTITY_DIGEST_RANGE: Range<usize> = 80..112;

/// Validate and encode the fixed-size persistent group/store binding.
pub fn encode_group_identity(
    identity: GroupIdentity,
) -> Result<[u8; GROUP_IDENTITY_BYTES], MetadataError> {
    identity.validate()?;
    let mut output = [0_u8; GROUP_IDENTITY_BYTES];
    output[0..8].copy_from_slice(IDENTITY_MAGIC);
    output.put_u16::<8>(METADATA_VERSION);
    output.put_u16::<10>(GROUP_IDENTITY_BYTES as u16);
    output.put::<16, 16>(*identity.group_id.as_bytes());
    output.put::<32, 16>(*identity.replica_node_id.as_bytes());
    output.put::<48, 16>(*identity.volume_id.as_bytes());
    output.put::<64, 16>(*identity.store_id.as_bytes());
    output.put_u64::<112>(identity.store_generation);
    let digest = hash_without_digest(IDENTITY_HASH_CONTEXT, &output, IDENTITY_DIGEST_RANGE.start);
    output[IDENTITY_DIGEST_RANGE].copy_from_slice(digest.as_bytes());
    Ok(output)
}

/// Validate and decode the fixed-size persistent group/store binding.
pub fn decode_group_identity(input: &[u8]) -> Result<GroupIdentity, MetadataError> {
    require_len(input, GROUP_IDENTITY_BYTES, "group identity")?;
    if input.len() != GROUP_IDENTITY_BYTES {
        return Err(MetadataError::TrailingBytes(
            input.len() - GROUP_IDENTITY_BYTES,
        ));
    }
    let input: &[u8; GROUP_IDENTITY_BYTES] = layout::record(input, 0)?;
    if input.array::<0, 8>() != *IDENTITY_MAGIC {
        return Err(MetadataError::WrongMagic("group identity"));
    }
    require_version(input.u16_at::<8>())?;
    if input.u16_at::<10>() != GROUP_IDENTITY_BYTES as u16
        || input.u32_at::<12>() != 0
        || !all_zero(input.slice::<120, { GROUP_IDENTITY_BYTES - 120 }>())
    {
        return Err(MetadataError::UnsupportedFields("group identity"));
    }
    verify_hash(
        IDENTITY_HASH_CONTEXT,
        input,
        IDENTITY_DIGEST_RANGE,
        "group identity",
    )?;
    GroupIdentity {
        group_id: GroupId::from_bytes(input.array::<16, 16>()),
        replica_node_id: NodeId::from_bytes(input.array::<32, 16>()),
        volume_id: VolumeId::from_bytes(input.array::<48, 16>()),
        store_id: StoreId::from_bytes(input.array::<64, 16>()),
        store_generation: input.u64_at::<112>(),
    }
    .validate()
}

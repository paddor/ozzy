#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    clippy::too_many_lines,
    clippy::too_many_arguments,
    reason = "test code"
)]

use ozzy_journal_segment::{
    CheckpointChunk, CheckpointError, CheckpointLimits, CheckpointManifest, Digest, LogPosition,
    decode_checkpoint_manifest, encode_checkpoint_manifest,
};
use ozzy_proto::{CheckpointId, GroupId, StoreId};

fn digest(byte: u8) -> Digest {
    Digest::from_bytes([byte; 32])
}

/// A 23-byte state split into chunks of at most 7 bytes.
fn manifest() -> CheckpointManifest {
    let chunks = [7, 7, 7, 2]
        .into_iter()
        .enumerate()
        .map(|(ordinal, bytes)| CheckpointChunk {
            ordinal: ordinal as u32,
            logical_offset: ordinal as u64 * 7,
            bytes,
            digest: digest(0x60 + ordinal as u8),
        })
        .collect();
    CheckpointManifest {
        group_id: GroupId::from_bytes([0x11; 16]),
        store_id: StoreId::from_bytes([0x22; 16]),
        checkpoint_id: CheckpointId::from_bytes([0x67; 16]),
        position: LogPosition {
            op_number: 19,
            digest: digest(0x33),
        },
        configuration_epoch: 4,
        source_manifest_generation: 7,
        source_manifest_digest: digest(0x44),
        state_schema_digest: digest(0x55),
        state_bytes: 23,
        chunk_bytes: 7,
        state_digest: digest(0x66),
        chunks,
    }
}

#[test]
fn manifest_decoder_rejects_every_truncation_and_corruption() {
    let limits = CheckpointLimits::default();
    let manifest = encode_checkpoint_manifest(&manifest(), limits).unwrap();
    assert_eq!(
        decode_checkpoint_manifest(&manifest, limits).unwrap(),
        self::manifest()
    );
    for length in 0..manifest.len() {
        assert!(decode_checkpoint_manifest(&manifest[..length], limits).is_err());
    }
    let mut corrupt = manifest;
    corrupt[128] ^= 1;
    assert!(matches!(
        decode_checkpoint_manifest(&corrupt, limits),
        Err(CheckpointError::DigestMismatch("checkpoint manifest"))
    ));
}

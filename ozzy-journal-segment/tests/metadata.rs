use ozzy_journal::operation::{ChainPosition, Digest};
use ozzy_journal_segment::{
    CURRENT_BYTES, CheckpointReference, CommitMode, CurrentReference, GROUP_IDENTITY_BYTES,
    GroupIdentity, LogPosition, Manifest, MetadataError, MetadataLimits, SEGMENT_HEADER_BYTES,
    SealedSegment, SegmentReference, decode_current, decode_group_identity, decode_manifest,
    encode_current, encode_group_identity, encode_manifest, encode_manifest_with_limits,
    manifest_digest,
};
use ozzy_proto::{CheckpointId, GroupId, NodeId, StoreId, VolumeId};

fn digest(byte: u8) -> Digest {
    Digest::from_bytes([byte; 32])
}

fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([0x11; 16]),
        replica_node_id: NodeId::from_bytes([0x22; 16]),
        volume_id: VolumeId::from_bytes([0x33; 16]),
        store_id: StoreId::from_bytes([0x44; 16]),
        store_generation: 1,
    }
}

fn manifest() -> Manifest {
    Manifest {
        generation: 2,
        parent_generation: 1,
        identity: identity(),
        configuration_epoch: 3,
        commit_mode: CommitMode::External,
        durable_evidence: false,
        promised_view: 5,
        last_normal_view: 4,
        accepted: LogPosition {
            op_number: 9,
            digest: digest(0x99),
        },
        committed: LogPosition {
            op_number: 8,
            digest: digest(0x88),
        },
        checkpoint: Some(CheckpointReference {
            checkpoint_id: CheckpointId::from_bytes([0x55; 16]),
            position: LogPosition {
                op_number: 6,
                digest: digest(0x66),
            },
            manifest_digest: digest(0x77),
        }),
        segments: vec![
            SegmentReference {
                segment_id: 7,
                file_generation: 0,
                first_group_number: 1,
                first_chain: ChainPosition::GENESIS,
                capacity: 64 * 1024,
                sealed: Some(SealedSegment {
                    valid_bytes: (SEGMENT_HEADER_BYTES + 4096) as u64,
                    digest: digest(0xaa),
                }),
            },
            SegmentReference {
                segment_id: 8,
                file_generation: 0,
                first_group_number: 3,
                first_chain: ChainPosition::new(7, digest(0x66)),
                capacity: 64 * 1024,
                sealed: None,
            },
        ],
    }
}

#[test]
fn identity_has_frozen_checksum_and_round_trips() {
    let encoded = encode_group_identity(identity()).unwrap();
    assert_eq!(encoded.len(), GROUP_IDENTITY_BYTES);
    assert_eq!(
        &encoded[80..112],
        &[
            104, 135, 68, 106, 189, 57, 160, 41, 227, 138, 216, 66, 206, 45, 135, 18, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    );
    assert_eq!(decode_group_identity(&encoded).unwrap(), identity());
    for length in 0..encoded.len() {
        assert!(decode_group_identity(&encoded[..length]).is_err());
    }
}

#[test]
fn old_group_metadata_fails_before_checksum_interpretation() {
    let mut bytes = encode_group_identity(identity()).unwrap();
    bytes[8..10].copy_from_slice(&1_u16.to_be_bytes());
    assert_eq!(
        decode_group_identity(&bytes),
        Err(MetadataError::UnsupportedVersion(1))
    );
}

#[test]
fn current_has_frozen_checksum_and_round_trips() {
    let current = CurrentReference {
        group_id: identity().group_id,
        store_id: identity().store_id,
        generation: 2,
        manifest_digest: digest(0xcc),
    };
    let encoded = encode_current(current).unwrap();
    assert_eq!(encoded.len(), CURRENT_BYTES);
    assert_eq!(
        &encoded[88..120],
        &[
            50, 255, 37, 170, 235, 106, 171, 77, 180, 2, 242, 162, 223, 100, 43, 124, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ]
    );
    assert_eq!(decode_current(&encoded).unwrap(), current);
    for length in 0..encoded.len() {
        assert!(decode_current(&encoded[..length]).is_err());
    }
}

#[test]
fn manifest_has_frozen_checksum_and_round_trips() {
    let manifest = manifest();
    let encoded = encode_manifest(&manifest).unwrap();
    let actual_digest = manifest_digest(&encoded, MetadataLimits::default()).unwrap();
    assert_eq!(
        actual_digest,
        Digest::from_bytes([
            178, 159, 164, 88, 211, 229, 170, 165, 107, 9, 170, 98, 120, 206, 108, 28, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ])
    );
    assert_eq!(
        decode_manifest(&encoded, MetadataLimits::default()).unwrap(),
        manifest
    );
    for length in 0..encoded.len() {
        assert!(decode_manifest(&encoded[..length], MetadataLimits::default()).is_err());
    }
}

#[test]
fn manifest_selects_exact_physical_incarnations_without_growing_references() {
    let mut manifest = manifest();
    let original_size = encode_manifest(&manifest).unwrap().len();
    manifest.segments[0].file_generation = u64::MAX;
    let bytes = encode_manifest(&manifest).unwrap();
    assert_eq!(bytes.len(), original_size);
    assert_eq!(
        decode_manifest(&bytes, MetadataLimits::default()).unwrap(),
        manifest
    );
}

#[test]
fn manifest_persists_local_commit_recovery_mode() {
    let mut manifest = manifest();
    manifest.commit_mode = CommitMode::LocalDurable;
    let encoded = encode_manifest(&manifest).unwrap();
    assert_eq!(u32::from_be_bytes(encoded[12..16].try_into().unwrap()), 3);
    assert_eq!(
        decode_manifest(&encoded, MetadataLimits::default())
            .unwrap()
            .commit_mode,
        CommitMode::LocalDurable
    );
}

#[test]
fn metadata_corruption_never_decodes() {
    let mut identity = encode_group_identity(identity()).unwrap();
    identity[64] ^= 1;
    assert!(matches!(
        decode_group_identity(&identity),
        Err(MetadataError::DigestMismatch("group identity"))
    ));

    let mut current = encode_current(CurrentReference {
        group_id: GroupId::from_bytes([1; 16]),
        store_id: StoreId::from_bytes([2; 16]),
        generation: 1,
        manifest_digest: digest(3),
    })
    .unwrap();
    current[48] ^= 1;
    assert!(matches!(
        decode_current(&current),
        Err(MetadataError::DigestMismatch("CURRENT"))
    ));

    let mut manifest = encode_manifest(&manifest()).unwrap();
    manifest[400] ^= 1;
    assert!(matches!(
        decode_manifest(&manifest, MetadataLimits::default()),
        Err(MetadataError::DigestMismatch("manifest"))
    ));
}

#[test]
fn manifest_bounds_precede_reference_allocation() {
    let encoded = encode_manifest(&manifest()).unwrap();
    let limits = MetadataLimits {
        max_manifest_bytes: encoded.len() - 1,
        max_segments: usize::MAX,
    };
    assert!(matches!(
        decode_manifest(&encoded, limits),
        Err(MetadataError::LimitExceeded {
            kind: "manifest bytes",
            ..
        })
    ));

    let limits = MetadataLimits {
        max_manifest_bytes: usize::MAX,
        max_segments: 1,
    };
    assert!(matches!(
        decode_manifest(&encoded, limits),
        Err(MetadataError::LimitExceeded {
            kind: "manifest segment count",
            ..
        })
    ));
    assert!(matches!(
        encode_manifest_with_limits(&manifest(), limits),
        Err(MetadataError::LimitExceeded {
            kind: "manifest segment count",
            ..
        })
    ));
}

#[test]
fn manifest_requires_one_final_active_segment() {
    let mut value = manifest();
    value.segments[0].sealed = None;
    assert_eq!(
        encode_manifest(&value),
        Err(MetadataError::InvalidActiveSegment)
    );

    let mut value = manifest();
    value.segments[1].sealed = Some(SealedSegment {
        valid_bytes: (SEGMENT_HEADER_BYTES + 4096) as u64,
        digest: digest(0xbb),
    });
    assert_eq!(
        encode_manifest(&value),
        Err(MetadataError::InvalidActiveSegment)
    );
}

#[test]
fn manifest_hard_state_cannot_forget_a_promise() {
    let mut value = manifest();
    value.promised_view = 3;
    value.last_normal_view = 4;
    assert_eq!(encode_manifest(&value), Err(MetadataError::InvalidView));
}

#[test]
fn manifest_cannot_name_unrepresentable_terminal_operation() {
    let mut value = manifest();
    value.accepted = LogPosition {
        op_number: u64::MAX,
        digest: digest(1),
    };
    assert_eq!(
        encode_manifest(&value),
        Err(MetadataError::InvalidLogPosition)
    );
}

#[test]
fn decode_rejects_zero_volume_identity() {
    let mut encoded = encode_manifest(&manifest()).unwrap();
    encoded[72..88].fill(0);
    let digest = {
        let mut value = manifest();
        value.identity.volume_id = VolumeId::from_bytes([0; 16]);
        encode_manifest(&value)
    };
    assert_eq!(digest, Err(MetadataError::ZeroIdentity("volume")));

    // Corrupting frozen bytes still fails before semantic validation.
    assert!(matches!(
        decode_manifest(&encoded, MetadataLimits::default()),
        Err(MetadataError::DigestMismatch("manifest"))
    ));
}

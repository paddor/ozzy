use std::fs;

use ozzy_journal::operation::{Digest, OperationLimits};
use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    DecodeLimits, GroupIdentity, MetadataLimits, MountPolicy, SegmentHeader, VOLUME_IDENTITY_BYTES,
    VolumeConfig, VolumeDirectory, VolumeError, VolumeIdentity, VolumeSet, decode_volume_identity,
    encode_volume_identity,
};
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};
use tempfile::TempDir;

fn volume_identity(byte: u8) -> VolumeIdentity {
    VolumeIdentity {
        volume_id: VolumeId::from_bytes([byte; 16]),
    }
}

fn group_identity(volume: VolumeIdentity) -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([0x20; 16]),
        replica_node_id: NodeId::from_bytes([0x30; 16]),
        volume_id: volume.volume_id,
        store_id: StoreId::from_bytes([0x40; 16]),
        store_generation: 1,
    }
}

#[test]
fn volume_identity_has_frozen_layout_and_rejects_every_truncation() {
    let identity = volume_identity(0x10);
    let encoded = encode_volume_identity(identity).unwrap();
    assert_eq!(encoded.len(), VOLUME_IDENTITY_BYTES);
    assert_eq!(&encoded[..8], b"OZYVOL\0\0");
    assert_eq!(&encoded[8..12], &[0, 2, 0x10, 0]);
    assert_eq!(&encoded[16..32], identity.volume_id.as_bytes());
    assert_eq!(decode_volume_identity(&encoded).unwrap(), identity);
    for length in 0..encoded.len() {
        assert!(decode_volume_identity(&encoded[..length]).is_err());
    }
    let mut corrupt = encoded;
    corrupt[100] = 1;
    assert!(matches!(
        decode_volume_identity(&corrupt),
        Err(VolumeError::UnsupportedFields)
    ));
}

#[test]
fn open_never_formats_and_identity_or_lock_mismatch_stops_ownership() {
    let temporary = TempDir::new().unwrap();
    let missing = temporary.path().join("missing");
    assert!(matches!(
        VolumeDirectory::open(
            &missing,
            volume_identity(0x10),
            MountPolicy::PortableIdentity,
        ),
        Err(VolumeError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound
    ));
    assert!(!missing.exists());

    let root = temporary.path().join("volume");
    fs::create_dir(&root).unwrap();
    let volume =
        VolumeDirectory::format_new(&root, volume_identity(0x10), MountPolicy::PortableIdentity)
            .unwrap();
    assert!(root.join("identity").is_file());
    assert!(root.join("volume.lock").is_file());
    assert!(root.join("groups").is_dir());
    assert!(root.join("staging").is_dir());
    assert!(matches!(
        VolumeDirectory::open(&root, volume_identity(0x10), MountPolicy::PortableIdentity,),
        Err(VolumeError::Locked)
    ));
    drop(volume);
    assert!(matches!(
        VolumeDirectory::open(&root, volume_identity(0x11), MountPolicy::PortableIdentity,),
        Err(VolumeError::IdentityMismatch)
    ));
}

#[test]
fn group_paths_are_deterministic_and_require_matching_volume_identity() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("volume");
    fs::create_dir(&root).unwrap();
    let identity = volume_identity(0x10);
    let volume =
        VolumeDirectory::format_new(&root, identity, MountPolicy::PortableIdentity).unwrap();
    let group = group_identity(identity);
    let header = SegmentHeader::new(group.group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap();
    let directory = volume.format_group(group, 1, &header).unwrap();
    assert_eq!(directory.root(), volume.group_root(group.group_id));
    drop(directory);

    let mut wrong = group;
    wrong.volume_id = VolumeId::from_bytes([0x11; 16]);
    assert!(matches!(
        volume.format_group(wrong, 1, &header),
        Err(VolumeError::GroupVolumeMismatch)
    ));
    let journal = volume
        .open_group(
            group,
            MetadataLimits::default(),
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(journal.directory().identity(), group);
}

#[test]
fn volume_set_rejects_duplicate_identities_and_overlapping_roots() {
    let temporary = TempDir::new().unwrap();
    let first_root = temporary.path().join("first");
    let second_root = temporary.path().join("second");
    fs::create_dir(&first_root).unwrap();
    fs::create_dir(&second_root).unwrap();
    let first_identity = volume_identity(0x10);
    let second_identity = volume_identity(0x11);
    drop(
        VolumeDirectory::format_new(&first_root, first_identity, MountPolicy::PortableIdentity)
            .unwrap(),
    );
    drop(
        VolumeDirectory::format_new(&second_root, second_identity, MountPolicy::PortableIdentity)
            .unwrap(),
    );
    let configs = [
        VolumeConfig {
            root: first_root.clone(),
            identity: first_identity,
            mount_policy: MountPolicy::PortableIdentity,
        },
        VolumeConfig {
            root: second_root,
            identity: second_identity,
            mount_policy: MountPolicy::PortableIdentity,
        },
    ];
    let volumes = VolumeSet::open(&configs).unwrap();
    assert_eq!(volumes.len(), 2);
    assert_eq!(
        volumes.get(second_identity.volume_id).unwrap().identity(),
        second_identity
    );
    drop(volumes);

    let duplicate = [configs[0].clone(), configs[0].clone()];
    assert!(matches!(
        VolumeSet::open(&duplicate),
        Err(VolumeError::DuplicateIdentity)
    ));

    let overlapping = [
        configs[0].clone(),
        VolumeConfig {
            root: first_root.join("groups"),
            identity: second_identity,
            mount_policy: MountPolicy::PortableIdentity,
        },
    ];
    assert!(matches!(
        VolumeSet::open(&overlapping),
        Err(VolumeError::OverlappingRoots)
    ));
}

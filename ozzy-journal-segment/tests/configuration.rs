use std::fs;

use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    CheckpointLimits, DecodeLimits, Digest, DirectoryError, GROUP_CONFIGURATION_MAX_BYTES,
    GroupDirectory, GroupIdentity, MetadataLimits, MountPolicy, OperationLimits, RelocationError,
    SegmentHeader, VolumeDirectory, VolumeIdentity, prepare_relocation,
};
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};
use tempfile::TempDir;

const CONFIGURATION: &[u8] = b"opaque prevalidated replica configuration";

fn identity() -> GroupIdentity {
    GroupIdentity {
        group_id: GroupId::from_bytes([1; 16]),
        replica_node_id: NodeId::from_bytes([2; 16]),
        volume_id: VolumeId::from_bytes([3; 16]),
        store_id: StoreId::from_bytes([4; 16]),
        store_generation: 1,
    }
}

fn segment() -> SegmentHeader {
    SegmentHeader::new(identity().group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap()
}

#[test]
fn immutable_configuration_is_required_exactly_and_keeps_exclusive_ownership() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("group");
    let directory = GroupDirectory::format_new_with_configuration(
        &root,
        identity(),
        1,
        &segment(),
        CONFIGURATION,
    )
    .unwrap();
    assert_eq!(directory.configuration(), Some(CONFIGURATION));
    assert_eq!(fs::read(root.join("CONFIGURATION")).unwrap(), CONFIGURATION);
    assert!(matches!(
        GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION
        ),
        Err(DirectoryError::Locked)
    ));
    drop(directory);
    assert!(matches!(
        GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            b"another configuration"
        ),
        Err(DirectoryError::ConfigurationMismatch)
    ));
    assert_eq!(fs::read(root.join("CONFIGURATION")).unwrap(), CONFIGURATION);
    let opened = GroupDirectory::open_with_configuration(
        &root,
        identity(),
        MetadataLimits::default(),
        CONFIGURATION,
    )
    .unwrap();
    assert_eq!(opened.configuration(), Some(CONFIGURATION));
}

#[test]
fn invalid_configuration_is_rejected_before_creating_any_store() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("group");
    for configuration in [&[][..], &vec![0; GROUP_CONFIGURATION_MAX_BYTES + 1]] {
        assert!(matches!(
            GroupDirectory::format_new_with_configuration(
                &root,
                identity(),
                1,
                &segment(),
                configuration
            ),
            Err(DirectoryError::ConfigurationLength)
        ));
        assert!(!root.exists());
    }
    assert!(
        GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION
        )
        .is_err()
    );
    assert!(!root.exists());
}

#[test]
fn missing_or_interrupted_configuration_never_gets_recreated_on_open() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("group");
    drop(GroupDirectory::format_new(&root, identity(), 1, &segment()).unwrap());
    assert!(matches!(
        GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION
        ),
        Err(DirectoryError::ConfigurationMismatch)
    ));
    assert!(!root.join("CONFIGURATION").exists());
    assert!(matches!(
        GroupDirectory::format_new_with_configuration(
            &root,
            identity(),
            1,
            &segment(),
            CONFIGURATION
        ),
        Err(DirectoryError::StoreAlreadyExists)
    ));
    for length in 0..CONFIGURATION.len() {
        fs::write(root.join("CONFIGURATION"), &CONFIGURATION[..length]).unwrap();
        assert!(
            GroupDirectory::open_with_configuration(
                &root,
                identity(),
                MetadataLimits::default(),
                CONFIGURATION
            )
            .is_err()
        );
        assert_eq!(
            fs::read(root.join("CONFIGURATION")).unwrap(),
            &CONFIGURATION[..length]
        );
    }
    fs::write(
        root.join("CONFIGURATION"),
        vec![0; GROUP_CONFIGURATION_MAX_BYTES + 1],
    )
    .unwrap();
    assert!(
        GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION
        )
        .is_err()
    );
}

#[cfg(unix)]
#[test]
fn configuration_symlinks_are_rejected_without_touching_the_target() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("group");
    drop(GroupDirectory::format_new(&root, identity(), 1, &segment()).unwrap());
    let target = temporary.path().join("operator-file");
    fs::write(&target, CONFIGURATION).unwrap();
    std::os::unix::fs::symlink(&target, root.join("CONFIGURATION")).unwrap();
    assert!(matches!(
        GroupDirectory::open_with_configuration(
            &root,
            identity(),
            MetadataLimits::default(),
            CONFIGURATION
        ),
        Err(DirectoryError::NotRegularFile(_))
    ));
    assert_eq!(fs::read(target).unwrap(), CONFIGURATION);
}

#[test]
fn relocation_preserves_configuration_and_rejects_a_changed_published_copy() {
    let temporary = TempDir::new().unwrap();
    let source_root = temporary.path().join("source");
    let destination_root = temporary.path().join("destination");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&destination_root).unwrap();
    let source = VolumeDirectory::format_new(
        &source_root,
        VolumeIdentity {
            volume_id: identity().volume_id,
        },
        MountPolicy::PortableIdentity,
    )
    .unwrap();
    let destination = VolumeDirectory::format_new(
        &destination_root,
        VolumeIdentity {
            volume_id: VolumeId::from_bytes([5; 16]),
        },
        MountPolicy::PortableIdentity,
    )
    .unwrap();
    let root = source.group_root(identity().group_id);
    let journal = GroupDirectory::format_new_with_configuration(
        &root,
        identity(),
        1,
        &segment(),
        CONFIGURATION,
    )
    .unwrap()
    .recover(
        JournalGeneration(1),
        DecodeLimits::default(),
        OperationLimits::default(),
    )
    .unwrap();
    let prepared = prepare_relocation(journal, &destination, CheckpointLimits::default()).unwrap();
    let artifact = prepared.artifact().clone();
    assert_eq!(
        fs::read(artifact.destination_root().join("CONFIGURATION")).unwrap(),
        CONFIGURATION
    );
    drop(prepared);
    let reopened = GroupDirectory::open_with_configuration(
        artifact.destination_root(),
        artifact.destination_identity(),
        MetadataLimits::default(),
        CONFIGURATION,
    )
    .unwrap();
    assert_eq!(reopened.configuration(), Some(CONFIGURATION));
    drop(reopened);
    fs::write(
        artifact.destination_root().join("CONFIGURATION"),
        b"changed membership",
    )
    .unwrap();
    let source_journal = GroupDirectory::open_with_configuration(
        &root,
        identity(),
        MetadataLimits::default(),
        CONFIGURATION,
    )
    .unwrap()
    .recover(
        JournalGeneration(2),
        DecodeLimits::default(),
        OperationLimits::default(),
    )
    .unwrap();
    assert!(matches!(
        prepare_relocation(source_journal, &destination, CheckpointLimits::default()),
        Err(RelocationError::DestinationMismatch)
    ));
    assert_eq!(
        fs::read(artifact.destination_root().join("CONFIGURATION")).unwrap(),
        b"changed membership"
    );
}

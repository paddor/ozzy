use std::fs;

use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    CanonicalOperation, CheckpointLimits, DecodeLimits, Digest, GroupIdentity, LogPosition,
    MetadataLimits, MountPolicy, OperationKind, OperationLimits, PlacementDirectory,
    PlacementEntry, PlacementError, PlacementLimits, RelocationError, SEGMENT_HEADER_BYTES,
    SegmentHeader, VolumeConfig, VolumeDirectory, VolumeIdentity, VolumeSet, prepare_relocation,
    retire_relocated_source,
};
use ozzy_proto::{CheckpointId, GroupId, NodeId, StoreId, VolumeId};
use tempfile::TempDir;

fn volume_identity(byte: u8) -> VolumeIdentity {
    VolumeIdentity {
        volume_id: VolumeId::from_bytes([byte; 16]),
    }
}

fn operation(identity: GroupIdentity) -> CanonicalOperation<'static> {
    CanonicalOperation {
        group_id: identity.group_id,
        configuration_epoch: 1,
        original_view: 0,
        op_number: 1,
        previous_digest: Digest::ZERO,
        kind: OperationKind::Barrier,
        body: &[0x51; 16],
    }
}

#[test]
#[expect(clippy::too_many_lines, reason = "linear relocation lifecycle")]
fn relocation_copies_valid_generation_switches_placement_then_retires_source() {
    let temporary = TempDir::new().unwrap();
    let source_root = temporary.path().join("source-volume");
    let destination_root = temporary.path().join("destination-volume");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&destination_root).unwrap();
    let source_volume = VolumeDirectory::format_new(
        &source_root,
        volume_identity(0x11),
        MountPolicy::PortableIdentity,
    )
    .unwrap();
    let destination_volume = VolumeDirectory::format_new(
        &destination_root,
        volume_identity(0x12),
        MountPolicy::PortableIdentity,
    )
    .unwrap();
    let identity = GroupIdentity {
        group_id: GroupId::from_bytes([0x21; 16]),
        replica_node_id: NodeId::from_bytes([0x22; 16]),
        volume_id: source_volume.identity().volume_id,
        store_id: StoreId::from_bytes([0x23; 16]),
        store_generation: 1,
    };
    let segment = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap();
    let directory = ozzy_journal_segment::GroupDirectory::format_new_with_durable_evidence(
        source_volume.group_root(identity.group_id),
        identity,
        1,
        &segment,
        b"evidence-preserving relocation configuration",
    )
    .unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal.append(&[operation(identity)]).unwrap();
    journal.sync_through(written).unwrap();
    let mut manifest = journal.directory().manifest().clone();
    manifest.generation += 1;
    manifest.parent_generation = journal.directory().manifest().generation;
    manifest.accepted = LogPosition {
        op_number: 1,
        digest: written.next_chain().previous_digest(),
    };
    manifest.committed = manifest.accepted;
    let journal = journal.install_metadata(manifest).unwrap();
    let checkpoint_id = CheckpointId::from_bytes([0x31; 16]);
    journal
        .checkpoint_plan(checkpoint_id, Digest::from_bytes([0x32; 32]), 8)
        .unwrap()
        .build(b"canonical committed state", CheckpointLimits::default())
        .unwrap();
    let journal = journal
        .install_checkpoint(checkpoint_id, CheckpointLimits::default())
        .unwrap();

    let placement_root = temporary.path().join("node-state");
    fs::create_dir(&placement_root).unwrap();
    let placement_root = placement_root.join("placement");
    let placement = PlacementDirectory::format_new(
        &placement_root,
        identity.replica_node_id,
        vec![PlacementEntry {
            group_id: identity.group_id,
            replica_node_id: identity.replica_node_id,
            volume_id: identity.volume_id,
            store_id: identity.store_id,
            store_generation: identity.store_generation,
        }],
        PlacementLimits::default(),
    )
    .unwrap();

    let stale_staging = destination_root
        .join("staging")
        .join(format!(".relocate-{}-2.tmp", "21".repeat(16)));
    fs::create_dir(&stale_staging).unwrap();
    fs::write(stale_staging.join("partial"), b"interrupted copy").unwrap();
    let prepared =
        prepare_relocation(journal, &destination_volume, CheckpointLimits::default()).unwrap();
    let artifact = prepared.artifact().clone();
    assert_eq!(artifact.destination_identity().store_generation, 2);
    assert_eq!(
        artifact.destination_identity().volume_id,
        destination_volume.identity().volume_id
    );
    assert!(artifact.destination_root().join("segments/1.log").is_file());
    assert_eq!(
        fs::metadata(artifact.destination_root().join("DURABLE"))
            .unwrap()
            .len(),
        128 * 1024
    );
    assert!(
        artifact
            .destination_root()
            .join(format!(
                "checkpoints/{}/manifest",
                ozzy_journal_segment::checkpoint_name(checkpoint_id)
            ))
            .is_file()
    );
    assert!(
        fs::read_dir(artifact.destination_root().join("indexes"))
            .unwrap()
            .next()
            .is_none()
    );

    // Crash after destination publication: retry reuses only an exact copy.
    drop(prepared);
    let journal = source_volume
        .open_group(
            identity,
            MetadataLimits::default(),
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let prepared =
        prepare_relocation(journal, &destination_volume, CheckpointLimits::default()).unwrap();
    assert_eq!(prepared.artifact(), &artifact);

    let placement = placement.install_relocation(&prepared).unwrap();
    let relocation_generation = placement.current().generation;
    let placement = placement.install_relocation(&prepared).unwrap();
    assert_eq!(placement.current().generation, relocation_generation);
    assert_eq!(
        placement.resolve(identity.group_id),
        Some(artifact.destination_placement())
    );
    let retired = retire_relocated_source(prepared, &placement).unwrap();
    assert!(!artifact.source_root().exists());
    assert!(retired.root().is_dir());
    retired.remove(&placement).unwrap();

    drop(source_volume);
    drop(destination_volume);
    let volumes = VolumeSet::open(&[
        VolumeConfig {
            root: source_root,
            identity: volume_identity(0x11),
            mount_policy: MountPolicy::PortableIdentity,
        },
        VolumeConfig {
            root: destination_root,
            identity: volume_identity(0x12),
            mount_policy: MountPolicy::PortableIdentity,
        },
    ])
    .unwrap();
    let reopened = placement
        .open_group(
            &volumes,
            identity.group_id,
            MetadataLimits::default(),
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(
        reopened.directory().identity(),
        artifact.destination_identity()
    );
    assert_eq!(reopened.committed_position().unwrap().op_number, 1);
}

#[test]
fn relocation_retry_revalidates_complete_destination_journal() {
    let temporary = TempDir::new().unwrap();
    let source_root = temporary.path().join("source-volume");
    let destination_root = temporary.path().join("destination-volume");
    fs::create_dir(&source_root).unwrap();
    fs::create_dir(&destination_root).unwrap();
    let source_volume = VolumeDirectory::format_new(
        &source_root,
        volume_identity(0x41),
        MountPolicy::PortableIdentity,
    )
    .unwrap();
    let destination_volume = VolumeDirectory::format_new(
        &destination_root,
        volume_identity(0x42),
        MountPolicy::PortableIdentity,
    )
    .unwrap();
    let identity = GroupIdentity {
        group_id: GroupId::from_bytes([0x43; 16]),
        replica_node_id: NodeId::from_bytes([0x44; 16]),
        volume_id: source_volume.identity().volume_id,
        store_id: StoreId::from_bytes([0x45; 16]),
        store_generation: 1,
    };
    let segment = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap();
    let directory = source_volume.format_group(identity, 1, &segment).unwrap();
    let mut journal = directory
        .recover(
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    let written = journal.append(&[operation(identity)]).unwrap();
    journal.sync_through(written).unwrap();
    let mut manifest = journal.directory().manifest().clone();
    manifest.generation += 1;
    manifest.parent_generation = journal.directory().manifest().generation;
    manifest.accepted = LogPosition {
        op_number: 1,
        digest: written.next_chain().previous_digest(),
    };
    manifest.committed = manifest.accepted;
    let journal = journal.install_metadata(manifest).unwrap();

    let prepared =
        prepare_relocation(journal, &destination_volume, CheckpointLimits::default()).unwrap();
    let destination_segment = prepared
        .artifact()
        .destination_root()
        .join("segments/1.log");

    let node_state = temporary.path().join("node-state");
    fs::create_dir(&node_state).unwrap();
    let placement = PlacementDirectory::format_new(
        node_state.join("placement"),
        identity.replica_node_id,
        vec![PlacementEntry {
            group_id: identity.group_id,
            replica_node_id: identity.replica_node_id,
            volume_id: identity.volume_id,
            store_id: identity.store_id,
            store_generation: identity.store_generation,
        }],
        PlacementLimits::default(),
    )
    .unwrap();

    let mut bytes = fs::read(&destination_segment).unwrap();
    bytes[SEGMENT_HEADER_BYTES + 32] ^= 0x80;
    fs::write(&destination_segment, bytes).unwrap();
    assert!(matches!(
        placement.install_relocation(&prepared),
        Err(PlacementError::RelocationDestination(_))
    ));
    drop(prepared);

    let journal = source_volume
        .open_group(
            identity,
            MetadataLimits::default(),
            JournalGeneration(2),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert!(matches!(
        prepare_relocation(journal, &destination_volume, CheckpointLimits::default()),
        Err(RelocationError::Directory(_))
    ));
}

use std::fs;

use ozzy_journal::operation::OperationLimits;
use ozzy_journal::progress::JournalGeneration;
use ozzy_journal_segment::{
    DecodeLimits, Digest, GroupIdentity, MetadataLimits, MountPolicy, PlacementCatalog,
    PlacementDirectory, PlacementEntry, PlacementError, PlacementLimits, SegmentHeader,
    VolumeConfig, VolumeDirectory, VolumeIdentity, VolumeSet, decode_placement_catalog,
    encode_placement_catalog,
};
use ozzy_proto::{GroupId, NodeId, StoreId, VolumeId};
use tempfile::TempDir;

fn node() -> NodeId {
    NodeId::from_bytes([0x10; 16])
}

fn entry(group: u8, volume: u8, store_generation: u64) -> PlacementEntry {
    PlacementEntry {
        group_id: GroupId::from_bytes([group; 16]),
        replica_node_id: node(),
        volume_id: VolumeId::from_bytes([volume; 16]),
        store_id: StoreId::from_bytes([group + 1; 16]),
        store_generation,
    }
}

#[test]
fn placement_catalog_round_trips_and_rejects_truncation_or_unsorted_entries() {
    let catalog = PlacementCatalog {
        generation: 2,
        parent_generation: 1,
        node_id: node(),
        entries: vec![entry(0x20, 0x30, 4), entry(0x21, 0x31, 8)],
    };
    let encoded = encode_placement_catalog(&catalog, PlacementLimits::default()).unwrap();
    assert_eq!(&encoded[..8], b"OZYPLC01");
    assert_eq!(
        decode_placement_catalog(&encoded, PlacementLimits::default()).unwrap(),
        catalog
    );
    for length in 0..encoded.len() {
        assert!(decode_placement_catalog(&encoded[..length], PlacementLimits::default()).is_err());
    }

    let mut unsorted = catalog;
    unsorted.entries.reverse();
    assert!(matches!(
        encode_placement_catalog(&unsorted, PlacementLimits::default()),
        Err(PlacementError::UnsortedOrDuplicate)
    ));
}

#[test]
fn current_selects_exact_catalog_and_install_survives_reopen() {
    let temporary = TempDir::new().unwrap();
    let root = temporary.path().join("placement");
    let directory = PlacementDirectory::format_new(
        &root,
        node(),
        vec![entry(0x20, 0x30, 1)],
        PlacementLimits::default(),
    )
    .unwrap();
    assert_eq!(directory.current().generation, 1);
    assert_eq!(
        directory.resolve(GroupId::from_bytes([0x20; 16])),
        Some(entry(0x20, 0x30, 1))
    );
    assert!(matches!(
        PlacementDirectory::open(&root, node(), PlacementLimits::default()),
        Err(PlacementError::Locked)
    ));

    let directory = directory
        .install(vec![entry(0x20, 0x31, 2), entry(0x21, 0x30, 1)])
        .unwrap();
    assert_eq!(directory.current().generation, 2);
    drop(directory);
    let reopened = PlacementDirectory::open(&root, node(), PlacementLimits::default()).unwrap();
    assert_eq!(reopened.catalog().parent_generation, 1);
    assert_eq!(
        reopened.resolve(GroupId::from_bytes([0x20; 16])),
        Some(entry(0x20, 0x31, 2))
    );
    fs::write(root.join("CATALOG.99"), b"abandoned catalog").unwrap();
    fs::write(root.join(".CATALOG.100.tmp"), b"abandoned temporary").unwrap();
    fs::write(root.join(".CURRENT.101.tmp"), b"abandoned current").unwrap();
    fs::write(root.join("CATALOG.01"), b"noncanonical name").unwrap();
    let cleanup = reopened.reclaim_unreferenced_metadata().unwrap();
    assert_eq!(cleanup.removed_catalog_generations, [1, 99]);
    assert_eq!(
        cleanup.removed_temporary_files,
        [".CATALOG.100.tmp", ".CURRENT.101.tmp"]
    );
    assert!(cleanup.reclaimed_bytes > 0);
    assert!(root.join("CATALOG.2").exists());
    assert!(root.join("CATALOG.01").exists());
    drop(reopened);

    let mut current = fs::read(root.join("CURRENT")).unwrap();
    current[40] ^= 1;
    fs::write(root.join("CURRENT"), current).unwrap();
    assert!(matches!(
        PlacementDirectory::open(&root, node(), PlacementLimits::default()),
        Err(PlacementError::DigestMismatch("placement CURRENT"))
    ));
}

#[test]
fn install_replaces_unfinished_temporaries_of_a_killed_install() {
    let temporary = TempDir::new().unwrap();
    // A killed process leaves a created temporary empty, partly written, or
    // complete with the bytes of another attempt.
    for (index, unfinished) in [&b""[..], &b"partial"[..], &[0xa5; 128][..]]
        .into_iter()
        .enumerate()
    {
        let root = temporary.path().join(format!("placement-{index}"));
        let directory = PlacementDirectory::format_new(
            &root,
            node(),
            vec![entry(0x20, 0x30, 1)],
            PlacementLimits::default(),
        )
        .unwrap();
        fs::write(root.join(".CURRENT.2.tmp"), unfinished).unwrap();

        let directory = directory.install(vec![entry(0x20, 0x31, 2)]).unwrap();
        assert_eq!(directory.current().generation, 2);
        assert!(!root.join(".CURRENT.2.tmp").exists());
        drop(directory);

        let reopened = PlacementDirectory::open(&root, node(), PlacementLimits::default()).unwrap();
        assert_eq!(reopened.current().generation, 2);
        assert_eq!(
            reopened.resolve(GroupId::from_bytes([0x20; 16])),
            Some(entry(0x20, 0x31, 2))
        );
    }
}

#[test]
fn install_reuses_matching_and_skips_occupied_catalog_generations() {
    let temporary = TempDir::new().unwrap();
    for (index, occupied_name) in ["CATALOG.2", ".CATALOG.2.tmp"].into_iter().enumerate() {
        let root = temporary.path().join(format!("placement-{index}"));
        let directory = PlacementDirectory::format_new(
            &root,
            node(),
            vec![entry(0x20, 0x30, 1)],
            PlacementLimits::default(),
        )
        .unwrap();
        let occupied = PlacementCatalog {
            generation: 2,
            parent_generation: 1,
            node_id: node(),
            entries: vec![entry(0x20, 0x31, 2)],
        };
        let occupied_bytes =
            encode_placement_catalog(&occupied, PlacementLimits::default()).unwrap();
        fs::write(root.join(occupied_name), &occupied_bytes).unwrap();

        let directory = directory.install(vec![entry(0x20, 0x32, 3)]).unwrap();
        assert_eq!(directory.current().generation, 3);
        assert_eq!(directory.catalog().parent_generation, 1);
        assert_eq!(fs::read(root.join(occupied_name)).unwrap(), occupied_bytes);
        drop(directory);

        let reopened = PlacementDirectory::open(&root, node(), PlacementLimits::default()).unwrap();
        assert_eq!(reopened.current().generation, 3);
        assert_eq!(
            reopened.resolve(GroupId::from_bytes([0x20; 16])),
            Some(entry(0x20, 0x32, 3))
        );
    }

    let root = temporary.path().join("matching");
    let directory = PlacementDirectory::format_new(
        &root,
        node(),
        vec![entry(0x20, 0x30, 1)],
        PlacementLimits::default(),
    )
    .unwrap();
    let matching = PlacementCatalog {
        generation: 2,
        parent_generation: 1,
        node_id: node(),
        entries: vec![entry(0x20, 0x31, 2)],
    };
    fs::write(
        root.join("CATALOG.2"),
        encode_placement_catalog(&matching, PlacementLimits::default()).unwrap(),
    )
    .unwrap();
    let directory = directory.install(matching.entries).unwrap();
    assert_eq!(directory.current().generation, 2);
}

#[test]
fn catalog_requires_every_selected_volume() {
    let temporary = TempDir::new().unwrap();
    let placement_root = temporary.path().join("placement");
    let directory = PlacementDirectory::format_new(
        &placement_root,
        node(),
        vec![entry(0x20, 0x30, 1)],
        PlacementLimits::default(),
    )
    .unwrap();
    assert!(matches!(
        directory.validate_volumes(&VolumeSet::open(&[]).unwrap()),
        Err(PlacementError::MissingVolume)
    ));

    let volume_root = temporary.path().join("volume");
    fs::create_dir(&volume_root).unwrap();
    let identity = VolumeIdentity {
        volume_id: VolumeId::from_bytes([0x30; 16]),
    };
    drop(
        VolumeDirectory::format_new(&volume_root, identity, MountPolicy::PortableIdentity).unwrap(),
    );
    let volumes = VolumeSet::open(&[VolumeConfig {
        root: volume_root,
        identity,
        mount_policy: MountPolicy::PortableIdentity,
    }])
    .unwrap();
    directory.validate_volumes(&volumes).unwrap();
    let selected = entry(0x20, 0x30, 1);
    let identity = GroupIdentity {
        group_id: selected.group_id,
        replica_node_id: selected.replica_node_id,
        volume_id: selected.volume_id,
        store_id: selected.store_id,
        store_generation: selected.store_generation,
    };
    let segment = SegmentHeader::new(identity.group_id, 1, None, Digest::ZERO, 8 * 1024).unwrap();
    drop(
        volumes
            .get(identity.volume_id)
            .unwrap()
            .format_group(identity, 1, &segment)
            .unwrap(),
    );
    let journal = directory
        .open_group(
            &volumes,
            identity.group_id,
            MetadataLimits::default(),
            JournalGeneration(1),
            DecodeLimits::default(),
            OperationLimits::default(),
        )
        .unwrap();
    assert_eq!(journal.directory().identity(), identity);
}

#[test]
fn zero_digest_current_is_rejected() {
    let result =
        ozzy_journal_segment::encode_placement_current(ozzy_journal_segment::PlacementCurrent {
            node_id: node(),
            generation: 1,
            catalog_digest: Digest::ZERO,
        });
    assert!(matches!(result, Err(PlacementError::InvalidCurrent)));
}

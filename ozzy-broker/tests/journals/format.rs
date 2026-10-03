use super::*;
use ozzy_broker::{DevicePools, format_partition_journals, initialize_volumes};
use ozzy_io::Local;

#[tokio::test]
async fn explicit_formatter_initializes_symmetric_shards_and_restart_keeps_election_fencing() {
    for mode in [DeploymentMode::Single, DeploymentMode::Three] {
        let temporary = tempfile::tempdir().unwrap();
        let policy = if mode == DeploymentMode::Single {
            Confirmation::LocalDurable
        } else {
            Confirmation::DiskQuorum
        };
        for (checked, local, journals) in fixture(temporary.path(), mode, policy, 6) {
            for device in checked.deployment.deployment().brokers[&checked.plan.name]
                .devices
                .values()
            {
                std::fs::create_dir(&device.root).unwrap();
            }
            initialize_volumes(&checked, &local).unwrap();
            format_partition_journals(&checked, &local).await.unwrap();
            let (devices, lanes) = DevicePools::start(&checked.plan).unwrap();
            let lanes: BTreeMap<_, _> = lanes
                .into_iter()
                .map(|lane| (lane.shard, Local::new(lane.client)))
                .collect();
            for partition in &journals.partitions {
                let opened = Box::pin(partition.clone().open(
                    lanes[&partition.placement.shard].clone(),
                    JournalGeneration(2),
                ))
                .await
                .unwrap();
                match opened.authority {
                    PartitionAuthority::Local(driver) => {
                        assert_eq!(driver.snapshot().applied.op.0, 0);
                    }
                    PartitionAuthority::Replicated(startup) => {
                        assert!(startup.recovered().is_some());
                        assert!(opened.journal.images().is_err());
                    }
                }
                opened.journal.shutdown().await.unwrap();
            }
            devices.shutdown().await;
            assert!(
                format_partition_journals(&checked, &local).await.is_err(),
                "second format must refuse established stores"
            );
        }
    }
}

#[tokio::test]
async fn mixed_established_and_absent_stores_are_rejected_before_formatting() {
    let temporary = tempfile::tempdir().unwrap();
    let (checked, local, _) = fixture(
        temporary.path(),
        DeploymentMode::Single,
        Confirmation::LocalDurable,
        2,
    )
    .pop()
    .unwrap();
    let root = &checked.deployment.deployment().brokers[&checked.plan.name].devices["ssd"].root;
    std::fs::create_dir(root).unwrap();
    initialize_volumes(&checked, &local).unwrap();
    std::fs::create_dir_all(&checked.plan.partitions[1].directory).unwrap();
    let marker = checked.plan.partitions[1].directory.join("partial-state");
    std::fs::write(&marker, b"preserve this state").unwrap();
    assert!(format_partition_journals(&checked, &local).await.is_err());
    assert!(!checked.plan.partitions[0].directory.exists());
    assert_eq!(std::fs::read(marker).unwrap(), b"preserve this state");
}

#[cfg(unix)]
#[tokio::test]
async fn symlink_hierarchy_directory_is_rejected_without_touching_its_target() {
    for relative in ["topics", "topics/orders", "topics/orders/partitions"] {
        let temporary = tempfile::tempdir().unwrap();
        let (checked, local, _) = fixture(
            temporary.path(),
            DeploymentMode::Single,
            Confirmation::LocalDurable,
            2,
        )
        .pop()
        .unwrap();
        let root = &checked.deployment.deployment().brokers[&checked.plan.name].devices["ssd"].root;
        std::fs::create_dir(root).unwrap();
        initialize_volumes(&checked, &local).unwrap();
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&outside).unwrap();
        let link = root.join(relative);
        std::fs::create_dir_all(link.parent().unwrap()).unwrap();
        std::os::unix::fs::symlink(&outside, link).unwrap();
        assert!(format_partition_journals(&checked, &local).await.is_err());
        assert_eq!(std::fs::read_dir(outside).unwrap().count(), 0);
    }
}

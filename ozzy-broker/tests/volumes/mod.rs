use super::{SINGLE, host, ids};
use ozzy_broker::{CheckedConfig, check_volumes, initialize_volumes};
use ozzy_config::{BrokerIdentity, Deployment, VolumeIdentity};
use std::fs;
use uuid::Uuid;

fn fixture(root: &std::path::Path) -> (CheckedConfig, BrokerIdentity) {
    let deployment =
        Deployment::parse(&SINGLE.replace("/var/lib/ozzy/data", root.to_str().unwrap()))
            .unwrap()
            .validate()
            .unwrap();
    let identity = deployment.initialize(ids()).unwrap();
    let local = deployment
        .initialize_broker_identity(&identity, "laptop", Uuid::now_v7)
        .unwrap();
    let plan = deployment.broker_plan("laptop", &host()).unwrap();
    (
        CheckedConfig {
            deployment,
            identity,
            plan,
        },
        local,
    )
}

#[test]
fn missing_volume_is_never_created_or_initialized_during_check() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("missing-mount");
    let (checked, local) = fixture(&root);
    assert!(check_volumes(&checked, &local).is_err());
    assert!(initialize_volumes(&checked, &local).is_err());
    assert!(!root.exists());
    fs::create_dir(&root).unwrap();
    assert!(check_volumes(&checked, &local).is_err());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
    initialize_volumes(&checked, &local).unwrap();
    check_volumes(&checked, &local).unwrap();
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    assert!(initialize_volumes(&checked, &local).is_err());
    // A missing mounted device may reveal an existing empty mount point.
    fs::rename(&root, directory.path().join("detached")).unwrap();
    fs::create_dir(&root).unwrap();
    assert!(check_volumes(&checked, &local).is_err());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 0);
}

#[test]
fn wrong_or_damaged_volume_marker_is_never_replaced() {
    let directory = tempfile::tempdir().unwrap();
    let (checked, local) = fixture(directory.path());
    initialize_volumes(&checked, &local).unwrap();
    let marker = directory.path().join("OZZY_VOLUME");
    let original = fs::read_to_string(&marker).unwrap();
    let expected = VolumeIdentity::decode(&original).unwrap();
    for changed in [
        VolumeIdentity {
            cluster: Uuid::now_v7(),
            ..expected
        },
        VolumeIdentity {
            broker: Uuid::now_v7(),
            ..expected
        },
        VolumeIdentity {
            volume: Uuid::now_v7(),
            ..expected
        },
    ] {
        let bytes = changed.encode().unwrap();
        fs::write(&marker, &bytes).unwrap();
        assert!(check_volumes(&checked, &local).is_err());
        assert!(initialize_volumes(&checked, &local).is_err());
        assert_eq!(fs::read_to_string(&marker).unwrap(), bytes);
    }
    for bytes in ["", "damaged", &original[..original.len() - 1]] {
        fs::write(&marker, bytes).unwrap();
        assert!(check_volumes(&checked, &local).is_err());
        assert!(initialize_volumes(&checked, &local).is_err());
        assert_eq!(fs::read_to_string(&marker).unwrap(), bytes);
    }
}

#[cfg(unix)]
#[test]
fn symlink_volume_and_marker_are_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let real = directory.path().join("real");
    let alias = directory.path().join("alias");
    fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    let (checked, local) = fixture(&alias);
    assert!(initialize_volumes(&checked, &local).is_err());
    assert!(check_volumes(&checked, &local).is_err());
    let (checked, local) = fixture(&real);
    initialize_volumes(&checked, &local).unwrap();
    let marker = real.join("OZZY_VOLUME");
    let moved = real.join("elsewhere");
    fs::rename(&marker, &moved).unwrap();
    std::os::unix::fs::symlink(&moved, &marker).unwrap();
    assert!(check_volumes(&checked, &local).is_err());
    assert!(initialize_volumes(&checked, &local).is_err());
}

#[test]
fn concurrent_initializers_publish_one_exact_volume_record() {
    let directory = tempfile::tempdir().unwrap();
    let (checked, local) = fixture(directory.path());
    let start = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let spawn = || {
            scope.spawn(|| {
                start.wait();
                initialize_volumes(&checked, &local)
            })
        };
        let left = spawn();
        let right = spawn();
        [left.join().unwrap(), right.join().unwrap()]
    });
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    check_volumes(&checked, &local).unwrap();
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

use super::{SINGLE, check_config, deployment, host, ids, initialize_identity};
use ozzy_broker::{initialize_broker_identity, load_broker_identity};
use uuid::Uuid;

fn local_ids() -> impl FnMut() -> Uuid {
    let mut next = 1000;
    move || {
        next += 1;
        Uuid::from_u128(next)
    }
}

#[test]
fn local_storage_identity_is_persisted_once_and_never_recreated_on_restart() {
    let directory = tempfile::tempdir().unwrap();
    let shared = directory.path().join("deployment.identity");
    let local = directory.path().join("broker.identity");
    initialize_identity(&deployment(), &shared, ids()).unwrap();
    let checked = check_config(deployment(), &shared, "laptop", &host()).unwrap();
    assert!(load_broker_identity(&checked, &local).is_err());
    assert!(!local.exists());
    let created = initialize_broker_identity(&checked, &local, local_ids()).unwrap();
    assert_eq!(load_broker_identity(&checked, &local).unwrap(), created);
    assert!(initialize_broker_identity(&checked, &local, local_ids()).is_err());
    let original = std::fs::read(&local).unwrap();
    assert_eq!(original, created.encode().unwrap().as_bytes());
    std::fs::write(&local, b"damaged").unwrap();
    assert!(load_broker_identity(&checked, &local).is_err());
    assert!(initialize_broker_identity(&checked, &local, local_ids()).is_err());
    assert_eq!(std::fs::read(&local).unwrap(), b"damaged");
}

#[test]
fn cli_local_identity_provisioning_never_opens_or_creates_partition_storage() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("absent-storage");
    let config = directory.path().join("deployment.toml");
    let shared = directory.path().join("shared.identity");
    let local = directory.path().join("local.identity");
    std::fs::write(
        &config,
        SINGLE.replace("/var/lib/ozzy/data", root.to_str().unwrap()),
    )
    .unwrap();
    let run = |args: &[&std::ffi::OsStr]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_ozzy_broker"))
            .arg("--config")
            .arg(&config)
            .args(args)
            .output()
            .unwrap()
    };
    let init = run(&["init".as_ref(), "--identity".as_ref(), shared.as_os_str()]);
    assert!(init.status.success(), "{init:?}");
    let arguments = [
        "init-broker".as_ref(),
        "--identity".as_ref(),
        shared.as_os_str(),
        "--broker".as_ref(),
        "laptop".as_ref(),
        "--local-identity".as_ref(),
        local.as_os_str(),
    ];
    let initialized = run(&arguments);
    assert!(initialized.status.success(), "{initialized:?}");
    assert!(!run(&arguments).status.success());
    let checked = run(&[
        "check".as_ref(),
        "--identity".as_ref(),
        shared.as_os_str(),
        "--broker".as_ref(),
        "laptop".as_ref(),
        "--local-identity".as_ref(),
        local.as_os_str(),
    ]);
    assert!(checked.status.success(), "{checked:?}");
    assert!(!root.exists());
}

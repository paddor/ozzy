use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Barrier};

use ozzy_broker::{check_config, initialize_identity, load_deployment};
use ozzy_config::{Deployment, HostResources, ValidatedDeployment};
use uuid::Uuid;

const SINGLE: &str = include_str!("../../ozzy-config/tests/fixtures/single.toml");

mod journals;
mod local_identity;
#[cfg(target_os = "linux")]
mod process;
mod volumes;

fn deployment() -> ValidatedDeployment {
    Deployment::parse(SINGLE).unwrap().validate().unwrap()
}

fn host() -> HostResources {
    HostResources {
        cpus: BTreeMap::from([(0, Some(0))]),
        memory_nodes: [0].into(),
        linux_aio: true,
    }
}

fn ids() -> impl FnMut() -> Uuid {
    let mut value = 0;
    move || {
        value += 1;
        Uuid::from_u128(value)
    }
}

#[test]
fn explicit_initialization_persists_exact_shared_identity() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("identity.toml");
    let created = initialize_identity(&deployment(), &path, ids()).unwrap();
    let checked = check_config(deployment(), &path, "laptop", &host()).unwrap();
    assert_eq!(checked.identity, created);
    assert_eq!(checked.plan.partitions.len(), 16);
    let catalog = checked.topic_catalog().unwrap();
    let page = catalog
        .page(
            &ozzy_proto::directory::TopicRequest {
                name: "orders".to_owned(),
                first: 0,
                maximum: 16,
            },
            65536,
        )
        .unwrap();
    assert_eq!(page.total, 16);
    assert_eq!(
        page.id.as_bytes(),
        checked.identity.topics["orders"].id.as_bytes()
    );
    assert_eq!(page.partitions.len(), 16);
    assert_eq!(page.brokers.len(), 1);
    assert_eq!(
        page.brokers[0].peer,
        checked.deployment.deployment().brokers["laptop"]
            .endpoints
            .peer
    );
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[test]
fn no_overwrite_even_when_existing_identity_is_damaged() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("identity.toml");
    for bytes in [b"broken".as_slice(), b"".as_slice()] {
        fs::write(&path, bytes).unwrap();
        assert!(initialize_identity(&deployment(), &path, ids()).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }
}

#[test]
fn missing_identity_is_not_bootstrapped_by_check() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing.toml");
    assert!(check_config(deployment(), &path, "laptop", &host()).is_err());
    assert!(!path.exists());
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn corrupt_or_truncated_identity_never_passes_restart_check() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("identity.toml");
    initialize_identity(&deployment(), &path, ids()).unwrap();
    let original = fs::read(&path).unwrap();
    for offset in [0, 28, 60, original.len() / 2, original.len() - 2] {
        let mut damaged = original.clone();
        damaged[offset] ^= 1;
        fs::write(&path, &damaged).unwrap();
        assert!(check_config(deployment(), &path, "laptop", &host()).is_err());
    }
    for end in [0, 1, 64, original.len() - 1] {
        fs::write(&path, &original[..end]).unwrap();
        assert!(check_config(deployment(), &path, "laptop", &host()).is_err());
    }
}

#[test]
fn concurrent_initializers_cannot_replace_each_others_authority() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("identity.toml");
    let barrier = Arc::new(Barrier::new(2));
    let threads: Vec<_> = (0..2)
        .map(|_| {
            let barrier = Arc::clone(&barrier);
            let path = path.clone();
            std::thread::spawn(move || {
                let deployment = deployment();
                barrier.wait();
                initialize_identity(&deployment, &path, Uuid::now_v7)
            })
        })
        .collect();
    let results: Vec<_> = threads
        .into_iter()
        .map(|thread| thread.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    let winner = results.into_iter().find_map(Result::ok).unwrap();
    assert_eq!(
        check_config(deployment(), &path, "laptop", &host())
            .unwrap()
            .identity,
        winner
    );
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[test]
fn loading_configuration_has_no_storage_side_effects() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("uncreated-data");
    let config = SINGLE.replace("/var/lib/ozzy/data", root.to_str().unwrap());
    let path = directory.path().join("deployment.toml");
    fs::write(&path, config).unwrap();
    let loaded = load_deployment(&path).unwrap();
    loaded.broker_plan("laptop", &host()).unwrap();
    assert!(!root.exists());
}

#[test]
fn startup_rejects_changed_partition_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("identity.toml");
    initialize_identity(&deployment(), &path, ids()).unwrap();
    let mut changed = Deployment::parse(SINGLE).unwrap();
    changed.topics.get_mut("orders").unwrap().partitions = 17;
    assert!(check_config(changed.validate().unwrap(), &path, "laptop", &host()).is_err());
}

#[test]
fn deployment_endpoint_must_fit_topic_lookup() {
    let mut oversized = Deployment::parse(SINGLE).unwrap();
    oversized.brokers.get_mut("laptop").unwrap().endpoints.peer =
        format!("tcp://{}:7100", "a".repeat(1024));
    assert!(oversized.validate().is_err());
}

#[test]
fn config_size_is_bounded_before_allocation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("huge.toml");
    fs::File::create(&path)
        .unwrap()
        .set_len(17 * 1024 * 1024)
        .unwrap();
    assert!(load_deployment(&path).is_err());
    assert!(load_deployment(directory.path()).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn final_symlinks_and_fifos_are_rejected_without_following_or_blocking() {
    use std::os::unix::fs::symlink;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source.toml");
    fs::write(&source, SINGLE).unwrap();
    let link = directory.path().join("link.toml");
    symlink(&source, &link).unwrap();
    assert!(load_deployment(&link).is_err());
    let fifo = directory.path().join("fifo.toml");
    rustix::fs::mknodat(
        rustix::fs::CWD,
        &fifo,
        rustix::fs::FileType::Fifo,
        rustix::fs::Mode::RUSR,
        0,
    )
    .unwrap();
    assert!(load_deployment(&fifo).is_err());
}

fn command(config: &Path) -> Command {
    let executable = std::env::var_os("OZZY_SOAK_BROKER_BINARY")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_ozy_broker").into());
    let mut command = Command::new(executable);
    command.arg("--config").arg(config);
    command
}

#[cfg(target_os = "linux")]
#[test]
fn cli_validate_init_and_check_are_distinct_and_never_start_a_broker() {
    let directory = tempfile::tempdir().unwrap();
    let config = directory.path().join("deployment.toml");
    let identity = directory.path().join("identity.toml");
    fs::write(&config, SINGLE).unwrap();
    let validated = command(&config)
        .args(["validate", "--broker", "laptop"])
        .output()
        .unwrap();
    assert!(
        validated.status.success(),
        "{}",
        String::from_utf8_lossy(&validated.stderr)
    );
    assert!(!identity.exists());
    let missing = command(&config)
        .args(["check", "--broker", "laptop", "--identity"])
        .arg(&identity)
        .output()
        .unwrap();
    assert!(!missing.status.success());
    assert!(!identity.exists());
    let created = command(&config)
        .args(["init", "--identity"])
        .arg(&identity)
        .output()
        .unwrap();
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let checked = command(&config)
        .args(["check", "--broker", "laptop", "--identity"])
        .arg(&identity)
        .output()
        .unwrap();
    assert!(
        checked.status.success(),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
    let overwritten = command(&config)
        .args(["init", "--identity"])
        .arg(&identity)
        .output()
        .unwrap();
    assert!(!overwritten.status.success());
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 2);
}

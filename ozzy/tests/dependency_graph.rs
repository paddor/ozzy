//! Enforces the workspace dependency direction with `cargo metadata`.
//!
//! Only normal dependencies count. Dev and build dependencies may cross layers
//! for tests and benchmarks. `ozzy-bench` may depend on any crate; no crate may
//! depend on it or on `ozzy-sim` outside dev dependencies.

use std::collections::BTreeSet;
use std::process::Command;

use serde_json::Value;

/// Libraries that mark a layer boundary. A normal dependency on one of these
/// must be listed in `ALLOWED`.
const BOUNDARY: &[&str] = &["fanring", "omq-tokio", "rustix", "tokio"];

/// Allowed normal dependencies on workspace crates and boundary libraries.
const ALLOWED: &[(&str, &[&str])] = &[
    ("ozzy-proto", &[]),
    ("ozzy-config", &[]),
    // Deployment assembly selects storage backends, starts application shards,
    // and binds the shared frontend. Lower layers never depend on the broker.
    (
        "ozzy-broker",
        &[
            "omq-tokio",
            "rustix",
            "ozzy-config",
            "ozzy-io",
            "ozzy-io-aio",
            "ozzy-io-pool",
            "ozzy-journal",
            "ozzy-journal-segment",
            "ozzy-proto",
            "ozzy-replication",
            "ozzy-runtime",
            "tokio",
        ],
    ),
    ("ozzy-io", &["fanring", "tokio"]),
    ("ozzy-io-pool", &["fanring", "rustix", "ozzy-io", "tokio"]),
    ("ozzy-io-aio", &["rustix", "ozzy-io", "ozzy-io-pool"]),
    ("ozzy-journal", &["ozzy-proto"]),
    ("ozzy-core", &["ozzy-journal", "ozzy-proto"]),
    (
        "ozzy-replication",
        &["ozzy-core", "ozzy-journal", "ozzy-proto"],
    ),
    // Typed canonical checkpoints, identity overlays, retention floors, and
    // canonical recovery consume `ozzy-core` state. This is the only
    // storage-to-application edge in the workspace.
    (
        "ozzy-journal-segment",
        &[
            "fanring",
            "rustix",
            "ozzy-core",
            "ozzy-io",
            // The legacy writer's AIO adapter remains until actor cutover.
            // Async journal code depends only on the backend-neutral contract.
            "ozzy-io-aio",
            "ozzy-journal",
            "ozzy-proto",
        ],
    ),
    (
        "ozzy-runtime",
        &[
            "fanring",
            "omq-tokio",
            "ozzy-core",
            "ozzy-io",
            "ozzy-journal",
            "ozzy-journal-segment",
            "ozzy-proto",
            "ozzy-replication",
            "tokio",
        ],
    ),
    ("ozzy", &["omq-tokio", "ozzy-proto", "ozzy-runtime"]),
    ("ozzy-sim", &["ozzy-core", "ozzy-journal"]),
];

fn workspace_metadata() -> Value {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    let output = Command::new(cargo)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--offline",
        ])
        .args(["--manifest-path", manifest])
        .output()
        .expect("run cargo metadata");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("cargo metadata JSON")
}

fn tracked(name: &str) -> bool {
    name.starts_with("ozzy") || BOUNDARY.contains(&name)
}

#[test]
fn executable_names_identify_ozzy_and_all_integration_tests_are_registered() {
    let metadata = workspace_metadata();
    for package in metadata["packages"].as_array().unwrap() {
        let targets = package["targets"].as_array().unwrap();
        for target in targets {
            // Binaries fit the 15-character kernel process name.
            let prefix = if target["kind"] == serde_json::json!(["bin"]) {
                "ozy_"
            } else {
                "ozzy_"
            };
            if target["kind"] != serde_json::json!(["lib"]) {
                assert!(
                    target["name"].as_str().unwrap().starts_with(prefix),
                    "unprefixed executable: {target}"
                );
            }
        }
        let root = std::path::Path::new(package["manifest_path"].as_str().unwrap())
            .parent()
            .unwrap();
        let tests = root.join("tests");
        if !tests.is_dir() {
            continue;
        }
        let registered: BTreeSet<_> = targets
            .iter()
            .filter(|t| t["kind"] == serde_json::json!(["test"]))
            .map(|t| std::path::PathBuf::from(t["src_path"].as_str().unwrap()))
            .collect();
        for entry in std::fs::read_dir(tests).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "rs") {
                assert!(
                    registered.contains(&path),
                    "unregistered integration test: {}",
                    path.display()
                );
            }
        }
    }
}

fn normal_dependencies(package: &Value) -> BTreeSet<String> {
    package["dependencies"]
        .as_array()
        .expect("dependency list")
        .iter()
        .filter(|dependency| dependency["kind"].is_null())
        .map(|dependency| dependency["name"].as_str().expect("dependency name"))
        .filter(|name| tracked(name))
        .map(str::to_owned)
        .collect()
}

#[test]
fn workspace_crates_only_depend_downward() {
    let metadata = workspace_metadata();
    let packages = metadata["packages"].as_array().expect("package list");
    let mut violations = Vec::new();
    let mut covered = BTreeSet::new();
    for package in packages {
        let name = package["name"].as_str().expect("package name");
        if name == "ozzy-bench" {
            continue;
        }
        let Some((_, allowed)) = ALLOWED.iter().find(|(rule, _)| *rule == name) else {
            violations.push(format!("{name}: no dependency rule"));
            continue;
        };
        covered.insert(name.to_owned());
        for dependency in normal_dependencies(package) {
            if !allowed.contains(&dependency.as_str()) {
                violations.push(format!("{name} -> {dependency}"));
            }
        }
    }
    for (name, _) in ALLOWED {
        assert!(
            covered.contains(*name),
            "dependency rule names a missing crate: {name}"
        );
    }
    assert!(
        violations.is_empty(),
        "dependency direction violations:\n{}",
        violations.join("\n")
    );
}

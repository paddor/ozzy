//! Build artifact separation and source and workload identity.

use super::*;

#[test]
fn separate_checkouts_cannot_share_benchmark_build_artifacts() {
    use ozzy_bench::automation::build_target;
    let main = build_target(Path::new("/workspace/main/ozzy"));
    let experiment = build_target(Path::new("/workspace/experiment/ozzy"));
    assert_ne!(main, experiment);
    assert!(main.starts_with(artifact_root().join("cargo-target/checkouts")));
    assert_eq!(main, build_target(Path::new("/workspace/main/ozzy")));
}

#[test]
fn launcher_creates_configured_artifacts_and_preserves_cargo_overrides() {
    let directory = tempfile::tempdir().unwrap();
    let checkout = ozzy_bench::automation::root();
    let key = ozzy_bench::automation::build_target(&checkout);
    for explicit_target in [false, true] {
        let artifacts = directory.path().join(if explicit_target {
            "explicit artifact root"
        } else {
            "artifact root"
        });
        let target = if explicit_target {
            directory.path().join("explicit target")
        } else {
            artifacts
                .join("cargo-target/checkouts")
                .join(key.file_name().unwrap())
        };
        let mut command = Command::new("bash");
        command
            .args([
                "--noprofile",
                "--norc",
                "-c",
                "source \"$1\"; printf '%s\\n' \"$OZZY_ARTIFACT_ROOT\" \"$CARGO_TARGET_DIR\" \"$TMPDIR\"",
                "ozzy-path-test",
            ])
            .arg(checkout.join("scripts/ozzy_tools.sh"))
            .env("OZZY_ARTIFACT_ROOT", &artifacts)
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("TMPDIR");
        if explicit_target {
            command.env("CARGO_TARGET_DIR", &target);
        }
        let output = command.output().unwrap();
        assert!(output.status.success(), "{output:?}");
        let output = String::from_utf8(output.stdout).unwrap();
        let paths: Vec<_> = output.lines().map(Path::new).collect();
        assert_eq!(
            paths,
            [artifacts.as_path(), target.as_path(), artifacts.as_path()]
        );
        assert!(artifacts.is_dir());
        assert!(target.join("ozzy-tools").is_dir());
    }
}

#[test]
fn source_hash_detects_dirty_and_untracked_contents_not_just_git_status() {
    let temp = tempfile::tempdir().unwrap();
    let git = |args: &[&str]| {
        assert!(
            Command::new("git")
                .args(args)
                .current_dir(temp.path())
                .output()
                .unwrap()
                .status
                .success()
        );
    };
    git(&["init", "-q"]);
    let tracked = temp.path().join("lib.rs");
    fs::write(&tracked, "original").unwrap();
    git(&["add", "lib.rs"]);
    git(&[
        "-c",
        "user.name=Test",
        "-c",
        "user.email=test@example.invalid",
        "-c",
        "core.hooksPath=/dev/null",
        "commit",
        "-qm",
        "fixture",
    ]);
    fs::write(&tracked, "dirty").unwrap();
    let untracked = temp.path().join("new.rs");
    fs::write(&untracked, "first").unwrap();
    let before = source::checkout(temp.path()).unwrap();
    fs::write(&untracked, "second").unwrap();
    let after = source::checkout(temp.path()).unwrap();
    assert_eq!(before["status"], after["status"]);
    assert_ne!(before["content_sha256"], after["content_sha256"]);
    fs::write(tracked, "changed again").unwrap();
    assert_ne!(
        after["content_sha256"],
        source::checkout(temp.path()).unwrap()["content_sha256"]
    );
    let worker = source::worker_checkout(temp.path()).unwrap();
    for name in [
        "doc/charts/single/buffered.svg",
        "ozzy-bench/src/automation/server/redpanda.rs",
        "ozzy-bench/src/bin/chart/fixed.rs",
    ] {
        let path = temp.path().join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "fixture or rendering changed").unwrap();
        assert_eq!(source::worker_checkout(temp.path()).unwrap(), worker);
    }
    for name in [
        "ozzy-bench/src/bin/timed_bench/timed/external/kafka.rs",
        "ozzy-bench/src/automation/mod.rs",
        "ozzy-runtime/src/lib.rs",
        "Cargo.lock",
    ] {
        let path = temp.path().join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "measured code changed").unwrap();
        assert_ne!(source::worker_checkout(temp.path()).unwrap(), worker);
        fs::remove_file(path).unwrap();
    }
}

#[test]
fn workload_hash_ignores_native_edits_but_detects_corpus_and_lock_changes() {
    let temp = tempfile::tempdir().unwrap();
    for name in [
        "Cargo.lock",
        "ozzy-bench/Cargo.toml",
        "ozzy-bench/src/bin/timed_bench/corpus.rs",
        "ozzy-runtime/src/lib.rs",
    ] {
        let path = temp.path().join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, "original").unwrap();
    }
    let original = source::workload_fingerprint(temp.path()).unwrap();
    fs::write(
        temp.path().join("ozzy-runtime/src/lib.rs"),
        "native changed",
    )
    .unwrap();
    assert_eq!(source::workload_fingerprint(temp.path()).unwrap(), original);
    for name in ["Cargo.lock", "ozzy-bench/src/bin/timed_bench/corpus.rs"] {
        fs::write(temp.path().join(name), "changed").unwrap();
        assert_ne!(source::workload_fingerprint(temp.path()).unwrap(), original);
        fs::write(temp.path().join(name), "original").unwrap();
    }
}

//! Freeze local dependency contents, compiler settings, and the workload contract.
use super::{Result, capture};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet, fs, io::Read, os::unix::fs::MetadataExt, path::Path, process::Command,
};

/// Compute the SHA-256 fingerprint of one artifact file.
pub fn sha256(path: &Path) -> Result<String> {
    let mut hash = Sha256::new();
    let mut file = fs::File::open(path)?;
    let mut buffer = vec![0_u8; 65536];
    loop {
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn git(root: &Path, args: &[&str]) -> Result<String> {
    capture(Command::new("git").args(args).current_dir(root))
}

/// Capture source revision, worktree changes, and checkout identity.
pub fn checkout(directory: &Path) -> Result<Value> {
    checkout_filtered(directory, |_| true)
}

fn checkout_filtered(directory: &Path, include: impl Fn(&str) -> bool) -> Result<Value> {
    let root = std::path::PathBuf::from(git(directory, &["rev-parse", "--show-toplevel"])?);
    let names = git(
        &root,
        &[
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
        ],
    )?;
    let mut hash = Sha256::new();
    for name in names
        .split('\0')
        .filter(|n| !n.is_empty())
        .collect::<BTreeSet<_>>()
    {
        if !include(name) {
            continue;
        }
        hash.update(name.as_bytes());
        hash.update([0]);
        let path = root.join(name);
        match fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_symlink() => {
                hash.update(b"link\0");
                hash.update(fs::read_link(path)?.as_os_str().as_encoded_bytes());
            }
            Ok(meta) if meta.is_file() => {
                hash.update(b"file\0");
                hash.update(meta.mode().to_string());
                hash.update([0]);
                let mut file = fs::File::open(path)?;
                let mut content = Sha256::new();
                let mut buffer = vec![0_u8; 65536];
                loop {
                    let n = file.read(&mut buffer)?;
                    if n == 0 {
                        break;
                    }
                    content.update(&buffer[..n]);
                }
                hash.update(content.finalize());
            }
            Ok(_) => {
                return Err(
                    format!("unfingerprinted directory/submodule: {}", path.display()).into(),
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => hash.update(b"missing\0"),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(
        json!({"root":root,"revision":git(&root,&["rev-parse","HEAD"])?,"status":git(&root,&["status","--short"])?,"content_sha256":format!("{:x}",hash.finalize())}),
    )
}

/// Inputs to the measured worker. Fixture orchestration and chart rendering
/// have separate run provenance; changing either must not require new timings.
pub fn worker_checkout(directory: &Path) -> Result<Value> {
    let mut value = checkout_filtered(directory, |name| {
        Path::new(name)
            .extension()
            .is_none_or(|extension| extension != "md")
            && !name.starts_with("doc/")
            && !name.starts_with("ozzy-bench/tests/")
            && (!name.starts_with("ozzy-bench/src/automation/")
                || name == "ozzy-bench/src/automation/mod.rs")
            && (!name.starts_with("ozzy-bench/src/bin/")
                || name == "ozzy-bench/src/bin/ozzy_timed_bench.rs"
                || name.starts_with("ozzy-bench/src/bin/timed_bench/"))
    })?;
    value.as_object_mut().unwrap().remove("status");
    Ok(value)
}

/// Fingerprint the checkout-specific benchmark worker executable.
pub fn worker_identity(root: &Path) -> Result<Value> {
    let mut value = identity(root)?;
    for checkout in value["checkouts"].as_array_mut().unwrap() {
        if checkout["root"] == root.to_string_lossy().as_ref() {
            *checkout = worker_checkout(root)?;
        } else {
            checkout.as_object_mut().unwrap().remove("status");
        }
    }
    Ok(value)
}

/// Capture source and worker identity, including sibling transport dependencies.
pub fn identity(root: &Path) -> Result<Value> {
    let compiler = capture(Command::new("rustc").arg("-vV"))?;
    let host = compiler
        .lines()
        .find_map(|l| l.strip_prefix("host: "))
        .ok_or("missing compiler host")?;
    let metadata: Value = serde_json::from_str(&capture(
        Command::new("cargo")
            .args([
                "metadata",
                "--offline",
                "--locked",
                "--filter-platform",
                host,
                "--features",
                "ozzy-bench/comparisons",
                "--format-version",
                "1",
            ])
            .current_dir(root),
    )?)?;
    let manifests = metadata["packages"]
        .as_array()
        .ok_or("missing packages")?
        .iter()
        .filter(|p| p["source"].is_null())
        .map(|p| p["manifest_path"].as_str().ok_or("missing manifest"))
        .collect::<std::result::Result<BTreeSet<_>, _>>()?;
    let roots = manifests
        .iter()
        .map(|m| {
            git(
                Path::new(m).parent().unwrap(),
                &["rev-parse", "--show-toplevel"],
            )
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let checkouts = roots
        .iter()
        .map(|r| checkout(Path::new(r)))
        .collect::<Result<Vec<_>>>()?;
    let environment = std::env::vars()
        .filter(|(k, _)| {
            matches!(
                k.as_str(),
                "RUSTFLAGS" | "CARGO_ENCODED_RUSTFLAGS" | "RUSTUP_TOOLCHAIN" | "CARGO_BUILD_TARGET"
            ) || k.starts_with("CARGO_PROFILE_")
        })
        .collect::<std::collections::BTreeMap<_, _>>();
    Ok(
        json!({"checkouts":checkouts,"local_manifests":manifests,"cargo_lock_sha256":sha256(&root.join("Cargo.lock"))?,"compiler":compiler,"cargo":capture(Command::new("cargo").arg("--version"))?,"build_environment":environment}),
    )
}

/// Reject source or executable changes since the supplied provenance capture.
pub fn require_unchanged(root: &Path, expected: &Value) -> Result<()> {
    if identity(root)? != *expected {
        return Err(
            "source, dependencies, or build settings changed; discard run and rebuild".into(),
        );
    }
    Ok(())
}

/// Fingerprint workload generators and adapter sources used by this checkout.
pub fn workload_fingerprint(root: &Path) -> Result<String> {
    fn visit(path: &Path, paths: &mut BTreeSet<std::path::PathBuf>) -> Result<()> {
        for entry in fs::read_dir(path)? {
            let path = entry?.path();
            if path.is_dir() {
                visit(&path, paths)?;
            } else if path.extension().is_some_and(|e| e == "rs")
                && path.file_name().is_some_and(|n| n != "ozzy_chart.rs")
                && !path.components().any(|c| c.as_os_str() == "chart")
            {
                paths.insert(path);
            }
        }
        Ok(())
    }
    let mut paths = BTreeSet::from([root.join("Cargo.lock"), root.join("ozzy-bench/Cargo.toml")]);
    visit(&root.join("ozzy-bench/src"), &mut paths)?;
    let mut hash = Sha256::new();
    for path in paths {
        hash.update(path.strip_prefix(root)?.as_os_str().as_encoded_bytes());
        hash.update([0]);
        hash.update(fs::read(path)?);
    }
    Ok(format!("{:x}", hash.finalize()))
}

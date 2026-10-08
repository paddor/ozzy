//! Pinned official relocatable distribution; no system install or JVM.
use crate::automation::{Result, artifact_root, cache, capture, json_file, read_json, source};
use serde_json::{Value, json};
use std::{fs, path::PathBuf, process::Command};

const RELEASE: &str = "26.2.2";
const ARCHIVE_SHA256: &str = "575fefbbc2cb929634e2b831acb87ecb0b23403b2ef8b356a34dbc0a00934aa5";
pub(super) fn directory() -> PathBuf {
    artifact_root().join(format!("redpanda-{RELEASE}"))
}
pub(super) fn binary() -> PathBuf {
    directory().join("libexec/redpanda")
}
pub(super) fn loader() -> PathBuf {
    directory().join("lib/ld.so")
}
pub(super) fn rpk() -> PathBuf {
    directory().join("libexec/rpk")
}
fn stamp() -> PathBuf {
    cache().join(format!("redpanda-{RELEASE}-build.json"))
}

fn identity() -> Result<Value> {
    let directory = directory();
    let mut libraries = std::collections::BTreeMap::new();
    for entry in fs::read_dir(directory.join("lib"))? {
        let path = entry?.path();
        if path.is_file() {
            libraries.insert(
                path.file_name().unwrap().to_string_lossy().into_owned(),
                source::sha256(&path)?,
            );
        }
    }
    Ok(json!({"release":RELEASE,"archive_sha256":ARCHIVE_SHA256,
        "binary_sha256":source::sha256(&binary())?,"rpk_sha256":source::sha256(&rpk())?,"libraries":libraries,
        "version":capture(Command::new(loader()).arg("--library-path").arg(directory.join("lib")).arg(binary()).arg("--version"))?}))
}

pub(super) fn verified_identity() -> Result<Value> {
    let actual = identity()?;
    if actual != read_json(&stamp())? {
        return Err("Redpanda install changed; rerun without --no-build".into());
    }
    Ok(actual)
}

/// Check or prepare the pinned Redpanda executable for comparisons.
pub fn prepare(no_build: bool) -> Result<()> {
    if no_build {
        return verified_identity().map(|_| ());
    }
    if stamp().exists() && verified_identity().is_ok() {
        return Ok(());
    }
    if std::env::consts::ARCH != "x86_64" {
        return Err("pinned Redpanda distribution requires x86_64".into());
    }
    let directory = directory();
    fs::create_dir_all(&directory)?;
    let archive = directory.join("archive.tar.gz");
    if !archive.exists() {
        let partial = directory.join("archive.part");
        capture(Command::new("curl").args(["--fail","--location","--silent","--show-error","--output"])
            .arg(&partial).arg(format!("https://vectorized-public.s3.us-west-2.amazonaws.com/releases/redpanda/{RELEASE}/redpanda-{RELEASE}-amd64.tar.gz")))?;
        if source::sha256(&partial)? != ARCHIVE_SHA256 {
            return Err("Redpanda archive checksum mismatch".into());
        }
        fs::rename(partial, &archive)?;
    }
    if source::sha256(&archive)? != ARCHIVE_SHA256 {
        return Err("Redpanda archive checksum mismatch".into());
    }
    capture(
        Command::new("tar")
            .arg("-xzf")
            .arg(&archive)
            .arg("-C")
            .arg(&directory),
    )?;
    // Invoke the bundled loader explicitly. Rewriting this distribution's
    // interpreter with patchelf corrupts its ELF layout, even with 2 MiB pages.
    json_file(&stamp(), &identity()?)
}

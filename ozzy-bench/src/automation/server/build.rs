//! Build the exact release before any measured process starts.
use super::{RELEASE, RELEASE_TAG, REVISION, Result};
use crate::automation::{SSD, cache, capture, json_file, read_json, source};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

pub(super) fn checkout() -> PathBuf {
    PathBuf::from(SSD).join("iggy-server-0.9.0-rc.1")
}
pub(crate) fn binary() -> PathBuf {
    PathBuf::from(SSD).join("cargo-target/iggy-0.9.0-rc.1/release/iggy-server")
}

fn dependencies() -> PathBuf {
    PathBuf::from(SSD).join("iggy-build-deps")
}

// A private Debian sysroot keeps the VM unchanged. Cargo only needs headers and
// linker metadata. The same private libraries are used when running the server.
fn prepare_dependencies() -> Result<()> {
    let directory = dependencies();
    if directory
        .join("usr/lib/x86_64-linux-gnu/pkgconfig/hwloc.pc")
        .exists()
        && library_path().join("libudev.so").exists()
    {
        return Ok(());
    }
    let packages = directory.join("packages");
    fs::create_dir_all(&packages)?;
    capture(
        Command::new("apt-get")
            .args([
                "download",
                "libhwloc-dev",
                "libhwloc15",
                "libnuma-dev",
                "libudev-dev",
                "libudev1",
            ])
            .current_dir(&packages),
    )?;
    for entry in fs::read_dir(packages)? {
        let path = entry?.path();
        if path.extension().is_some_and(|extension| extension == "deb") {
            capture(Command::new("dpkg-deb").arg("-x").arg(path).arg(&directory))?;
        }
    }
    Ok(())
}

pub(crate) fn library_path() -> PathBuf {
    dependencies().join("usr/lib/x86_64-linux-gnu")
}
fn identity() -> Result<Value> {
    let directory = checkout();
    let revision = capture(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&directory),
    )?;
    let status = capture(
        Command::new("git")
            .args(["status", "--porcelain"])
            .current_dir(&directory),
    )?;
    if revision != REVISION || !status.is_empty() {
        return Err("Iggy checkout must be the unmodified pinned release".into());
    }
    let tag_revision = capture(
        Command::new("git")
            .args(["rev-parse", &format!("refs/tags/{RELEASE_TAG}^{{}}")])
            .current_dir(&directory),
    )?;
    if tag_revision != REVISION {
        return Err("Iggy release tag differs from pinned source".into());
    }
    Ok(
        json!({"release_tag":RELEASE_TAG,"revision":revision,"executable_sha256":source::sha256(&binary())?,"compiler":capture(Command::new("rustc").arg("-vV").current_dir(directory))?,"features":["mimalloc"],"default_features":false,"hwloc_sha256":source::sha256(&library_path().join("libhwloc.so.15"))?,"udev_sha256":source::sha256(&library_path().join("libudev.so.1"))?}),
    )
}

pub fn prepare(no_build: bool, logs: &Path) -> Result<()> {
    let directory = checkout();
    let stamp = cache().join(format!("iggy-{RELEASE}-build.json"));
    if no_build {
        if read_json(&stamp)? != identity()? {
            return Err("Iggy build stamp changed; rerun without --no-build".into());
        }
        return Ok(());
    }
    if !directory.exists() {
        capture(
            Command::new("git")
                .args([
                    "clone",
                    "--depth",
                    "1",
                    "--branch",
                    RELEASE_TAG,
                    "https://github.com/apache/iggy.git",
                ])
                .arg(&directory),
        )?;
    }
    // Existing RC artifacts contain the same source commit as the final release.
    // Fetch its tag once, retaining those build directories and their warm cache.
    if !Command::new("git")
        .args([
            "show-ref",
            "--verify",
            "--quiet",
            &format!("refs/tags/{RELEASE_TAG}"),
        ])
        .current_dir(&directory)
        .status()?
        .success()
    {
        capture(
            Command::new("git")
                .args(["fetch", "--depth=1", "origin", "tag", RELEASE_TAG])
                .current_dir(&directory),
        )?;
    }
    if capture(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&directory),
    )? != REVISION
        || !capture(
            Command::new("git")
                .args(["status", "--porcelain"])
                .current_dir(&directory),
        )?
        .is_empty()
    {
        return Err("Iggy source differs from pinned release".into());
    }
    let arguments = [
        "build",
        "--locked",
        "--release",
        "-p",
        "server",
        "--bin",
        "iggy-server",
        "--no-default-features",
        "--features",
        "mimalloc",
    ];
    prepare_dependencies()?;
    println!("CHECK Iggy cargo {}", arguments.join(" "));
    let output = Command::new("cargo")
        .args(arguments)
        .current_dir(&directory)
        .env(
            "CARGO_TARGET_DIR",
            format!("{SSD}/cargo-target/iggy-0.9.0-rc.1"),
        )
        .env("TMPDIR", SSD)
        .env("PKG_CONFIG_PATH", library_path().join("pkgconfig"))
        .env("PKG_CONFIG_SYSROOT_DIR", dependencies())
        .output()?;
    let log = [output.stdout, output.stderr].concat();
    fs::write(logs.join("iggy-build.log"), &log)?;
    print!("{}", String::from_utf8_lossy(&log));
    if !output.status.success() || String::from_utf8_lossy(&log).contains("warning:") {
        return Err("Iggy release build failed or emitted warnings".into());
    }
    json_file(&stamp, &identity()?)
}

pub(super) fn verified_identity() -> Result<Value> {
    let expected = read_json(&cache().join(format!("iggy-{RELEASE}-build.json")))?;
    let actual = identity()?;
    if actual != expected {
        return Err("Iggy source or executable changed".into());
    }
    Ok(actual)
}

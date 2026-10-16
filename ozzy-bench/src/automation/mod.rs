//! Benchmark control stays outside measured broker and client work.

pub mod compare;
pub mod cpus;
mod device;
pub mod distributed;
pub mod isolation;
pub mod records;
pub mod server;
pub mod source;
pub mod supervise;
pub mod validation;
pub mod workloads;

use serde_json::Value;
use std::{
    error::Error,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

/// Failure returned by benchmark automation and its supervised subprocesses.
pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
/// Configured artifact root, defaulting to the system temporary directory.
pub fn artifact_root() -> PathBuf {
    let directory = std::env::var_os("OZZY_ARTIFACT_ROOT")
        .filter(|value| !value.is_empty())
        .map_or_else(std::env::temp_dir, PathBuf::from);
    if directory.is_absolute() {
        directory
    } else {
        root().join(directory)
    }
}

/// Require observable block-backed storage before measuring persistent workloads.
pub fn require_artifact_disk() -> Result<()> {
    let directory = artifact_root();
    fs::create_dir_all(&directory)?;
    device::Snapshot::take(&directory)
        .map(drop)
        .map_err(|error| {
            format!(
                "benchmark artifacts require block-backed storage; set OZZY_ARTIFACT_ROOT: {error}"
            )
            .into()
        })
}

/// The three-broker mode.
pub fn cluster_mode(mode: &str) -> bool {
    mode == "disk-quorum"
}

static CANCELED: std::sync::OnceLock<std::sync::Arc<std::sync::atomic::AtomicBool>> =
    std::sync::OnceLock::new();

/// Register cancellation on SIGINT and SIGTERM for this automation process.
pub fn install_signals() -> Result<()> {
    let flag =
        CANCELED.get_or_init(|| std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)));
    for signal in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        signal_hook::flag::register(signal, flag.clone())?;
    }
    Ok(())
}

/// Fail if a registered termination signal canceled the run.
pub fn check_canceled() -> Result<()> {
    if CANCELED
        .get()
        .is_some_and(|flag| flag.load(std::sync::atomic::Ordering::Relaxed))
    {
        return Err("benchmark canceled".into());
    }
    Ok(())
}

/// Source checkout containing this benchmark automation crate.
pub fn root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    // The bench crate is a workspace member, so its directory has a parent.
    manifest.parent().unwrap_or(manifest).to_path_buf()
}

/// Benchmark workers must never reuse another checkout's Cargo artifacts.
pub fn build_target(checkout: &Path) -> PathBuf {
    use sha2::{Digest, Sha256};
    let key = Sha256::digest(checkout.as_os_str().as_encoded_bytes());
    artifact_root()
        .join("cargo-target/checkouts")
        .join(format!("{key:x}"))
}

/// Checkout-specific release worker executable on the artifact disk.
pub fn worker_binary() -> PathBuf {
    build_target(&root()).join("release/ozzy_timed_bench")
}

/// Append-only benchmark result ledger directory under the user cache.
pub fn cache() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or("HOME is required for the result ledger")?;
    Ok(PathBuf::from(home).join(".cache/ozzy"))
}

/// Disposable run artifacts; the home cache holds append-only result ledgers.
pub fn artifacts() -> PathBuf {
    artifact_root().join("ozzy-artifacts")
}

/// Hold `directory/runner.lock` so no other run using that directory starts
/// meanwhile; the lock is released when the returned file drops.
pub fn lock_runner(directory: &Path) -> Result<fs::File> {
    fs::create_dir_all(directory)?;
    let lock = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(directory.join("runner.lock"))?;
    lock.try_lock()?;
    Ok(lock)
}

/// Run a command to completion; return UTF-8 stdout or its failed status and diagnostics.
pub fn capture(command: &mut Command) -> Result<String> {
    let output = command.output()?;
    if !output.status.success() {
        return Err(format!(
            "{command:?}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

/// Write one formatted JSON artifact, replacing its existing contents.
pub fn json_file(path: &Path, value: &Value) -> Result<()> {
    let mut file = fs::File::create(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    Ok(())
}

/// Read and decode one JSON artifact.
pub fn read_json(path: &Path) -> Result<Value> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

/// Generate a timestamped unique benchmark run identifier.
pub fn run_id() -> Result<String> {
    Ok(format!(
        "{}-{}",
        capture(Command::new("date").args(["-u", "+%Y%m%dT%H%M%SZ"]))?,
        &uuid::Uuid::now_v7().simple().to_string()[24..]
    ))
}

/// The `field` of a JSON object read through `get`, such as `Value::as_str`,
/// or an error naming the missing or mistyped field.
pub fn json_field<'a, T>(
    value: &'a Value,
    field: &str,
    get: impl FnOnce(&'a Value) -> Option<T>,
) -> Result<T> {
    get(&value[field]).ok_or_else(|| format!("missing or mistyped JSON field `{field}`").into())
}

/// As [`json_field`], for mutable access such as `Value::as_object_mut`.
pub fn json_field_mut<'a, T>(
    value: &'a mut Value,
    field: &str,
    get: impl FnOnce(&'a mut Value) -> Option<T>,
) -> Result<T> {
    value
        .get_mut(field)
        .and_then(get)
        .ok_or_else(|| format!("missing or mistyped JSON field `{field}`").into())
}

/// Decode a finite JSON number, rejecting missing or non-finite measurements.
pub fn finite(value: &Value) -> Result<f64> {
    value
        .as_f64()
        .filter(|n| n.is_finite() && *n >= 0.0)
        .ok_or_else(|| "missing, negative, or nonfinite measurement".into())
}

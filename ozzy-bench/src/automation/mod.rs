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
/// Disk-backed root for disposable build, profile, and benchmark artifacts.
pub const SSD: &str = "/mnt/ssd/tmp";

/// Exactly three brokers, regardless of confirmation or persistence policy.
pub fn cluster_mode(mode: &str) -> bool {
    matches!(mode, "disk-quorum" | "replicated-persisting")
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
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Benchmark workers must never reuse another checkout's Cargo artifacts.
pub fn build_target(checkout: &Path) -> PathBuf {
    use sha2::{Digest, Sha256};
    let key = Sha256::digest(checkout.as_os_str().as_encoded_bytes());
    PathBuf::from(SSD)
        .join("cargo-target/checkouts")
        .join(format!("{key:x}"))
}

/// Checkout-specific release worker executable on the artifact disk.
pub fn worker_binary() -> PathBuf {
    build_target(&root()).join("release/ozy_timed_bench")
}

/// Append-only benchmark result ledger directory under the user cache.
pub fn cache() -> PathBuf {
    PathBuf::from(std::env::var_os("HOME").expect("HOME is required")).join(".cache/ozzy")
}

/// Disposable run artifacts; the home cache holds append-only result ledgers.
pub fn artifacts() -> PathBuf {
    PathBuf::from(SSD).join("ozzy-artifacts")
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

/// Decode a finite JSON number, rejecting missing or non-finite measurements.
pub fn finite(value: &Value) -> Result<f64> {
    value
        .as_f64()
        .filter(|n| n.is_finite() && *n >= 0.0)
        .ok_or_else(|| "missing, negative, or nonfinite measurement".into())
}

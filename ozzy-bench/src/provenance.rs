//! Identify the running artifact separately from the checkout observed at run start.

use std::error::Error;
use std::io::Read;
use std::path::Path;
use std::process::Command;

use serde_json::Value;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

pub fn capture() -> Result<Value> {
    let checkout = Path::new(env!("CARGO_MANIFEST_DIR"));
    let git = |arguments: &[&str]| -> Result<String> {
        let output = Command::new("git")
            .current_dir(checkout)
            .args(arguments)
            .output()?;
        if !output.status.success() {
            return Err(std::io::Error::other("cannot identify benchmark source checkout").into());
        }
        Ok(String::from_utf8(output.stdout)?.trim().to_owned())
    };
    Ok(serde_json::json!({
        "source_checkout_revision": git(&["rev-parse", "HEAD"])?,
        "source_checkout_dirty": !git(&["status", "--porcelain", "--untracked-files=normal"])?.is_empty(),
        "executable_xxh3_128": digest(&std::env::current_exe()?)?,
        "cargo_lock_xxh3_128": digest(&checkout.join("../Cargo.lock"))?,
    }))
}

pub fn digest(path: &Path) -> Result<String> {
    let mut hasher = ozzy_journal::integrity::IntegrityHasher::new("ozzy benchmark artifact v1");
    let mut file = std::fs::File::open(path)?;
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
    }
    Ok(hex(&hasher.finish().as_bytes()[..16]))
}

/// Plain hexadecimal formatting, including canonical digest slots.
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 15)]));
    }
    output
}

/// Parse the complete canonical integrity slot, without a hash-algorithm wrapper.
pub fn parse_digest(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(std::io::Error::other("invalid canonical digest").into());
    }
    let mut result = [0; 32];
    for (byte, pair) in result.iter_mut().zip(value.as_bytes().as_chunks::<2>().0) {
        *byte = u8::from_str_radix(std::str::from_utf8(pair)?, 16)?;
    }
    Ok(result)
}

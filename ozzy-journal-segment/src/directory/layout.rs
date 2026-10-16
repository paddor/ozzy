//! Store file names and the parsers that recognize them.

use std::ffi::OsStr;
use std::path::PathBuf;

use ozzy_proto::CheckpointId;

use crate::SegmentReference;

pub(crate) const CURRENT_FILE: &str = "CURRENT";
pub(crate) const IDENTITY_FILE: &str = "identity";
pub(crate) const CONFIGURATION_FILE: &str = "CONFIGURATION";
pub(crate) const LOCK_FILE: &str = "group.lock";
pub(crate) const DATA_DIRECTORIES: [&str; 4] = ["segments", "checkpoints", "indexes", "staging"];
/// A retired segment file kept in `segments/` for the next roll.
pub(crate) const SPARE_SEGMENT: &str = "spare.log";

pub(crate) fn is_index_staging_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str().and_then(|name| name.strip_prefix("index-")) else {
        return false;
    };
    let mut fields = name.split('-');
    matches!(
        (fields.next(), fields.next(), fields.next(), fields.next()),
        (Some(segment), Some(process), Some(sequence), None)
            if parse_decimal_name(segment).is_some()
                && parse_decimal_name(process).is_some()
                && parse_decimal_name(sequence).is_some()
    )
}

pub(crate) fn is_checkpoint_staging_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str().and_then(|name| {
        name.strip_prefix(".checkpoint-")
            .and_then(|name| name.strip_suffix(".tmp"))
    }) else {
        return false;
    };
    let mut fields = name.split('-');
    matches!(
        (fields.next(), fields.next(), fields.next(), fields.next()),
        (Some(checkpoint), Some(process), Some(sequence), None)
            if parse_checkpoint_name(OsStr::new(checkpoint)).is_some()
                && parse_decimal_name(process).is_some()
                && parse_decimal_name(sequence).is_some()
    )
}

pub(crate) fn parse_segment_name(name: &OsStr) -> Option<u64> {
    let stem = name.to_str()?.strip_suffix(".log")?;
    if let Some((id, generation)) = stem.split_once('.') {
        let generation = parse_decimal_name(generation)?;
        if generation == 0 {
            return None;
        }
        parse_decimal_name(id)
    } else {
        parse_decimal_name(stem)
    }
}

pub(crate) fn parse_manifest_name(name: &OsStr) -> Option<u64> {
    parse_decimal_name(name.to_str()?.strip_prefix("MANIFEST.")?)
}

pub(crate) fn parse_manifest_temporary_name(name: &OsStr) -> Option<u64> {
    parse_decimal_name(
        name.to_str()?
            .strip_prefix(".MANIFEST.")?
            .strip_suffix(".tmp")?,
    )
}

pub(crate) fn parse_current_temporary_name(name: &OsStr) -> Option<u64> {
    parse_decimal_name(
        name.to_str()?
            .strip_prefix(".CURRENT.")?
            .strip_suffix(".tmp")?,
    )
}

fn parse_decimal_name(name: &str) -> Option<u64> {
    let value: u64 = name.parse().ok()?;
    (name == value.to_string()).then_some(value)
}

pub(crate) fn parse_checkpoint_name(name: &OsStr) -> Option<CheckpointId> {
    let name = name.to_str()?;
    if name.len() != 32 || !is_lower_hex(name) {
        return None;
    }
    let mut bytes = [0_u8; 16];
    let (pairs, remainder) = name.as_bytes().as_chunks::<2>();
    debug_assert_eq!(remainder.len(), 0);
    for (output, pair) in bytes.iter_mut().zip(pairs) {
        *output = (hex_value(pair[0])? << 4) | hex_value(pair[1])?;
    }
    Some(CheckpointId::from_bytes(bytes))
}

const fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

pub(crate) fn index_name_segment_id(name: &OsStr) -> Option<u64> {
    let (segment, digest) = name.to_str()?.strip_suffix(".idx")?.split_once('-')?;
    if digest.len() != 64 || !is_lower_hex(digest) {
        return None;
    }
    parse_decimal_name(segment)
}

fn is_lower_hex(text: &str) -> bool {
    text.bytes().all(|byte| hex_value(byte).is_some())
}

pub(crate) fn segment_name(segment_id: u64) -> PathBuf {
    PathBuf::from("segments").join(format!("{segment_id}.log"))
}

pub(crate) fn spare_segment_name() -> PathBuf {
    PathBuf::from("segments").join(SPARE_SEGMENT)
}

pub(crate) fn segment_reference_name(
    reference: impl std::borrow::Borrow<SegmentReference>,
) -> PathBuf {
    PathBuf::from("segments").join(reference.borrow().file_name())
}

pub(crate) fn manifest_name(generation: u64) -> String {
    format!("MANIFEST.{generation}")
}

/// Lowercase hexadecimal digits of `bytes`, for content-bound file names.
pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(
        String::with_capacity(2 * bytes.len()),
        |mut output, byte| {
            // Formatting into a String cannot fail.
            let _ = write!(output, "{byte:02x}");
            output
        },
    )
}

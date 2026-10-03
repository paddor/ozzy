//! Optional process-wide counts of successfully appended journal group bytes.
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) static ENCODED_GROUP_BYTES: AtomicU64 = AtomicU64::new(0);

/// Encoded operations, group seals, and alignment padding across all journals.
/// Excludes file headers, indexes, manifests, failed writes, and unused capacity.
/// This measures logical journal bytes, not physical device write amplification.
pub fn encoded_group_bytes() -> u64 {
    ENCODED_GROUP_BYTES.load(Ordering::Relaxed)
}

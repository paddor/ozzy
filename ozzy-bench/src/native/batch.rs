//! One calculation for worker APPEND bounds and measured-setting validation.

use crate::{BenchResult, bench_error};

/// Default benchmark collection ceiling; independent of production SDK defaults.
pub const DEFAULT_SDK_BATCH_TARGET_BYTES: usize = 832 * 1024;

/// Reader frame slots within the default comparison's live payload byte window.
pub fn reader_queue_messages(payload_bytes: usize) -> usize {
    const DEFAULT_MESSAGES: usize = 64;
    (DEFAULT_MESSAGES * DEFAULT_SDK_BATCH_TARGET_BYTES / payload_bytes.max(1))
        .clamp(1, DEFAULT_MESSAGES)
}

/// Maximum record shape of an indivisible prepared canonical APPEND.
#[derive(Debug, Clone, Copy)]
pub struct AppendBudget {
    /// Logical writer batches reserved in the canonical operation header.
    pub writers: usize,
    /// Maximum records reserved in the operation descriptor table.
    pub records: usize,
    /// Bytes in one benchmark record.
    pub record_bytes: usize,
    /// Requested SDK collection target before record and segment clamping.
    pub target_bytes: usize,
    /// Outcomes and due times the operation metadata carries.
    pub metadata: AppendMetadata,
}

impl AppendBudget {
    /// Fixed metadata and payload ceilings. Reserve both raw and encoded backing.
    pub fn bounds(self, body_capacity: usize) -> BenchResult<(usize, usize)> {
        let overflow = || bench_error("prepared APPEND budget overflow");
        let requested = self
            .records
            .checked_mul(self.record_bytes)
            .ok_or_else(overflow)?
            .min(self.target_bytes.max(self.record_bytes));
        let fixed = self
            .metadata
            .bound(self.writers, self.records)
            .ok_or_else(overflow)?;
        let payload = requested.min(body_capacity.saturating_sub(fixed) / 2);
        if self.record_bytes == 0 || payload < self.record_bytes {
            return Err(bench_error(
                "segment cannot hold one prepared record operation",
            ));
        }
        Ok((fixed, payload))
    }

    /// Effective SDK payload after record count, journal and broker PEER clamping.
    pub fn effective_payload(self, segment: usize, decoded: usize) -> BenchResult<usize> {
        let (fixed, payload) = self.bounds(segment_body_capacity(segment, decoded))?;
        let records = self
            .records
            .min(ozzy_runtime::replicated::MAX_APPEND_RECORDS);
        let body = fixed
            .checked_add(
                payload
                    .checked_mul(2)
                    .ok_or_else(|| bench_error("prepared APPEND budget overflow"))?,
            )
            .ok_or_else(|| bench_error("prepared APPEND budget overflow"))?;
        let message = body
            .max(
                self.metadata
                    .bound(1, ozzy_runtime::replicated::MAX_APPEND_RECORDS)
                    .ok_or_else(|| bench_error("prepared APPEND budget overflow"))?
                    + payload,
            )
            .max(1024)
            .min(ozzy_config::MAX_APPEND_BYTES as usize)
            .min(segment / 2);
        Ok(payload
            .min(peer_payload_capacity(message, self.metadata)?)
            .min(records * self.record_bytes))
    }
}

/// What one topic's APPEND metadata carries besides its record table.
#[derive(Debug, Clone, Copy, Default)]
pub struct AppendMetadata {
    /// Canonical operations include producer attempt outcomes and retry proofs.
    pub resolved: bool,
    /// Each record carries a due time, after a first schedule sequence.
    pub scheduled: bool,
}

impl AppendMetadata {
    /// The metadata bound for `batches` holding `records` in all.
    #[must_use]
    pub fn bound(self, batches: usize, records: usize) -> Option<usize> {
        let due = if self.scheduled { 8 } else { 0 };
        ozzy_journal::operation::append_metadata_bound(batches, records, self.resolved)?
            .checked_add(records.checked_add(batches)?.checked_mul(due)?)
    }
}

/// Payload fitting broker PEER framing at the production APPEND record ceiling.
pub fn peer_payload_capacity(message: usize, metadata: AppendMetadata) -> BenchResult<usize> {
    let bound = |records| {
        metadata
            .bound(1, records)
            .ok_or_else(|| bench_error("APPEND metadata bound overflow"))
    };
    let header = bound(0)?;
    let row = bound(1)? - header;
    let records = ozzy_runtime::replicated::MAX_APPEND_RECORDS
        .min(message.saturating_sub(header) / (row + 1))
        .max(1);
    Ok(message.saturating_sub(header + row * records))
}

/// Available canonical body bytes after physical framing and decoded-segment bounds.
pub fn segment_body_capacity(segment: usize, decoded: usize) -> usize {
    use ozzy_journal_segment::{
        ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, SEGMENT_HEADER_BYTES, WRITE_GROUP_ALIGNMENT,
    };
    let usable = segment.saturating_sub(SEGMENT_HEADER_BYTES);
    let aligned = usable / WRITE_GROUP_ALIGNMENT * WRITE_GROUP_ALIGNMENT;
    aligned
        .saturating_sub(ENTRY_HEADER_BYTES + GROUP_SEAL_BYTES)
        .min(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_count_and_target_both_bound_prepared_backing() {
        for (record_bytes, target_bytes, expected) in [
            (128, 4 * 1024 * 1024, 256 * 1024),
            (8192, DEFAULT_SDK_BATCH_TARGET_BYTES, 832 * 1024),
            (8192, 4 * 1024 * 1024, 4 * 1024 * 1024),
            (8192, 16 * 1024 * 1024, 16 * 1024 * 1024),
        ] {
            let (fixed, payload) = AppendBudget {
                writers: 8,
                records: 2048,
                record_bytes,
                target_bytes,
                metadata: AppendMetadata::default(),
            }
            .bounds(64 * 1024 * 1024)
            .unwrap();
            assert_eq!(fixed, 49_773);
            assert_eq!(payload, expected);
            let (_, clamped) = AppendBudget {
                writers: 8,
                records: 2048,
                record_bytes,
                target_bytes,
                metadata: AppendMetadata::default(),
            }
            .bounds(fixed + record_bytes * 2)
            .unwrap();
            assert_eq!(clamped, record_bytes);
        }
    }

    #[test]
    fn peer_framing_can_further_clamp_small_segment_payloads() {
        let budget = AppendBudget {
            writers: 8,
            records: 2048,
            record_bytes: 8192,
            target_bytes: 4 * 1024 * 1024,
            metadata: AppendMetadata::default(),
        };
        assert_eq!(
            budget
                .effective_payload(4 * 1024 * 1024, 64 * 1024 * 1024)
                .unwrap(),
            2 * 1024 * 1024 - 49_241
        );
        assert_eq!(
            peer_payload_capacity(0, AppendMetadata::default()).unwrap(),
            0
        );
        // Each record carries a due time, and each batch a first sequence.
        let scheduled = AppendMetadata {
            resolved: false,
            scheduled: true,
        };
        assert_eq!(
            scheduled.bound(2, 100).unwrap() - AppendMetadata::default().bound(2, 100).unwrap(),
            8 * 102
        );
        assert!(budget.bounds(49_773 + 2 * 8192 - 1).is_err());
    }
}

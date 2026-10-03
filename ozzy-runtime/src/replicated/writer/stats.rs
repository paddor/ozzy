//! Request-size diagnostics, independent of record confirmation and retry state.

/// All successful socket admissions since writer creation, including retries.
/// These counters do not prove broker receipt or confirmation. Empty statistics
/// have zero minima. Counters saturate; no diagnostic can interrupt delivery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriterStats {
    /// APPEND requests admitted to OMQ, each containing one or more records.
    pub requests: u64,
    /// Largest number of APPEND requests awaiting full confirmation in a session.
    pub max_inflight_appends: usize,
    /// Record transmissions, including repeated records on retry.
    pub records: u64,
    /// Payload bytes transmitted, excluding envelope and descriptors.
    pub payload_bytes: u64,
    /// Smallest observed request record count.
    pub min_records: usize,
    /// Largest observed request record count.
    pub max_records: usize,
    /// Smallest observed request payload size, including empty payloads.
    pub min_payload_bytes: usize,
    /// Largest observed request payload size.
    pub max_payload_bytes: usize,
    /// Noncumulative upper bounds 1, 2, 4, ..., 2^30, then `u32::MAX`.
    pub record_count_buckets: [u64; 32],
}

impl WriterStats {
    pub(super) fn record(&mut self, records: usize, bytes: usize) {
        if self.requests == 0 {
            self.min_records = records;
            self.min_payload_bytes = bytes;
        }
        self.requests = self.requests.saturating_add(1);
        self.records = self.records.saturating_add(records as u64);
        self.payload_bytes = self.payload_bytes.saturating_add(bytes as u64);
        self.min_records = self.min_records.min(records);
        self.max_records = self.max_records.max(records);
        self.min_payload_bytes = self.min_payload_bytes.min(bytes);
        self.max_payload_bytes = self.max_payload_bytes.max(bytes);
        let index = ((usize::BITS - records.saturating_sub(1).leading_zeros()) as usize).min(31);
        self.record_count_buckets[index] = self.record_count_buckets[index].saturating_add(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_requests_not_confirmations_and_keeps_empty_payload_minimum() {
        let mut stats = WriterStats::default();
        for (records, bytes) in [(1, 0), (3, 5), (1024, 128 * 1024)] {
            stats.record(records, bytes);
        }
        assert_eq!(
            (stats.requests, stats.records, stats.payload_bytes),
            (3, 1028, 131_077)
        );
        assert_eq!((stats.min_records, stats.max_records), (1, 1024));
        assert_eq!(
            (stats.min_payload_bytes, stats.max_payload_bytes),
            (0, 131_072)
        );
        assert_eq!(
            &stats.record_count_buckets[..11],
            &[1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1]
        );
        stats.record(u32::MAX as usize, 0);
        assert_eq!(stats.record_count_buckets[31], 1);
    }
}

/// One driver writes relaxed atomic diagnostics. Snapshots may span a request.
#[derive(Debug, Default)]
pub(super) struct Counters {
    requests: std::sync::atomic::AtomicU64,
    max_inflight_appends: std::sync::atomic::AtomicUsize,
    records: std::sync::atomic::AtomicU64,
    payload_bytes: std::sync::atomic::AtomicU64,
    min_records: std::sync::atomic::AtomicUsize,
    max_records: std::sync::atomic::AtomicUsize,
    min_payload_bytes: std::sync::atomic::AtomicUsize,
    max_payload_bytes: std::sync::atomic::AtomicUsize,
    buckets: [std::sync::atomic::AtomicU64; 32],
}
impl Counters {
    pub(super) fn snapshot(&self) -> WriterStats {
        use std::sync::atomic::Ordering::Relaxed;
        WriterStats {
            requests: self.requests.load(Relaxed),
            max_inflight_appends: self.max_inflight_appends.load(Relaxed),
            records: self.records.load(Relaxed),
            payload_bytes: self.payload_bytes.load(Relaxed),
            min_records: self.min_records.load(Relaxed),
            max_records: self.max_records.load(Relaxed),
            min_payload_bytes: self.min_payload_bytes.load(Relaxed),
            max_payload_bytes: self.max_payload_bytes.load(Relaxed),
            record_count_buckets: std::array::from_fn(|i| self.buckets[i].load(Relaxed)),
        }
    }
    pub(super) fn record(&self, records: usize, bytes: usize, inflight_appends: usize) {
        use std::sync::atomic::Ordering::Relaxed;
        let mut value = self.snapshot();
        value.record(records, bytes);
        self.max_inflight_appends
            .store(value.max_inflight_appends.max(inflight_appends), Relaxed);
        self.requests.store(value.requests, Relaxed);
        self.records.store(value.records, Relaxed);
        self.payload_bytes.store(value.payload_bytes, Relaxed);
        self.min_records.store(value.min_records, Relaxed);
        self.max_records.store(value.max_records, Relaxed);
        self.min_payload_bytes
            .store(value.min_payload_bytes, Relaxed);
        self.max_payload_bytes
            .store(value.max_payload_bytes, Relaxed);
        let index = ((usize::BITS - records.saturating_sub(1).leading_zeros()) as usize).min(31);
        self.buckets[index].store(value.record_count_buckets[index], Relaxed);
    }
}

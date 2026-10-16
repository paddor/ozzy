//! Optional process-wide local segment counters for measurement builds.
//! Updates happen per reply batch or prepared operation, never per record.
//! Snapshots include concurrent journal owners and are not a visibility fence.

use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) static JOURNAL_RECORDS: AtomicU64 = AtomicU64::new(0);
pub(crate) static BACKGROUND_RESIDENT_READS: AtomicU64 = AtomicU64::new(0);
pub(crate) static DIRECT_RESIDENT_READS: AtomicU64 = AtomicU64::new(0);
pub(crate) static HISTORICAL_READ_DELIVERIES: AtomicU64 = AtomicU64::new(0);
pub(crate) static SHARED_READER_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static COPIED_READER_BYTES: AtomicU64 = AtomicU64::new(0);
pub(crate) static PROGRESS_UPDATES: AtomicU64 = AtomicU64::new(0);
pub(crate) static PROGRESS_QUERIES: AtomicU64 = AtomicU64::new(0);
static DEDUP_INDEX_IMAGES: AtomicU64 = AtomicU64::new(0);
static DEDUP_INDEX_BYTES: AtomicU64 = AtomicU64::new(0);
static DEDUP_INDEX_ENTRIES: AtomicU64 = AtomicU64::new(0);
static DEDUP_LIVE_BYTES: AtomicU64 = AtomicU64::new(0);
static DEDUP_LIVE_ENTRIES: AtomicU64 = AtomicU64::new(0);
static PREPARED_OPERATIONS: AtomicU64 = AtomicU64::new(0);
static PREPARED_PAYLOAD_BYTES_MAX: AtomicU64 = AtomicU64::new(0);

/// Cumulative counters plus a process-lifetime operation-size maximum.
#[derive(Clone, Copy, Debug, Default)]
pub struct Snapshot {
    /// Installed dedup index images, including recovery rebuilds.
    pub dedup_index_images: u64,
    /// Sum of installed images' physical table/queue/scratch reservations.
    pub dedup_index_bytes: u64,
    /// Sum of installed images' configured identity capacities.
    pub dedup_index_entries: u64,
    /// Current reservations belonging to live installed identity images.
    pub dedup_live_bytes: u64,
    /// Configured identity capacity belonging to those live images.
    pub dedup_live_entries: u64,
    /// Decoded source progress update requests, including retries and refusals.
    pub progress_updates: u64,
    /// Decoded source progress queries, including retries and refusals.
    pub progress_queries: u64,
    /// Records copied into replies through the journal's resident or stored path.
    pub journal_records: u64,
    /// Reply batches selected from the accepted background-persistence backlog.
    pub background_resident_reads: u64,
    /// Native group replies encoded directly from shared RAM selections.
    pub direct_resident_reads: u64,
    /// Native group replies assembled by a historical reader worker.
    pub historical_read_deliveries: u64,
    /// Reader payload bytes shared with existing immutable backing.
    pub shared_reader_bytes: u64,
    /// Reader payload bytes copied into the independent transport buffer.
    pub copied_reader_bytes: u64,
    /// Local canonical APPEND operations constructed, including repeated preparation.
    pub prepared_operations: u64,
    /// Largest local canonical APPEND payload since process start, not a delta.
    pub prepared_payload_bytes_max: u64,
}

/// Read counters and maxima without stopping journal workers.
pub fn snapshot() -> Snapshot {
    Snapshot {
        dedup_index_images: DEDUP_INDEX_IMAGES.load(Ordering::Relaxed),
        dedup_index_bytes: DEDUP_INDEX_BYTES.load(Ordering::Relaxed),
        dedup_index_entries: DEDUP_INDEX_ENTRIES.load(Ordering::Relaxed),
        dedup_live_bytes: DEDUP_LIVE_BYTES.load(Ordering::Relaxed),
        dedup_live_entries: DEDUP_LIVE_ENTRIES.load(Ordering::Relaxed),
        progress_updates: PROGRESS_UPDATES.load(Ordering::Relaxed),
        progress_queries: PROGRESS_QUERIES.load(Ordering::Relaxed),
        journal_records: JOURNAL_RECORDS.load(Ordering::Relaxed),
        background_resident_reads: BACKGROUND_RESIDENT_READS.load(Ordering::Relaxed),
        direct_resident_reads: DIRECT_RESIDENT_READS.load(Ordering::Relaxed),
        historical_read_deliveries: HISTORICAL_READ_DELIVERIES.load(Ordering::Relaxed),
        shared_reader_bytes: SHARED_READER_BYTES.load(Ordering::Relaxed),
        copied_reader_bytes: COPIED_READER_BYTES.load(Ordering::Relaxed),
        prepared_operations: PREPARED_OPERATIONS.load(Ordering::Relaxed),
        prepared_payload_bytes_max: PREPARED_PAYLOAD_BYTES_MAX.load(Ordering::Relaxed),
    }
}

pub(crate) fn prepared_operation(payload_bytes: usize) {
    PREPARED_OPERATIONS.fetch_add(1, Ordering::Relaxed);
    PREPARED_PAYLOAD_BYTES_MAX.fetch_max(payload_bytes as u64, Ordering::Relaxed);
}

pub(crate) fn add(counter: &AtomicU64, records: usize) {
    counter.fetch_add(records as u64, Ordering::Relaxed);
}

pub(crate) fn dedup_image(bytes: usize, entries: usize) -> DedupReservation {
    DEDUP_INDEX_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    DEDUP_INDEX_ENTRIES.fetch_add(entries as u64, Ordering::Relaxed);
    DEDUP_INDEX_IMAGES.fetch_add(1, Ordering::Relaxed);
    DEDUP_LIVE_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
    DEDUP_LIVE_ENTRIES.fetch_add(entries as u64, Ordering::Relaxed);
    DedupReservation {
        bytes: bytes as u64,
        entries: entries as u64,
    }
}

#[derive(Debug)]
pub(crate) struct DedupReservation {
    bytes: u64,
    entries: u64,
}

impl Drop for DedupReservation {
    fn drop(&mut self) {
        DEDUP_LIVE_BYTES.fetch_sub(self.bytes, Ordering::Relaxed);
        DEDUP_LIVE_ENTRIES.fetch_sub(self.entries, Ordering::Relaxed);
    }
}

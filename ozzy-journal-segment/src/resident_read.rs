//! Captured RAM-only reads shared by synchronous and asynchronous journals.

use ozzy_journal::ReadLimits;

use crate::active_read_index::CapturedReadEntries;
use crate::{IndexSource, JournalIndexError};

/// Immutable RAM selection. Owns only selected operation references and compact
/// selectors, with no file access, cache lock, or temporary deletion protection.
#[derive(Debug)]
pub struct ResidentRecordRead {
    source: IndexSource,
    entries: CapturedReadEntries,
    records: crate::reader::ResidentOperations,
    limits: ReadLimits,
}

impl ResidentRecordRead {
    pub(crate) const fn from_captured(
        source: IndexSource,
        entries: CapturedReadEntries,
        records: crate::reader::ResidentOperations,
        limits: ReadLimits,
    ) -> Self {
        Self {
            source,
            entries,
            records,
            limits,
        }
    }

    /// Visit contiguous batch ranges. Return the consumed prefix length; a
    /// partial consumption stops delivery without widening the captured range.
    pub fn visit_spans(
        self,
        mut receive: impl FnMut(&crate::RecordSpan<'_>) -> usize,
    ) -> Result<(), JournalIndexError> {
        let mut bytes = 0;
        let mut records = 0;
        for (entry, count) in self.entries.ranges() {
            #[cfg(feature = "storage-metrics")]
            crate::read_metrics::RESIDENT_SELECTIONS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let batch = self.records.select(self.source, entry)?;
            let span = batch.span(entry, count)?;
            if records == 0 {
                let first = span.records().next().expect("nonempty captured range");
                if first.payload_bytes() > self.limits.max_bytes {
                    return Err(JournalIndexError::RecordExceedsReadLimit {
                        actual: first.payload_bytes(),
                        limit: self.limits.max_bytes,
                    });
                }
            }
            let span = span.limit(
                self.limits.max_records - records,
                self.limits.max_bytes - bytes,
            );
            if span.is_empty() {
                break;
            }
            let consumed = receive(&span);
            if consumed > span.len() {
                return Err(crate::IndexedReadError::InvalidSelector.into());
            }
            if consumed < span.len() {
                break;
            }
            records += consumed;
            bytes += span.payload_bytes();
            if consumed < count || records == self.limits.max_records {
                break;
            }
        }
        Ok(())
    }

    /// Visit shared, already validated records on the application thread.
    /// Returning false leaves the current record unconsumed.
    pub fn visit(
        self,
        mut receive: impl FnMut(&crate::RecordView<'_>) -> bool,
    ) -> Result<(), JournalIndexError> {
        let mut entries = self.entries.entries();
        let Some(mut entry) = entries.next() else {
            return Ok(());
        };
        let mut bytes = 0usize;
        let mut count = 0;
        loop {
            #[cfg(feature = "storage-metrics")]
            crate::read_metrics::RESIDENT_SELECTIONS
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let batch = self.records.select(self.source, entry)?;
            while let Some(record) = batch.record(entry)? {
                let size = record.payload_bytes();
                if count == 0 && size > self.limits.max_bytes {
                    return Err(JournalIndexError::RecordExceedsReadLimit {
                        actual: size,
                        limit: self.limits.max_bytes,
                    });
                }
                if count == self.limits.max_records || size > self.limits.max_bytes - bytes {
                    return Ok(());
                }
                if !receive(&record) {
                    return Ok(());
                }
                bytes += size;
                count += 1;
                let Some(next) = entries.next() else {
                    return Ok(());
                };
                entry = next;
            }
        }
    }
}

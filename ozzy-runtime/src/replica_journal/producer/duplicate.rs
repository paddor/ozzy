//! Check the discarded attempts in one original operation in a single pass.
//! No payload cache, mutable shared state, or unbounded proof table is created.

use super::{AppendAdmissionError, JournalError, ProducerResultSource, Retry};
use ozzy_journal::operation::{
    AppendRecordList, OperationKind, OperationLimits, decode_resolved_append, retry_fingerprint,
};
use ozzy_journal_segment::{IndexedReadError, JournalIndexError};
use ozzy_proto::append::AppendDisposition;

#[derive(Clone, Copy)]
pub(in crate::replica_journal) struct DuplicateProof {
    pub(in crate::replica_journal) timestamp: u64,
    first: u64,
    end: u64,
}

impl DuplicateProof {
    pub(in crate::replica_journal) fn contains(self, sequence: u64) -> bool {
        self.first <= sequence && sequence < self.end
    }
}

impl Retry {
    /// Verify every overlapping duplicate once, even when stored attempts
    /// separate duplicate result spans in this operation. Return its range and
    /// timestamp; neither proves the operation has crossed confirmation.
    pub(in crate::replica_journal) async fn verify_duplicates(
        &self,
        number: u64,
        kind: OperationKind,
        body: &[u8],
        expected: &AppendRecordList<'_>,
        limits: OperationLimits,
    ) -> Result<DuplicateProof, JournalError> {
        if kind != OperationKind::ResolvedAppend {
            return Err(mismatch());
        }
        let original = decode_resolved_append(body, limits)
            .map_err(IndexedReadError::from)
            .map_err(JournalIndexError::from)?;
        let batch = original.batch.summary;
        if batch.partition != self.batch.partition
            || batch.owner_epoch != self.batch.owner_epoch
            || batch.producer_id != self.batch.producer_id
            || batch.producer_epoch != self.batch.producer_epoch
        {
            return Err(mismatch());
        }
        let first = batch
            .first_sequence
            .get()
            .max(self.batch.first_sequence.get());
        let original_end = batch
            .first_sequence
            .get()
            .checked_add(u64::from(original.results.attempts()))
            .ok_or_else(mismatch)?;
        let end = original_end.min(self.batch.first_sequence.get() + self.retained as u64);
        if first >= end {
            return Err(mismatch());
        }
        let mut found = false;
        let mut work_bytes = 0usize;
        let mut work_records = 0usize;
        let attempts = original
            .results
            .attempts_with_fingerprints()
            .take((end - batch.first_sequence.get()) as usize);
        for (delta, (outcome, fingerprint)) in attempts.enumerate() {
            let sequence = batch.first_sequence.get() + delta as u64;
            work_records += 1;
            if work_records >= 64 || work_bytes >= 64 * 1024 {
                tokio::task::yield_now().await;
                work_bytes = 0;
                work_records = 0;
            }
            if sequence < first {
                continue;
            }
            let index = (sequence - self.batch.first_sequence.get()) as usize;
            let source = self.source(index)?;
            if outcome.offset != self.offset(index)? {
                return Err(mismatch());
            }
            match (source, outcome.disposition, fingerprint) {
                (ProducerResultSource::Stored, AppendDisposition::Stored, None) => {}
                (
                    ProducerResultSource::Duplicate { operation },
                    AppendDisposition::Duplicate,
                    Some(fingerprint),
                ) if operation == number => {
                    let record = expected.get(index).ok_or(JournalError::AppendMismatch)?;
                    if retry_fingerprint(record)? != fingerprint {
                        return Err(AppendAdmissionError::RetryConflict.into());
                    }
                    found = true;
                    work_bytes = work_bytes
                        .saturating_add(record.parts.len().saturating_mul(8))
                        .saturating_add(record.parts.iter().map(<[u8]>::len).sum::<usize>());
                }
                _ => return Err(mismatch()),
            }
        }
        if !found {
            return Err(mismatch());
        }
        Ok(DuplicateProof {
            timestamp: original.batch.append_timestamp_millis,
            first: batch.first_sequence.get(),
            end: original_end,
        })
    }
}

fn mismatch() -> JournalError {
    JournalIndexError::Read(IndexedReadError::RecordMismatch).into()
}

//! Reversible canonical resolution. Original producer bytes stay available
//! until admission; unique SDK payload blocks remain byte-exact.

use super::{
    AppendBuffer, Descriptor, Digest, JournalError, PreparedOperation, ProducerRetryResults,
    body::Body,
};
use ozzy_core::{
    dedup::PreparedAdmission,
    state::{ProducerResultSource, ProducerResultSpan},
};
use ozzy_journal::operation::{
    Append, AppendBatch, AppendBatchView, AppendPackScratch, AppendRecord, AppendResults,
    OperationBody, OperationKind, OperationLimits, ResolvedAppend, RetryFingerprint,
    append_operation_body_to, append_results, decode_append_batches, pack_append_payload,
    retry_fingerprint,
};
use ozzy_proto::{ProducerSequence, append::AppendDisposition};

#[derive(Debug)]
pub(in crate::replica_journal) struct Original {
    bodies: Body,
    entries: Vec<Descriptor>,
    results: Option<Box<ProducerRetryResults>>,
    prepared: Vec<PreparedOperation>,
    digests: Vec<Digest>,
    _charge: Option<crate::memory::Charge>,
}

#[derive(Debug)]
pub(in crate::replica_journal) struct Rewrite {
    bodies: Body,
    entries: Vec<Descriptor>,
    results: Option<Box<ProducerRetryResults>>,
    charge: Option<crate::memory::Charge>,
}

#[derive(Debug)]
pub(in crate::replica_journal) struct Staged<'a> {
    buffer: &'a mut AppendBuffer,
    committed: bool,
}

impl Drop for Staged<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.buffer.restore_dedup_original();
        }
    }
}

impl Staged<'_> {
    pub(in crate::replica_journal) fn buffer(&mut self) -> &mut AppendBuffer {
        self.buffer
    }
    pub(in crate::replica_journal) fn commit(mut self) {
        self.committed = true;
    }
}

/// Codec bytes independent of the batch: the state itself and its tables.
const CODEC_FIXED_BYTES: usize = size_of::<Codec>() + 2048 * size_of::<u32>() + 1024;

/// Reusable mixed/all-duplicate codec state, bounded with the identity budget.
#[derive(Debug)]
pub(in crate::replica_journal) struct Codec {
    packer: AppendPackScratch,
    bytes: Vec<u8>,
    proofs: Vec<RetryFingerprint>,
    payload_peak: usize,
    records_peak: usize,
    parts_peak: usize,
}

/// The owner's reusable codec for batches with duplicates, and the bytes it
/// may grow to within the identity budget.
#[derive(Debug)]
pub(in crate::replica_journal) struct CodecScratch<'a> {
    pub(in crate::replica_journal) codec: &'a mut Option<Box<Codec>>,
    pub(in crate::replica_journal) budget: usize,
}

impl CodecScratch<'_> {
    /// Encode a batch with its duplicates removed, creating the codec on
    /// first use if the budget allows it.
    fn encode(
        &mut self,
        batch: &AppendBatchView<'_>,
        resolution: ozzy_core::dedup::OperationAdmission<'_>,
        limits: OperationLimits,
    ) -> Result<&[u8], JournalError> {
        if self.codec.is_none() && self.budget < CODEC_FIXED_BYTES {
            return Err(JournalError::AppendCapacity);
        }
        let codec = self
            .codec
            .get_or_insert_with(|| Box::new(Codec::new(limits.max_payload_bytes)));
        codec.reserve_budget(batch, self.budget)?;
        codec.encode(batch, resolution, limits)
    }
}

impl Codec {
    pub(in crate::replica_journal) fn new(max_payload: usize) -> Self {
        Self {
            packer: AppendPackScratch::new(max_payload),
            bytes: Vec::new(),
            proofs: Vec::new(),
            payload_peak: 0,
            records_peak: 0,
            parts_peak: 0,
        }
    }

    fn reserve_budget(
        &mut self,
        batch: &AppendBatchView<'_>,
        budget: usize,
    ) -> Result<(), JournalError> {
        let (mut parts, mut payload) = (0_usize, 0_usize);
        for record in batch.descriptors() {
            for bytes in record.part_lengths {
                parts = parts.checked_add(1).ok_or(JournalError::AppendCapacity)?;
                payload = payload
                    .checked_add(bytes)
                    .ok_or(JournalError::AppendCapacity)?;
            }
        }
        let payload_peak = self.payload_peak.max(payload);
        let records_peak = self.records_peak.max(batch.summary.record_count);
        let parts_peak = self.parts_peak.max(parts);
        let bound = payload_peak
            .checked_mul(6)
            .and_then(|bytes| bytes.checked_add(records_peak.checked_mul(256)?))
            .and_then(|bytes| bytes.checked_add(parts_peak.checked_mul(64)?))
            .and_then(|bytes| bytes.checked_add(CODEC_FIXED_BYTES))
            .ok_or(JournalError::AppendCapacity)?;
        if bound > budget {
            return Err(JournalError::AppendCapacity);
        }
        self.payload_peak = payload_peak;
        self.records_peak = records_peak;
        self.parts_peak = parts_peak;
        Ok(())
    }

    fn encode(
        &mut self,
        source: &AppendBatchView<'_>,
        resolution: ozzy_core::dedup::OperationAdmission<'_>,
        limits: OperationLimits,
    ) -> Result<&[u8], JournalError> {
        let batch = source.materialize()?;
        self.proofs.clear();
        self.proofs
            .try_reserve_exact(resolution.outcomes.len())
            .map_err(std::io::Error::other)?;
        let mut stored = Vec::new();
        stored
            .try_reserve_exact(batch.records.len())
            .map_err(std::io::Error::other)?;
        for (record, outcome) in batch.records.iter().zip(resolution.outcomes) {
            match outcome.disposition {
                AppendDisposition::Stored => stored.push(AppendRecord {
                    encoding: record.encoding,
                    message_id: record.message_id,
                    parts: record.parts.iter().collect(),
                }),
                AppendDisposition::Duplicate => self.proofs.push(retry_fingerprint(record)?),
                _ => return Err(JournalError::AppendMismatch),
            }
        }
        let results = AppendResults::classified(
            resolution.logical_time,
            resolution.outcomes,
            &self.proofs,
            limits,
        )?;
        let batch = AppendBatch {
            first_offset: resolution.first_offset,
            records: stored.into(),
            ..batch
        };
        self.bytes.clear();
        let expanded = OperationLimits {
            max_body_bytes: limits
                .max_body_bytes
                .checked_add(limits.max_payload_bytes)
                .ok_or(JournalError::AppendCapacity)?,
            ..limits
        };
        if batch.records.is_empty() {
            Self::encode_body(
                &mut self.bytes,
                &OperationBody::ResolvedAppend(ResolvedAppend { batch, results }),
                expanded,
            )?;
        } else {
            Self::encode_body(
                &mut self.bytes,
                &OperationBody::Append(Append {
                    batches: vec![batch],
                }),
                expanded,
            )?;
            pack_append_payload(&mut self.bytes, expanded, &mut self.packer)?;
            append_results(&mut self.bytes, results, limits)?;
        }
        if self.bytes.len() > limits.max_body_bytes {
            return Err(JournalError::AppendCapacity);
        }
        Ok(&self.bytes)
    }

    fn encode_body(
        output: &mut Vec<u8>,
        body: &OperationBody<'_>,
        limits: OperationLimits,
    ) -> Result<(), JournalError> {
        let mut size = super::body::EncodedSize::default();
        append_operation_body_to(&mut size, body, limits)?;
        output
            .try_reserve_exact(size.0.saturating_sub(output.len()))
            .map_err(std::io::Error::other)?;
        append_operation_body_to(output, body, limits)?;
        Ok(())
    }
}

impl AppendBuffer {
    pub(in crate::replica_journal) fn producer_batches(
        &self,
        limits: OperationLimits,
    ) -> Result<smallvec::SmallVec<[AppendBatchView<'_>; 4]>, JournalError> {
        let mut batches = smallvec::SmallVec::new();
        for (operation, proof) in self.operations().zip(self.proofs()) {
            if operation.kind != OperationKind::Append {
                return Err(JournalError::AppendMismatch);
            }
            let batch = if let Some(proof) = proof {
                proof.batch(operation.body, limits)?
            } else {
                let mut decoded = decode_append_batches(operation.body, limits)?;
                match (decoded.pop(), decoded.is_empty()) {
                    (Some(batch), true) => batch,
                    _ => return Err(JournalError::AppendMismatch),
                }
            };
            batches.push(batch);
        }
        Ok(batches)
    }

    pub(in crate::replica_journal) fn resolve_dedup(
        &self,
        admission: &PreparedAdmission,
        mut codec: CodecScratch<'_>,
        limits: OperationLimits,
        charge: Option<crate::memory::Charge>,
    ) -> Result<Rewrite, JournalError> {
        let mut rewrite = self.empty_rewrite(charge)?;
        let first_operation = admission
            .operation()
            .checked_sub(self.len() as u64 - 1)
            .ok_or(JournalError::AppendMismatch)?;
        let batches = self.producer_batches(limits)?;
        for (index, ((entry, operation), batch)) in self
            .entries
            .iter()
            .zip(self.operations())
            .zip(&batches)
            .enumerate()
        {
            let number = first_operation + index as u64;
            let resolution = admission
                .operation_results(number)
                .ok_or(JournalError::AppendMismatch)?;
            let start = rewrite.bodies.len();
            if resolution
                .outcomes
                .iter()
                .all(|outcome| outcome.disposition == AppendDisposition::Stored)
            {
                self.rewrite_stored(&mut rewrite, operation.body, resolution, limits)?;
            } else {
                let body = codec.encode(batch, resolution, limits)?;
                rewrite.reserve(
                    body.len(),
                    self.limits.max_body_bytes,
                    limits.max_body_bytes,
                )?;
                rewrite.bodies.extend_from_slice(body)?;
            }
            rewrite.push_resolved(entry, start);
            if self.len() == 1 {
                rewrite.results = Some(Box::new(
                    self.resolved_result_ranges(batch, number, resolution)?,
                ));
            }
        }
        Ok(rewrite)
    }

    /// A rewrite on this buffer's allocator, with room for each entry.
    fn empty_rewrite(
        &self,
        charge: Option<crate::memory::Charge>,
    ) -> Result<Rewrite, JournalError> {
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(self.len())
            .map_err(std::io::Error::other)?;
        Ok(Rewrite {
            bodies: self
                .bodies
                .allocator()
                .map_or_else(|| Body::new(0), Body::charged),
            entries,
            results: None,
            charge,
        })
    }

    /// An all-stored batch keeps its body; its position and results follow.
    fn rewrite_stored(
        &self,
        rewrite: &mut Rewrite,
        body: &[u8],
        resolution: ozzy_core::dedup::OperationAdmission<'_>,
        limits: OperationLimits,
    ) -> Result<(), JournalError> {
        let results =
            AppendResults::classified(resolution.logical_time, resolution.outcomes, &[], limits)?;
        let mut size = super::body::EncodedSize::default();
        append_results(&mut size, results, limits)?;
        let bytes = body
            .len()
            .checked_add(size.0)
            .ok_or(JournalError::AppendCapacity)?;
        rewrite.reserve(bytes, self.limits.max_body_bytes, limits.max_body_bytes)?;
        let start = rewrite.bodies.len();
        let target = rewrite.bodies.mutable()?;
        target.extend_from_slice(body);
        // Same fixed position field patched by producer assignment.
        target[start + 60..start + 68]
            .copy_from_slice(&resolution.first_offset.get().to_be_bytes());
        append_results(target, results, limits)?;
        Ok(())
    }

    fn resolved_result_ranges(
        &self,
        batch: &AppendBatchView<'_>,
        operation: u64,
        resolution: ozzy_core::dedup::OperationAdmission<'_>,
    ) -> Result<ProducerRetryResults, JournalError> {
        let mut ranges = smallvec::SmallVec::new();
        let first = batch.summary.first_sequence.get();
        if let Some(previous) = &self.retry_results {
            for span in &previous.ranges {
                if span.first_sequence.get() >= first {
                    break;
                }
                let span = ProducerResultSpan {
                    records: span.records.min(first - span.first_sequence.get()),
                    ..*span
                };
                crate::replica_journal::producer::push_result(&mut ranges, span);
            }
        }
        let mut sequence = first;
        for run in ozzy_proto::append::OutcomeRuns::new(resolution.outcomes) {
            let source = match run.disposition {
                AppendDisposition::Stored => ProducerResultSource::Stored,
                AppendDisposition::Duplicate => ProducerResultSource::Duplicate { operation },
                _ => return Err(JournalError::AppendMismatch),
            };
            crate::replica_journal::producer::push_result(
                &mut ranges,
                ProducerResultSpan {
                    first_sequence: ProducerSequence::new(sequence),
                    first_offset: run.first_offset,
                    records: u64::from(run.records),
                    source,
                },
            );
            sequence = sequence
                .checked_add(u64::from(run.records))
                .ok_or(JournalError::AppendMismatch)?;
        }
        Ok(ProducerRetryResults {
            ranges,
            trimmed: self
                .retry_results
                .as_ref()
                .is_some_and(|results| results.trimmed),
        })
    }

    pub(in crate::replica_journal) fn stage_dedup(&mut self, rewrite: Rewrite) -> Staged<'_> {
        if self.dedup_original.is_none() {
            self.dedup_original = Some(Box::new(Original {
                bodies: std::mem::replace(&mut self.bodies, rewrite.bodies),
                entries: std::mem::replace(&mut self.entries, rewrite.entries),
                results: std::mem::replace(&mut self.retry_results, rewrite.results),
                prepared: std::mem::take(&mut self.prepared),
                digests: std::mem::take(&mut self.body_digests),
                _charge: None,
            }));
        } else {
            // A streaming retry saved its full input before trimming. Replace
            // only the private working suffix, preserving that earlier backup.
            self.bodies = rewrite.bodies;
            self.entries = rewrite.entries;
            self.retry_results = rewrite.results;
            self.prepared.clear();
            self.body_digests.clear();
        }
        self.dedup_charge = rewrite.charge;
        self.validated = false;
        Staged {
            buffer: self,
            committed: false,
        }
    }

    pub(in crate::replica_journal) fn preserve_dedup_original(
        &mut self,
        charge: Option<crate::memory::Charge>,
    ) -> Result<(), JournalError> {
        if self.dedup_original.is_some() || self.dedup_charge.is_some() {
            return Err(JournalError::AppendMismatch);
        }
        let mut bodies = self
            .bodies
            .allocator()
            .map_or_else(|| Body::new(0), Body::charged);
        bodies.extend_from_slice(&self.bodies)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(self.len())
            .map_err(std::io::Error::other)?;
        entries.extend(self.entries.iter().cloned());
        self.dedup_original = Some(Box::new(Original {
            bodies: std::mem::replace(&mut self.bodies, bodies),
            entries: std::mem::replace(&mut self.entries, entries),
            results: self.retry_results.take(),
            prepared: std::mem::take(&mut self.prepared),
            digests: std::mem::take(&mut self.body_digests),
            _charge: charge,
        }));
        self.validated = false;
        Ok(())
    }

    pub(in crate::replica_journal) fn restore_dedup_original(&mut self) {
        if let Some(previous) = self.dedup_original.take() {
            self.bodies = previous.bodies;
            self.entries = previous.entries;
            self.retry_results = previous.results;
            self.prepared = previous.prepared;
            self.body_digests = previous.digests;
            self.dedup_charge = None;
            self.validated = false;
        }
    }
}

impl Rewrite {
    fn reserve(
        &mut self,
        bytes: usize,
        arena: usize,
        operation: usize,
    ) -> Result<(), JournalError> {
        if bytes > operation || bytes > arena.saturating_sub(self.bodies.len()) {
            return Err(JournalError::AppendCapacity);
        }
        self.bodies.reserve(bytes)
    }

    /// Describe the resolved body written since `start` for `entry`.
    fn push_resolved(&mut self, entry: &Descriptor, start: usize) {
        self.entries.push(Descriptor {
            envelope: ozzy_journal::operation::CanonicalOperation {
                kind: OperationKind::ResolvedAppend,
                ..entry.envelope
            },
            body: start..self.bodies.len(),
            digest: None,
            proof: None,
            primary_payload_validated: true,
        });
    }
}

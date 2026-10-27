//! One validated record table shared by the pending log and resident read index.

use super::{
    Arc, Bytes, CachedBatch, CachedOperation, DecodedBatches, IndexedReadError, RecordView,
    cache_batch, cache_resolved_batch,
};
use crate::OperationLocation;
use ozzy_journal::operation::{
    AppendSummary, OperationHeader, OperationKind, OperationLimits, ResolvedAppendView,
    ValidatedWireAppend, decode_append_summary_and_batches,
    decode_append_summary_and_batches_with_validated_payload, decode_release_batch,
    decode_resolved_append, validate_operation_body,
};
use ozzy_proto::{Offset, PartitionIncarnation};

/// Immutable canonical bytes and compact record selectors. Construction validates
/// syntax, not confirmation or persistence. Owners supply those boundaries.
/// Clones share both payload storage and the validated selector tables.
#[derive(Debug, Clone)]
pub struct PreparedOperationRecords {
    header: OperationHeader,
    body: Bytes,
    shared_backing_bytes: Option<usize>,
    batches: Arc<[CachedBatch]>,
}

impl PreparedOperationRecords {
    /// Resolve retained ID/time metadata without decoding or copying payloads.
    pub async fn observe_seek(
        &self,
        query: crate::SeekQuery,
        result: &mut ozzy_core::reader::seek::Selection,
    ) {
        if self.header.op_number > query.through {
            return;
        }
        let mut budget = crate::cooperative::Budget::default();
        for batch in self
            .batches
            .iter()
            .filter(|batch| batch.summary.partition == query.partition)
        {
            let first = batch
                .summary
                .first_offset
                .get()
                .max(query.retained_from.get());
            let end = (batch.summary.first_offset.get() + batch.summary.record_count as u64)
                .min(query.confirmed_end.get());
            match query.start {
                ozzy_proto::reader::Start::Timestamp(timestamp)
                    if batch.timestamp_millis >= timestamp && first < end =>
                {
                    result.observe_match(Offset::new(first));
                }
                ozzy_proto::reader::Start::RecordId { id, .. } => {
                    for offset in first..end {
                        let index = (offset - batch.summary.first_offset.get()) as usize;
                        if batch
                            .records
                            .get(index, &self.body)
                            .is_some_and(|record| record.message_id == id)
                        {
                            result.observe_match(Offset::new(offset));
                        }
                        budget.charge(64).await;
                    }
                }
                _ => {}
            }
            budget.charge(64).await;
        }
    }

    pub(crate) fn matches_location(
        &self,
        group: ozzy_proto::GroupId,
        location: OperationLocation,
    ) -> bool {
        self.header.group_id == group && self.header.op_number == location.op_number
    }
    /// Read-owned decompression cells for this operation's views. They drop with
    /// the read; these shared records never retain decompressed payloads.
    pub fn decoded_batches(&self) -> DecodedBatches {
        DecodedBatches::for_batches(&self.batches)
    }

    /// Reuse `decoded` when it already belongs to this operation.
    pub fn reuse_decoded(&self, decoded: &mut DecodedBatches) {
        if decoded.owner != self.batches.as_ptr() as usize
            || decoded.cells.len() != self.batches.len()
        {
            *decoded = self.decoded_batches();
        }
    }

    /// Selected batch ranges without expanding them into individual record views.
    pub fn spans<'a>(
        &'a self,
        partition: PartitionIncarnation,
        start: Offset,
        end: Offset,
        decoded: &'a DecodedBatches,
    ) -> impl Iterator<Item = super::RecordSpan<'a>> {
        self.batches
            .iter()
            .enumerate()
            .filter_map(move |(index, batch)| {
                if batch.summary.partition != partition {
                    return None;
                }
                let first = batch.summary.first_offset.get();
                let from = start
                    .get()
                    .saturating_sub(first)
                    .min(batch.records.len() as u64) as usize;
                let to = end
                    .get()
                    .saturating_sub(first)
                    .min(batch.records.len() as u64) as usize;
                (from < to).then(|| super::RecordSpan {
                    body: &self.body,
                    shared_backing_bytes: self.shared_backing_bytes,
                    batch,
                    decoded: decoded.cell(&self.batches, index),
                    range: from..to,
                })
            })
    }

    /// Validate an immutable canonical APPEND and capture its record descriptors.
    pub fn new(
        header: OperationHeader,
        body: Bytes,
        limits: OperationLimits,
    ) -> Result<Self, IndexedReadError> {
        if header.kind == OperationKind::Append {
            return Self::new_append(header, body, limits).map(|(_, records)| records);
        }
        if header.kind == OperationKind::ResolvedAppend {
            let view = decode_resolved_append(&body, limits)?;
            return Self::new_resolved_append(header, body.clone(), &view);
        }
        if header.kind == OperationKind::Release {
            let batch = cache_batch(&body, &decode_release_batch(&body, limits)?)?;
            return Ok(Self {
                header,
                body,
                shared_backing_bytes: None,
                batches: vec![batch].into(),
            });
        }
        validate_operation_body(header.kind, &body, limits)?;
        Ok(Self {
            header,
            body,
            shared_backing_bytes: None,
            batches: Arc::from([]),
        })
    }

    /// Capture selectors from a resolved view already validated for these exact
    /// immutable body bytes. Outcome metadata stays in the body, outside the
    /// resident record table. Boundaries are checked against its shared backing.
    pub fn new_resolved_append(
        header: OperationHeader,
        body: Bytes,
        view: &ResolvedAppendView<'_>,
    ) -> Result<Self, IndexedReadError> {
        if header.kind != OperationKind::ResolvedAppend {
            return Err(IndexedReadError::NotAppend);
        }
        let batches = cache_resolved_batch(&body, view)?
            .into_iter()
            .collect::<Vec<_>>();
        Ok(Self {
            header,
            body,
            shared_backing_bytes: None,
            batches: batches.into(),
        })
    }

    /// Validate one Append once and build both its state summary and resident
    /// record selectors from the same borrowed batch views.
    pub fn new_append(
        header: OperationHeader,
        body: Bytes,
        limits: OperationLimits,
    ) -> Result<(AppendSummary, Self), IndexedReadError> {
        Self::new_append_inner::<true>(header, body, limits)
    }

    /// Build resident selectors after the whole-payload codec was already
    /// validated on the application thread or produced by the local encoder.
    pub fn new_append_with_validated_payload(
        header: OperationHeader,
        body: Bytes,
        limits: OperationLimits,
    ) -> Result<(AppendSummary, Self), IndexedReadError> {
        Self::new_append_inner::<false>(header, body, limits)
    }

    /// Build selectors directly from the proof emitted while canonicalizing a
    /// validated producer request. Descriptor iteration happens only in the
    /// selector builder; the codec and schema are not walked again. Check the
    /// destination's limits and patched positions before building selectors.
    pub fn new_validated_wire_append(
        header: OperationHeader,
        body: Bytes,
        proof: ValidatedWireAppend,
        limits: OperationLimits,
    ) -> Result<(AppendSummary, Self), IndexedReadError> {
        if header.kind != OperationKind::Append {
            return Err(IndexedReadError::NotAppend);
        }
        let batch = proof.batch(&body, limits)?;
        let summary = AppendSummary::single(batch.summary);
        let cached = cache_batch(&body, &batch)?;
        Ok((
            summary,
            Self {
                header,
                body,
                shared_backing_bytes: None,
                batches: vec![cached].into(),
            },
        ))
    }

    fn new_append_inner<const VALIDATE_PREPARED: bool>(
        header: OperationHeader,
        body: Bytes,
        limits: OperationLimits,
    ) -> Result<(AppendSummary, Self), IndexedReadError> {
        if header.kind != OperationKind::Append {
            return Err(IndexedReadError::NotAppend);
        }
        let (summary, batches) = if VALIDATE_PREPARED {
            decode_append_summary_and_batches(&body, limits)?
        } else {
            decode_append_summary_and_batches_with_validated_payload(&body, limits)?
        };
        let cached = batches
            .iter()
            .map(|batch| cache_batch(&body, batch))
            .collect::<Result<Vec<_>, _>>()?;
        drop(batches);
        Ok((
            summary,
            Self {
                header,
                body,
                shared_backing_bytes: None,
                batches: cached.into(),
            },
        ))
    }

    /// Record a known upper bound on the complete backing allocation, not just
    /// this body's slice length. The caller must own that allocation accounting.
    /// Without this bound, reader transport copies into its reserved buffer.
    #[must_use]
    pub fn with_shared_backing_bytes(mut self, bytes: usize) -> Self {
        assert!(
            bytes >= self.body.len(),
            "backing must cover the body slice"
        );
        self.shared_backing_bytes = Some(bytes);
        self
    }

    pub(crate) fn bind(&self, location: OperationLocation) -> Option<Arc<CachedOperation>> {
        matches!(
            self.header.kind,
            OperationKind::Append | OperationKind::ResolvedAppend | OperationKind::Release
        )
        .then(|| {
            Arc::new(CachedOperation {
                location,
                body: self.body.clone(),
                shared_backing_bytes: self.shared_backing_bytes,
                batches: Arc::clone(&self.batches),
            })
        })
    }

    /// Whether one of this operation's batches holds `offset` of `partition`.
    pub fn contains(&self, partition: PartitionIncarnation, offset: Offset) -> bool {
        self.batches.iter().any(|batch| {
            batch.summary.partition == partition
                && offset
                    .get()
                    .checked_sub(batch.summary.first_offset.get())
                    .is_some_and(|index| index < batch.records.len() as u64)
        })
    }

    /// First partition-global offset in this selected record span.
    pub fn first_offset(&self, partition: PartitionIncarnation) -> Option<Offset> {
        self.batches
            .iter()
            .find(|batch| batch.summary.partition == partition)
            .map(|batch| batch.summary.first_offset)
    }

    /// Whether every APPEND batch already stores a whole-payload codec block.
    pub fn all_payloads_prepared(&self) -> bool {
        !self.batches.is_empty() && self.batches.iter().all(|batch| batch.prepared.is_some())
    }

    /// Borrow one exact record from validated canonical operation backing.
    pub fn record<'a>(
        &'a self,
        partition: PartitionIncarnation,
        offset: Offset,
        decoded: &'a DecodedBatches,
    ) -> Option<RecordView<'a>> {
        self.batches
            .iter()
            .enumerate()
            .find_map(|(batch_index, batch)| {
                if batch.summary.partition != partition {
                    return None;
                }
                let index = offset.get().checked_sub(batch.summary.first_offset.get())?;
                let record = batch
                    .records
                    .get(usize::try_from(index).ok()?, &self.body)?;
                Some(RecordView {
                    body: &self.body,
                    shared_backing_bytes: self.shared_backing_bytes,
                    batch,
                    decoded: decoded.cell(&self.batches, batch_index),
                    record,
                    index,
                })
            })
    }

    /// Seek by batch bounds, then visit only the selected record range. This
    /// constructs no owning frames and never walks earlier record descriptors.
    pub fn records<'a>(
        &'a self,
        partition: PartitionIncarnation,
        start: Offset,
        end: Offset,
        decoded: &'a DecodedBatches,
    ) -> impl Iterator<Item = RecordView<'a>> {
        self.batches
            .iter()
            .enumerate()
            .filter(move |(_, batch)| batch.summary.partition == partition)
            .flat_map(move |(batch_index, batch)| {
                let cell = decoded.cell(&self.batches, batch_index);
                let first = batch.summary.first_offset.get();
                let from = start
                    .get()
                    .saturating_sub(first)
                    .min(batch.records.len() as u64) as usize;
                let to = end
                    .get()
                    .saturating_sub(first)
                    .min(batch.records.len() as u64) as usize;
                (from..to.max(from)).map(move |index| RecordView {
                    body: &self.body,
                    shared_backing_bytes: self.shared_backing_bytes,
                    batch,
                    decoded: cell,
                    record: batch.records.record(index, &self.body),
                    index: index as u64,
                })
            })
    }
}

impl crate::active_read_index::ReadIndexBody for &PreparedOperationRecords {
    fn kind(&self) -> OperationKind {
        self.header.kind
    }

    fn batches(&self) -> impl Iterator<Item = crate::active_read_index::ReadBatch> {
        self.batches
            .iter()
            .map(|batch| crate::active_read_index::ReadBatch {
                partition: batch.summary.partition,
                first_offset: batch.summary.first_offset,
                records: batch.summary.record_count,
                append_timestamp_millis: batch.timestamp_millis,
            })
    }
}

#[cfg(test)]
mod tests;

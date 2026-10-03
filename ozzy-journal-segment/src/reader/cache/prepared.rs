//! One validated record table shared by the pending log and resident read index.

use super::{
    Arc, Bytes, CachedBatch, CachedOperation, DecodedBatches, IndexedReadError, RecordView,
    cache_batch,
};
use crate::OperationLocation;
use ozzy_journal::operation::{
    AppendSummary, OperationHeader, OperationKind, OperationLimits, ValidatedWireAppend,
    decode_append_summary_and_batches, decode_append_summary_and_batches_with_validated_payload,
    validate_operation_body,
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

    pub fn new(
        header: OperationHeader,
        body: Bytes,
        limits: OperationLimits,
    ) -> Result<Self, IndexedReadError> {
        if header.kind == OperationKind::Append {
            return Self::new_append(header, body, limits).map(|(_, records)| records);
        }
        validate_operation_body(header.kind, &body, limits)?;
        Ok(Self {
            header,
            body,
            shared_backing_bytes: None,
            batches: Arc::from([]),
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
    /// selector builder; the codec and schema are not walked again.
    pub fn new_validated_wire_append(
        header: OperationHeader,
        body: Bytes,
        proof: ValidatedWireAppend,
    ) -> Result<(AppendSummary, Self), IndexedReadError> {
        if header.kind != OperationKind::Append {
            return Err(IndexedReadError::NotAppend);
        }
        let batch = proof.batch(&body)?;
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
        (self.header.kind == OperationKind::Append).then(|| {
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
                let record = batch.records.get(usize::try_from(index).ok()?)?;
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
                    record: batch.records.get(index).expect("bounded record range"),
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
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_journal::operation::Digest;
    use ozzy_proto::GroupId;

    fn header() -> OperationHeader {
        OperationHeader {
            group_id: GroupId::from_bytes([1; 16]),
            configuration_epoch: 1,
            original_view: 0,
            op_number: 1,
            previous_digest: Digest::ZERO,
            kind: OperationKind::Append,
        }
    }

    #[test]
    fn prepared_reads_share_bytes_and_tables_through_binding_and_source_drop() {
        let body = super::super::tests::body(40, 80);
        let records =
            PreparedOperationRecords::new(header(), body.clone(), OperationLimits::default())
                .unwrap();
        let partition = records.batches[0].summary.partition;
        let decoded = records.decoded_batches();
        for start in 39..44 {
            for end in start..44 {
                let actual = records
                    .records(partition, Offset::new(start), Offset::new(end), &decoded)
                    .map(|record| record.offset().get())
                    .collect::<Vec<_>>();
                assert_eq!(
                    actual,
                    (start..end)
                        .filter(|offset| (40..42).contains(offset))
                        .collect::<Vec<_>>()
                );
            }
        }
        let location = OperationLocation {
            segment_id: 1,
            entry_offset: 4096,
            entry_bytes: 4096,
            op_number: 1,
            operation_digest: Digest::ZERO,
        };
        let cached = records.bind(location).unwrap();
        assert_eq!(cached.body.as_ptr(), body.as_ptr());
        assert!(Arc::ptr_eq(&cached.batches, &records.batches));
        let delivered = records
            .record(partition, Offset::new(41), &decoded)
            .unwrap()
            .materialize();
        assert_eq!(delivered.producer_sequence.get(), 81);
        assert_eq!(delivered.parts[0].len(), 0);
        let borrowed = records
            .record(partition, Offset::new(41), &decoded)
            .unwrap();
        assert_eq!(
            delivered.parts[1].as_ptr(),
            borrowed.parts().nth(1).unwrap().as_ptr()
        );
        drop((decoded, records, body));
        assert_eq!(delivered.parts[1], b"payload".as_slice());
        assert_eq!(cached.batches[0].records.len(), 2);
    }

    #[test]
    fn shared_readers_account_for_full_backing_instead_of_body_slice() {
        let body = super::super::tests::body(40, 80);
        let mut arena = Vec::with_capacity(body.len() + 8192);
        arena.extend_from_slice(&body);
        let capacity = arena.capacity();
        let body = Bytes::from(arena).slice(..body.len());
        let records =
            PreparedOperationRecords::new(header(), body, OperationLimits::default()).unwrap();
        let partition = records.batches[0].summary.partition;
        let decoded = records.decoded_batches();
        assert!(
            records
                .record(partition, Offset::new(40), &decoded)
                .unwrap()
                .payload_backing(usize::MAX)
                .is_none()
        );
        let records = records.with_shared_backing_bytes(capacity);
        let cloned = records.clone();
        drop(records);
        for record in cloned.records(partition, Offset::new(40), Offset::new(42), &decoded) {
            assert!(record.payload_backing(capacity - 1).is_none());
            assert!(record.payload_backing(capacity).is_some());
        }
        let cached = cloned
            .bind(OperationLocation {
                segment_id: 1,
                entry_offset: 4096,
                entry_bytes: 4096,
                op_number: 1,
                operation_digest: Digest::ZERO,
            })
            .unwrap();
        assert_eq!(cached.shared_backing_bytes, Some(capacity));
        assert!(cached.body.len() < capacity);
    }

    #[test]
    fn preparation_rejects_invalid_bounds_and_trailing_bytes() {
        let body = super::super::tests::body(0, 0);
        let limits = OperationLimits {
            max_records: 1,
            ..OperationLimits::default()
        };
        assert!(PreparedOperationRecords::new(header(), body.clone(), limits).is_err());
        let mut extra = body.to_vec();
        extra.push(0);
        assert!(
            PreparedOperationRecords::new(header(), extra.into(), OperationLimits::default())
                .is_err()
        );
        assert!(
            PreparedOperationRecords::new(
                header(),
                body.slice(..body.len() - 1),
                OperationLimits::default()
            )
            .is_err()
        );
    }
}

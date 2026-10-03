//! Pure repair bounds, fragment salvage and donor-range validation.
//! Shared by synchronous workers and backend-neutral asynchronous repair.

use super::{DirectoryError, RepairRange, SealedRepairLimits};
use crate::directory::validate_operation_bodies;
use crate::{
    CanonicalOperation, DecodeLimits, Digest, LogPosition, Manifest, OperationLimits,
    SegmentHeader, SegmentReference,
};
use crate::{TailState, scan_segment};
use ozzy_journal::operation::{logical_operation_digest, validate_operation_body};

#[derive(Debug)]
pub(crate) struct State {
    pub(crate) ranges: Vec<RepairRange>,
    pub(crate) next: usize,
    pub(crate) view: u64,
    pub(crate) decode: DecodeLimits,
    pub(crate) operations: OperationLimits,
    pub(crate) limits: SealedRepairLimits,
    pub(crate) source: Vec<Option<crate::DecodedOperation<'static>>>,
    pub(crate) source_bytes: usize,
    pub(crate) missing: usize,
    pub(crate) missing_end: usize,
}

impl State {
    pub(crate) fn validate_limits(
        decode: DecodeLimits,
        limits: SealedRepairLimits,
    ) -> Result<(), DirectoryError> {
        if limits.max_chunk_operations == 0
            || limits.max_chunk_operations > decode.max_entries
            || limits.max_chunk_body_bytes == 0
            || limits.max_chunk_body_bytes > decode.max_group_decoded_body_bytes
            || limits.max_orphan_probes == 0
            || !limits.body_encoding.is_supported()
        {
            return Err(DirectoryError::ConfigurationMismatch);
        }
        Ok(())
    }

    pub(crate) fn new(
        manifest: &Manifest,
        ranges: Vec<RepairRange>,
        view: u64,
        source_accepted: LogPosition,
        decode: DecodeLimits,
        operations: OperationLimits,
        limits: SealedRepairLimits,
    ) -> Result<Self, DirectoryError> {
        Self::validate_limits(decode, limits)?;
        for range in &ranges {
            if range.through.op_number > source_accepted.op_number
                || (range.through.op_number == source_accepted.op_number
                    && range.through != source_accepted)
            {
                return Err(DirectoryError::CurrentMismatch);
            }
        }
        Ok(Self {
            ranges,
            next: 0,
            view: view.max(manifest.promised_view),
            decode,
            operations,
            limits,
            source: Vec::new(),
            source_bytes: 0,
            missing: 0,
            missing_end: 0,
        })
    }
    pub(crate) fn pending(&self) -> Option<RepairRange> {
        let mut range = self.ranges.get(self.next).copied()?;
        let first = self.missing;
        let end = self.missing_end;
        if first > 0 {
            let previous = self.source[first - 1]
                .as_ref()
                .expect("preceding valid operation");
            range.after = LogPosition {
                op_number: previous.op_number,
                digest: previous.digest,
            };
        }
        if end < self.source.len() {
            let next = self.source[end]
                .as_ref()
                .expect("following valid operation");
            range.through = LogPosition {
                op_number: next.op_number - 1,
                digest: next.previous_digest,
            };
        }
        Some(range)
    }

    pub(crate) fn append(
        &mut self,
        manifest: &crate::Manifest,
        operations: &[CanonicalOperation<'_>],
    ) -> Result<(), DirectoryError> {
        let range = self.pending().ok_or(DirectoryError::CurrentMismatch)?;
        if operations.is_empty() || operations.len() > self.limits.max_chunk_operations {
            return Err(DirectoryError::CurrentMismatch);
        }
        let mut after = range.after;
        let mut bytes = 0usize;
        for operation in operations {
            bytes = bytes
                .checked_add(operation.body.len())
                .ok_or(DirectoryError::CurrentMismatch)?;
            if bytes > self.limits.max_chunk_body_bytes
                || operation.body.len() > self.decode.max_decoded_body_bytes
                || operation.group_id != manifest.identity.group_id
                || operation.configuration_epoch != manifest.configuration_epoch
                || operation.original_view > self.view
                || Some(operation.op_number) != after.op_number.checked_add(1)
                || operation.previous_digest != after.digest
                || operation.op_number > range.through.op_number
            {
                return Err(DirectoryError::CurrentMismatch);
            }
            validate_operation_body(operation.kind, operation.body, self.operations)?;
            after = LogPosition {
                op_number: operation.op_number,
                digest: logical_operation_digest(operation),
            };
        }
        if after.op_number == range.through.op_number && after != range.through {
            return Err(DirectoryError::CurrentMismatch);
        }
        if bytes
            > self
                .decode
                .max_segment_decoded_body_bytes
                .saturating_sub(self.source_bytes)
        {
            return Err(DirectoryError::SegmentMismatch(range.segment_id));
        }
        let first = self.ranges[self.next].after.op_number + 1;
        for operation in operations {
            let body_digest = ozzy_journal::operation::canonical_body_digest(operation.body);
            self.source[(operation.op_number - first) as usize] = Some(crate::DecodedOperation {
                entry_offset: 0,
                entry_bytes: 0,
                group_id: operation.group_id,
                configuration_epoch: operation.configuration_epoch,
                original_view: operation.original_view,
                op_number: operation.op_number,
                previous_digest: operation.previous_digest,
                digest: ozzy_journal::operation::logical_operation_digest_with_body_digest(
                    operation,
                    body_digest,
                ),
                body_digest,
                kind: operation.kind,
                body: std::borrow::Cow::Owned(operation.body.to_vec()),
            });
        }
        self.source_bytes += bytes;
        Ok(())
    }

    pub(crate) fn prepare(
        &mut self,
        range: RepairRange,
        reference: SegmentReference,
    ) -> Result<(), DirectoryError> {
        let count = usize::try_from(range.through.op_number - range.after.op_number)
            .map_err(|_| DirectoryError::SegmentMismatch(range.segment_id))?;
        if count as u64 > reference.capacity / crate::ENTRY_HEADER_BYTES as u64 {
            return Err(DirectoryError::SegmentMismatch(range.segment_id));
        }
        self.source.resize_with(count, || None);
        Ok(())
    }

    pub(crate) fn load(
        &mut self,
        manifest: &Manifest,
        range: RepairRange,
        reference: SegmentReference,
        image: &[u8],
    ) -> Result<(), DirectoryError> {
        let header = SegmentHeader::new(
            manifest.identity.group_id,
            reference.segment_id,
            None,
            Digest::ZERO,
            reference.capacity,
        )?;
        let entries = crate::codec::salvage_entries(
            &header,
            image,
            range.after.op_number + 1,
            range.through.op_number,
            self.decode,
        )?;
        for entry in entries {
            if entry.configuration_epoch != manifest.configuration_epoch
                || entry.original_view > manifest.promised_view
                || validate_operation_body(entry.kind, &entry.body, self.operations).is_err()
            {
                continue;
            }
            let slot = &mut self.source[(entry.op_number - range.after.op_number - 1) as usize];
            if slot.is_some() {
                self.discard_local();
                return Ok(());
            }
            self.source_bytes += entry.body.len();
            *slot = Some(entry);
        }
        // Disagreeing independently valid fragments are ambiguous. Ask the donor
        // for the whole range instead of choosing one possible local history.
        let mut previous = Some(range.after.digest);
        for entry in &self.source {
            if let Some(entry) = entry {
                if previous.is_some_and(|digest| digest != entry.previous_digest) {
                    self.discard_local();
                    return Ok(());
                }
                previous = Some(entry.digest);
            } else {
                previous = None;
            }
        }
        if previous.is_some_and(|digest| digest != range.through.digest) {
            self.discard_local();
        }
        Ok(())
    }

    fn discard_local(&mut self) {
        self.source.iter_mut().for_each(|slot| *slot = None);
        self.source_bytes = 0;
    }
}

pub(crate) fn validate_image(
    manifest: &Manifest,
    index: usize,
    image: &[u8],
    protected: LogPosition,
    decode: DecodeLimits,
    operations: OperationLimits,
) -> Result<(), DirectoryError> {
    let reference = manifest.segments[index];
    let scan = scan_segment(
        image,
        reference.first_group_number,
        reference.first_chain,
        decode,
    )?;
    if scan.header.group_id() != manifest.identity.group_id
        || scan.header.segment_id() != reference.segment_id
        || scan.header.file_generation() != reference.file_generation
        || scan.header.capacity() != reference.capacity
        || (index > 0
            && (scan.header.predecessor_segment_id()
                != Some(manifest.segments[index - 1].segment_id)
                || scan.header.predecessor_digest() != reference.first_chain.previous_digest()))
    {
        return Err(DirectoryError::SegmentMismatch(reference.segment_id));
    }
    validate_operation_bodies(
        &scan,
        operations,
        manifest.configuration_epoch,
        manifest.promised_view,
    )?;
    if let Some(sealed) = reference.sealed {
        if scan.valid_bytes != sealed.valid_bytes
            || scan.digest != sealed.digest
            || scan.next_chain != manifest.segments[index + 1].first_chain
            || matches!(scan.tail, TailState::Truncated { .. })
        {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id));
        }
    } else {
        for protected in [protected, manifest.committed] {
            if protected.op_number >= reference.first_chain.next_op_number()
                && !crate::directory::scan_contains_position(
                    &scan,
                    reference.first_chain,
                    protected.following_chain()?,
                )
            {
                return Err(DirectoryError::SegmentMismatch(reference.segment_id));
            }
        }
    }
    Ok(())
}

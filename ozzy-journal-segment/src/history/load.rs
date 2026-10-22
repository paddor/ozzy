//! Segment loading: one checked full scan, or verified extension of a loaded
//! prefix by newly sealed groups.

use super::{HistoryError, HistoryState, IndexedOperation, LoadedSegment, Source, before};
use crate::codec::SegmentDigestBuilder;
use crate::{
    CodecError, DecodedOperation, LogPosition, SegmentScan, TailState, decode_group, scan_segment,
};

impl<S: Source> HistoryState<S> {
    pub(super) fn index_segment(
        &self,
        index: usize,
        entries: &mut Vec<IndexedOperation>,
    ) -> Result<LoadedSegment, HistoryError> {
        let scan = self.checked_scan(index)?;
        let mut digest = SegmentDigestBuilder::new(&scan.header);
        for group in &scan.groups {
            digest.push(group.digest);
        }
        for operation in scan.groups.iter().flat_map(|group| &group.operations) {
            push_indexed(entries, self.entry_limit, operation)?;
        }
        Ok(LoadedSegment {
            index,
            header: scan.header,
            valid_bytes: scan.valid_bytes,
            next_group_number: scan.next_group_number,
            next_chain: scan.next_chain,
            digest,
            groups: scan.groups.len(),
            decoded_body_bytes: scan.decoded_body_bytes,
        })
    }

    /// Extend a previously checked, immutable prefix from a newer captured
    /// suffix. The old bytes and group digest state remain the trust anchor;
    /// every new group still gets physical and canonical validation.
    pub(super) fn extend_loaded(&mut self, index: usize) -> Result<(), HistoryError> {
        let mut loaded = self.loaded.take().ok_or(HistoryError::Source)?;
        if loaded.index != index || loaded.valid_bytes as usize > self.bytes.len() {
            return Err(HistoryError::Source);
        }
        let mut offset = loaded.valid_bytes as usize;
        while offset < self.bytes.len() {
            offset = self.extend_group(&mut loaded, offset)?;
        }
        if offset as u64 != self.seal(index).valid_bytes
            || loaded.digest.finish() != self.seal(index).digest
            || before(loaded.next_chain) != self.segment_end(index)
        {
            return Err(HistoryError::Source);
        }
        loaded.valid_bytes = offset as u64;
        self.loaded = Some(loaded);
        Ok(())
    }

    /// Decode, check, and index the group at `offset`; return its end.
    fn extend_group(
        &mut self,
        loaded: &mut LoadedSegment,
        offset: usize,
    ) -> Result<usize, HistoryError> {
        if loaded.groups == self.decode.max_groups {
            return Err(CodecError::LimitExceeded {
                kind: "physical group count",
                actual: loaded.groups + 1,
                limit: self.decode.max_groups,
            }
            .into());
        }
        let group = decode_group(
            &loaded.header,
            crate::GroupStart::new(loaded.next_group_number, offset as u64, loaded.next_chain),
            &self.bytes[offset..],
            self.decode,
        )?;
        for operation in &group.operations {
            self.admit_extended(loaded, operation)?;
            push_indexed(&mut self.entries, self.entry_limit, operation)?;
        }
        let end = usize::try_from(group.end_offset).map_err(|_| HistoryError::Capacity)?;
        loaded.next_group_number = loaded
            .next_group_number
            .checked_add(1)
            .ok_or(CodecError::InvalidGroupNumber)?;
        loaded.next_chain = group.next_chain;
        loaded.digest.push(group.digest);
        loaded.groups += 1;
        Ok(end)
    }

    /// Admit one new operation as `check_operation` does. Its body is charged
    /// to the segment decoded-body limit.
    fn admit_extended(
        &self,
        loaded: &mut LoadedSegment,
        operation: &DecodedOperation<'_>,
    ) -> Result<(), HistoryError> {
        self.check_operation(operation)?;
        loaded.decoded_body_bytes = loaded
            .decoded_body_bytes
            .checked_add(operation.body.len())
            .ok_or(HistoryError::Capacity)?;
        if loaded.decoded_body_bytes > self.decode.max_segment_decoded_body_bytes {
            return Err(CodecError::SegmentDecodedBodyLimit {
                actual: loaded.decoded_body_bytes,
                limit: self.decode.max_segment_decoded_body_bytes,
            }
            .into());
        }
        Ok(())
    }

    fn checked_scan(&self, index: usize) -> Result<SegmentScan<'_>, HistoryError> {
        #[cfg(test)]
        self.full_scans.set(self.full_scans.get() + 1);
        let reference = self.pin.references()[index];
        let scan = scan_segment(
            &self.bytes,
            reference.first_group_number,
            reference.first_chain,
            self.decode,
        )?;
        let seal = self.seal(index);
        if scan.header.group_id() != self.identity.group_id
            || scan.header.segment_id() != reference.segment_id
            || scan.header.capacity() != reference.capacity
            || scan.valid_bytes != seal.valid_bytes
            || scan.digest != seal.digest
            || before(scan.next_chain) != self.segment_end(index)
            || scan.tail != TailState::Clean
        {
            return Err(HistoryError::Source);
        }
        for operation in scan.groups.iter().flat_map(|group| &group.operations) {
            self.check_operation(operation)?;
        }
        Ok(scan)
    }
}

/// Index one decoded operation; the entry count stays within `limit`.
fn push_indexed(
    entries: &mut Vec<IndexedOperation>,
    limit: usize,
    operation: &DecodedOperation<'_>,
) -> Result<(), HistoryError> {
    if entries.len() == limit {
        return Err(HistoryError::Capacity);
    }
    entries.push(IndexedOperation {
        offset: usize::try_from(operation.entry_offset).map_err(|_| HistoryError::Capacity)?,
        bytes: usize::try_from(operation.entry_bytes).map_err(|_| HistoryError::Capacity)?,
        position: LogPosition {
            op_number: operation.op_number,
            digest: operation.digest,
        },
        body_bytes: operation.body.len(),
    });
    Ok(())
}

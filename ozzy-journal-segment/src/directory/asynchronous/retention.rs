use super::{
    DirectoryError, Journal, SegmentReference, validate_operation_bodies, validate_replay_scan,
};
use crate::retention::{PreparedSegmentLease, scan_is_below_floors};
use crate::{
    RetentionError, RetentionFloors, RetiredPrefix, async_files::Access, scan_segment_async,
};
use std::{collections::HashSet, path::PathBuf};

/// Deterministic whole-segment scan limits. Async file jobs yield separately;
/// application actors must still budget CPU turns for decoding and hashing.
#[derive(Debug, Clone, Copy)]
pub struct RetirementBudget {
    pub max_segments: usize,
    pub max_read_bytes: usize,
}

/// Captured sealed-segment metadata plus temporary deletion protection.
/// Metadata contains no payloads. Dropping it never closes an OS file inline.
#[derive(Debug)]
pub struct SegmentFiles {
    references: Vec<SegmentReference>,
    sources: Vec<crate::IndexSource>,
    root: PathBuf,
    access: Access,
    chunk_bytes: usize,
    decode: crate::DecodeLimits,
    operations: crate::OperationLimits,
    _leases: Vec<PreparedSegmentLease>,
}

impl SegmentFiles {
    /// Read one index-selected record from an exact captured source. Physical
    /// checksums and canonical selectors are revalidated through the shared
    /// reader decoder; returned payloads do not retain the whole segment extent.
    pub async fn read_record(
        &self,
        source: crate::IndexSource,
        entry: crate::OffsetIndexEntry,
    ) -> Result<crate::IndexedRecord, crate::IndexedReadError> {
        let at = self
            .sources
            .iter()
            .position(|candidate| *candidate == source)
            .ok_or(crate::IndexedReadError::SourceMismatch)?;
        crate::reader::asynchronous::read_record(
            &self.access,
            self.root.join(self.references[at].file_name()),
            source,
            entry,
            self.decode,
            self.operations,
            self.chunk_bytes,
        )
        .await
    }

    pub fn references(&self) -> &[SegmentReference] {
        &self.references
    }

    /// Read bounded raw bytes from one captured generation. The caller still
    /// validates the relevant segment/index checksums before using payloads.
    pub async fn read_segment(&self, segment_id: u64) -> Result<Vec<u8>, DirectoryError> {
        let reference = self
            .references
            .iter()
            .find(|reference| reference.segment_id == segment_id)
            .ok_or(DirectoryError::SegmentMismatch(segment_id))?;
        Ok(self
            .access
            .read_file(
                self.root.join(reference.file_name()),
                reference.capacity as usize,
                self.chunk_bytes,
            )
            .await?)
    }
}

impl Journal {
    /// Protect selected sealed generations across later rolls and retirement.
    /// Active-segment capture requires a separate frozen-prefix read contract.
    pub fn capture_sealed_segments(&self, ids: &[u64]) -> Result<SegmentFiles, DirectoryError> {
        self.healthy()?;
        if ids.is_empty() {
            return Err(RetentionError::EmptyPin.into());
        }
        if ids.len() > self.manifest.segments.len() {
            return Err(RetentionError::DuplicateSegment.into());
        }
        let mut references = Vec::with_capacity(ids.len());
        let mut sources = Vec::with_capacity(ids.len());
        let mut leases = Vec::with_capacity(ids.len());
        let mut seen = HashSet::with_capacity(ids.len());
        for &id in ids {
            let at = self
                .manifest
                .segments
                .binary_search_by_key(&id, |reference| reference.segment_id)
                .map_err(|_| DirectoryError::SegmentNotSealed(id))?;
            let reference = self.manifest.segments[at];
            if reference.sealed.is_none() {
                return Err(DirectoryError::SegmentNotSealed(id));
            }
            if !seen.insert(id) {
                return Err(RetentionError::DuplicateSegment.into());
            }
            leases.push(self.pins.protect_prepared_segment(id)?);
            sources.push(crate::directory::sealed_source(
                self.manifest.identity.group_id,
                &reference,
                self.manifest
                    .segments
                    .get(at + 1)
                    .ok_or(DirectoryError::SegmentNotSealed(id))?,
            )?);
            references.push(reference);
        }
        Ok(SegmentFiles {
            references,
            sources,
            root: self.root().join("segments"),
            access: self.access.clone(),
            chunk_bytes: self.limits.io.chunk_bytes,
            decode: self.limits.decode,
            operations: self.limits.operations,
            _leases: leases,
        })
    }

    /// Revalidate selected authority before deleting or retiring any artifact.
    pub async fn validate_authority_files(&mut self) -> Result<(), DirectoryError> {
        self.healthy()?;
        let selected = self
            .directory
            .read_selected(
                self.manifest.identity,
                self.limits.metadata,
                self.configuration.as_deref(),
            )
            .await?;
        if selected.current != self.current || selected.manifest != self.manifest {
            return Err(DirectoryError::CurrentMismatch);
        }
        Ok(())
    }

    /// Retire a checkpoint-covered sealed prefix. Floors must come from that
    /// checkpoint's committed state. Physical files remain until orphan cleanup;
    /// captured readers keep their temporary deletion protection independently.
    pub async fn retire_sealed_prefix(
        &mut self,
        floors: &RetentionFloors,
        budget: RetirementBudget,
    ) -> Result<RetiredPrefix, DirectoryError> {
        self.healthy()?;
        if budget.max_segments == 0 || budget.max_read_bytes == 0 {
            return Err(DirectoryError::RetentionScanBudget);
        }
        self.validate_authority_files().await?;
        let checkpoint = self
            .manifest
            .checkpoint
            .ok_or(DirectoryError::RetentionRequiresCheckpoint)?;
        super::opening::selected_checkpoint(&self.access, self.root(), &self.manifest, self.limits)
            .await?;
        let mut result = RetiredPrefix {
            unreferenced_segment_ids: Vec::new(),
            scanned_segments: 0,
            scanned_bytes: 0,
        };
        for pair in self.manifest.segments.windows(2) {
            if result.scanned_segments == budget.max_segments {
                break;
            }
            let reference = pair[0];
            let last = pair[1]
                .first_chain
                .next_op_number()
                .checked_sub(1)
                .ok_or(DirectoryError::SegmentMismatch(reference.segment_id))?;
            if last > checkpoint.position.op_number {
                break;
            }
            let capacity = usize::try_from(reference.capacity)
                .map_err(|_| DirectoryError::SegmentMismatch(reference.segment_id))?;
            if capacity > budget.max_read_bytes - result.scanned_bytes {
                if result.scanned_segments == 0 {
                    return Err(DirectoryError::RetentionScanBudget);
                }
                break;
            }
            let bytes = self.segment_image(reference).await?;
            result.scanned_segments += 1;
            result.scanned_bytes += bytes.len();
            let scan = scan_segment_async(
                &bytes,
                reference.first_group_number,
                reference.first_chain,
                self.limits.decode,
            )
            .await?;
            validate_operation_bodies(
                &scan,
                self.limits.operations,
                self.manifest.configuration_epoch,
                self.manifest.promised_view,
            )
            .await?;
            validate_replay_scan(&reference, &scan, self.writer.state())?;
            if !scan_is_below_floors(
                &scan,
                checkpoint.position.op_number,
                floors,
                self.limits.operations,
            )? {
                break;
            }
            result.unreferenced_segment_ids.push(reference.segment_id);
        }
        if !result.unreferenced_segment_ids.is_empty() {
            let mut next = self.next_manifest()?;
            next.segments.drain(..result.unreferenced_segment_ids.len());
            self.install_selected(next).await?;
        }
        Ok(result)
    }
}

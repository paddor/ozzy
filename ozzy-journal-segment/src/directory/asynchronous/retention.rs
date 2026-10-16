use super::{DirectoryError, Journal, SegmentReference, groups::GroupScope};

mod scan;
use crate::retention::PreparedSegmentLease;
use crate::{RetentionError, RetentionFloors, RetiredPrefix, async_files::Access};
use ozzy_proto::{Offset, PartitionIncarnation};
use std::{collections::HashSet, path::PathBuf};

/// Deterministic whole-segment scan limits. Async file jobs yield separately;
/// application actors must still budget CPU turns for decoding and hashing.
#[derive(Debug, Clone, Copy)]
pub struct RetirementBudget {
    /// Maximum selected or scanned physical segments.
    pub max_segments: usize,
    /// Maximum physical bytes read in this maintenance step.
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
        crate::reader::asynchronous::ExtentReader {
            access: &self.access,
            path: self.root.join(self.references[at].file_name()),
            source,
            decode: self.decode,
            operations: self.operations,
            chunk: self.chunk_bytes,
        }
        .record(entry)
        .await
    }

    /// Exact protected segment references in caller selection order.
    pub fn references(&self) -> &[SegmentReference] {
        &self.references
    }

    /// Read the sealed bytes of one captured generation. The caller still
    /// validates the relevant segment/index checksums before using payloads.
    pub async fn read_segment(&self, segment_id: u64) -> Result<Vec<u8>, DirectoryError> {
        let reference = self
            .references
            .iter()
            .find(|reference| reference.segment_id == segment_id)
            .ok_or(DirectoryError::SegmentMismatch(segment_id))?;
        let sealed = reference
            .sealed
            .ok_or(DirectoryError::SegmentNotSealed(segment_id))?;
        Ok(self
            .access
            .read_prefix(
                self.root.join(reference.file_name()),
                sealed.valid_bytes,
                reference.capacity,
                self.chunk_bytes,
            )
            .await?)
    }
}

impl Journal {
    /// Validate one selected sealed generation and summarize its append metadata.
    /// Scratch retains one decoder-bounded group; physical reads and CPU work yield.
    pub async fn retention_segment(
        &self,
        segment_id: u64,
        partition: ozzy_proto::PartitionIncarnation,
    ) -> Result<ozzy_core::retention::Segment, DirectoryError> {
        self.healthy()?;
        let reference = self
            .manifest
            .segments
            .iter()
            .find(|reference| reference.segment_id == segment_id && reference.sealed.is_some())
            .ok_or(DirectoryError::SegmentNotSealed(segment_id))?;
        Ok(self
            .scan_retention(*reference, Some(partition), None)
            .await?
            .segment)
    }

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
        super::metadata::require_current(
            &mut self.directory,
            self.current,
            &self.manifest,
            self.configuration.as_deref(),
            self.limits.metadata,
        )
        .await
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
        let checkpoint = self
            .manifest
            .checkpoint
            .ok_or(DirectoryError::RetentionRequiresCheckpoint)?;
        let scan =
            self.prepare_retirement_scan(floors.clone(), checkpoint.position, budget, &[])?;
        let prefix = scan.scan().await?;
        self.install_retirement(None, prefix).await
    }

    /// Capture the leading sealed segments that end at or below `through`,
    /// within `budget`, for a scan that borrows no journal. Reads no files.
    /// A segment with a proof for its exact source is checked against the
    /// floors here; the scan reads the others, which the capture protects from
    /// deletion. When the selected checkpoint is at `through`, the scan
    /// revalidates its files first.
    pub fn prepare_retirement_scan(
        &self,
        floors: RetentionFloors,
        through: crate::LogPosition,
        budget: RetirementBudget,
        proofs: &[RetirementProof],
    ) -> Result<RetirementScan, DirectoryError> {
        self.healthy()?;
        if budget.max_segments == 0 || budget.max_read_bytes == 0 {
            return Err(DirectoryError::RetentionScanBudget);
        }
        let mut candidates = Vec::new();
        let mut leases = Vec::new();
        let mut bytes = 0;
        for pair in self.manifest.segments.windows(2) {
            if candidates.len() == budget.max_segments {
                break;
            }
            let reference = pair[0];
            let last = pair[1]
                .first_chain
                .next_op_number()
                .checked_sub(1)
                .ok_or(DirectoryError::SegmentMismatch(reference.segment_id))?;
            if last > through.op_number {
                break;
            }
            let source = crate::directory::sealed_source(
                self.manifest.identity.group_id,
                &reference,
                &pair[1],
            )?;
            if let Some(proof) = proofs.iter().find(|proof| proof.source == source) {
                if !proof.below(&floors) {
                    break;
                }
                candidates.push(Candidate::Proven(reference));
                continue;
            }
            let capacity = usize::try_from(reference.capacity)
                .map_err(|_| DirectoryError::SegmentMismatch(reference.segment_id))?;
            if capacity > budget.max_read_bytes - bytes {
                if candidates.is_empty() {
                    return Err(DirectoryError::RetentionScanBudget);
                }
                break;
            }
            bytes += capacity;
            leases.push(self.pins.protect_prepared_segment(reference.segment_id)?);
            candidates.push(Candidate::Scan(Box::new(self.group_scope(reference)?)));
        }
        Ok(RetirementScan {
            candidates,
            selected: self.selected_checkpoint_at(through),
            floors,
            through,
            leases,
        })
    }

    /// The selected checkpoint's files, for revalidation, when it sits
    /// exactly at `through`.
    fn selected_checkpoint_at(&self, through: crate::LogPosition) -> Option<SelectedCheckpoint> {
        self.manifest
            .checkpoint
            .is_some_and(|checkpoint| checkpoint.position == through)
            .then(|| SelectedCheckpoint {
                access: self.access.clone(),
                root: self.root().to_path_buf(),
                manifest: self.manifest.clone(),
                limits: self.limits,
            })
    }

    /// Unreference a scanned prefix in one manifest publication. With
    /// `checkpoint`, select it in the same publication; otherwise the selected
    /// checkpoint must already cover the scan. Revalidates authority files and
    /// requires the scanned segments to still lead the selection.
    pub async fn install_retirement(
        &mut self,
        checkpoint: Option<&super::CheckpointFiles>,
        prefix: ScannedPrefix,
    ) -> Result<RetiredPrefix, DirectoryError> {
        self.healthy()?;
        self.validate_authority_files().await?;
        let selection = match checkpoint {
            Some(files) => Some(self.checkpoint_selection(&files.checkpoint).await?),
            None => None,
        };
        let covered = selection
            .or(self.manifest.checkpoint)
            .ok_or(DirectoryError::RetentionRequiresCheckpoint)?;
        let count = prefix.references.len();
        if covered.position != prefix.through
            || count >= self.manifest.segments.len()
            || self.manifest.segments[..count] != prefix.references[..]
        {
            return Err(DirectoryError::RetentionPrefixMismatch);
        }
        if count != 0 || selection.is_some() {
            let mut next = self.next_manifest()?;
            next.checkpoint = selection.or(next.checkpoint);
            next.segments.drain(..count);
            self.install_selected(next).await?;
        }
        if let Some(files) = checkpoint {
            self.checkpoint = Some(files.checkpoint.clone());
        }
        Ok(RetiredPrefix {
            unreferenced_segment_ids: prefix
                .references
                .iter()
                .map(|reference| reference.segment_id)
                .collect(),
            scanned_segments: prefix.scanned_segments,
            scanned_bytes: prefix.scanned_bytes,
        })
    }
}

/// Leading sealed segments captured with deletion protection for a scan
/// against committed floors that borrows no journal.
#[derive(Debug)]
pub struct RetirementScan {
    candidates: Vec<Candidate>,
    selected: Option<SelectedCheckpoint>,
    floors: RetentionFloors,
    through: crate::LogPosition,
    leases: Vec<PreparedSegmentLease>,
}

impl RetirementScan {
    /// Validate each candidate in order and keep the leading run whose
    /// records and operations all lie below the floors and `through`.
    pub async fn scan(self) -> Result<ScannedPrefix, DirectoryError> {
        if let Some(selected) = &self.selected {
            super::opening::selected_checkpoint(
                &selected.access,
                &selected.root,
                &selected.manifest,
                selected.limits,
            )
            .await?;
        }
        let mut prefix = ScannedPrefix {
            references: Vec::with_capacity(self.candidates.len()),
            through: self.through,
            scanned_segments: 0,
            scanned_bytes: 0,
            _leases: self.leases,
        };
        for candidate in self.candidates {
            let scope = match candidate {
                Candidate::Proven(reference) => {
                    prefix.references.push(reference);
                    continue;
                }
                Candidate::Scan(scope) => *scope,
            };
            let reference = scope.reference();
            let summary = scope
                .scan_retention(None, Some((&self.floors, self.through.op_number)))
                .await?;
            prefix.scanned_segments += 1;
            prefix.scanned_bytes += reference.capacity as usize;
            if !summary.below_floors {
                break;
            }
            prefix.references.push(reference);
        }
        Ok(prefix)
    }
}

/// Leading sealed segments a scan proved retirable, still protected from
/// deletion until installed or dropped.
#[derive(Debug)]
pub struct ScannedPrefix {
    references: Vec<SegmentReference>,
    through: crate::LogPosition,
    scanned_segments: usize,
    scanned_bytes: usize,
    _leases: Vec<PreparedSegmentLease>,
}

impl ScannedPrefix {
    /// Exact segment references, oldest first.
    pub fn references(&self) -> &[SegmentReference] {
        &self.references
    }
}

/// One leading segment: already checked against the floors from its proof,
/// or read and checked by the scan.
#[derive(Debug)]
enum Candidate {
    Proven(SegmentReference),
    Scan(Box<GroupScope>),
}

/// Inputs to revalidate the checkpoint selected when a scan was captured.
#[derive(Debug)]
struct SelectedCheckpoint {
    access: Access,
    root: PathBuf,
    manifest: crate::Manifest,
    limits: super::Limits,
}

/// Record ends and newest append times a sealed segment's read index held at
/// roll, bound to that segment's exact digest and operation range. Retirement
/// checks committed floors against it instead of scanning the segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetirementProof {
    pub(super) source: crate::IndexSource,
    pub(super) capacity: u64,
    pub(super) ends: Vec<(PartitionIncarnation, Offset, u64)>,
}

impl RetirementProof {
    /// Physical segment identity this proof describes.
    pub const fn segment_id(&self) -> u64 {
        self.source.segment_id
    }

    /// Retention summary for one partition, matching a scan of the segment.
    pub fn retention_segment(
        &self,
        partition: PartitionIncarnation,
    ) -> ozzy_core::retention::Segment {
        let end = self
            .ends
            .iter()
            .find(|(candidate, _, _)| *candidate == partition);
        ozzy_core::retention::Segment {
            id: self.source.segment_id,
            capacity: self.capacity,
            sealed: true,
            last_operation: self.source.last_op_number,
            record_end: end.map_or(Offset::ZERO, |(_, end, _)| *end),
            newest_append_millis: end.map(|(_, _, newest)| *newest),
        }
    }

    /// Whether every stored record and operation lies below the floors.
    fn below(&self, floors: &RetentionFloors) -> bool {
        floors
            .protected_operation()
            .is_none_or(|floor| self.source.last_op_number < floor)
            && self.ends.iter().all(|(partition, end, _)| {
                floors.get(*partition).is_some_and(|floor| *end <= floor)
            })
    }
}

//! The journal owns state on its caller's shard. File effects use the shared
//! asynchronous backend; codecs and publication/recovery decisions stay here.

mod canonical;
mod checkpoint;
mod unselected;
pub use canonical::Candidate;
pub use checkpoint::{
    CheckpointBuildPlan, CheckpointCursor, CheckpointFiles, CheckpointInbox, CheckpointInboxStream,
    CheckpointSource, CheckpointStream, CheckpointWriter,
};
pub(crate) mod groups;
mod history;
mod index;
pub use index::{BuiltIndex, IndexJob};
mod format;
mod metadata;
mod opening;
pub use opening::Opening;
mod pack;
pub use pack::{PACK_CHUNK_BYTES, PackJob, PackLimits, PackOutcome, PackedSegments};
mod partition_read;
mod pipeline;
pub use partition_read::{
    Index as PartitionIndex, Limits as PartitionReadLimits, OperationRead, Read as PartitionRead,
    ReadRange as PartitionReadRange, WrittenOperation,
};
pub use pipeline::{Completed, CompletedSync, Pipeline, Prepared, PreparedSync};
mod prepared_roll;
pub use prepared_roll::{
    Completed as CompletedRoll, Pending as PendingRoll, Preparation as SegmentPreparation,
    Prepared as PreparedRoll, Segment as PreparedSegment,
};
mod recovery;
mod replay;
pub use recovery::{RecoveryDirectory, Repair};
mod retention;
mod roll;
mod snapshot;
pub use snapshot::SealedSchedules;
mod spare;
pub use spare::SparePreparation;
mod suffix;
pub use retention::{
    RetirementBudget, RetirementProof, RetirementScan, ScannedPrefix, SegmentFiles,
};
pub use suffix::{CanonicalCheckpointImport, CheckpointRecovery, Installer, SuffixRequest};
pub use unselected::UnselectedFiles;
#[cfg(test)]
mod tests;

use super::{
    DirectoryError, OperationEnvelope, ReplayError, ReplayedOperation, evidence,
    segment_reference_name, validate_operation,
};
use crate::{
    BodyEncoding, CanonicalOperation, CheckpointLimits, CodecError, CommitMode, CurrentReference,
    DecodeLimits, GroupIdentity, LogPosition, Manifest, MetadataLimits, OperationLimits,
    SegmentHeader, SegmentOptions, SegmentReference, SegmentWriter, WriterError, WriterPosition,
    async_files::Access, async_metadata, checkpoint::asynchronous::Checkpoint,
    metadata::position_before,
};
use std::path::Path;

/// Per-journal limits complement, never replace, the shared device budget.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Selected-manifest decoding bounds.
    pub metadata: MetadataLimits,
    /// Physical segment framing and decoded-body bounds.
    pub decode: DecodeLimits,
    /// Canonical operation-body decoding bounds.
    pub operations: OperationLimits,
    /// Checkpoint construction and decoding bounds.
    pub checkpoint: CheckpointLimits,
    /// Physical transfer, segment capacity, and write-mode bounds.
    pub io: SegmentOptions,
    /// Maximum entries returned by one bounded directory listing.
    pub directory_entries: usize,
    /// Maximum combined filename bytes per directory listing.
    pub directory_name_bytes: usize,
}

/// Explicit new-store specification. Opening a store never constructs this.
#[derive(Debug)]
pub struct Format {
    /// Exact group, node, volume, store, and store-generation binding.
    pub identity: GroupIdentity,
    /// Selected membership/configuration epoch.
    pub configuration_epoch: u64,
    /// Explicit journal durability and metadata-acceptance mode.
    pub commit_mode: CommitMode,
    /// Validated genesis segment header for explicit formatting.
    pub first_segment: SegmentHeader,
    /// Exact immutable adapter configuration bytes.
    pub configuration: Vec<u8>,
}

/// The established store an open must find. Opening never formats.
#[derive(Debug, Clone, Copy)]
pub struct ExpectedStore<'a> {
    /// Exact group, node, volume, store, and store-generation binding.
    pub identity: GroupIdentity,
    /// Adapter configuration bytes the store must hold exactly; `None`
    /// accepts the stored configuration, if any.
    pub configuration: Option<&'a [u8]>,
}

/// One mutable journal, with no OS descriptors or independently running owner.
/// Failed/canceled metadata mutations fence it until an explicit reopen.
#[derive(Debug)]
pub struct Journal {
    directory: async_metadata::Directory,
    access: Access,
    manifest: Manifest,
    current: CurrentReference,
    configuration: Option<Vec<u8>>,
    writer: SegmentWriter,
    evidence: Option<evidence::Copies>,
    checkpoint: Option<Checkpoint>,
    limits: Limits,
    interrupted: bool,
    pins: std::rc::Rc<crate::retention::PinRegistry>,
    /// Set while a cleanup capture lives; checkpoint selection waits for it.
    cleaning: std::rc::Rc<std::cell::Cell<bool>>,
    validated_indexes: std::cell::RefCell<std::collections::BTreeMap<u64, crate::IndexSource>>,
    /// Packed layouts of sealed segments, by segment ID and file generation.
    pack_layouts: std::cell::RefCell<std::collections::BTreeMap<(u64, u64), pack::Layout>>,
    /// Validated active-segment prefix reused by the next built snapshot.
    active_prefix: Option<Box<crate::active_index::ActivePrefix>>,
}

impl Journal {
    /// Filesystem directory bound to this owner.
    pub fn root(&self) -> &Path {
        self.directory.root()
    }
    /// Clone the existing backend lane for an explicitly fenced owner transition.
    pub fn backend(&self) -> ozzy_io::Local {
        self.access.io.clone()
    }

    /// Exact validated metadata manifest held by this object.
    pub const fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    /// Validated reference selecting the current manifest generation.
    pub const fn current(&self) -> CurrentReference {
        self.current
    }

    /// Whether live deletion protection released any of these unselected
    /// objects. Reads only the in-memory pin registry.
    pub fn pins_released(
        &self,
        segments: &[u64],
        checkpoints: &[ozzy_proto::CheckpointId],
    ) -> bool {
        segments.iter().any(|&id| !self.pins.is_pinned(id))
            || checkpoints
                .iter()
                .any(|&id| !self.pins.checkpoint_is_pinned(id))
    }
    /// Exact persisted adapter configuration bytes, when present.
    pub fn configuration(&self) -> Option<&[u8]> {
        self.configuration.as_deref()
    }
    /// Borrow the active physical segment writer.
    pub const fn writer(&self) -> &SegmentWriter {
        &self.writer
    }
    /// Write zeros over up to `bytes` of the active segment's unused capacity,
    /// so appends there overwrite written blocks instead of converting
    /// unwritten extents. The broker does this once when it formats a store.
    pub async fn zero_ahead(&mut self, bytes: u64) -> Result<(), DirectoryError> {
        self.healthy()?;
        Ok(self.writer.zero_ahead(bytes).await?)
    }
    /// Whether failure or canceled mutation fences further use.
    pub fn is_faulted(&self) -> bool {
        self.interrupted || self.writer.is_faulted() || self.directory.is_faulted()
    }

    fn healthy(&self) -> Result<(), DirectoryError> {
        if self.is_faulted() {
            return Err(WriterError::Faulted.into());
        }
        Ok(())
    }

    /// Exact written canonical prefix; this does not prove durability.
    pub fn written_position(&self) -> Result<LogPosition, DirectoryError> {
        Ok(position_before(
            self.writer.written_position().next_chain(),
        )?)
    }
    /// Canonical prefix accepted under this journal mode.
    pub fn accepted_position(&self) -> Result<LogPosition, DirectoryError> {
        Ok(position_before(
            if self.manifest.commit_mode == CommitMode::LocalDurable {
                self.writer.written_position()
            } else {
                self.writer.durable_position()
            }
            .next_chain(),
        )?)
    }
    /// Confirmed canonical prefix exposed by this journal mode.
    pub fn committed_position(&self) -> Result<LogPosition, DirectoryError> {
        self.healthy()?;
        if self.manifest.commit_mode == CommitMode::LocalDurable {
            Ok(position_before(
                self.writer.durable_position().next_chain(),
            )?)
        } else {
            Ok(self.manifest.committed)
        }
    }

    /// Close an idle writer asynchronously, without adding durability or clean
    /// restart evidence. Captured readers keep their group lock independently.
    /// Device shutdown must still drain backend-owned jobs and deferred closes.
    /// Failed/canceled mutations instead require drop and explicit reopen.
    pub async fn close(self) -> Result<(), DirectoryError> {
        self.healthy()?;
        self.writer.close().await?;
        Ok(())
    }

    /// Write canonical operations to the active segment; writing alone does not confirm records.
    pub async fn append(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        encoding: BodyEncoding,
    ) -> Result<WriterPosition, DirectoryError> {
        self.healthy()?;
        self.validate_append_limits(operations)?;
        for operation in operations {
            validate_operation(
                OperationEnvelope {
                    kind: operation.kind,
                    body: operation.body,
                    op_number: operation.op_number,
                    configuration_epoch: operation.configuration_epoch,
                    original_view: operation.original_view,
                },
                self.limits.operations,
                self.manifest.configuration_epoch,
                self.manifest.promised_view,
            )?;
        }
        let additional = operations
            .iter()
            .try_fold(0usize, |sum, op| sum.checked_add(op.body.len()))
            .ok_or(CodecError::LengthOverflow)?;
        let actual = self
            .writer
            .written_position()
            .decoded_body_bytes()
            .checked_add(additional)
            .ok_or(CodecError::LengthOverflow)?;
        if actual > self.limits.decode.max_segment_decoded_body_bytes {
            return Err(CodecError::SegmentDecodedBodyLimit {
                actual,
                limit: self.limits.decode.max_segment_decoded_body_bytes,
            }
            .into());
        }
        Ok(self
            .writer
            .append_with_limits(operations, encoding, Some(self.limits.decode))
            .await?)
    }

    fn validate_append_limits(
        &self,
        operations: &[CanonicalOperation<'_>],
    ) -> Result<(), DirectoryError> {
        let active = active_segment(&self.manifest.segments)?;
        let groups =
            self.writer.written_position().group_number() - (active.first_group_number - 1);
        let groups = usize::try_from(groups)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or(CodecError::LengthOverflow)?;
        let decoded = operations
            .iter()
            .try_fold(0usize, |sum, op| sum.checked_add(op.body.len()))
            .ok_or(CodecError::LengthOverflow)?;
        for (kind, actual, limit) in [
            (
                "physical group count",
                groups,
                self.limits.decode.max_groups,
            ),
            (
                "entry count",
                operations.len(),
                self.limits.decode.max_entries,
            ),
            (
                "decoded body bytes",
                operations.iter().map(|op| op.body.len()).max().unwrap_or(0),
                self.limits.decode.max_decoded_body_bytes,
            ),
            (
                "physical group decoded body bytes",
                decoded,
                self.limits.decode.max_group_decoded_body_bytes,
            ),
        ] {
            if actual > limit {
                return Err(CodecError::LimitExceeded {
                    kind,
                    actual,
                    limit,
                }
                .into());
            }
        }
        Ok(())
    }

    /// Synchronize an exact written generation and prefix without granting application commitment.
    pub async fn sync_through(
        &mut self,
        position: WriterPosition,
    ) -> Result<WriterPosition, DirectoryError> {
        self.healthy()?;
        Ok(self.writer.sync_through(position).await?)
    }

    /// Materialize only the selected, verified checkpoint. Borrowing the journal
    /// prevents retention from deleting it until this future settles or drops.
    pub async fn read_checkpoint_state(&self) -> Result<Option<Vec<u8>>, DirectoryError> {
        self.healthy()?;
        match &self.checkpoint {
            Some(checkpoint) => Ok(Some(checkpoint.read_state(self.limits.checkpoint).await?)),
            None => Ok(None),
        }
    }

    /// The valid prefix of a selected segment: its sealed bytes, or what the
    /// writer has written.
    async fn segment_image(&self, reference: SegmentReference) -> Result<Vec<u8>, DirectoryError> {
        if reference.capacity > self.limits.io.max_segment_bytes {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id));
        }
        let valid_bytes = reference.sealed.map_or_else(
            || self.writer.written_position().end_offset(),
            |sealed| sealed.valid_bytes,
        );
        Ok(self
            .access
            .read_prefix(
                self.root().join(segment_reference_name(reference)),
                valid_bytes,
                reference.capacity,
                self.limits.io.chunk_bytes,
            )
            .await?)
    }
}

async fn validate_operation_bodies(
    scan: &crate::SegmentScan<'_>,
    limits: OperationLimits,
    configuration_epoch: u64,
    promised_view: u64,
) -> Result<(), DirectoryError> {
    let mut budget = crate::cooperative::Budget::default();
    for operation in scan.groups.iter().flat_map(|group| &group.operations) {
        validate_operation(
            OperationEnvelope {
                kind: operation.kind,
                body: operation.body.as_ref(),
                op_number: operation.op_number,
                configuration_epoch: operation.configuration_epoch,
                original_view: operation.original_view,
            },
            limits,
            configuration_epoch,
            promised_view,
        )?;
        budget.charge(operation.body.len()).await;
    }
    Ok(())
}

async fn scan_contains_position(
    scan: &crate::SegmentScan<'_>,
    initial: crate::ChainPosition,
    target: crate::ChainPosition,
) -> bool {
    if initial == target {
        return true;
    }
    let mut budget = crate::cooperative::Budget::default();
    for operation in scan.groups.iter().flat_map(|group| &group.operations) {
        if operation.op_number.checked_add(1) == Some(target.next_op_number())
            && operation.digest == target.previous_digest()
        {
            return true;
        }
        budget.charge(0).await;
    }
    false
}

impl Journal {
    /// The group lock an opened journal holds; leases keep it alive.
    fn group_lock(&self) -> Result<ozzy_io::Handle, DirectoryError> {
        self.access.protection.clone().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "journal holds no group lock",
            )
            .into()
        })
    }
}

/// The writable last segment of a selected chain.
pub(crate) fn active_segment(
    segments: &[SegmentReference],
) -> Result<&SegmentReference, DirectoryError> {
    segments.last().ok_or(DirectoryError::MissingActiveSegment)
}

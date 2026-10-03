//! The journal owns state on its caller's shard. File effects use the shared
//! asynchronous backend; codecs and publication/recovery decisions stay here.

mod canonical;
mod checkpoint;
pub use canonical::Candidate;
pub use checkpoint::CheckpointFiles;
mod history;
mod index;
mod memory_voting;
mod metadata;
mod metadata_cleanup;
mod opening;
pub use opening::Opening;
mod orphans;
mod partition_read;
mod pipeline;
pub use partition_read::{
    Index as PartitionIndex, Limits as PartitionReadLimits, Read as PartitionRead,
};
pub use pipeline::{Completed, CompletedSync, Pipeline, Prepared, PreparedSync};
mod prepared_roll;
pub use prepared_roll::{
    Completed as CompletedRoll, Pending as PendingRoll, Preparation as SegmentPreparation,
    Prepared as PreparedRoll, Segment as PreparedSegment,
};
mod recovery;
pub use recovery::{RecoveryDirectory, Repair};
mod retention;
mod roll;
mod snapshot;
mod suffix;
pub use retention::{RetirementBudget, SegmentFiles};
pub use suffix::Installer;
#[cfg(test)]
mod tests;

use super::{
    DirectoryError, GroupIdentity, LogPosition, Manifest, OperationEnvelope, ReplayError,
    ReplayedOperation, SegmentReference, evidence, position_before, segment_reference_name,
    validate_operation, validate_replay_scan,
};
use crate::{
    AsyncSegmentOptions, AsyncSegmentWriter, BodyEncoding, CanonicalOperation, CheckpointLimits,
    CodecError, CommitMode, CurrentReference, DecodeLimits, MetadataLimits, OperationLimits,
    SegmentHeader, WriterError, WriterPosition, async_files::Access, async_metadata,
    checkpoint::asynchronous::Checkpoint, scan_segment_async,
};
use std::path::Path;

/// Per-journal limits complement, never replace, the shared device budget.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub metadata: MetadataLimits,
    pub decode: DecodeLimits,
    pub operations: OperationLimits,
    pub checkpoint: CheckpointLimits,
    pub io: AsyncSegmentOptions,
    pub directory_entries: usize,
    pub directory_name_bytes: usize,
}

/// Explicit new-store specification. Opening a store never constructs this.
#[derive(Debug)]
pub struct Format {
    pub identity: GroupIdentity,
    pub configuration_epoch: u64,
    pub commit_mode: CommitMode,
    pub first_segment: SegmentHeader,
    pub configuration: Vec<u8>,
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
    writer: AsyncSegmentWriter,
    evidence: Option<evidence::Copies>,
    checkpoint: Option<Checkpoint>,
    limits: Limits,
    interrupted: bool,
    pins: std::sync::Arc<crate::retention::PinRegistry>,
}

impl Journal {
    pub fn root(&self) -> &Path {
        self.directory.root()
    }
    pub const fn manifest(&self) -> &Manifest {
        &self.manifest
    }
    pub const fn current(&self) -> CurrentReference {
        self.current
    }
    pub fn configuration(&self) -> Option<&[u8]> {
        self.configuration.as_deref()
    }
    pub const fn writer(&self) -> &AsyncSegmentWriter {
        &self.writer
    }
    pub fn is_faulted(&self) -> bool {
        self.interrupted || self.writer.is_faulted() || self.directory.is_faulted()
    }

    fn healthy(&self) -> Result<(), DirectoryError> {
        if self.is_faulted() {
            return Err(WriterError::Faulted.into());
        }
        Ok(())
    }

    pub fn written_position(&self) -> Result<LogPosition, DirectoryError> {
        position_before(self.writer.written_position().next_chain())
    }
    pub fn accepted_position(&self) -> Result<LogPosition, DirectoryError> {
        position_before(
            if self.manifest.commit_mode == CommitMode::LocalDurable {
                self.writer.written_position()
            } else {
                self.writer.durable_position()
            }
            .next_chain(),
        )
    }
    pub fn committed_position(&self) -> Result<LogPosition, DirectoryError> {
        self.healthy()?;
        if self.manifest.commit_mode == CommitMode::LocalDurable {
            position_before(self.writer.durable_position().next_chain())
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
        let active = self
            .manifest
            .segments
            .last()
            .expect("validated active segment");
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

    async fn segment_image(&self, reference: SegmentReference) -> Result<Vec<u8>, DirectoryError> {
        if reference.capacity > self.limits.io.max_segment_bytes {
            return Err(DirectoryError::SegmentMismatch(reference.segment_id));
        }
        Ok(self
            .access
            .read_file(
                self.root().join(segment_reference_name(reference)),
                reference.capacity as usize,
                self.limits.io.chunk_bytes,
            )
            .await?)
    }

    /// Replay after the selected checkpoint. Each callback sees one validated
    /// operation borrowed from a bounded segment image, never a filesystem handle.
    pub async fn replay_accepted<E>(
        &self,
        mut visit: impl FnMut(ReplayedOperation<'_>) -> Result<(), E>,
    ) -> Result<(), ReplayError<E>>
    where
        E: std::error::Error + 'static,
    {
        self.replay_accepted_async(async |item| visit(item)).await
    }

    /// Replay with asynchronous visitors, for example to resolve cold control
    /// identities before synchronous canonical-state validation. One bounded
    /// segment image owns the borrowed operation across the visitor's awaits.
    pub async fn replay_accepted_async<E>(
        &self,
        mut visit: impl AsyncFnMut(ReplayedOperation<'_>) -> Result<(), E>,
    ) -> Result<(), ReplayError<E>>
    where
        E: std::error::Error + 'static,
    {
        self.healthy().map_err(ReplayError::Journal)?;
        let start = self
            .manifest
            .checkpoint
            .map_or(LogPosition::GENESIS, |value| value.position);
        let accepted = self.accepted_position().map_err(ReplayError::Journal)?;
        let committed = self.committed_position().map_err(ReplayError::Journal)?;
        let mut replayed = start;
        let mut budget = crate::cooperative::Budget::default();
        for reference in &self.manifest.segments {
            let bytes = self
                .segment_image(*reference)
                .await
                .map_err(ReplayError::Journal)?;
            let scan = scan_segment_async(
                &bytes,
                reference.first_group_number,
                reference.first_chain,
                self.limits.decode,
            )
            .await
            .map_err(DirectoryError::from)
            .map_err(ReplayError::Journal)?;
            validate_operation_bodies(
                &scan,
                self.limits.operations,
                self.manifest.configuration_epoch,
                self.manifest.promised_view,
            )
            .await
            .map_err(ReplayError::Journal)?;
            validate_replay_scan(reference, &scan, self.writer.state())
                .map_err(ReplayError::Journal)?;
            for operation in scan.groups.iter().flat_map(|group| &group.operations) {
                budget.charge(operation.body.len()).await;
                if operation.op_number <= start.op_number {
                    continue;
                }
                if operation.op_number > accepted.op_number {
                    break;
                }
                visit(ReplayedOperation {
                    operation,
                    committed: operation.op_number <= committed.op_number,
                })
                .await
                .map_err(ReplayError::Visitor)?;
                replayed = LogPosition {
                    op_number: operation.op_number,
                    digest: operation.digest,
                };
            }
        }
        if replayed != accepted {
            return Err(ReplayError::Journal(DirectoryError::PositionMismatch(
                accepted.op_number,
            )));
        }
        Ok(())
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

//! Owned physical writes with read-only journal access during the syscall.

use super::{
    Arc, DirectoryError, File, OpenGroupJournal, OperationEnvelope, StoreLock, validate_operation,
    validate_operation_metadata,
};
use crate::codec::{PackedOperation, PreparedGroupBodies};
use crate::writer::prepared::{WriteBytes, WritePlan};
use crate::{BodyEncoding, CanonicalOperation, OperationLocation};
use ozzy_journal::operation::{
    Digest, canonical_body_digest, validate_operation_body_with_validated_payload,
};

#[cfg(target_os = "linux")]
mod aio;
#[cfg(target_os = "linux")]
pub use aio::JournalAio;
mod pipeline;
pub use pipeline::JournalWritePipeline;
mod shared;
pub use shared::SharedJournalOperation;
mod encoding;
mod writeback;
pub use encoding::{JournalGroupEncoder, JournalGroupEncoding};
pub use writeback::JournalWriteback;
pub(crate) use writeback::start_range as start_writeback_range;

/// Physical batch boundaries for optional timing. Finished events also cover errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JournalWriteEvent {
    WriteStarted,
    WriteFinished,
    WritebackStarted,
    WritebackFinished,
}

/// Owned encoded bodies, prepared independently of the preceding physical write.
/// Physical headers are finalized when the owner selects or reserves placement.
#[derive(Debug)]
pub struct PreencodedJournalGroup {
    pub(super) operations: Vec<PackedOperation>,
    pub(super) bodies: PreparedGroupBodies,
    pub(super) shared: Vec<bytes::Bytes>,
}

/// One complete journal group ready for the shard's physical writer.
/// Holds the directory lock even if its observer or original owner disappears.
#[derive(Debug)]
#[must_use = "execute the write and install its exact completion"]
pub struct PreparedJournalWrite {
    key: Arc<()>,
    store_lock: Arc<StoreLock>,
    file: Arc<File>,
    plan: WritePlan,
    bytes: WriteBytes,
    locations: Vec<OperationLocation>,
    buffered: bool,
    /// `file` is the writer's `O_DIRECT` descriptor.
    direct: bool,
}

/// Exact success/failure and the output allocation retained by a physical write.
#[derive(Debug)]
#[must_use = "install this completion on its pending journal"]
pub struct CompletedJournalWrite {
    work: PreparedJournalWrite,
    result: std::io::Result<()>,
}

/// Exclusive journal ownership while a physical write is outstanding.
/// Exposes reads and successor compression, but no competing journal mutation.
#[derive(Debug)]
#[must_use = "settle the physical write before reopening mutable journal access"]
pub struct PendingJournalWrite {
    journal: OpenGroupJournal,
    key: Arc<()>,
}

impl PreparedJournalWrite {
    /// Bytes this write adds to the segment.
    pub const fn physical_bytes(&self) -> u64 {
        self.plan.after.end_offset() - self.plan.before.end_offset()
    }

    /// Perform only physical I/O after encoding. This call never publishes
    /// application or quorum state; the journal owner installs its exact result.
    pub fn write(mut self) -> CompletedJournalWrite {
        let offset = self.plan.before.end_offset();
        let result = if self.direct {
            crate::writer::direct::write(
                &self.file,
                offset,
                usize::try_from(self.physical_bytes()).unwrap_or(usize::MAX),
                self.bytes.slices(),
                std::num::NonZeroUsize::MAX,
            )
        } else {
            self.bytes.write(&mut self.file, offset)
        }
        .and_then(|()| self.start_writeback(self.plan.after.end_offset()));
        CompletedJournalWrite { work: self, result }
    }

    fn start_writeback(&self, end: u64) -> std::io::Result<()> {
        if self.buffered {
            writeback::start(&self.file, self.plan.before.end_offset(), end)?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn fail(self) -> CompletedJournalWrite {
        CompletedJournalWrite {
            work: self,
            result: Err(std::io::Error::other("injected physical write failure")),
        }
    }
}

impl OpenGroupJournal {
    /// Validate canonical bodies and encode one owned group without writing.
    /// The adapter bounds operation/body bytes before calling; storage decoder
    /// limits are checked independently. Caller payloads may be released afterward.
    pub fn preencode_journal_group(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        encoding: BodyEncoding,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        let descriptors = describe_operations(
            operations.iter().copied().map(|op| (op, None)),
            self.decode_limits,
            self.operation_limits,
            BodyCheck::Full,
            self.directory.manifest.configuration_epoch,
            self.directory.manifest.promised_view,
        )?;
        let bodies = self
            .writer
            .preencode_owned_bodies(operations.iter().map(|op| op.body), encoding)?;
        Ok(PreencodedJournalGroup {
            operations: descriptors,
            bodies,
            shared: Vec::new(),
        })
    }

    /// Test segment capacity before transferring exclusive journal ownership.
    /// The same preencoded group can be reused after a required segment roll.
    pub fn check_preencoded_journal_group(
        &self,
        group: &PreencodedJournalGroup,
    ) -> Result<(), DirectoryError> {
        self.require_roll_published()?;
        self.require_additional_decoded_capacity(Some(group.bodies.decoded_body_bytes()))?;
        group.bodies.require_capacity(
            self.writer.written_position().end_offset(),
            self.writer.header().capacity(),
        )?;
        Ok(())
    }

    /// Transfer mutation ownership until this exact write finishes. Errors consume
    /// the owner and require reopen; use the capacity check before a possible roll.
    pub fn begin_preencoded_journal_write(
        mut self,
        group: PreencodedJournalGroup,
    ) -> Result<(PendingJournalWrite, PreparedJournalWrite), DirectoryError> {
        if !group.shared.is_empty() {
            return Err(crate::WriterError::InvalidSyncPosition.into());
        }
        self.check_preencoded_journal_group(&group)?;
        if !self.writer.data_sync() {
            return Err(crate::WriterError::InvalidSyncPosition.into());
        }
        let file = self.writer.write_handle();
        let lock = Arc::clone(&self.directory.lock);
        let locations = group.locations(
            self.writer.header(),
            self.writer.written_position().end_offset(),
        )?;
        let (plan, bytes) = self
            .writer
            .prepare_owned_descriptors(&group.operations, group.bodies)?;
        let key = Arc::new(());
        let work = PreparedJournalWrite {
            key: key.clone(),
            store_lock: lock,
            file,
            plan,
            bytes,
            locations,
            buffered: false,
            direct: self.writer.is_direct(),
        };
        Ok((PendingJournalWrite { journal: self, key }, work))
    }
}

/// How much of each body encoding walks again before describing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BodyCheck {
    /// Walk the whole body, including any whole-payload LZ4 block.
    Full,
    /// Walk descriptors and limits; the caller validated or produced each
    /// body's LZ4 block.
    Descriptors,
    /// The caller decoded every body with the journal's limits.
    Metadata,
}

fn describe_operations<'a>(
    operations: impl ExactSizeIterator<Item = (CanonicalOperation<'a>, Option<Digest>)>,
    decode: crate::DecodeLimits,
    limits: crate::OperationLimits,
    check: BodyCheck,
    configuration_epoch: u64,
    promised_view: u64,
) -> Result<Vec<PackedOperation>, DirectoryError> {
    if operations.len() == 0 {
        return Err(crate::CodecError::EmptyGroup.into());
    }
    require_limit("group entries", operations.len(), decode.max_entries)?;
    let mut body_bytes = 0usize;
    let mut descriptors = Vec::with_capacity(operations.len());
    for (operation, body_digest) in operations {
        let envelope = OperationEnvelope {
            kind: operation.kind,
            body: operation.body,
            op_number: operation.op_number,
            configuration_epoch: operation.configuration_epoch,
            original_view: operation.original_view,
        };
        match check {
            BodyCheck::Full => {
                validate_operation(envelope, limits, configuration_epoch, promised_view)?;
            }
            BodyCheck::Descriptors => {
                validate_operation_body_with_validated_payload(
                    envelope.kind,
                    envelope.body,
                    limits,
                )?;
                validate_operation_metadata(envelope, configuration_epoch, promised_view)?;
            }
            BodyCheck::Metadata => {
                validate_operation_metadata(envelope, configuration_epoch, promised_view)?;
            }
        }
        body_bytes = body_bytes
            .checked_add(operation.body.len())
            .ok_or(crate::CodecError::LengthOverflow)?;
        require_limit(
            "decoded operation bytes",
            operation.body.len(),
            decode.max_decoded_body_bytes,
        )?;
        require_limit(
            "decoded group bytes",
            body_bytes,
            decode.max_group_decoded_body_bytes,
        )?;
        descriptors.push(PackedOperation {
            header: operation.header(),
            body_bytes: operation.body.len(),
            body_digest: body_digest.unwrap_or_else(|| canonical_body_digest(operation.body)),
        });
    }
    Ok(descriptors)
}

impl PreencodedJournalGroup {
    pub(super) fn locations(
        &self,
        header: &crate::SegmentHeader,
        offset: u64,
    ) -> Result<Vec<OperationLocation>, DirectoryError> {
        let layouts = self.bodies.entry_layouts(offset)?;
        Ok(self
            .operations
            .iter()
            .zip(layouts)
            .map(|(op, layout)| OperationLocation {
                segment_id: header.segment_id(),
                entry_offset: layout.entry_offset,
                entry_bytes: layout.entry_bytes,
                op_number: op.header.op_number,
                operation_digest: op.header.digest(op.body_digest),
            })
            .collect())
    }
}

fn require_limit(kind: &'static str, actual: usize, limit: usize) -> Result<(), DirectoryError> {
    if actual > limit {
        Err(crate::CodecError::LimitExceeded {
            kind,
            actual,
            limit,
        }
        .into())
    } else {
        Ok(())
    }
}

impl PendingJournalWrite {
    /// Prior physical history remains readable while its successor is being written.
    pub const fn journal(&self) -> &OpenGroupJournal {
        &self.journal
    }

    /// Prepare a bounded successor on the journal owner while the writer is busy.
    pub fn preencode_journal_group(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        encoding: BodyEncoding,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        self.journal.preencode_journal_group(operations, encoding)
    }

    /// Publish only the exact completed physical prefix. Successful `O_DSYNC`
    /// advances local persistence without an additional data-sync syscall.
    /// Application state, quorum votes and restart evidence remain adapter-owned.
    pub fn complete(
        mut self,
        completed: CompletedJournalWrite,
    ) -> Result<(OpenGroupJournal, Vec<OperationLocation>), DirectoryError> {
        if !Arc::ptr_eq(&self.key, &completed.work.key) {
            return Err(crate::WriterError::InvalidSyncPosition.into());
        }
        let CompletedJournalWrite { work, result } = completed;
        let PreparedJournalWrite {
            plan,
            bytes,
            locations,
            ..
        } = work;
        let written = self.journal.writer.complete_write(plan, result);
        let WriteBytes::Contiguous(bytes) = bytes else {
            unreachable!("owned contiguous group")
        };
        self.journal.writer.restore_write_bytes(bytes);
        self.journal.sync_through(written?)?;
        Ok((self.journal, locations))
    }
}

#[cfg(test)]
mod tests;

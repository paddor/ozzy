//! Ordered physical reservations. Planning never publishes written history.

use super::{
    Arc, BodyEncoding, CanonicalOperation, CompletedJournalWrite, DirectoryError, OpenGroupJournal,
    PreencodedJournalGroup, PreparedJournalWrite, WriteBytes, describe_operations,
};
use crate::writer::prepared::WriteLayout;
use crate::{CodecError, OperationLocation, WriterError, WriterPosition};

/// One mutable journal owner with a bounded sequence of prepared writes.
/// Workers own immutable output; only ordered completions advance this journal.
/// No roll or authority mutation is possible until all reservations settle.
#[derive(Debug)]
pub struct JournalWritePipeline {
    journal: OpenGroupJournal,
    key: Arc<()>,
    reserved: WriterPosition,
    pending: usize,
    capacity: usize,
    faulted: bool,
}

impl OpenGroupJournal {
    /// Freeze authority/roll mutation while bounded consecutive writes are reserved.
    pub fn begin_write_pipeline(
        self,
        capacity: usize,
    ) -> Result<JournalWritePipeline, DirectoryError> {
        self.require_roll_published()?;
        if capacity == 0 {
            return Err(WriterError::InvalidSyncPosition.into());
        }
        let reserved = self.writer.written_position();
        Ok(JournalWritePipeline {
            journal: self,
            key: Arc::new(()),
            reserved,
            pending: 0,
            capacity,
            faulted: false,
        })
    }
}

impl JournalWritePipeline {
    /// Read-only access to the installed physical prefix, excluding reservations.
    pub const fn journal(&self) -> &OpenGroupJournal {
        &self.journal
    }

    /// Prepared writes whose exact completions have not been installed.
    pub const fn pending(&self) -> usize {
        self.pending
    }

    /// Capture `DURABLE` publication for the installed prefix while later
    /// reservations are still in physical I/O. Those are excluded.
    pub fn prepare_durable_progress(
        &self,
    ) -> Result<Option<super::super::PreparedDurableProgress>, DirectoryError> {
        if self.faulted {
            return Err(WriterError::Faulted.into());
        }
        self.journal.prepare_durable_progress()
    }

    /// Install one `DURABLE` publication captured from this journal.
    pub fn complete_durable_progress(
        &mut self,
        completed: super::super::CompletedDurableProgress,
    ) -> Result<(), DirectoryError> {
        let result = self.journal.complete_durable_progress(completed);
        if result.is_err() {
            self.faulted = true;
        }
        result
    }

    /// Capture independently bounded CPU work even while physical reservations are full.
    pub fn begin_group_encoding(&mut self) -> Result<super::JournalGroupEncoding, DirectoryError> {
        if self.faulted {
            return Err(WriterError::Faulted.into());
        }
        self.journal.begin_group_encoding()
    }

    /// Compress one collected chunk on the owner, including while earlier
    /// chunks are in physical I/O. No segment placement is baked into its body.
    pub fn preencode(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        encoding: BodyEncoding,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        self.preencode_inner(operations.iter().copied().map(|op| (op, None)), encoding)
    }

    /// Keep raw canonical bodies shared through physical I/O. Only the small
    /// headers, padding, and seal occupy the reusable encode buffer.
    pub fn preencode_shared_raw(
        &mut self,
        operations: Vec<super::SharedJournalOperation>,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        self.require_space()?;
        super::shared::preencode(&mut self.journal, operations)
    }

    /// Reuse body digests computed by the owner over these exact immutable bytes.
    /// The caller must preserve that association. Schema, authority, capacity,
    /// and chain checks still run; stored-byte verification still hashes on read.
    pub fn preencode_verified(
        &mut self,
        operations: &[(CanonicalOperation<'_>, ozzy_journal::operation::Digest)],
        encoding: BodyEncoding,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        self.preencode_inner(
            operations.iter().map(|&(op, digest)| (op, Some(digest))),
            encoding,
        )
    }

    fn preencode_inner<'a>(
        &mut self,
        operations: impl ExactSizeIterator<
            Item = (
                CanonicalOperation<'a>,
                Option<ozzy_journal::operation::Digest>,
            ),
        > + Clone,
        encoding: BodyEncoding,
    ) -> Result<PreencodedJournalGroup, DirectoryError> {
        self.require_space()?;
        let operations_metadata = describe_operations(
            operations.clone(),
            self.journal.decode_limits,
            self.journal.operation_limits,
            super::BodyCheck::Full,
            self.journal.directory.manifest.configuration_epoch,
            self.journal.directory.manifest.promised_view,
        )?;
        let bodies = self
            .journal
            .writer
            .preencode_owned_shared_bodies(operations.map(|(op, _)| op.body), encoding)?;
        Ok(PreencodedJournalGroup {
            operations: operations_metadata,
            bodies,
            shared: Vec::new(),
        })
    }

    /// Capacity is checked against all reservations, not just installed writes.
    /// A full segment leaves the encoded chunk reusable after draining and roll.
    pub fn check(&self, group: &PreencodedJournalGroup) -> Result<(), DirectoryError> {
        self.require_space()?;
        let first = self
            .journal
            .directory
            .manifest
            .segments
            .last()
            .expect("active")
            .first_group_number;
        if self.reserved.group_number() - (first - 1)
            >= self.journal.decode_limits.max_groups as u64
        {
            return Err(CodecError::GroupExceedsSegment.into());
        }
        let decoded = self
            .reserved
            .decoded_body_bytes()
            .checked_add(group.bodies.decoded_body_bytes())
            .ok_or(CodecError::LengthOverflow)?;
        let limit = self.journal.decode_limits.max_segment_decoded_body_bytes;
        if decoded > limit {
            return Err(CodecError::SegmentDecodedBodyLimit {
                actual: decoded,
                limit,
            }
            .into());
        }
        group.bodies.require_capacity(
            self.reserved.end_offset(),
            self.journal.writer.header().capacity(),
        )?;
        Ok(())
    }

    /// Reserve consecutive placement and finalize bytes without performing I/O.
    pub fn prepare(
        &mut self,
        mut group: PreencodedJournalGroup,
    ) -> Result<PreparedJournalWrite, DirectoryError> {
        self.check(&group)?;
        let file = self.journal.writer.write_handle();
        let layout = WriteLayout {
            header: self.journal.writer.header().clone(),
            before: self.reserved,
            group_number: self
                .reserved
                .group_number()
                .checked_add(1)
                .ok_or(WriterError::GroupNumberExhausted)?,
        };
        let locations = group.locations(&layout.header, layout.before.end_offset())?;
        let finalized = crate::codec::finalize_group_descriptors(
            &layout.header,
            layout.group_number,
            layout.before.end_offset(),
            layout.before.next_chain(),
            group.operations.iter().copied(),
            &mut group.bodies,
        )?;
        let plan = layout.plan(finalized, group.bodies.decoded_body_bytes())?;
        self.reserved = plan.after;
        self.pending += 1;
        Ok(PreparedJournalWrite {
            key: Arc::clone(&self.key),
            store_lock: Arc::clone(&self.journal.directory.lock),
            file,
            plan,
            bytes: if group.shared.is_empty() {
                WriteBytes::Contiguous(group.bodies.into_owned_bytes())
            } else {
                WriteBytes::SharedRaw {
                    framing: group.bodies.into_raw_framing(),
                    bodies: group.shared,
                }
            },
            locations,
            // Direct writes leave no dirty pages to hint.
            buffered: !self.journal.writer.data_sync() && !self.journal.writer.is_direct(),
            direct: self.journal.writer.is_direct(),
        })
    }

    /// Install only this owner's next physical completion; any error fences it.
    pub fn complete(
        &mut self,
        completed: CompletedJournalWrite,
    ) -> Result<Vec<OperationLocation>, DirectoryError> {
        let valid = !self.faulted
            && self.pending != 0
            && Arc::ptr_eq(&self.key, &completed.work.key)
            && self.journal.writer.written_position() == completed.work.plan.before;
        self.faulted = true;
        if !valid {
            return Err(WriterError::InvalidSyncPosition.into());
        }
        let CompletedJournalWrite { work, result } = completed;
        let written = self.journal.writer.complete_write(work.plan, result)?;
        // Buffered completion advances only written history. Never manufacture
        // stable-storage evidence from page-cache acceptance.
        if self.journal.writer.data_sync() {
            self.journal.sync_through(written)?;
        }
        self.journal
            .writer
            .restore_write_bytes(work.bytes.into_journal_scratch());
        self.pending -= 1;
        self.faulted = false;
        Ok(work.locations)
    }

    /// Restore mutable journal access only after every reservation succeeds.
    pub fn finish(self) -> Result<OpenGroupJournal, DirectoryError> {
        if self.faulted
            || self.pending != 0
            || self.journal.writer.written_position() != self.reserved
        {
            return Err(WriterError::InvalidSyncPosition.into());
        }
        Ok(self.journal)
    }

    fn require_space(&self) -> Result<(), DirectoryError> {
        if self.faulted || self.pending >= self.capacity {
            return Err(WriterError::InvalidSyncPosition.into());
        }
        Ok(())
    }
}

impl CompletedJournalWrite {
    /// Physical result only. The owner must still install the exact completion.
    pub fn succeeded(&self) -> bool {
        self.result.is_ok()
    }
}

impl PreparedJournalWrite {
    /// Drain already-ready chunks into one vectored write using the file's mode. Every chunk
    /// retains its own seal and exact completion. A partial failure confirms none.
    pub fn write_batch(chunks: Vec<Self>) -> Vec<CompletedJournalWrite> {
        Self::write_batch_bounded(chunks, std::num::NonZeroUsize::MAX)
    }

    /// Limit bytes per physical write without splitting group completion or encoding.
    /// Every constituent write must succeed before any chunk can be installed.
    pub fn write_batch_bounded(
        chunks: Vec<Self>,
        limit: std::num::NonZeroUsize,
    ) -> Vec<CompletedJournalWrite> {
        Self::write_batch_observing(chunks, limit, |_| {})
    }

    /// Observe writes separately from buffered writeback hints. Observers must not panic.
    /// No clocks are read here; the caller chooses whether and how to time events.
    pub fn write_batch_observing(
        chunks: Vec<Self>,
        limit: std::num::NonZeroUsize,
        observe: impl FnMut(super::JournalWriteEvent),
    ) -> Vec<CompletedJournalWrite> {
        Self::write_batch_scheduling(
            chunks,
            limit,
            |hint| hint.map_or(Ok(()), super::JournalWriteback::start),
            observe,
        )
    }

    /// Schedule owned hints after successful writes. The scheduler must bound its
    /// queue, retain errors, and drain before rolling or publishing a clean stop.
    /// Called after every physical batch, including batches with no new hint.
    pub fn write_batch_scheduling(
        chunks: Vec<Self>,
        limit: std::num::NonZeroUsize,
        mut schedule: impl FnMut(Option<super::JournalWriteback>) -> std::io::Result<()>,
        mut observe: impl FnMut(super::JournalWriteEvent),
    ) -> Vec<CompletedJournalWrite> {
        use super::JournalWriteEvent::{
            WriteFinished, WriteStarted, WritebackFinished, WritebackStarted,
        };
        Self::write_batch_with(chunks, |chunks| {
            let Some((first, rest)) = chunks.split_first_mut() else {
                return Ok(());
            };
            let buffers =
                std::iter::once(&first.bytes).chain(rest.iter().map(|chunk| &chunk.bytes));
            let end = rest.last().map_or(first.plan.after.end_offset(), |last| {
                last.plan.after.end_offset()
            });
            observe(WriteStarted);
            let result = if first.direct {
                crate::writer::direct::write(
                    &first.file,
                    first.plan.before.end_offset(),
                    usize::try_from(end - first.plan.before.end_offset()).unwrap_or(usize::MAX),
                    buffers.flat_map(WriteBytes::slices),
                    limit,
                )
            } else {
                crate::writer::extents::write_extents_bounded(
                    &mut first.file,
                    first.plan.before.end_offset(),
                    buffers.flat_map(WriteBytes::slices),
                    limit,
                )
            };
            observe(WriteFinished);
            result?;
            observe(WritebackStarted);
            let result = schedule(super::JournalWriteback::capture(first, end));
            observe(WritebackFinished);
            result
        })
    }

    pub(super) fn write_batch_with(
        mut chunks: Vec<Self>,
        write: impl FnOnce(&mut [Self]) -> std::io::Result<()>,
    ) -> Vec<CompletedJournalWrite> {
        let valid = chunks.windows(2).all(|pair| {
            Arc::ptr_eq(&pair[0].key, &pair[1].key) && pair[0].plan.after == pair[1].plan.before
        });
        let result = if valid {
            write(&mut chunks)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "nonconsecutive journal writes",
            ))
        };
        chunks
            .into_iter()
            .map(|work| CompletedJournalWrite {
                work,
                result: result.as_ref().copied().map_err(|error| {
                    error.raw_os_error().map_or_else(
                        || std::io::Error::new(error.kind(), error.to_string()),
                        std::io::Error::from_raw_os_error,
                    )
                }),
            })
            .collect()
    }
}

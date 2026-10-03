//! Pure placement reservations and backend-owned writes. Physical completion
//! is distinct from installing the exact next prefix on this owner.

use super::Journal;
use crate::async_files::Access;
use crate::writer::prepared::{WriteLayout, WritePlan};
use crate::{
    CodecError, DirectoryError, JournalGroupEncoding, OperationLocation, PreencodedJournalGroup,
    WriterError, WriterPosition,
};
use bytes::Bytes;
use ozzy_io::{Class, Operation, WriteBuffer};
use std::{io, sync::Arc};
mod sync;
pub use sync::{CompletedSync, PreparedSync};

/// Exclusive journal mutation while a bounded ordered set of writes is reserved.
/// File work may finish out of order; callers install results in reservation order.
#[derive(Debug)]
pub struct Pipeline {
    journal: Journal,
    key: Arc<()>,
    reserved: WriterPosition,
    pending: usize,
    capacity: usize,
    sync: Option<Arc<()>>,
    faulted: bool,
}

/// Immutable placement, buffers and handles for one backend write. Dropping this
/// without installing a completion leaves its pipeline unable to finish.
#[derive(Debug)]
#[must_use = "execute and install this exact reserved write"]
pub struct Prepared {
    key: Arc<()>,
    access: Access,
    plan: WritePlan,
    job: Option<Operation>,
    framing: Bytes,
    backing_bytes: usize,
    locations: Vec<OperationLocation>,
}

/// Physical result only. It grants no journal, durability-evidence or consensus
/// authority until installed on the originating owner in exact order.
#[derive(Debug)]
#[must_use = "install on the originating pipeline"]
pub struct Completed {
    work: Prepared,
    result: io::Result<()>,
}

impl Journal {
    /// Capture placement-independent encoding with no file work. The owner must
    /// bound captured CPU jobs and drain them before changing journal authority.
    pub fn begin_group_encoding(&mut self) -> Result<JournalGroupEncoding, DirectoryError> {
        self.healthy()?;
        Ok(JournalGroupEncoding::captured(
            self.limits.decode,
            self.limits.operations,
            self.manifest.configuration_epoch,
            self.manifest.promised_view,
            self.writer.take_encode_buffer(),
        ))
    }

    /// Freeze roll/authority mutation until every reserved write is installed.
    /// The shared backend still enforces aggregate device and per-shard budgets.
    pub fn begin_write_pipeline(self, capacity: usize) -> Result<Pipeline, DirectoryError> {
        self.healthy()?;
        if capacity == 0 {
            return Err(WriterError::InvalidSyncPosition.into());
        }
        let reserved = self.writer.written_position();
        Ok(Pipeline {
            journal: self,
            key: Arc::new(()),
            reserved,
            pending: 0,
            capacity,
            sync: None,
            faulted: false,
        })
    }
}

impl Pipeline {
    pub const fn journal(&self) -> &Journal {
        &self.journal
    }
    pub const fn pending(&self) -> usize {
        self.pending
    }

    pub const fn sync_pending(&self) -> bool {
        self.sync.is_some()
    }

    pub fn is_faulted(&self) -> bool {
        self.faulted || self.journal.is_faulted()
    }

    /// Read/index only the installed prefix while later writes remain reserved.
    /// Building disposable indexes changes no placement or consensus authority.
    /// Cancellation may fence the underlying journal, as for other index jobs.
    pub async fn build_index_snapshot(
        &mut self,
        boundary: crate::JournalIndexBoundary,
        limits: crate::IndexBuildLimits,
    ) -> Result<crate::AsyncJournalIndexSnapshot, crate::JournalIndexError> {
        self.healthy()?;
        self.journal.build_index_snapshot(boundary, limits).await
    }

    fn healthy(&self) -> Result<(), DirectoryError> {
        self.journal.healthy()?;
        if self.faulted {
            Err(WriterError::Faulted.into())
        } else {
            Ok(())
        }
    }

    fn space(&self) -> Result<(), DirectoryError> {
        self.healthy()?;
        if self.pending >= self.capacity {
            Err(WriterError::InvalidSyncPosition.into())
        } else {
            Ok(())
        }
    }

    pub fn begin_group_encoding(&mut self) -> Result<JournalGroupEncoding, DirectoryError> {
        self.healthy()?;
        self.journal.begin_group_encoding()
    }

    /// Placement uses all reservations, not only installed writes. No I/O or
    /// reservation changes occur on rejection; a full-segment group can be reused
    /// after draining and rolling the journal.
    pub fn check(&self, group: &PreencodedJournalGroup) -> Result<(), DirectoryError> {
        self.space()?;
        group
            .bodies
            .validate_decode_limits(self.journal.limits.decode)?;
        for descriptor in &group.operations {
            if descriptor.header.configuration_epoch != self.journal.manifest.configuration_epoch
                || descriptor.header.original_view > self.journal.manifest.promised_view
            {
                return Err(DirectoryError::CurrentMismatch);
            }
        }
        let first = self
            .journal
            .manifest
            .segments
            .last()
            .expect("active")
            .first_group_number;
        if self.reserved.group_number() - (first - 1)
            >= self.journal.limits.decode.max_groups as u64
        {
            return Err(CodecError::GroupExceedsSegment.into());
        }
        let decoded = self
            .reserved
            .decoded_body_bytes()
            .checked_add(group.bodies.decoded_body_bytes())
            .ok_or(CodecError::LengthOverflow)?;
        let limit = self.journal.limits.decode.max_segment_decoded_body_bytes;
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

    /// Reserve placement without submitting I/O. For shared raw bodies the
    /// caller must charge their full backing allocations in `shared_backing_bytes`,
    /// not just visible slices. Use zero for owned encoding. Framing and transfer
    /// descriptors are charged separately. Impossible device charges reject now.
    pub fn prepare(
        &mut self,
        mut group: PreencodedJournalGroup,
        shared_backing_bytes: usize,
    ) -> Result<Prepared, DirectoryError> {
        self.check(&group)?;
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
        let (framing, parts, backing_bytes) = output(group, shared_backing_bytes)?;
        let job = Operation::Write {
            handle: self.journal.writer.output(),
            offset: plan.before.end_offset(),
            data: WriteBuffer::shared(parts, backing_bytes)?,
        };
        self.journal.access.check_data(&job)?;
        self.reserved = plan.after;
        self.pending += 1;
        Ok(Prepared {
            key: self.key.clone(),
            access: self.journal.access.clone(),
            plan,
            job: Some(job),
            framing,
            backing_bytes,
            locations,
        })
    }

    /// Install only this owner's exact next result. Foreign, reversed or failed
    /// completions fence the pipeline. Buffered writes do not become durable.
    pub fn complete(
        &mut self,
        completed: Completed,
    ) -> Result<Vec<OperationLocation>, DirectoryError> {
        self.healthy()?;
        let valid = self.pending != 0
            && Arc::ptr_eq(&self.key, &completed.work.key)
            && self.journal.writer.written_position() == completed.work.plan.before;
        self.faulted = true;
        if !valid {
            return Err(WriterError::InvalidSyncPosition.into());
        }
        let Completed { work, result } = completed;
        self.journal
            .writer
            .complete_prepared(work.plan, result, work.framing.into())?;
        self.pending -= 1;
        self.faulted = false;
        Ok(work.locations)
    }

    /// Barrier only the installed prefix while later reservations remain private.
    pub async fn sync_installed(&mut self) -> Result<WriterPosition, DirectoryError> {
        self.healthy()?;
        self.require_no_sync()?;
        let position = self.journal.writer.written_position();
        self.journal.sync_through(position).await?;
        Ok(position)
    }

    /// Publish restart evidence only for the installed synchronized prefix.
    /// Later physical completions and reservations never enter this evidence.
    pub async fn publish_durable_progress(&mut self) -> Result<(), DirectoryError> {
        self.healthy()?;
        self.require_no_sync()?;
        self.journal.publish_durable_progress().await
    }

    /// Restore mutation only after every reservation completes successfully.
    pub fn finish(self) -> Result<Journal, DirectoryError> {
        self.healthy()?;
        self.require_no_sync()?;
        if self.pending != 0 || self.journal.writer.written_position() != self.reserved {
            return Err(WriterError::InvalidSyncPosition.into());
        }
        Ok(self.journal)
    }
}

impl Prepared {
    pub fn physical_bytes(&self) -> u64 {
        self.plan.after.end_offset() - self.plan.before.end_offset()
    }

    /// Backend owns physical work even when this future or its owner is dropped.
    /// Short writes fail the complete group; recovery resolves uncertain bytes.
    pub async fn write(mut self) -> Completed {
        let length = self.physical_bytes() as usize;
        let result = self
            .access
            .write(
                Class::Data,
                self.job.take().expect("unsubmitted write"),
                length,
            )
            .await;
        Completed { work: self, result }
    }

    /// Coalesce consecutive groups up to a physical byte limit. All writes must
    /// succeed before any result reports success. Bound the input vector by the
    /// originating pipeline's capacity. Device byte shares constrain coalescing
    /// too. Direct I/O requires an aligned byte limit (or no limit).
    pub async fn write_batch(
        mut chunks: Vec<Self>,
        limit: std::num::NonZeroUsize,
    ) -> Vec<Completed> {
        let valid = chunks.windows(2).all(|pair| {
            Arc::ptr_eq(&pair[0].key, &pair[1].key) && pair[0].plan.after == pair[1].plan.before
        });
        let result = if valid {
            write_chunks(&mut chunks, limit.get()).await
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "nonconsecutive journal writes",
            ))
        };
        chunks
            .into_iter()
            .map(|work| Completed {
                work,
                result: result.as_ref().copied().map_err(|error| {
                    error.raw_os_error().map_or_else(
                        || io::Error::new(error.kind(), error.to_string()),
                        io::Error::from_raw_os_error,
                    )
                }),
            })
            .collect()
    }
}

impl Completed {
    pub fn succeeded(&self) -> bool {
        self.result.is_ok()
    }
}

async fn write_chunks(chunks: &mut [Prepared], limit: usize) -> io::Result<()> {
    let mut first = 0;
    while first < chunks.len() {
        let mut end = first;
        let mut length = 0usize;
        let mut backing = 0usize;
        let mut parts = Vec::new();
        while end < chunks.len() {
            let chunk = &chunks[end];
            let additional =
                usize::try_from(chunk.physical_bytes()).map_err(|_| io::ErrorKind::InvalidInput)?;
            if end > first && additional > limit.saturating_sub(length) {
                break;
            }
            let previous = (backing, parts.len());
            length = length
                .checked_add(additional)
                .ok_or(io::ErrorKind::InvalidInput)?;
            backing = backing
                .checked_add(chunk.backing_bytes)
                .ok_or(io::ErrorKind::InvalidInput)?;
            let Some(Operation::Write { data, .. }) = &chunk.job else {
                return Err(io::ErrorKind::InvalidInput.into());
            };
            parts.extend(data.parts().iter().cloned());
            let Some(Operation::Write { handle, .. }) = &chunks[first].job else {
                return Err(io::ErrorKind::InvalidInput.into());
            };
            let candidate = Operation::Write {
                handle: handle.clone(),
                offset: chunks[first].plan.before.end_offset(),
                data: WriteBuffer::shared(parts.clone(), backing)?,
            };
            if let Err(error) = chunks[first].access.check_data(&candidate) {
                if end == first {
                    return Err(error);
                }
                backing = previous.0;
                parts.truncate(previous.1);
                break;
            }
            end += 1;
        }
        let Some(Operation::Write { handle, offset, .. }) = &chunks[first].job else {
            return Err(io::ErrorKind::InvalidInput.into());
        };
        let job = Operation::Write {
            handle: handle.clone(),
            offset: *offset,
            data: WriteBuffer::shared(parts.into_boxed_slice().into_vec(), backing)?,
        };
        chunks[first].access.check_data(&job)?;
        // Release per-group descriptor clones before executing the combined job.
        for chunk in &mut chunks[first..end] {
            chunk.job = None;
        }
        write_bounded(&chunks[first].access, job, backing, limit).await?;
        first = end;
    }
    Ok(())
}

async fn write_bounded(
    access: &Access,
    job: Operation,
    backing: usize,
    limit: usize,
) -> io::Result<()> {
    let Operation::Write {
        handle,
        offset,
        data,
    } = job
    else {
        return Err(io::ErrorKind::InvalidInput.into());
    };
    let mut written = 0usize;
    let mut part = 0usize;
    let mut within = 0usize;
    while written < data.len() {
        let count = limit.min(data.len() - written);
        let mut remaining = count;
        let mut parts = Vec::new();
        while remaining != 0 {
            let source = &data.parts()[part];
            let taken = remaining.min(source.len() - within);
            if taken != 0 {
                parts.push(source.slice(within..within + taken));
            }
            remaining -= taken;
            within += taken;
            if within == source.len() {
                part += 1;
                within = 0;
            }
        }
        let job = Operation::Write {
            handle: handle.clone(),
            offset: offset + written as u64,
            data: WriteBuffer::shared(parts, backing)?,
        };
        access.check_data(&job)?;
        access.write(Class::Data, job, count).await?;
        written += count;
    }
    Ok(())
}

fn output(
    group: PreencodedJournalGroup,
    shared_backing: usize,
) -> Result<(Bytes, Vec<Bytes>, usize), DirectoryError> {
    if group.shared.is_empty() {
        if shared_backing != 0 {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        let bytes = group.bodies.into_owned_bytes();
        let backing = bytes.capacity();
        let framing = Bytes::from(bytes);
        return Ok((framing.clone(), vec![framing], backing));
    }
    let body_bytes = group
        .shared
        .iter()
        .try_fold(0usize, |sum, body| sum.checked_add(body.len()))
        .ok_or(CodecError::LengthOverflow)?;
    if shared_backing < body_bytes {
        return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
    }
    let bytes = group.bodies.into_raw_framing();
    let backing = bytes
        .capacity()
        .checked_add(shared_backing)
        .ok_or(CodecError::LengthOverflow)?;
    let framing = Bytes::from(bytes);
    let count = group.shared.len();
    let mut parts = Vec::with_capacity(
        count
            .checked_mul(3)
            .and_then(|v| v.checked_add(1))
            .ok_or(CodecError::LengthOverflow)?,
    );
    for (index, body) in group.shared.into_iter().enumerate() {
        let start = index * crate::ENTRY_HEADER_BYTES;
        let padding = (8 - body.len() % 8) % 8;
        parts.push(framing.slice(start..start + crate::ENTRY_HEADER_BYTES));
        parts.push(body);
        if padding != 0 {
            parts.push(Bytes::from_static(&[0; 8]).slice(..padding));
        }
    }
    parts.push(framing.slice(count * crate::ENTRY_HEADER_BYTES..));
    // Shared padding is a static allocation, but the I/O contract also requires
    // the declared backing charge to cover complete logical output length.
    let length = parts
        .iter()
        .try_fold(0usize, |sum, part| sum.checked_add(part.len()))
        .ok_or(CodecError::LengthOverflow)?;
    Ok((framing, parts, backing.max(length)))
}

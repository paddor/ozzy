//! Segment state stays on its caller's shard. Every physical effect, including
//! opening and dropping descriptors, belongs to the shared I/O backend.

use super::SegmentState;
use crate::async_files as files;
mod open;
#[cfg(test)]
mod tests;

use super::prepared::WriteBytes;
use crate::{
    BodyEncoding, CanonicalOperation, ChainPosition, DecodeLimits, SEGMENT_HEADER_BYTES,
    SegmentHeader, SegmentWriteMode, WRITE_GROUP_ALIGNMENT, WriterError, WriterPosition,
};
use ozzy_io::{Class, Handle, Local, Operation, WriteBuffer};
use ozzy_journal::{operation::canonical_body_digest, progress::JournalGeneration};
use std::{io, path::PathBuf};

/// Chain position at this segment's start, not a declaration of durability.
#[derive(Debug, Clone, Copy)]
pub struct Start {
    /// Owning journal generation fencing obsolete physical completions.
    pub generation: JournalGeneration,
    /// First expected physical write-group number in this segment.
    pub first_group_number: u64,
    /// Expected canonical operation number and predecessor digest at open.
    pub initial_chain: ChainPosition,
}

/// A segment file path and the backend that runs its jobs. Optional
/// protection retains the caller's group lock through every job.
#[derive(Debug, Clone)]
pub struct File {
    /// Segment file path.
    pub path: PathBuf,
    /// Shard-local backend handle.
    pub io: Local,
    /// Caller lock retained through every job, if any.
    pub protection: Option<Handle>,
}

impl File {
    fn into_access(self) -> (PathBuf, files::Access) {
        (self.path, files::Access::new(self.io, self.protection))
    }
}

/// An open buffered segment file with its path and backend access.
#[derive(Debug)]
pub(crate) struct Opened {
    pub(crate) path: PathBuf,
    pub(crate) access: files::Access,
    pub(crate) buffered: Handle,
}

/// Physical I/O policy. Recovery images and each scratch transfer are bounded.
/// Prepared append bytes must also fit the shard's backend data-byte share.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Maximum physical segment bytes.
    pub max_segment_bytes: u64,
    /// Maximum physical bytes submitted in one transfer.
    pub chunk_bytes: usize,
    /// Request direct I/O with backend-owned aligned staging.
    pub direct: bool,
    /// Explicit physical durability mode for successful writes.
    pub write_mode: SegmentWriteMode,
}

impl Options {
    pub(crate) fn validate(self, io: &Local) -> io::Result<()> {
        self.validate_backend(io.admission().limits(), io.shard())
    }

    /// Validate physical limits before constructing workers or opening files.
    pub fn validate_backend(self, limits: ozzy_io::Limits, shard: usize) -> io::Result<()> {
        limits.validate()?;
        if shard >= limits.shards {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        if self.max_segment_bytes < SEGMENT_HEADER_BYTES as u64
            || usize::try_from(self.max_segment_bytes).is_err()
            || i64::try_from(self.max_segment_bytes).is_err()
            || self.chunk_bytes < WRITE_GROUP_ALIGNMENT
            || !self.chunk_bytes.is_multiple_of(WRITE_GROUP_ALIGNMENT)
            || self.chunk_bytes as u64 > self.max_segment_bytes
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid segment I/O limits",
            ));
        }
        let needed = self.chunk_bytes.checked_mul(2).and_then(|bytes| {
            bytes.checked_add(
                4096 + size_of::<Operation>() + size_of::<Handle>() + size_of::<bytes::Bytes>(),
            )
        });
        let progress = limits.share(shard, Class::Progress)?.bytes;
        if needed.is_none_or(|bytes| bytes > progress) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment transfer exceeds progress share",
            ));
        }
        Ok(())
    }
}

/// Single-owner segment driver. Cancellation after starting a mutation fences
/// further mutations, even if the backend later completes the write. Reads of
/// installed positions remain memory-only. No OS file lives in this object.
#[derive(Debug)]
pub struct Writer {
    state: SegmentState,
    access: files::Access,
    buffered: Handle,
    output: Handle,
    options: Options,
    interrupted: bool,
}

impl Writer {
    pub(crate) const fn state(&self) -> &SegmentState {
        &self.state
    }

    pub(crate) fn take_encode_buffer(&mut self) -> Vec<u8> {
        self.state.take_encode_buffer()
    }

    pub(crate) fn output(&self) -> Handle {
        self.output.clone()
    }

    pub(crate) fn buffered(&self) -> Handle {
        self.buffered.clone()
    }

    pub(crate) fn complete_prepared(
        &mut self,
        plan: super::prepared::WritePlan,
        result: io::Result<()>,
        scratch: Vec<u8>,
    ) -> Result<WriterPosition, WriterError> {
        self.healthy()?;
        self.interrupted = true;
        let written = self.state.complete_write(plan, result)?;
        self.state.encode_buffer = scratch;
        self.state.encode_buffer.clear();
        if self.state.data_sync {
            self.state.complete_sync_result(written, Ok(()))?;
        }
        self.interrupted = false;
        Ok(written)
    }

    pub(crate) fn transfer_buffers_to(&mut self, successor: &mut Self) {
        self.state.transfer_encode_buffers_to(&mut successor.state);
    }

    pub(crate) async fn restore_allocation(&mut self) -> Result<(), WriterError> {
        self.healthy()?;
        if self.access.length(&self.buffered).await? == self.header().capacity() {
            return Ok(());
        }
        self.interrupted = true;
        let result = async {
            self.access
                .done(Operation::Allocate {
                    handle: self.buffered.clone(),
                    offset: 0,
                    length: self.header().capacity(),
                })
                .await?;
            self.access.sync(&self.buffered).await
        }
        .await;
        self.interrupted = result.is_err();
        result.map_err(Into::into)
    }

    /// Validated physical segment header.
    pub const fn header(&self) -> &SegmentHeader {
        self.state.header()
    }
    /// Exact written canonical prefix; this does not prove durability.
    pub const fn written_position(&self) -> WriterPosition {
        self.state.written_position()
    }
    /// Exact matching written prefix covered by a successful data barrier.
    pub const fn durable_position(&self) -> WriterPosition {
        self.state.durable_position()
    }
    /// Whether failure or canceled mutation fences further use.
    pub const fn is_faulted(&self) -> bool {
        self.interrupted || self.state.is_faulted()
    }

    fn healthy(&self) -> Result<(), WriterError> {
        if self.is_faulted() {
            Err(WriterError::Faulted)
        } else {
            Ok(())
        }
    }

    /// Encode using the existing group machinery, then install only this exact
    /// write's completion. Physical writes do not publish group durability.
    pub async fn append(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        encoding: BodyEncoding,
    ) -> Result<WriterPosition, WriterError> {
        self.append_with_limits(operations, encoding, None).await
    }

    pub(crate) async fn append_with_limits(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        encoding: BodyEncoding,
        limits: Option<DecodeLimits>,
    ) -> Result<WriterPosition, WriterError> {
        self.healthy()?;
        let digests: Vec<_> = operations
            .iter()
            .map(|op| canonical_body_digest(op.body))
            .collect();
        self.append_with_limits_and_digests(operations, &digests, encoding, limits)
            .await
    }

    pub(crate) fn reserve_encode_buffer(
        &mut self,
        bytes: usize,
        encoding: BodyEncoding,
    ) -> Result<(), WriterError> {
        self.state.reserve_encode_buffer(bytes, encoding)
    }

    pub(crate) async fn append_with_limits_and_digests(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        digests: &[crate::Digest],
        encoding: BodyEncoding,
        limits: Option<DecodeLimits>,
    ) -> Result<WriterPosition, WriterError> {
        self.healthy()?;
        let prepared = self
            .state
            .preencode_owned_bodies(operations.iter().map(|op| op.body), encoding)?;
        if let Some(limits) = limits {
            prepared.validate_decode_limits(limits)?;
        }
        let (plan, bytes) = self
            .state
            .prepare_owned_encoded(operations, digests, prepared)?;
        let WriteBytes::Contiguous(bytes) = bytes;
        let expected = bytes.len();
        let retained = bytes.capacity();
        let bytes = bytes::Bytes::from(bytes);
        let operation = Operation::Write {
            handle: self.output.clone(),
            offset: plan.before.end_offset(),
            data: WriteBuffer::shared(vec![bytes.clone()], retained)?,
        };
        // Reject an impossible charge before entering uncertain-write state.
        self.access.check_data(&operation)?;
        self.interrupted = true;
        let result = self.access.write(Class::Data, operation, expected).await;
        // Physical completion has released the backend's clone. Recover the
        // existing encoding allocation instead of discarding it after each job.
        self.state.encode_buffer = bytes.into();
        self.state.encode_buffer.clear();
        let result = self.state.complete_write(plan, result);
        self.interrupted = result.is_err();
        result
    }

    /// Establish a barrier through a previously installed position. Newer bytes
    /// may also reach storage, but do not expand this returned evidence.
    pub async fn sync_through(
        &mut self,
        position: WriterPosition,
    ) -> Result<WriterPosition, WriterError> {
        self.healthy()?;
        self.state.validate_sync_position(position)?;
        if position.group_number() <= self.durable_position().group_number() {
            return Ok(position);
        }
        self.interrupted = true;
        let result = if self.state.data_sync {
            Ok(())
        } else {
            self.access.sync(&self.buffered).await
        };
        let result = self.state.complete_sync_result(position, result);
        self.interrupted = result.is_err();
        result
    }

    pub(crate) fn detached_sync(
        &self,
        position: WriterPosition,
    ) -> Result<Option<Operation>, WriterError> {
        self.healthy()?;
        self.state.validate_sync_position(position)?;
        Ok((!self.state.data_sync
            && position.group_number() > self.durable_position().group_number())
        .then(|| Operation::Sync {
            handle: self.buffered.clone(),
            mode: ozzy_io::SyncMode::Data,
        }))
    }

    pub(crate) fn complete_detached_sync(
        &mut self,
        position: WriterPosition,
    ) -> Result<WriterPosition, WriterError> {
        self.healthy()?;
        self.state.complete_sync_result(position, Ok(()))
    }

    /// Zero up to `bytes` of unused capacity after the written prefix, without
    /// changing history. Only legal when every installed group is already
    /// synchronized.
    pub async fn zero_ahead(&mut self, bytes: u64) -> Result<(), WriterError> {
        self.healthy()?;
        if self.written_position() != self.durable_position() {
            return Err(WriterError::InvalidSyncPosition);
        }
        let start = self.written_position().end_offset();
        let end = start.saturating_add(bytes).min(self.header().capacity());
        self.interrupted = true;
        let result = async {
            self.access
                .zero(&self.buffered, start..end, self.options.chunk_bytes)
                .await?;
            self.access.sync(&self.buffered).await
        }
        .await;
        self.interrupted = result.is_err();
        result.map_err(Into::into)
    }

    /// Explicitly close an idle writer through the backend. A canceled mutation
    /// must instead release handles by drop and let backend ownership drain it.
    pub async fn close(self) -> Result<(), WriterError> {
        self.healthy()?;
        if self.options.direct || self.options.write_mode == SegmentWriteMode::DataSync {
            self.access
                .done(Operation::Close {
                    handle: self.output,
                })
                .await?;
        }
        self.access
            .done(Operation::Close {
                handle: self.buffered,
            })
            .await?;
        Ok(())
    }
}

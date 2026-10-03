//! Segment state stays on its caller's shard. Every physical effect, including
//! opening and dropping descriptors, belongs to the shared I/O backend.

use crate::async_files as files;
#[cfg(test)]
mod tests;

use super::{RecoveryValidation, prepared::WriteBytes, recovery};
use crate::{
    BodyEncoding, CanonicalOperation, CanonicalRecoveryRequirements, ChainPosition, CodecError,
    DecodeLimits, SEGMENT_HEADER_BYTES, SegmentHeader, SegmentWriteMode, SegmentWriter,
    WRITE_GROUP_ALIGNMENT, WriterError, WriterPosition, encode_segment_header,
};
use ozzy_io::{Class, Handle, Local, OpenMode, Operation, WriteBuffer};
use ozzy_journal::{operation::canonical_body_digest, progress::JournalGeneration};
use std::{io, path::PathBuf};

/// Chain position at this segment's start, not a declaration of durability.
#[derive(Debug, Clone, Copy)]
pub struct Start {
    pub generation: JournalGeneration,
    pub first_group_number: u64,
    pub initial_chain: ChainPosition,
}

/// Physical I/O policy. Recovery images and each scratch transfer are bounded.
/// Prepared append bytes must also fit the shard's backend data-byte share.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    pub max_segment_bytes: u64,
    pub chunk_bytes: usize,
    pub direct: bool,
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
        if needed.is_none_or(|bytes| bytes > limits.share(shard, Class::Progress).bytes) {
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
    state: SegmentWriter<()>,
    access: files::Access,
    buffered: Handle,
    output: Handle,
    options: Options,
    interrupted: bool,
}

impl Writer {
    pub(crate) const fn state(&self) -> &SegmentWriter<()> {
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

    /// A freshly allocated, exclusively owned successor. Only the directory's
    /// private prepared-file type may supply this handle; never adopt leftovers.
    pub(crate) async fn initialize_prepared(
        path: PathBuf,
        access: files::Access,
        buffered: Handle,
        header: SegmentHeader,
        start: Start,
        options: Options,
    ) -> Result<Self, WriterError> {
        options.validate(&access.io)?;
        if start.first_group_number == 0 {
            return Err(CodecError::InvalidGroupNumber.into());
        }
        if header.capacity() > options.max_segment_bytes
            || access.length(&buffered).await? > header.capacity()
        {
            return Err(WriterError::InvalidSyncPosition);
        }
        access
            .write_all(&buffered, 0, &encode_segment_header(&header))
            .await?;
        access.sync(&buffered).await?;
        let state = SegmentWriter::empty(
            (),
            header,
            start.generation,
            start.first_group_number,
            start.initial_chain,
        );
        Self::finish_open(path, state, access, buffered, options).await
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
    /// Create a new preallocated segment and synchronize its header. The
    /// directory owner must still publish and synchronize its directory entry.
    /// Optional protection retains the caller's group lock through every job.
    pub async fn create(
        path: PathBuf,
        io: Local,
        protection: Option<Handle>,
        header: SegmentHeader,
        start: Start,
        options: Options,
    ) -> Result<Self, WriterError> {
        options.validate(&io)?;
        if start.first_group_number == 0 {
            return Err(CodecError::InvalidGroupNumber.into());
        }
        if header.capacity() > options.max_segment_bytes {
            return Err(WriterError::RecoveryLimit {
                actual: header.capacity(),
                limit: options.max_segment_bytes,
            });
        }
        let access = files::Access { io, protection };
        let buffered = access
            .open(path.clone(), OpenMode::CreateNew, false, false)
            .await?;
        access
            .done(Operation::Allocate {
                handle: buffered.clone(),
                offset: 0,
                length: header.capacity(),
            })
            .await?;
        access
            .write_all(&buffered, 0, &encode_segment_header(&header))
            .await?;
        access.sync(&buffered).await?;
        let state = SegmentWriter::empty(
            (),
            header,
            start.generation,
            start.first_group_number,
            start.initial_chain,
        );
        Self::finish_open(path, state, access, buffered, options).await
    }

    /// Validate the existing bytes before changing a damaged tail. Canonical
    /// requirements control protected-prefix checks and permitted tail repair;
    /// `None` retains the low-level writer's strict structural recovery mode.
    pub async fn recover(
        path: PathBuf,
        io: Local,
        protection: Option<Handle>,
        start: Start,
        options: Options,
        limits: DecodeLimits,
        requirements: Option<CanonicalRecoveryRequirements>,
    ) -> Result<Self, WriterError> {
        options.validate(&io)?;
        let access = files::Access { io, protection };
        let buffered = access
            .open(path.clone(), OpenMode::ReadWrite, false, false)
            .await?;
        let image = access.read_image(&buffered, options).await?;
        let validation = match requirements {
            Some(required) => RecoveryValidation {
                protected: required.protected,
                operation_limits: Some(required.operation_limits),
                configuration_epoch: required.configuration_epoch,
                promised_view: required.promised_view,
                discard_damaged_tail: required.discard_damaged_tail,
            },
            None => RecoveryValidation {
                protected: [None; 2],
                operation_limits: None,
                configuration_epoch: None,
                promised_view: None,
                discard_damaged_tail: false,
            },
        };
        let recovered = recovery::prepare_async(
            &image,
            start.generation,
            start.first_group_number,
            start.initial_chain,
            limits,
            validation,
        )
        .await?;
        if recovered.writer.header().capacity() > options.max_segment_bytes {
            return Err(WriterError::RecoveryLimit {
                actual: recovered.writer.header().capacity(),
                limit: options.max_segment_bytes,
            });
        }
        drop(image);
        if let Some(length) = recovered.truncate {
            access
                .done(Operation::SetLength {
                    handle: buffered.clone(),
                    length,
                })
                .await?;
        }
        access
            .zero(&buffered, recovered.zero, options.chunk_bytes)
            .await?;
        access.sync(&buffered).await?;
        Self::finish_open(path, recovered.writer, access, buffered, options).await
    }

    async fn finish_open(
        path: PathBuf,
        mut state: SegmentWriter<()>,
        access: files::Access,
        buffered: Handle,
        options: Options,
    ) -> Result<Self, WriterError> {
        let data_sync = options.write_mode == SegmentWriteMode::DataSync;
        let output = if options.direct || data_sync {
            access
                .open(path, OpenMode::ReadWrite, options.direct, data_sync)
                .await?
        } else {
            buffered.clone()
        };
        state.data_sync = data_sync;
        Ok(Self {
            state,
            access,
            buffered,
            output,
            options,
            interrupted: false,
        })
    }

    pub const fn header(&self) -> &SegmentHeader {
        self.state.header()
    }
    pub const fn written_position(&self) -> WriterPosition {
        self.state.written_position()
    }
    pub const fn durable_position(&self) -> WriterPosition {
        self.state.durable_position()
    }
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
        let WriteBytes::Contiguous(bytes) = bytes else {
            unreachable!("owned encoding is contiguous")
        };
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

    /// Prepare unused extents without changing history. Only legal when every
    /// installed group is already synchronized.
    pub async fn zero_remainder(&mut self) -> Result<(), WriterError> {
        self.healthy()?;
        if self.written_position() != self.durable_position() {
            return Err(WriterError::InvalidSyncPosition);
        }
        self.interrupted = true;
        let result = async {
            self.access
                .zero(
                    &self.buffered,
                    self.written_position().end_offset()..self.header().capacity(),
                    self.options.chunk_bytes,
                )
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

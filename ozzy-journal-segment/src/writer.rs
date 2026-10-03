use std::fs::File;
use std::io::{self, IoSlice, Read, Seek, SeekFrom};
use std::sync::Arc;

use ozzy_journal::operation::{OperationCodecError, OperationLimits, canonical_body_digest};
use ozzy_journal::progress::JournalGeneration;
use thiserror::Error;

use crate::codec::{
    BodyEncodeScratch, FinalizedGroup, INLINE_GROUP_OPERATIONS, PreparedGroupBodies,
    SegmentDigestBuilder, finalize_group_bodies, prepare_group_bodies, prepare_raw_group_extents,
};
use crate::{
    BodyEncoding, CanonicalOperation, ChainPosition, CodecError, DecodeLimits, Digest,
    SEGMENT_HEADER_BYTES, SegmentHeader, TailState, encode_segment_header, scan_segment,
};

pub(crate) mod asynchronous;
#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod data_sync_tests;
pub(crate) mod direct;
pub(crate) mod extents;
pub(crate) mod prepared;
mod recovery;

/// Active-segment write completion policy, independent of record confirmation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SegmentWriteMode {
    /// Writes complete in the OS page cache. Explicit flush still synchronizes.
    Buffered,
    /// Every write supplies data-integrity durability before returning.
    #[default]
    DataSync,
}

/// Blocking random-access operations required by one segment writer.
///
/// Implementations are exclusively owned by a dedicated journal worker. They
/// must not run on an OMQ or asynchronous executor thread.
pub trait SegmentIo: std::fmt::Debug + Send {
    fn file_len(&mut self) -> io::Result<u64>;
    fn read_all(&mut self) -> io::Result<Vec<u8>>;
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<usize>;
    /// Write an ordered prefix of the supplied slices at an explicit position.
    /// The default preserves portable backends; file-backed Linux uses pwritev.
    fn write_vectored_at(&mut self, offset: u64, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
        match buffers.iter().find(|buffer| !buffer.is_empty()) {
            Some(buffer) => self.write_at(offset, buffer),
            None => Ok(0),
        }
    }
    fn set_len(&mut self, len: u64) -> io::Result<()>;
    fn sync_data(&mut self) -> io::Result<()>;
    /// Start writeback of written bytes without waiting. This establishes no
    /// durability; backends without such a hint ignore it.
    fn start_writeback(&mut self, _range: std::ops::Range<u64>) -> io::Result<()> {
        Ok(())
    }
}

impl SegmentIo for File {
    fn file_len(&mut self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }

    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        self.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        self.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        file_write_at(self, offset, bytes)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn write_vectored_at(&mut self, offset: u64, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
        rustix::io::pwritev(self, buffers, offset).map_err(Into::into)
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        File::set_len(self, len)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        File::sync_data(self)
    }

    fn start_writeback(&mut self, range: std::ops::Range<u64>) -> io::Result<()> {
        crate::directory::start_writeback_range(self, range)
    }
}

/// One open segment file shared by its writer and in-flight write jobs. A job
/// gets an `Arc` clone: no descriptor duplication and no close per write.
impl SegmentIo for std::sync::Arc<File> {
    fn file_len(&mut self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }

    fn read_all(&mut self) -> io::Result<Vec<u8>> {
        let mut file: &File = self;
        file.seek(SeekFrom::Start(0))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<usize> {
        shared_write_at(self, offset, bytes)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn write_vectored_at(&mut self, offset: u64, buffers: &[IoSlice<'_>]) -> io::Result<usize> {
        rustix::io::pwritev(&**self, buffers, offset).map_err(Into::into)
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        File::set_len(self, len)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        File::sync_data(self)
    }

    fn start_writeback(&mut self, range: std::ops::Range<u64>) -> io::Result<()> {
        crate::directory::start_writeback_range(self, range)
    }
}

#[cfg(any(unix, windows))]
fn shared_write_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<usize> {
    file_write_at(file, offset, bytes)
}

#[cfg(not(any(unix, windows)))]
fn shared_write_at(_file: &File, _offset: u64, _bytes: &[u8]) -> io::Result<usize> {
    Err(io::ErrorKind::Unsupported.into())
}

#[cfg(unix)]
fn file_write_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, bytes, offset)
}

#[cfg(windows)]
fn file_write_at(file: &File, offset: u64, bytes: &[u8]) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, bytes, offset)
}

#[cfg(not(any(unix, windows)))]
fn file_write_at(file: &mut File, offset: u64, bytes: &[u8]) -> io::Result<usize> {
    file.seek(SeekFrom::Start(offset))?;
    std::io::Write::write(file, bytes)
}

/// Exact complete prefix named by a write or synchronization result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriterPosition {
    generation: JournalGeneration,
    segment_id: u64,
    group_number: u64,
    end_offset: u64,
    decoded_body_bytes: usize,
    next_chain: ChainPosition,
}

/// Semantic checks required while reopening one active segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CanonicalRecoveryRequirements {
    pub operation_limits: OperationLimits,
    pub protected: [Option<ChainPosition>; 2],
    pub configuration_epoch: Option<u64>,
    pub promised_view: Option<u64>,
    /// End the log at the first group that fails to decode and zero the rest,
    /// provided the valid prefix holds every protected position. Only for
    /// stores whose protected positions cover every durably confirmed
    /// operation, and whose segments begin with no protected position when a
    /// sealed predecessor already holds them all.
    pub discard_damaged_tail: bool,
}

#[derive(Debug, Clone, Copy)]
struct RecoveryValidation {
    protected: [Option<ChainPosition>; 2],
    operation_limits: Option<OperationLimits>,
    configuration_epoch: Option<u64>,
    promised_view: Option<u64>,
    discard_damaged_tail: bool,
}

impl WriterPosition {
    pub const fn generation(self) -> JournalGeneration {
        self.generation
    }

    pub const fn segment_id(self) -> u64 {
        self.segment_id
    }

    pub const fn group_number(self) -> u64 {
        self.group_number
    }

    pub const fn end_offset(self) -> u64 {
        self.end_offset
    }

    pub const fn decoded_body_bytes(self) -> usize {
        self.decoded_body_bytes
    }

    pub const fn next_chain(self) -> ChainPosition {
        self.next_chain
    }
}

/// Single-owner writer for one initialized physical segment.
#[derive(Debug)]
pub struct SegmentWriter<I> {
    io: I,
    generation: JournalGeneration,
    header: SegmentHeader,
    written: WriterPosition,
    durable: WriterPosition,
    next_group_number: u64,
    segment_digest: SegmentDigestBuilder,
    encode_buffer: Vec<u8>,
    body_encode_scratch: Option<BodyEncodeScratch>,
    data_sync: bool,
    /// Separate `O_DIRECT` descriptor for write jobs. Headers, zeroing and
    /// recovery keep using `io`.
    direct: Option<Arc<File>>,
    faulted: bool,
}

impl<I> SegmentWriter<I> {
    /// Initialize an empty, exclusively owned target and synchronize its header.
    ///
    /// Directory-entry synchronization and physical allocation are responsibilities
    /// of `GroupDirectory`. This low-level method does not make a segment eligible
    /// in a manifest.
    pub fn initialize(
        io: I,
        header: SegmentHeader,
        generation: JournalGeneration,
    ) -> Result<Self, WriterError>
    where
        I: SegmentIo,
    {
        Self::initialize_at(io, header, generation, 1, ChainPosition::GENESIS)
    }

    /// Initialize a rolled segment at an existing physical/logical chain position.
    pub fn initialize_at(
        io: I,
        header: SegmentHeader,
        generation: JournalGeneration,
        first_group_number: u64,
        initial_chain: ChainPosition,
    ) -> Result<Self, WriterError>
    where
        I: SegmentIo,
    {
        Self::initialize_at_inner(
            io,
            header,
            generation,
            first_group_number,
            initial_chain,
            true,
            0,
        )
    }

    /// Initialize a freshly allocated target whose zero contents the directory owns.
    /// Existing orphan contents must be checked before calling this method.
    pub(crate) fn initialize_allocated_at(
        io: I,
        header: SegmentHeader,
        generation: JournalGeneration,
        first_group_number: u64,
        initial_chain: ChainPosition,
        synchronize_header: bool,
    ) -> Result<Self, WriterError>
    where
        I: SegmentIo,
    {
        let allocated_bytes = header.capacity();
        Self::initialize_at_inner(
            io,
            header,
            generation,
            first_group_number,
            initial_chain,
            synchronize_header,
            allocated_bytes,
        )
    }

    fn initialize_at_inner(
        mut io: I,
        header: SegmentHeader,
        generation: JournalGeneration,
        first_group_number: u64,
        initial_chain: ChainPosition,
        synchronize_header: bool,
        expected_bytes: u64,
    ) -> Result<Self, WriterError>
    where
        I: SegmentIo,
    {
        if first_group_number == 0 {
            return Err(CodecError::InvalidGroupNumber.into());
        }
        let length = io.file_len()?;
        if length != expected_bytes {
            return Err(WriterError::NonemptyTarget(length));
        }
        let encoded = encode_segment_header(&header);
        write_fully(&mut io, 0, &encoded)?;
        if synchronize_header {
            io.sync_data()?;
        }
        Ok(Self::empty(
            io,
            header,
            generation,
            first_group_number,
            initial_chain,
        ))
    }

    /// Reopen and validate a segment, repairing only an EOF-truncated final group.
    ///
    /// `max_segment_bytes` bounds allocation before the persisted header is read.
    /// Other corruption is returned without modifying the file.
    pub fn recover(
        io: I,
        generation: JournalGeneration,
        first_group_number: u64,
        initial_chain: ChainPosition,
        limits: DecodeLimits,
        max_segment_bytes: u64,
    ) -> Result<Self, WriterError>
    where
        I: SegmentIo,
    {
        Self::recover_inner(
            io,
            generation,
            first_group_number,
            initial_chain,
            limits,
            max_segment_bytes,
            RecoveryValidation {
                protected: [None; 2],
                operation_limits: None,
                configuration_epoch: None,
                promised_view: None,
                discard_damaged_tail: false,
            },
        )
    }

    /// Reopen while requiring one manifest-protected logical prefix.
    pub fn recover_protecting(
        io: I,
        generation: JournalGeneration,
        first_group_number: u64,
        initial_chain: ChainPosition,
        limits: DecodeLimits,
        max_segment_bytes: u64,
        protected: ChainPosition,
    ) -> Result<Self, WriterError>
    where
        I: SegmentIo,
    {
        Self::recover_inner(
            io,
            generation,
            first_group_number,
            initial_chain,
            limits,
            max_segment_bytes,
            RecoveryValidation {
                protected: [Some(protected), None],
                operation_limits: None,
                configuration_epoch: None,
                promised_view: None,
                discard_damaged_tail: false,
            },
        )
    }

    /// Reopen after validating every typed canonical operation body.
    pub fn recover_canonical(
        io: I,
        generation: JournalGeneration,
        first_group_number: u64,
        initial_chain: ChainPosition,
        limits: DecodeLimits,
        max_segment_bytes: u64,
        requirements: CanonicalRecoveryRequirements,
    ) -> Result<Self, WriterError>
    where
        I: SegmentIo,
    {
        Self::recover_inner(
            io,
            generation,
            first_group_number,
            initial_chain,
            limits,
            max_segment_bytes,
            RecoveryValidation {
                protected: requirements.protected,
                operation_limits: Some(requirements.operation_limits),
                configuration_epoch: requirements.configuration_epoch,
                promised_view: requirements.promised_view,
                discard_damaged_tail: requirements.discard_damaged_tail,
            },
        )
    }

    fn recover_inner(
        mut io: I,
        generation: JournalGeneration,
        first_group_number: u64,
        initial_chain: ChainPosition,
        limits: DecodeLimits,
        max_segment_bytes: u64,
        validation: RecoveryValidation,
    ) -> Result<Self, WriterError>
    where
        I: SegmentIo,
    {
        let length = io.file_len()?;
        if length > max_segment_bytes {
            return Err(WriterError::RecoveryLimit {
                actual: length,
                limit: max_segment_bytes,
            });
        }
        let image = io.read_all()?;
        let recovery = recovery::prepare(
            io,
            &image,
            generation,
            first_group_number,
            initial_chain,
            limits,
            validation,
        )?;
        let mut writer = recovery.writer;
        if let Some(length) = recovery.truncate {
            writer.io.set_len(length)?;
        }
        if !recovery.zero.is_empty() {
            zero_damaged_tail(&mut writer.io, &image, recovery.zero.start)?;
        }
        // Complete groups left in page cache are not recovery durability.
        writer.io.sync_data()?;
        Ok(writer)
    }

    /// Append one complete sealed group without claiming durability.
    pub fn append(
        &mut self,
        operations: &[CanonicalOperation<'_>],
    ) -> Result<WriterPosition, WriterError>
    where
        I: SegmentIo,
    {
        self.append_with_body_encoding(operations, BodyEncoding::Raw)
    }

    /// Append one complete group with one independently decodable body policy.
    pub fn append_with_body_encoding(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        encoding: BodyEncoding,
    ) -> Result<WriterPosition, WriterError>
    where
        I: SegmentIo,
    {
        self.require_healthy()?;
        // Public callers supply bytes, never trusted digests. Keep hashing here,
        // but reuse the same encoder/context as the internal prehashed path.
        let digests: smallvec::SmallVec<[Digest; INLINE_GROUP_OPERATIONS]> = operations
            .iter()
            .map(|operation| canonical_body_digest(operation.body))
            .collect();
        self.append_with_body_encoding_and_digests(operations, &digests, encoding)
    }

    pub(crate) fn append_with_body_encoding_and_digests(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        body_digests: &[Digest],
        encoding: BodyEncoding,
    ) -> Result<WriterPosition, WriterError>
    where
        I: SegmentIo,
    {
        let prepared =
            self.prepare_group_bodies(operations.iter().map(|operation| operation.body), encoding)?;
        self.append_prepared_with_digests(operations, body_digests, prepared)
    }

    pub(crate) fn prepare_group_bodies<'a>(
        &mut self,
        bodies: impl Iterator<Item = &'a [u8]> + Clone,
        encoding: BodyEncoding,
    ) -> Result<PreparedGroupBodies, WriterError> {
        self.require_healthy()?;
        if encoding == BodyEncoding::Raw {
            return Ok(prepare_raw_group_extents(
                bodies,
                std::mem::take(&mut self.encode_buffer),
            )?);
        }
        Ok(prepare_group_bodies(
            bodies,
            encoding,
            std::mem::take(&mut self.encode_buffer),
            self.body_encode_scratch
                .as_mut()
                .ok_or(WriterError::InvalidSyncPosition)?,
        )?)
    }

    pub(crate) fn append_prepared_with_digests(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        body_digests: &[Digest],
        mut prepared: PreparedGroupBodies,
    ) -> Result<WriterPosition, WriterError>
    where
        I: SegmentIo,
    {
        self.require_healthy()?;
        let finalized = finalize_group_bodies(
            &self.header,
            self.next_group_number,
            self.written.end_offset,
            self.written.next_chain,
            operations,
            body_digests,
            &mut prepared,
        );
        let finalized = match finalized {
            Ok(finalized) => finalized,
            Err(error) => {
                self.restore_encode_buffer(prepared);
                return Err(error.into());
            }
        };
        let result = self.append_extents(operations, &prepared, finalized);
        self.restore_encode_buffer(prepared);
        result
    }

    fn restore_encode_buffer(&mut self, encoded: PreparedGroupBodies) {
        self.encode_buffer = encoded.into_bytes();
        self.encode_buffer.clear();
    }

    fn append_extents(
        &mut self,
        operations: &[CanonicalOperation<'_>],
        prepared: &PreparedGroupBodies,
        finalized: FinalizedGroup,
    ) -> Result<WriterPosition, WriterError>
    where
        I: SegmentIo,
    {
        self.append_slices(
            prepared.extents(operations),
            prepared.decoded_body_bytes(),
            finalized,
        )
    }

    fn append_slices<'a>(
        &mut self,
        slices: impl Iterator<Item = &'a [u8]>,
        additional_body_bytes: usize,
        finalized: FinalizedGroup,
    ) -> Result<WriterPosition, WriterError>
    where
        I: SegmentIo,
    {
        let following_group_number = self
            .next_group_number
            .checked_add(1)
            .ok_or(WriterError::GroupNumberExhausted)?;
        let decoded_body_bytes = self
            .written
            .decoded_body_bytes
            .checked_add(additional_body_bytes)
            .ok_or(WriterError::Codec(CodecError::LengthOverflow))?;
        if let Err(error) = extents::write_extents(&mut self.io, self.written.end_offset, slices) {
            self.faulted = true;
            return Err(error.into());
        }
        self.segment_digest.push(finalized.digest);
        #[cfg(feature = "storage-metrics")]
        crate::write_metrics::ENCODED_GROUP_BYTES.fetch_add(
            finalized.end_offset - self.written.end_offset,
            std::sync::atomic::Ordering::Relaxed,
        );
        let position = WriterPosition {
            generation: self.generation,
            segment_id: self.header.segment_id(),
            group_number: self.next_group_number,
            end_offset: finalized.end_offset,
            decoded_body_bytes,
            next_chain: finalized.next_chain,
        };
        self.next_group_number = following_group_number;
        self.written = position;
        Ok(position)
    }

    /// Freeze the exact currently complete prefix for a later barrier.
    pub const fn begin_sync(&self) -> WriterPosition {
        self.written
    }

    /// Synchronize storage, but return evidence only through the frozen position.
    pub fn sync_through(&mut self, position: WriterPosition) -> Result<WriterPosition, WriterError>
    where
        I: SegmentIo,
    {
        self.validate_sync_position(position)?;
        if position.group_number <= self.durable.group_number {
            return Ok(position);
        }
        let result = if self.data_sync {
            Ok(())
        } else {
            self.io.sync_data()
        };
        self.complete_sync_result(position, result)
    }

    pub(crate) fn validate_sync_position(
        &self,
        position: WriterPosition,
    ) -> Result<(), WriterError> {
        self.require_healthy()?;
        if position.generation != self.generation
            || position.segment_id != self.header.segment_id()
            || position.group_number > self.written.group_number
            || position.end_offset > self.written.end_offset
        {
            return Err(WriterError::InvalidSyncPosition);
        }
        Ok(())
    }

    // Only the storage drivers may install physical evidence. A public caller
    // cannot manufacture successful storage evidence.
    pub(crate) fn complete_sync_result(
        &mut self,
        position: WriterPosition,
        result: io::Result<()>,
    ) -> Result<WriterPosition, WriterError> {
        self.validate_sync_position(position)?;
        if let Err(error) = result {
            self.faulted = true;
            return Err(error.into());
        }
        if position.group_number > self.durable.group_number {
            self.durable = position;
        }
        Ok(position)
    }

    pub const fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub const fn written_position(&self) -> WriterPosition {
        self.written
    }

    pub const fn durable_position(&self) -> WriterPosition {
        self.durable
    }

    pub(crate) fn structural_digest(&self) -> Digest {
        self.segment_digest.finish()
    }

    pub(crate) fn transfer_encode_buffers_to(&mut self, successor: &mut Self) {
        successor.encode_buffer = std::mem::take(&mut self.encode_buffer);
        successor.body_encode_scratch = std::mem::take(&mut self.body_encode_scratch);
    }

    pub const fn is_faulted(&self) -> bool {
        self.faulted
    }

    pub(crate) fn fence(&mut self) {
        self.faulted = true;
    }

    /// Reserve physical output and codec scratch before serving appends.
    ///
    /// `capacity` covers raw bodies, entry headers, seal, and write alignment.
    /// Both public and internal append paths reuse these buffers across groups
    /// and segment rolls. Descriptor scratch stays inline through 64 operations;
    /// larger groups may allocate.
    pub fn reserve_encode_buffer(
        &mut self,
        capacity: usize,
        encoding: BodyEncoding,
    ) -> Result<(), WriterError> {
        let additional = capacity.saturating_sub(self.encode_buffer.len());
        self.encode_buffer
            .try_reserve_exact(additional)
            .map_err(|_| WriterError::EncodeBufferAllocation)?;
        self.body_encode_scratch
            .as_mut()
            .ok_or(WriterError::InvalidSyncPosition)?
            .reserve(capacity, encoding)?;
        Ok(())
    }

    /// Consume the writer, including after a fault, for shutdown or recovery.
    pub fn into_inner(self) -> I {
        self.io
    }

    fn empty(
        io: I,
        header: SegmentHeader,
        generation: JournalGeneration,
        next_group_number: u64,
        next_chain: ChainPosition,
    ) -> Self {
        let position = WriterPosition {
            generation,
            segment_id: header.segment_id(),
            group_number: next_group_number - 1,
            end_offset: SEGMENT_HEADER_BYTES as u64,
            decoded_body_bytes: 0,
            next_chain,
        };
        Self {
            segment_digest: SegmentDigestBuilder::new(&header),
            io,
            generation,
            header,
            written: position,
            durable: position,
            next_group_number,
            encode_buffer: Vec::new(),
            body_encode_scratch: Some(BodyEncodeScratch::default()),
            data_sync: false,
            direct: None,
            faulted: false,
        }
    }

    fn require_healthy(&self) -> Result<(), WriterError> {
        if self.faulted {
            Err(WriterError::Faulted)
        } else {
            Ok(())
        }
    }
}

impl SegmentWriter<Arc<File>> {
    // Only this method may enable the barrier shortcut. Every subsequent write
    // uses the O_DSYNC descriptor; partial/failed writes still fence the writer.
    pub(crate) fn set_write_mode(
        &mut self,
        path: &std::path::Path,
        mode: SegmentWriteMode,
    ) -> io::Result<()> {
        let enabled = mode == SegmentWriteMode::DataSync;
        if enabled == self.data_sync {
            return Ok(());
        }
        let result = self.reopen_write_mode(path, enabled);
        self.faulted |= result.is_err();
        result
    }

    fn reopen_write_mode(&mut self, path: &std::path::Path, enabled: bool) -> io::Result<()> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            use std::os::unix::fs::OpenOptionsExt;
            // rustix 1.1.4's linux_raw OFlags::DSYNC aliases O_SYNC. Use the
            // platform constant so this does not request full metadata sync.
            let flags = libc::O_DSYNC;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(if enabled { flags } else { 0 })
                .open(path)?;
            // Cover earlier buffered groups before enabling the shortcut.
            // Initialization/recovery already synchronized their own prefix.
            if enabled && self.written != self.durable {
                self.io.sync_data()?;
            }
            self.io = Arc::new(file);
            self.data_sync = enabled;
            if self.direct.is_some() {
                self.direct = Some(direct::open(path, enabled)?);
            }
            Ok(())
        }
        #[cfg(not(any(target_os = "linux", target_os = "android")))]
        {
            let _ = (path, enabled);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "O_DSYNC requires Linux",
            ))
        }
    }

    pub const fn data_sync(&self) -> bool {
        self.data_sync
    }

    /// Route write jobs through a separate `O_DIRECT` descriptor on `path`, or
    /// back through `io`. It follows later `set_write_mode` changes. A failure
    /// fences the writer.
    pub(crate) fn set_direct(&mut self, path: &std::path::Path, enabled: bool) -> io::Result<()> {
        let result = if enabled {
            direct::open(path, self.data_sync).map(Some)
        } else {
            Ok(None)
        };
        match result {
            Ok(handle) => {
                self.direct = handle;
                Ok(())
            }
            Err(error) => {
                self.faulted = true;
                Err(error)
            }
        }
    }

    /// Whether write jobs use `O_DIRECT`.
    pub const fn is_direct(&self) -> bool {
        self.direct.is_some()
    }

    /// The descriptor headers, zeroing and recovery use.
    #[cfg(test)]
    pub(crate) fn buffered_io(&self) -> &File {
        &self.io
    }

    /// Overwrite the unused remainder after the durable end with `zeros`, one
    /// chunk at a time, and synchronize it. Later synchronized writes then land
    /// on written blocks and skip the filesystem's unwritten-extent conversion.
    /// Recovery reads an all-zero remainder as unused, so this changes no
    /// recoverable state. Only while nothing unsynchronized is written.
    pub fn zero_remainder(&mut self, zeros: &[u8]) -> Result<(), WriterError> {
        self.require_healthy()?;
        debug_assert!(zeros.iter().all(|&byte| byte == 0));
        if self.written != self.durable || zeros.is_empty() {
            return Err(WriterError::InvalidSyncPosition);
        }
        let capacity = self.header.capacity();
        let mut offset = self.durable.end_offset;
        while offset < capacity {
            let length = usize::try_from(capacity - offset)
                .map_or(zeros.len(), |rest| rest.min(zeros.len()));
            let mut written = 0;
            while written < length {
                let count = self
                    .io
                    .write_at(offset + written as u64, &zeros[written..length])?;
                if count == 0 {
                    return Err(io::Error::from(io::ErrorKind::WriteZero).into());
                }
                written += count;
            }
            offset += length as u64;
        }
        self.io.sync_data()?;
        Ok(())
    }
}

/// Segment writer initialization, recovery, or I/O failure.
#[derive(Debug, Error)]
pub enum WriterError {
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Operation(#[from] OperationCodecError),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("segment writer is faulted")]
    Faulted,
    #[error("new segment target contains {0} bytes")]
    NonemptyTarget(u64),
    #[error("segment recovery image is {actual} bytes; limit is {limit}")]
    RecoveryLimit { actual: u64, limit: u64 },
    #[error("sync position does not belong to the complete writer prefix")]
    InvalidSyncPosition,
    #[error("physical group number space exhausted")]
    GroupNumberExhausted,
    #[error("physical group encode buffer allocation failed")]
    EncodeBufferAllocation,
    #[error("recovered journal does not contain protected operation prefix {0}")]
    ProtectedPrefixMismatch(u64),
    #[error("operation {op_number} has configuration epoch {actual}; expected {expected}")]
    ConfigurationMismatch {
        op_number: u64,
        actual: u64,
        expected: u64,
    },
    #[error("operation {op_number} has view {actual} beyond promise {promised}")]
    ViewBeyondPromise {
        op_number: u64,
        actual: u64,
        promised: u64,
    },
}

/// Overwrite `image` from `start` through its last nonzero 4 KiB block with zeros.
fn zero_damaged_tail<I: SegmentIo>(io: &mut I, image: &[u8], start: u64) -> io::Result<()> {
    const CHUNK: usize = 1024 * 1024;
    let start = usize::try_from(start).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    let end = damaged_tail_end(image, start);
    let zeros = vec![0; (end - start).min(CHUNK)];
    let mut offset = start;
    while offset < end {
        let length = (end - offset).min(CHUNK);
        write_fully(io, offset as u64, &zeros[..length])?;
        offset += length;
    }
    Ok(())
}

fn write_fully<I: SegmentIo>(io: &mut I, mut offset: u64, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        match io.write_at(offset, bytes) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(written) => {
                if written > bytes.len() {
                    return Err(io::ErrorKind::InvalidData.into());
                }
                offset = offset
                    .checked_add(written as u64)
                    .ok_or(io::ErrorKind::InvalidInput)?;
                bytes = &bytes[written..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn damaged_tail_end(image: &[u8], start: usize) -> usize {
    const BLOCK: usize = crate::codec::WRITE_GROUP_ALIGNMENT;
    let mut end = image.len();
    while end > start {
        let block = ((end - 1) / BLOCK * BLOCK).max(start);
        if image[block..end].iter().any(|byte| *byte != 0) {
            break;
        }
        end = block;
    }
    end
}

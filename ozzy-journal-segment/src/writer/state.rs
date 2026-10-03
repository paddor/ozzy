//! Segment positions, encoding scratch, and matching physical completion checks.

use super::{
    BodyEncodeScratch, BodyEncoding, ChainPosition, Digest, JournalGeneration, PreparedGroupBodies,
    SEGMENT_HEADER_BYTES, SegmentDigestBuilder, SegmentHeader, WriterError, WriterPosition, io,
    prepare_group_bodies, prepare_raw_group_extents,
};

/// Sans-I/O segment state shared by asynchronous and blocking file executors.
#[derive(Debug)]
pub struct SegmentState {
    pub(super) generation: JournalGeneration,
    pub(super) header: SegmentHeader,
    pub(super) written: WriterPosition,
    pub(super) durable: WriterPosition,
    pub(super) next_group_number: u64,
    pub(super) segment_digest: SegmentDigestBuilder,
    pub(super) encode_buffer: Vec<u8>,
    pub(super) body_encode_scratch: Option<BodyEncodeScratch>,
    pub(super) data_sync: bool,
    pub(super) faulted: bool,
}

impl SegmentState {
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

    pub(super) fn restore_encode_buffer(&mut self, encoded: PreparedGroupBodies) {
        self.encode_buffer = encoded.into_bytes();
        self.encode_buffer.clear();
    }

    /// Freeze the exact currently complete prefix for a later barrier.
    pub const fn begin_sync(&self) -> WriterPosition {
        self.written
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

    // Only storage drivers may install physical evidence.
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

    /// Validated physical segment header.
    pub const fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Exact written canonical prefix; this does not prove durability.
    pub const fn written_position(&self) -> WriterPosition {
        self.written
    }

    /// Exact matching written prefix covered by a successful data barrier.
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

    /// Whether failure or canceled mutation fences further use.
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

    pub(super) fn empty(
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
            generation,
            header,
            written: position,
            durable: position,
            next_group_number,
            encode_buffer: Vec::new(),
            body_encode_scratch: Some(BodyEncodeScratch::default()),
            data_sync: false,
            faulted: false,
        }
    }

    pub(super) fn require_healthy(&self) -> Result<(), WriterError> {
        if self.faulted {
            Err(WriterError::Faulted)
        } else {
            Ok(())
        }
    }

    /// Whether successful writes already carry their required data barrier.
    pub const fn data_sync(&self) -> bool {
        self.data_sync
    }
}

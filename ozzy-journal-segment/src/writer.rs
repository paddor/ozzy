use std::io::{self};

use ozzy_journal::operation::{OperationCodecError, OperationLimits};
use ozzy_journal::progress::JournalGeneration;
use thiserror::Error;

use crate::codec::{
    BodyEncodeScratch, FinalizedGroup, PreparedGroupBodies, SegmentDigestBuilder,
    finalize_group_bodies,
};
use crate::{
    BodyEncoding, CanonicalOperation, ChainPosition, CodecError, DecodeLimits, Digest,
    SEGMENT_HEADER_BYTES, SegmentHeader, TailState,
};

pub(crate) mod asynchronous;
pub(crate) mod prepared;
mod recovery;
mod state;
pub use state::SegmentState;

/// Active-segment write completion policy, independent of record confirmation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SegmentWriteMode {
    /// Writes complete in the OS page cache. Explicit flush still synchronizes.
    Buffered,
    /// Every write supplies data-integrity durability before returning.
    #[default]
    DataSync,
}

#[cfg(not(any(unix, windows)))]
fn shared_write_at(_file: &File, _offset: u64, _bytes: &[u8]) -> io::Result<usize> {
    Err(io::ErrorKind::Unsupported.into())
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
    /// Canonical operation decoding and resource bounds.
    pub operation_limits: OperationLimits,
    /// Exact prefixes whose integrity must survive recovery.
    pub protected: [Option<ChainPosition>; 2],
    /// Selected membership/configuration epoch.
    pub configuration_epoch: Option<u64>,
    /// Promised election view constraining retained history and authority.
    pub promised_view: Option<u64>,
    /// End the log at the first group that fails to decode and invalidate
    /// later groups of this use, provided the valid prefix holds every
    /// protected position. Only for stores whose protected positions cover
    /// every durably confirmed operation, and whose segments begin with no
    /// protected position when a sealed predecessor already holds them all.
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
    /// Journal-owner generation fencing these captured bytes or completions.
    pub const fn generation(self) -> JournalGeneration {
        self.generation
    }

    /// Physical segment identity.
    pub const fn segment_id(self) -> u64 {
        self.segment_id
    }

    /// Consecutive physical write-group number.
    pub const fn group_number(self) -> u64 {
        self.group_number
    }

    /// Exclusive physical byte offset after this write group.
    pub const fn end_offset(self) -> u64 {
        self.end_offset
    }

    /// Combined canonical body bytes after physical decoding.
    pub const fn decoded_body_bytes(self) -> usize {
        self.decoded_body_bytes
    }

    /// Next canonical operation number and predecessor digest.
    pub const fn next_chain(self) -> ChainPosition {
        self.next_chain
    }
}

/// Segment writer initialization, recovery, or I/O failure.
#[derive(Debug, Error)]
pub enum WriterError {
    #[error(transparent)]
    /// Physical segment framing or integrity validation failed.
    Codec(#[from] CodecError),
    #[error(transparent)]
    /// Canonical operation-body validation failed.
    Operation(#[from] OperationCodecError),
    #[error(transparent)]
    /// A physical file operation failed.
    Io(#[from] io::Error),
    #[error("segment writer is faulted")]
    /// Segment writer is faulted.
    Faulted,
    #[error("new segment target contains {0} bytes")]
    /// New segment target contains bytes.
    NonemptyTarget(u64),
    #[error("segment recovery image is {actual} bytes; limit is {limit}")]
    /// Segment recovery image is bytes; limit is.
    RecoveryLimit {
        #[doc = "Observed size, count, or fenced field value."]
        actual: u64,
        #[doc = "Configured maximum for the reported resource."]
        limit: u64,
    },
    #[error("sync position does not belong to the complete writer prefix")]
    /// Sync position does not belong to the complete writer prefix.
    InvalidSyncPosition,
    #[error("physical group number space exhausted")]
    /// Physical group number space exhausted.
    GroupNumberExhausted,
    #[error("physical group encode buffer allocation failed")]
    /// Physical group encode buffer allocation failed.
    EncodeBufferAllocation,
    #[error("recovered journal does not contain protected operation prefix {0}")]
    /// Recovered journal does not contain protected operation prefix.
    ProtectedPrefixMismatch(u64),
    #[error("physical group {0} is damaged, but a later group marks it durable")]
    /// A later valid group marks this damaged group durable: its write
    /// completed, so the damage is a fault, not an unfinished write.
    DurableGroupDamaged(u64),
    #[error("operation {op_number} has configuration epoch {actual}; expected {expected}")]
    /// Operation has configuration epoch; expected.
    ConfigurationMismatch {
        /// Partition-local canonical operation number.
        op_number: u64,
        /// Observed size, count, or fenced field value.
        actual: u64,
        /// Expected size, count, or fenced field value.
        expected: u64,
    },
    #[error("operation {op_number} has view {actual} beyond promise {promised}")]
    /// Operation has view beyond promise.
    ViewBeyondPromise {
        /// Partition-local canonical operation number.
        op_number: u64,
        /// Observed size, count, or fenced field value.
        actual: u64,
        /// Promised upper election-view fence.
        promised: u64,
    },
}

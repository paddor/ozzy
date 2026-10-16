//! Stable indexed-read snapshot spanning sealed and active segments.

pub(crate) mod asynchronous;
pub(crate) mod seek;

use std::io::{self};

use ozzy_journal::{ReadLimits, operation::OperationLimits};
use ozzy_proto::{MessageId, Offset, OperationId, PartitionIncarnation};
use thiserror::Error;

use crate::{
    ActiveSegmentIndex, DecodeLimits, IndexedMessageLocation, IndexedOffsetLocation,
    IndexedOperation, IndexedOperationLocation, IndexedReadError, IndexedRecord, LogPosition,
    TailState, scan_segment,
};

/// Journal boundary captured by one stable lookup snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalIndexBoundary {
    /// Complete local writes, including unsynced speculative records. Not durability.
    Written,
    /// Accepted prefix under the journal's mode; external mode requires local sync.
    Accepted,
    /// Locally published committed prefix, which may lag live quorum commit.
    Committed,
}

/// Journal-index construction, lookup, or authoritative-read failure.
#[derive(Debug, Error)]
pub enum JournalIndexError {
    #[error(transparent)]
    /// A physical file operation failed.
    Io(#[from] io::Error),
    #[error(transparent)]
    /// Physical segment framing or integrity validation failed.
    Codec(#[from] crate::CodecError),
    #[error(transparent)]
    /// Canonical operation-body validation failed.
    Operation(#[from] ozzy_journal::operation::OperationCodecError),
    #[error(transparent)]
    /// Journal directory validation or publication failed.
    Directory(#[from] crate::DirectoryError),
    #[error(transparent)]
    /// The active-segment index rejected this prefix.
    Active(#[from] crate::ActiveIndexError),
    #[error(transparent)]
    /// The captured index catalog rejected its source or selection.
    Catalog(#[from] crate::IndexCatalogError),
    #[error(transparent)]
    /// Indexed record reading or validation failed.
    Read(#[from] IndexedReadError),
    #[error("segment {0} was not pinned by this index snapshot")]
    /// Segment was not pinned by this index snapshot.
    UnpinnedSegment(u64),
    #[error("segment {0} cannot prove the requested index lineage")]
    /// Segment cannot prove the requested index lineage.
    LineageSegmentMismatch(u64),
    #[error("index boundary {requested} exceeds snapshot boundary {available}")]
    /// Index boundary exceeds snapshot boundary.
    BoundaryBeyondSnapshot {
        #[doc = "Requested canonical operation number."]
        requested: u64,
        #[doc = "Available bytes, capacity, or canonical prefix at failure."]
        available: u64,
    },
    #[error("active message index has no matching offset entry")]
    /// Active message index has no matching offset entry.
    InconsistentActiveIndex,
    #[error("active read index exceeds configured entry or file-size limits")]
    /// Active read index exceeds configured entry or file-size limits.
    ReadIndexCapacity,
    #[error("sealed index catalog no longer matches the selected manifest")]
    /// Sealed index catalog no longer matches the selected manifest.
    StaleCatalog,
    #[error("indexed read limits must be nonzero")]
    /// Indexed read limits must be nonzero.
    InvalidReadLimits,
    #[error("record payload is {actual} bytes; read limit is {limit}")]
    /// Record payload is bytes; read limit is.
    RecordExceedsReadLimit {
        #[doc = "Observed size, count, or fenced field value."]
        actual: usize,
        #[doc = "Configured maximum for the reported resource."]
        limit: usize,
    },
    #[error("committed range is missing offset {0:?}")]
    /// Committed range is missing offset.
    MissingOffset(Offset),
    #[error("retained retry evidence is missing operation {0}")]
    /// A retained producer result has no authoritative operation selector.
    MissingOperationNumber(u64),
    #[error("partition offset space exhausted during range read")]
    /// Partition offset space exhausted during range read.
    OffsetExhausted,
}

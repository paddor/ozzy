#![warn(missing_docs)]
#![doc = "Append-only segment journal implementation for Ozzy."]
#![deny(unsafe_code)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::too_many_lines,
        clippy::too_many_arguments,
        reason = "product code is linted without cfg(test)"
    )
)]

mod active_index;
mod active_read_index;
mod async_files;
mod async_metadata;
mod canonical_checkpoint;
mod canonical_recovery;
mod checkpoint;
mod codec;
pub(crate) use codec::{decode_indexed_operation, scan_segment_async};
mod cooperative;
mod directory;
mod history;
mod identity_index;
mod index;
mod index_builder;
mod index_catalog;
mod index_file;
mod journal_index;
mod metadata;
mod reader;
mod retention;
mod suffix;
#[cfg(test)]
mod test_io;
mod writer;

#[cfg(any(test, feature = "simulation"))]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable,
    reason = "simulation test support"
)]
pub mod simulation;

pub use active_index::ActiveIndexError;
pub(crate) use active_index::ActiveSegmentIndex;
pub use canonical_checkpoint::CanonicalCheckpointError;
pub use canonical_recovery::{CanonicalRecoveryLimits, CanonicalStateRecoveryError};
pub use checkpoint::{
    CHECKPOINT_CHUNK_ENTRY_BYTES, CHECKPOINT_HEADER_BYTES, CheckpointChunk, CheckpointError,
    CheckpointLimits, CheckpointManifest, CheckpointSpec, checkpoint_manifest_digest,
    checkpoint_name, decode_checkpoint_manifest, encode_checkpoint_manifest,
};
pub use codec::{
    BodyEncoding, CodecError, DecodeLimits, DecodedGroup, DecodedOperation, ENTRY_HEADER_BYTES,
    EncodedGroup, GROUP_SEAL_BYTES, GroupStart, SEGMENT_HEADER_BYTES, SegmentHeader, SegmentScan,
    TailState, WRITE_GROUP_ALIGNMENT, decode_group, decode_segment_header, encode_group,
    encode_group_with_body_encoding, encode_segment_header, scan_segment,
};
pub use directory::asynchronous::{
    BuiltIndex as BuiltJournalIndex, Candidate as CanonicalRecoveryCandidate,
    CanonicalCheckpointImport, CheckpointBuildPlan, CheckpointCursor, CheckpointFiles,
    CheckpointInbox, CheckpointInboxStream, CheckpointRecovery, CheckpointSource, CheckpointStream,
    CheckpointWriter, Completed as CompletedJournalWrite, CompletedRoll as CompletedJournalRoll,
    CompletedSync as CompletedJournalSync, ExpectedStore, Format as JournalFormat,
    IndexJob as JournalIndexJob, Installer as SuffixInstaller, Journal as GroupJournal,
    Limits as JournalLimits, Opening as JournalOpening, PendingRoll as PendingJournalRoll,
    Pipeline as JournalWritePipeline, Prepared as PreparedJournalWrite,
    PreparedRoll as PreparedJournalRoll, PreparedSegment, PreparedSync as PreparedJournalSync,
    RecoveryDirectory, Repair as SealedRepair, RetirementBudget, RetirementProof, RetirementScan,
    ScannedPrefix, SegmentFiles, SegmentPreparation, SuffixRequest, UnselectedFiles,
};
pub use directory::asynchronous::{
    PartitionIndex as JournalPartitionIndex, PartitionRead, PartitionReadLimits, PartitionReadRange,
};
pub use directory::{
    DirectoryError, GROUP_CONFIGURATION_MAX_BYTES, JournalGroupEncoder, JournalGroupEncoding,
    MaintenanceBudget, OrphanCleanupStep, PreencodedJournalGroup, RecoveryPublication,
    RecoveryPublicationError, RepairRange, RepairSource, ReplayError, ReplayedOperation,
    SealedRepairLimits, SharedJournalOperation,
};
pub use history::asynchronous::{
    History as JournalHistory, Metadata as JournalHistoryMetadata, Validation as StorageValidation,
};
pub use history::{HistoryChunk, HistoryError, StorageValidationBudget, StorageValidationStep};
pub use identity_index::JournalIdentityHandoffError;
pub use identity_index::asynchronous::{
    Index as JournalIdentityIndex, ResolveError as IdentityResolveError,
};
pub use index::{
    DerivedIndexEntries, IndexError, MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry,
    OperationLocation, RecordLocation, derive_index_entries,
};
pub use index_builder::{IndexBuildError, IndexBuildLimits, SegmentIndex, segment_index_name};
pub use index_catalog::{
    IndexCatalogError, IndexedMessageLocation, IndexedOffsetLocation, IndexedOperation,
    IndexedOperationLocation,
};
pub use index_file::{
    INDEX_HEADER_BYTES, IndexEntries, IndexFileError, IndexLimits, IndexSource,
    MESSAGE_INDEX_ENTRY_BYTES, OFFSET_INDEX_ENTRY_BYTES, OPERATION_INDEX_ENTRY_BYTES,
    OPERATION_LOCATION_BYTES, SegmentIndexImage, SegmentIndexView, decode_segment_index,
    encode_segment_index,
};
pub use journal_index::asynchronous::Snapshot as JournalIndexSnapshot;
pub use journal_index::seek::SeekQuery;
pub use journal_index::{JournalIndexBoundary, JournalIndexError};
pub use metadata::{
    CURRENT_BYTES, CheckpointReference, CommitMode, CurrentReference, GROUP_IDENTITY_BYTES,
    GroupIdentity, LogPosition, MANIFEST_HEADER_BYTES, Manifest, MetadataError, MetadataLimits,
    SEGMENT_REFERENCE_BYTES, SealedSegment, SegmentReference, decode_current,
    decode_group_identity, decode_manifest, encode_current, encode_group_identity, encode_manifest,
    encode_manifest_with_limits, manifest_digest,
};
pub use reader::{
    DEFAULT_RESIDENT_BYTES, DecodedBatches, IndexedReadError, IndexedRecord,
    PreparedOperationRecords, RecordSpan, RecordView,
};
pub use retention::{
    RetentionError, RetentionFloors, RetiredPrefix, UnreferencedCheckpointCleanup,
    UnreferencedMetadataCleanup, UnreferencedSegmentCleanup,
};
mod resident_read;
pub use ozzy_journal::operation::{
    CanonicalOperation, ChainPosition, Digest, OperationKind, OperationLimits,
    logical_operation_digest,
};
pub use resident_read::ResidentRecordRead;
pub use suffix::{
    SuffixReplacement, SuffixReplacementError, SuffixReplacementLimits, SuffixStreamLimits,
};
pub use writer::asynchronous::{
    File as SegmentFile, Options as SegmentOptions, Start as SegmentStart, Writer as SegmentWriter,
};
pub use writer::{
    CanonicalRecoveryRequirements, SegmentState, SegmentWriteMode, WriterError, WriterPosition,
};

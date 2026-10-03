#![doc = "Append-only segment journal implementation for Ozzy."]
#![deny(unsafe_code)]

mod active_index;
mod active_read_index;
mod async_files;
pub mod async_metadata;
#[cfg(target_os = "linux")]
pub use ozzy_io_aio::kernel as aio;
mod canonical_checkpoint;
mod canonical_recovery;
mod checkpoint;
mod codec;
pub(crate) use codec::scan_segment_async;
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
mod placement;
mod reader;
mod relocation;
mod retention;
mod store_lock;
mod suffix;
#[cfg(test)]
mod test_io;
mod volume;
mod writer;

#[cfg(any(test, feature = "simulation"))]
pub mod simulation;

pub use active_index::{ActiveIndexError, ActiveSegmentIndex};
pub use canonical_checkpoint::CanonicalCheckpointError;
pub use canonical_recovery::{
    CanonicalRecoveryCandidate, CanonicalRecoveryLimits, CanonicalStateRecoveryError,
};
pub use checkpoint::{
    CHECKPOINT_CHUNK_ENTRY_BYTES, CHECKPOINT_HEADER_BYTES, CheckpointChunk, CheckpointError,
    CheckpointImage, CheckpointLimits, CheckpointManifest, CheckpointPlan, CheckpointSpec,
    build_checkpoint, checkpoint_manifest_digest, checkpoint_name, decode_checkpoint_manifest,
    encode_checkpoint_manifest, open_checkpoint,
};
pub use codec::{
    BodyEncoding, CodecError, DecodeLimits, DecodedGroup, DecodedOperation, ENTRY_HEADER_BYTES,
    EncodedGroup, GROUP_SEAL_BYTES, SEGMENT_HEADER_BYTES, SegmentHeader, SegmentScan, TailState,
    WRITE_GROUP_ALIGNMENT, decode_group, decode_indexed_operation, decode_segment_header,
    decode_standalone_operation, encode_group, encode_group_with_body_encoding,
    encode_segment_header, scan_segment,
};
#[cfg(target_os = "linux")]
pub use directory::JournalAio;
pub use directory::asynchronous::{
    Candidate as AsyncCanonicalRecoveryCandidate, CheckpointFiles as AsyncCheckpointFiles,
    Completed as AsyncCompletedJournalWrite, CompletedRoll as AsyncCompletedJournalRoll,
    CompletedSync as AsyncCompletedJournalSync, Format as AsyncJournalFormat,
    Installer as AsyncSuffixInstaller, Journal as AsyncGroupJournal, Limits as AsyncJournalLimits,
    Opening as AsyncJournalOpening, PendingRoll as AsyncPendingJournalRoll,
    Pipeline as AsyncJournalWritePipeline, Prepared as AsyncPreparedJournalWrite,
    PreparedRoll as AsyncPreparedJournalRoll, PreparedSegment as AsyncPreparedSegment,
    PreparedSync as AsyncPreparedJournalSync, RecoveryDirectory as AsyncRecoveryDirectory,
    Repair as AsyncSealedRepair, RetirementBudget as AsyncRetirementBudget,
    SegmentFiles as AsyncSegmentFiles, SegmentPreparation as AsyncSegmentPreparation,
};
pub use directory::asynchronous::{
    PartitionIndex as AsyncJournalPartitionIndex, PartitionRead as AsyncPartitionRead,
    PartitionReadLimits as AsyncPartitionReadLimits,
};
pub use directory::{
    BufferedRollPublication, CompletedDurableProgress, CompletedJournalRoll, CompletedJournalWrite,
    DirectoryError, GROUP_CONFIGURATION_MAX_BYTES, GroupDirectory, JournalGroupEncoder,
    JournalGroupEncoding, JournalWriteEvent, JournalWritePipeline, JournalWriteback,
    MaintenanceBudget, MetadataCleanup, MetadataCleanupStep, OpenGroupJournal, OrphanCleanup,
    OrphanCleanupStep, PendingJournalRoll, PendingJournalWrite, PreencodedJournalGroup,
    PreparedDurableProgress, PreparedJournalRoll, PreparedJournalWrite, PreparedSegment,
    PublishedBufferedRoll, RecoveryPublication, RecoveryPublicationError, RepairRange, ReplayError,
    ReplayedOperation, SealedRepair, SealedRepairLimits, SegmentPreparation,
    SharedJournalOperation,
};
pub use history::asynchronous::{
    History as AsyncJournalHistory, Metadata as AsyncJournalHistoryMetadata,
    Validation as AsyncStorageValidation,
};
pub use history::{
    HistoryChunk, HistoryError, JournalHistory, StorageValidation, StorageValidationBudget,
    StorageValidationStep,
};
pub use identity_index::asynchronous::{
    Index as AsyncJournalIdentityIndex, ResolveError as AsyncIdentityResolveError,
};
pub use identity_index::{JournalIdentityHandoffError, JournalIdentityIndex};
pub use index::{
    DerivedIndexEntries, IndexError, MessageIndexEntry, OffsetIndexEntry, OperationIndexEntry,
    OperationLocation, RecordLocation, derive_index_entries,
};
pub use index_builder::{
    IndexBuildError, IndexBuildLimits, SegmentIndex, build_segment_index, open_segment_index,
    segment_index_name,
};
pub use index_catalog::{
    IndexCatalogError, IndexedMessageLocation, IndexedOffsetLocation, IndexedOperationLocation,
    SegmentIndexCatalog,
};
pub use index_file::{
    INDEX_HEADER_BYTES, IndexFileError, IndexLimits, IndexSource, MESSAGE_INDEX_ENTRY_BYTES,
    OFFSET_INDEX_ENTRY_BYTES, OPERATION_INDEX_ENTRY_BYTES, SegmentIndexImage, SegmentIndexView,
    decode_segment_index, encode_segment_index,
};
pub use journal_index::asynchronous::Snapshot as AsyncJournalIndexSnapshot;
pub use journal_index::{JournalIndexBoundary, JournalIndexError, JournalIndexSnapshot};
pub use metadata::{
    CURRENT_BYTES, CheckpointReference, CommitMode, CurrentReference, GROUP_IDENTITY_BYTES,
    GroupIdentity, LogPosition, MANIFEST_HEADER_BYTES, Manifest, MetadataError, MetadataLimits,
    SEGMENT_REFERENCE_BYTES, SealedSegment, SegmentReference, decode_current,
    decode_group_identity, decode_manifest, encode_current, encode_group_identity, encode_manifest,
    encode_manifest_with_limits, manifest_digest,
};
pub use placement::{
    PLACEMENT_CURRENT_BYTES, PLACEMENT_ENTRY_BYTES, PLACEMENT_HEADER_BYTES, PlacementCatalog,
    PlacementCleanup, PlacementCurrent, PlacementDirectory, PlacementEntry, PlacementError,
    PlacementLimits, decode_placement_catalog, decode_placement_current, encode_placement_catalog,
    encode_placement_current, placement_catalog_digest,
};
pub use reader::{
    DEFAULT_RESIDENT_BYTES, DecodedBatches, IndexedReadError, IndexedRecord,
    PreparedOperationRecords, RecordSpan, RecordView, read_indexed_message, read_indexed_record,
};
pub use relocation::{
    PreparedRelocation, RelocationArtifact, RelocationError, RetiredGroup, prepare_relocation,
    retire_relocated_source,
};
pub use retention::{
    CheckpointPin, RetentionError, RetentionFloors, RetentionResult, RetentionScanBudget,
    RetiredPrefix, SegmentPin, UnreferencedCheckpointCleanup, UnreferencedMetadataCleanup,
    UnreferencedSegmentCleanup,
};
mod resident_read;
pub use ozzy_journal::operation::{
    CanonicalOperation, ChainPosition, Digest, OperationKind, OperationLimits,
    logical_operation_digest,
};
pub use resident_read::ResidentRecordRead;
pub use suffix::{
    SuffixInstaller, SuffixReplacement, SuffixReplacementError, SuffixReplacementLimits,
    SuffixStreamLimits,
};
pub use volume::{
    MountPolicy, VOLUME_IDENTITY_BYTES, VolumeConfig, VolumeDirectory, VolumeError, VolumeIdentity,
    VolumeSet, decode_volume_identity, encode_volume_identity,
};
pub use writer::asynchronous::{
    Options as AsyncSegmentOptions, Start as AsyncSegmentStart, Writer as AsyncSegmentWriter,
};
pub use writer::{
    CanonicalRecoveryRequirements, SegmentIo, SegmentWriteMode, SegmentWriter, WriterError,
    WriterPosition,
};

#[cfg(feature = "storage-metrics")]
pub mod read_metrics;

#[cfg(feature = "storage-metrics")]
pub mod write_metrics;

//! Canonical replicated journal operations and their bounded body codec.

mod codec;
mod error;
mod integrity;
mod schema;

mod append_encoder;
mod append_pack;
mod output;
mod records;
mod view;
pub use append_encoder::{
    AppendBodyEncoder, AppendHeader, DescriptorLayout, ValidatedWireAppend,
    append_prepared_record_batch, append_record_batches, append_wire_record_batch,
};
pub use append_pack::{
    AppendPackResult, AppendPackScratch, pack_append_payload, pack_validated_wire_append_payload,
};
pub use output::OperationOutput;
pub use records::{AppendRecordList, RecordIter, RecordParts, RecordRef};
pub use view::{
    AppendBatchView, AppendBatches, AppendPartLengths, AppendParts, AppendRecordDescriptor,
    AppendRecordDescriptors, AppendRecordView, AppendRecords, AppendView, PreparedAppendPayload,
    decode_append_batches, decode_append_batches_indexed,
    decode_append_batches_with_validated_payload, decode_append_records, decode_append_view,
    decode_packed_append_records,
};

const TINY_RECORDS: u32 = 1 << 31;
const PREPARED_PAYLOAD: u32 = 1 << 30;
const RECORD_COUNT_MASK: u32 = !(TINY_RECORDS | PREPARED_PAYLOAD);

use codec::{
    AppendTotals, Decoder, Encoder, decode_append_batch, encode_append_records, enforce_limit,
    records_are_tiny, validate_append_descriptors, validate_limits, validate_position_count,
    validate_tiny_descriptors,
};
pub use codec::{
    append_operation_body, append_operation_body_to, decode_append_summary,
    decode_append_summary_and_batches, decode_append_summary_and_batches_indexed,
    decode_append_summary_and_batches_with_validated_payload, decode_append_summary_and_view,
    decode_operation_body, encode_operation_body, validate_operation_body,
    validate_operation_body_with_validated_payload,
};
pub use error::OperationCodecError;
pub use integrity::{
    canonical_body_digest, canonical_body_digest_parts, canonical_body_hasher,
    logical_operation_digest, logical_operation_digest_with_body_digest,
};
pub use schema::{
    Append, AppendBatch, AppendBatchSummary, AppendRecord, AppendSummary, Assign, Barrier,
    CanonicalOperation, ChainPosition, CreatePartition, Digest, OpenProducer, OperationBody,
    OperationHeader, OperationKind, OperationLimits, PartitionPolicy, ProducerResultFloor,
    Progress, ProgressOwner, RecordPosition, RetentionPolicy, Trim,
};

//! Execution-neutral canonical validation. No filesystem access or authority changes.

use super::{AppendBuffer, JournalError, MAX_APPEND_OPERATIONS};
use ozzy_core::state::{CanonicalImages, IdentityIndex, PreparedCanonicalGroup};
use ozzy_journal::operation::{
    Barrier, OperationBody, OperationKind, OperationLimits, decode_operation_body,
};
use ozzy_journal_segment::{DecodeLimits, PreparedOperationRecords};
use ozzy_proto::OperationId;

pub(super) fn segment_full(error: &ozzy_journal_segment::DirectoryError) -> bool {
    use ozzy_journal_segment::{CodecError, DirectoryError, WriterError};
    matches!(
        error,
        DirectoryError::Writer(WriterError::Codec(CodecError::GroupExceedsSegment))
            | DirectoryError::Codec(
                CodecError::GroupExceedsSegment | CodecError::SegmentDecodedBodyLimit { .. }
            )
    )
}

/// An indivisible operation's raw fallback must fit an empty segment and its
/// recovery decoder. Compression must not determine whether it is admissible.
pub(super) fn physical_capacity(
    buffer: &AppendBuffer,
    capacity: u64,
    decode: DecodeLimits,
) -> Result<(), JournalError> {
    use ozzy_journal_segment::{
        ENTRY_HEADER_BYTES, GROUP_SEAL_BYTES, SEGMENT_HEADER_BYTES, WRITE_GROUP_ALIGNMENT,
    };
    let available = capacity
        .checked_sub(SEGMENT_HEADER_BYTES as u64)
        .ok_or(JournalError::AppendCapacity)?;
    for operation in buffer.operations() {
        let entry_bytes = operation
            .body
            .len()
            .checked_add(ENTRY_HEADER_BYTES + 7)
            .map(|bytes| bytes & !7)
            .ok_or(JournalError::AppendCapacity)?;
        let group_bytes = entry_bytes
            .checked_add(GROUP_SEAL_BYTES + WRITE_GROUP_ALIGNMENT - 1)
            .map(|bytes| bytes & !(WRITE_GROUP_ALIGNMENT - 1))
            .ok_or(JournalError::AppendCapacity)?;
        if operation.body.len() > decode.max_decoded_body_bytes
            || operation.body.len() > decode.max_group_decoded_body_bytes
            || entry_bytes > decode.max_entry_bytes
            || group_bytes as u64 > available
        {
            return Err(JournalError::AppendCapacity);
        }
    }
    Ok(())
}

pub(super) fn prepare<I: IdentityIndex + Clone>(
    images: &CanonicalImages<I>,
    buffer: &mut AppendBuffer,
    limits: OperationLimits,
    primary_payloads_validated: bool,
) -> Result<(PreparedCanonicalGroup, Vec<PreparedOperationRecords>), JournalError> {
    if buffer
        .operations()
        .all(|operation| operation.kind == OperationKind::Append)
    {
        let validated = prepare_appends(images, buffer, limits, primary_payloads_validated)?;
        // Every path checks these limits. Immutable bodies retain the earlier
        // payload validation, so storage need not walk their descriptors again.
        buffer.set_validated(true);
        return Ok(validated);
    }
    // Synchronous bounded scratch, never retained across an await.
    let mut bodies: [_; MAX_APPEND_OPERATIONS] = std::array::from_fn(|_| {
        (
            0,
            OperationBody::Barrier(Barrier {
                operation_id: OperationId::from_bytes([0; 16]),
            }),
        )
    });
    for (slot, operation) in bodies.iter_mut().zip(buffer.operations()) {
        *slot = (
            operation.op_number,
            decode_operation_body(operation.kind, operation.body, limits)?,
        );
    }
    let plan = images.prepare_group(&bodies[..buffer.len()])?;
    drop(bodies);
    buffer.set_validated(true);
    Ok((plan, Vec::new()))
}

fn prepare_appends<I: IdentityIndex + Clone>(
    images: &CanonicalImages<I>,
    buffer: &mut AppendBuffer,
    limits: OperationLimits,
    primary_payloads_validated: bool,
) -> Result<(PreparedCanonicalGroup, Vec<PreparedOperationRecords>), JournalError> {
    let mut summaries = smallvec::SmallVec::<[_; 4]>::new();
    let bodies = buffer.shared_bodies();
    let retained_bytes = buffer.retained_bytes();
    let mut records = Vec::with_capacity(buffer.len());
    for (operation, proof) in buffer.operations().zip(buffer.proofs()) {
        let body = bodies.slice_ref(operation.body);
        let (summary, prepared) = if let Some(proof) = proof {
            PreparedOperationRecords::new_validated_wire_append(
                operation.header(),
                body,
                proof,
                limits,
            )
            .map_err(records_error)?
        } else if primary_payloads_validated {
            PreparedOperationRecords::new_append_with_validated_payload(
                operation.header(),
                body,
                limits,
            )
            .map_err(records_error)?
        } else {
            PreparedOperationRecords::new_append(operation.header(), body, limits)
                .map_err(records_error)?
        };
        summaries.push((operation.op_number, summary));
        records.push(prepared.with_shared_backing_bytes(retained_bytes));
    }
    Ok((images.prepare_append_group(&summaries)?, records))
}

fn records_error(error: ozzy_journal_segment::IndexedReadError) -> JournalError {
    match error {
        ozzy_journal_segment::IndexedReadError::Operation(error) => error.into(),
        error => ozzy_journal_segment::JournalIndexError::from(error).into(),
    }
}

pub(super) fn retained_records(
    append: &super::append::RetainedAppend,
    limits: OperationLimits,
) -> Result<Vec<PreparedOperationRecords>, JournalError> {
    append
        .operations()
        .zip(append.proofs())
        .map(|(operation, proof)| {
            let body = append.payload(operation.body);
            let decoded = if let Some(proof) = proof {
                PreparedOperationRecords::new_validated_wire_append(
                    operation.header(),
                    body,
                    proof,
                    limits,
                )
                .map(|(_, records)| records)
            } else if operation.kind == OperationKind::Append {
                PreparedOperationRecords::new_append_with_validated_payload(
                    operation.header(),
                    body,
                    limits,
                )
                .map(|(_, records)| records)
            } else {
                PreparedOperationRecords::new(operation.header(), body, limits)
            };
            decoded
                .map(|records| records.with_shared_backing_bytes(append.retained_bytes()))
                .map_err(ozzy_journal_segment::JournalIndexError::from)
                .map_err(JournalError::from)
        })
        .collect()
}

const _: () = assert!(
    std::mem::size_of::<[(u64, OperationBody<'static>); MAX_APPEND_OPERATIONS]>() <= 64 * 1024
);

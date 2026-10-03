//! Pure sealed-segment metadata translation shared by both journal owners.

use ozzy_proto::GroupId;

use super::DirectoryError;
use crate::{IndexSource, SegmentReference};

pub(crate) fn sealed_source(
    group_id: GroupId,
    reference: &SegmentReference,
    successor: &SegmentReference,
) -> Result<IndexSource, DirectoryError> {
    let segment_id = reference.segment_id;
    let sealed = reference
        .sealed
        .ok_or(DirectoryError::SegmentNotSealed(segment_id))?;
    let last_op_number = successor
        .first_chain
        .next_op_number()
        .checked_sub(1)
        .ok_or(DirectoryError::SegmentMismatch(segment_id))?;
    Ok(IndexSource {
        group_id,
        segment_id,
        valid_bytes: sealed.valid_bytes,
        segment_digest: sealed.digest,
        first_op_number: reference.first_chain.next_op_number(),
        last_op_number,
        last_operation_digest: successor.first_chain.previous_digest(),
    })
}

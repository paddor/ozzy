use super::{
    Digest, Hasher, INDEX_DIGEST_END, INDEX_DIGEST_START, INDEX_HASH_CONTEXT, IndexFileError,
    IndexLimits, SegmentIndexView, decode_sections, digest_at, section_checks, validate_header,
};
use crate::cooperative::Budget;

impl super::SegmentIndexImage {
    pub(crate) async fn new_async(
        source: super::IndexSource,
        build_memory_limit: u64,
        mut offsets: Vec<crate::OffsetIndexEntry>,
        mut messages: Vec<crate::MessageIndexEntry>,
        mut operations: Vec<crate::OperationIndexEntry>,
    ) -> Result<Self, IndexFileError> {
        use crate::cooperative::sort_by;
        sort_by(&mut offsets, super::compare_offset_entries).await;
        sort_by(&mut messages, super::compare_message_entries).await;
        sort_by(&mut operations, super::compare_operation_entries).await;
        let image = Self {
            source,
            build_memory_limit,
            offsets,
            messages,
            operations,
        };
        super::validate_image_header(&image)?;
        let mut budget = Budget::default();
        for check in super::image_checks(&image) {
            budget.charge(check?).await;
        }
        Ok(image)
    }
}

/// Whole-file integrity and every table check remain mandatory. Pending work
/// owns only an immutable borrow, and cancellation exposes no partial index.
pub(crate) async fn decode_segment_index_async(
    input: &[u8],
    limits: IndexLimits,
) -> Result<SegmentIndexView<'_>, IndexFileError> {
    validate_header(input, limits)?;
    let expected = digest_at(input, INDEX_DIGEST_START..INDEX_DIGEST_END);
    if digest(input).await != expected {
        return Err(IndexFileError::DigestMismatch);
    }
    let view = decode_sections(input, limits)?;
    let mut budget = Budget::default();
    for check in section_checks(view) {
        budget.charge(check?).await;
    }
    Ok(view)
}

async fn digest(input: &[u8]) -> Digest {
    let mut hasher = Hasher::new(INDEX_HASH_CONTEXT);
    hasher.update(&input[..INDEX_DIGEST_START]);
    hasher.update(&[0; INDEX_DIGEST_END - INDEX_DIGEST_START]);
    let mut budget = Budget::default();
    for chunk in input[INDEX_DIGEST_END..].chunks(64 * 1024) {
        hasher.update(chunk);
        budget.charge(chunk.len()).await;
    }
    hasher.finish()
}

#[cfg(test)]
mod tests;

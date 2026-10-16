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
        mut locations: Vec<crate::OperationLocation>,
    ) -> Result<Self, IndexFileError> {
        use crate::cooperative::sort_by;
        sort_by(&mut offsets, super::compare_offset_entries).await;
        sort_by(&mut messages, super::compare_message_entries).await;
        sort_by(&mut operations, super::compare_operation_entries).await;
        sort_by(&mut locations, super::compare_locations).await;
        let image = Self {
            source,
            build_memory_limit,
            offsets,
            messages,
            operations,
            locations,
        };
        super::validate_image_header(&image)?;
        let mut budget = Budget::default();
        for check in super::image_checks(&image) {
            budget.charge(check?).await;
        }
        Ok(image)
    }

    /// Extend a validated image with unsorted selectors for later operations.
    /// Only the new selectors are sorted; the merged image is validated in full.
    pub(crate) async fn extend_async(
        &self,
        source: super::IndexSource,
        mut offsets: Vec<crate::OffsetIndexEntry>,
        mut messages: Vec<crate::MessageIndexEntry>,
        mut operations: Vec<crate::OperationIndexEntry>,
        mut locations: Vec<crate::OperationLocation>,
    ) -> Result<Self, IndexFileError> {
        use crate::cooperative::sort_by;
        sort_by(&mut offsets, super::compare_offset_entries).await;
        sort_by(&mut messages, super::compare_message_entries).await;
        sort_by(&mut operations, super::compare_operation_entries).await;
        sort_by(&mut locations, super::compare_locations).await;
        let image = Self {
            source,
            build_memory_limit: self.build_memory_limit,
            offsets: merge(&self.offsets, offsets, super::compare_offset_entries).await,
            messages: merge(&self.messages, messages, super::compare_message_entries).await,
            operations: merge(
                &self.operations,
                operations,
                super::compare_operation_entries,
            )
            .await,
            locations: merge(&self.locations, locations, super::compare_locations).await,
        };
        super::validate_image_header(&image)?;
        let mut budget = Budget::default();
        for check in super::image_checks(&image) {
            budget.charge(check?).await;
        }
        Ok(image)
    }
}

/// Merge two sorted runs into a new allocation, charging copied bytes.
async fn merge<T: Copy>(
    old: &[T],
    new: Vec<T>,
    compare: fn(&T, &T) -> std::cmp::Ordering,
) -> Vec<T> {
    let mut merged = Vec::with_capacity(old.len() + new.len());
    let (mut left, mut right) = (old.iter().peekable(), new.into_iter().peekable());
    let mut budget = Budget::default();
    let mut pending = 0;
    loop {
        let next = match (left.peek(), right.peek()) {
            (Some(a), Some(b)) if compare(a, b) != std::cmp::Ordering::Greater => {
                *left.next().unwrap()
            }
            (_, Some(_)) => right.next().unwrap(),
            (Some(_), None) => *left.next().unwrap(),
            (None, None) => break,
        };
        merged.push(next);
        pending += size_of::<T>();
        if pending >= crate::cooperative::TURN_BYTES {
            budget.charge(std::mem::take(&mut pending)).await;
        }
    }
    merged
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

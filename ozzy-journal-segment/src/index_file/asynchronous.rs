use super::{
    Digest, Hasher, INDEX_DIGEST_START, INDEX_HASH_CONTEXT, IndexEntries, IndexFileError,
    IndexLimits, IndexSource, SegmentIndexImage, SegmentIndexView, compare_locations,
    compare_message_entries, compare_offset_entries, compare_operation_entries, decode_sections,
    validate::{image_checks, section_checks, validate_image_header},
    validate_header,
};
use crate::cooperative::{Budget, sort_by};

impl IndexEntries {
    /// Sort each section, yielding between bounded steps.
    async fn sort_async(&mut self) {
        sort_by(&mut self.offsets, compare_offset_entries).await;
        sort_by(&mut self.messages, compare_message_entries).await;
        sort_by(&mut self.operations, compare_operation_entries).await;
        sort_by(&mut self.locations, compare_locations).await;
    }
}

impl SegmentIndexImage {
    pub(crate) async fn new_async(
        source: IndexSource,
        build_memory_limit: u64,
        mut entries: IndexEntries,
    ) -> Result<Self, IndexFileError> {
        entries.sort_async().await;
        Self {
            source,
            build_memory_limit,
            entries,
        }
        .validated_async()
        .await
    }

    /// Extend a validated image with unsorted selectors for later operations.
    /// Only the new selectors are sorted; the merged image is validated in full.
    pub(crate) async fn extend_async(
        &self,
        source: IndexSource,
        mut entries: IndexEntries,
    ) -> Result<Self, IndexFileError> {
        entries.sort_async().await;
        let old = &self.entries;
        let entries = IndexEntries {
            offsets: merge(&old.offsets, entries.offsets, compare_offset_entries).await,
            messages: merge(&old.messages, entries.messages, compare_message_entries).await,
            operations: merge(
                &old.operations,
                entries.operations,
                compare_operation_entries,
            )
            .await,
            locations: merge(&old.locations, entries.locations, compare_locations).await,
        };
        Self {
            source,
            build_memory_limit: self.build_memory_limit,
            entries,
        }
        .validated_async()
        .await
    }

    /// Run every image check, yielding between bounded steps.
    async fn validated_async(self) -> Result<Self, IndexFileError> {
        validate_image_header(&self)?;
        let mut budget = Budget::default();
        for check in image_checks(&self) {
            budget.charge(check?).await;
        }
        Ok(self)
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
                left.next().copied()
            }
            (Some(_), None) => left.next().copied(),
            (_, Some(_)) => right.next(),
            (None, None) => None,
        };
        let Some(next) = next else {
            break;
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
    let expected = Digest::from_bytes(*ozzy_proto::layout::record(input, INDEX_DIGEST_START)?);
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
    // An input too short for the field is hashed whole, as the sync digest does.
    let tail = hasher
        .update_through_digest(input, INDEX_DIGEST_START)
        .unwrap_or(input);
    let mut budget = Budget::default();
    for chunk in tail.chunks(64 * 1024) {
        hasher.update(chunk);
        budget.charge(chunk.len()).await;
    }
    hasher.finish()
}

#[cfg(test)]
mod tests;

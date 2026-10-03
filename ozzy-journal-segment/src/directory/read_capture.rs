//! Pin exact current segment sources for detached reads and transfer.

use std::collections::HashSet;
use std::sync::Arc;

use super::{DirectoryError, OpenGroupJournal, sealed_source};
use crate::retention::segments_root;
use crate::{IndexSource, RetentionError, SegmentPin, SegmentReference};

impl OpenGroupJournal {
    /// Pin exact current segment generations for detached reads or transfer.
    pub fn pin_segments(&self, segment_ids: &[u64]) -> Result<SegmentPin, DirectoryError> {
        let segments = &self.directory.manifest.segments;
        let mut references = Vec::with_capacity(segment_ids.len());
        let mut seen = HashSet::new();
        for &segment_id in segment_ids {
            let position = segments
                .binary_search_by_key(&segment_id, |reference| reference.segment_id)
                .map_err(|_| DirectoryError::SegmentMismatch(segment_id))?;
            // Singleton reads need no duplicate-set allocation. Preserve the
            // public request order and first missing/duplicate error boundary.
            if segment_ids.len() > 1 && !seen.insert(segment_id) {
                return Err(RetentionError::DuplicateSegment.into());
            }
            references.push(segments[position]);
        }
        self.pin_references(references)
    }

    /// Internal callers already hold the validated current manifest. Do not
    /// allocate an ID list only to rediscover these same references by ID.
    pub(crate) fn pin_all_segments(&self) -> Result<SegmentPin, DirectoryError> {
        self.pin_references(self.directory.manifest.segments.clone())
    }

    fn pin_references(
        &self,
        references: Vec<SegmentReference>,
    ) -> Result<SegmentPin, DirectoryError> {
        Ok(self.pins.acquire(
            segments_root(&self.directory.root),
            references,
            Arc::clone(&self.directory.lock),
        )?)
    }

    pub(crate) fn sealed_index_source(
        &self,
        segment_id: u64,
    ) -> Result<(&SegmentReference, IndexSource), DirectoryError> {
        let segments = &self.directory.manifest.segments;
        let position = segments
            .binary_search_by_key(&segment_id, |reference| reference.segment_id)
            .map_err(|_| DirectoryError::SegmentNotSealed(segment_id))?;
        let reference = &segments[position];
        let successor = segments
            .get(position + 1)
            .ok_or(DirectoryError::SegmentNotSealed(segment_id))?;
        Ok((
            reference,
            sealed_source(self.directory.identity.group_id, reference, successor)?,
        ))
    }

    pub(crate) fn sealed_index_sources(&self) -> Result<Vec<IndexSource>, DirectoryError> {
        let segments = &self.directory.manifest.segments;
        let mut sources = Vec::with_capacity(segments.len().saturating_sub(1));
        for pair in segments.windows(2) {
            sources.push(sealed_source(
                self.directory.identity.group_id,
                &pair[0],
                &pair[1],
            )?);
        }
        Ok(sources)
    }
}

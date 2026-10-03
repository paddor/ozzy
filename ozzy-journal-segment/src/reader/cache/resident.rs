//! Recently written operations of the active segment and one sealed
//! predecessor, kept for readers. A byte budget evicts the oldest operations
//! first; readers of evicted operations load them from the segment file.

use std::collections::VecDeque;
use std::sync::Arc;

use ahash::AHashMap as HashMap;

use super::{CachedOperation, DecodedCell, IndexedReadError, SelectedBatch};
use crate::{IndexSource, OffsetIndexEntry, OperationLocation};

/// Default resident budget: compressed bodies plus record selector tables.
pub const DEFAULT_RESIDENT_BYTES: usize = 512 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct ResidentOperations {
    current: Option<Segment>,
    previous: Option<Segment>,
    // Insertion order across both segments, oldest first.
    order: VecDeque<Retained>,
    bytes: usize,
    limit: usize,
}

#[derive(Debug, Clone)]
struct Segment {
    group: ozzy_proto::GroupId,
    id: u64,
    operations: HashMap<u64, Arc<CachedOperation>>,
}

#[derive(Debug, Clone, Copy)]
struct Retained {
    group: ozzy_proto::GroupId,
    segment: u64,
    op_number: u64,
    bytes: usize,
}

impl Default for ResidentOperations {
    fn default() -> Self {
        Self::with_limit(DEFAULT_RESIDENT_BYTES)
    }
}

impl ResidentOperations {
    /// Empty store that retains at most `limit` bytes. The newest operation
    /// stays even when it alone exceeds the limit.
    pub(crate) fn with_limit(limit: usize) -> Self {
        Self {
            current: None,
            previous: None,
            order: VecDeque::new(),
            bytes: 0,
            limit,
        }
    }

    pub(crate) fn insert_prepared(
        &mut self,
        group: ozzy_proto::GroupId,
        segment: u64,
        prepared: &[Arc<CachedOperation>],
    ) {
        for operation in prepared {
            self.insert(group, segment, Arc::clone(operation));
        }
    }

    fn insert(
        &mut self,
        group: ozzy_proto::GroupId,
        segment: u64,
        operation: Arc<CachedOperation>,
    ) {
        if self
            .current
            .as_ref()
            .is_none_or(|current| current.group != group || current.id != segment)
        {
            if let Some(dropped) = self.previous.take() {
                self.forget(dropped.group, dropped.id);
            }
            self.previous = self.current.take();
            self.current = Some(Segment {
                group,
                id: segment,
                operations: HashMap::new(),
            });
        }
        let op_number = operation.location.op_number;
        let bytes = operation.resident_bytes();
        let current = self.current.as_mut().expect("selected resident segment");
        if let Some(replaced) = current.operations.insert(op_number, operation) {
            // Rare replacement: drop the old order entry so bytes stay exact.
            let position = self
                .order
                .iter()
                .position(|retained| {
                    retained.group == group
                        && retained.segment == segment
                        && retained.op_number == op_number
                })
                .expect("ordered resident operation");
            self.order.remove(position);
            self.bytes -= replaced.resident_bytes();
        }
        self.order.push_back(Retained {
            group,
            segment,
            op_number,
            bytes,
        });
        self.bytes += bytes;
        while self.bytes > self.limit && self.order.len() > 1 {
            let oldest = self.order.pop_front().expect("nonempty order");
            self.bytes -= oldest.bytes;
            for segment in self.current.iter_mut().chain(self.previous.iter_mut()) {
                if segment.group == oldest.group && segment.id == oldest.segment {
                    segment.operations.remove(&oldest.op_number);
                }
            }
        }
    }

    /// Release order entries of a segment that left the store.
    fn forget(&mut self, group: ozzy_proto::GroupId, segment: u64) {
        let mut released = 0;
        self.order.retain(|retained| {
            let keep = retained.group != group || retained.segment != segment;
            if !keep {
                released += retained.bytes;
            }
            keep
        });
        self.bytes -= released;
    }

    /// Bytes currently retained by resident operations.
    #[cfg(test)]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.bytes
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, source: IndexSource, location: OperationLocation) -> bool {
        self.segment(source).is_some_and(|segment| {
            segment
                .operations
                .get(&location.op_number)
                .is_some_and(|operation| operation.location == location)
        })
    }

    /// Selected operations of one read. `None` when any of them was evicted or
    /// the segment is no longer resident; the caller then reads the file.
    pub(crate) fn capture(
        &self,
        source: IndexSource,
        locations: impl Iterator<Item = OperationLocation>,
    ) -> Result<Option<Self>, IndexedReadError> {
        let Some(segment) = self.segment(source) else {
            return Ok(None);
        };
        let mut operations = HashMap::new();
        for location in locations {
            let Some(operation) = segment.operations.get(&location.op_number) else {
                return Ok(None);
            };
            if operation.location != location {
                return Err(IndexedReadError::InvalidLocation);
            }
            operations
                .entry(location.op_number)
                .or_insert_with(|| Arc::clone(operation));
        }
        Ok(Some(Self {
            current: Some(Segment {
                group: segment.group,
                id: segment.id,
                operations,
            }),
            previous: None,
            order: VecDeque::new(),
            bytes: 0,
            limit: usize::MAX,
        }))
    }

    fn segment(&self, source: IndexSource) -> Option<&Segment> {
        self.current
            .iter()
            .chain(self.previous.iter())
            .find(|segment| segment.group == source.group_id && segment.id == source.segment_id)
    }

    pub(crate) fn select(
        &self,
        source: IndexSource,
        entry: OffsetIndexEntry,
    ) -> Result<SelectedBatch<'_>, IndexedReadError> {
        let segment = self
            .segment(source)
            .ok_or(IndexedReadError::InvalidLocation)?;
        // Callers check `contains` or capture exact entries first. A missing or
        // stale entry here is an invariant failure, never a silent disk read.
        let cached = segment
            .operations
            .get(&entry.location.operation.op_number)
            .filter(|cached| cached.location == entry.location.operation)
            .ok_or(IndexedReadError::InvalidLocation)?;
        let batch = cached
            .batches
            .get(entry.location.batch_index as usize)
            .ok_or(IndexedReadError::InvalidSelector)?;
        Ok(SelectedBatch {
            location: &cached.location,
            body: &cached.body,
            shared_backing_bytes: cached.shared_backing_bytes,
            batch,
            batch_index: entry.location.batch_index,
            decoded: DecodedCell::default(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_journal::operation::Digest;
    use ozzy_proto::GroupId;

    fn operation(segment: u64, op: u64, bytes: usize) -> Arc<CachedOperation> {
        Arc::new(CachedOperation {
            location: OperationLocation {
                segment_id: segment,
                entry_offset: 4096,
                entry_bytes: bytes as u64,
                op_number: op,
                operation_digest: Digest::ZERO,
            },
            body: bytes::Bytes::from(vec![0; bytes]),
            shared_backing_bytes: None,
            batches: Arc::from([]),
        })
    }

    #[test]
    fn byte_budget_evicts_oldest_operations_across_segments() {
        let group = GroupId::from_bytes([1; 16]);
        let source = |segment| IndexSource {
            group_id: group,
            segment_id: segment,
            valid_bytes: u64::MAX,
            segment_digest: Digest::ZERO,
            first_op_number: 0,
            last_op_number: 0,
            last_operation_digest: Digest::ZERO,
        };
        let mut resident = ResidentOperations::with_limit(300);
        for op in 1..=3 {
            resident.insert_prepared(group, 1, &[operation(1, op, 100)]);
        }
        assert_eq!(resident.retained_bytes(), 300);
        // A roll keeps the predecessor; its oldest entries leave first.
        resident.insert_prepared(group, 2, &[operation(2, 4, 150)]);
        assert_eq!(resident.retained_bytes(), 250);
        let contains = |resident: &ResidentOperations, segment, op, bytes| {
            resident.contains(source(segment), operation(segment, op, bytes).location)
        };
        assert!(!contains(&resident, 1, 1, 100));
        assert!(!contains(&resident, 1, 2, 100));
        assert!(contains(&resident, 1, 3, 100));
        assert!(contains(&resident, 2, 4, 150));
        // A third segment drops the old predecessor and its accounting.
        resident.insert_prepared(group, 3, &[operation(3, 5, 10)]);
        assert_eq!(resident.retained_bytes(), 160);
        assert!(!contains(&resident, 1, 3, 100));
        // One operation larger than the budget stays alone.
        resident.insert_prepared(group, 3, &[operation(3, 6, 1000)]);
        assert_eq!(resident.retained_bytes(), 1000);
        assert!(contains(&resident, 3, 6, 1000));
        assert!(!contains(&resident, 3, 5, 10));
        assert!(
            resident
                .capture(source(3), [operation(3, 5, 10).location].into_iter())
                .unwrap()
                .is_none(),
            "an evicted operation makes the read use the file"
        );
    }
}

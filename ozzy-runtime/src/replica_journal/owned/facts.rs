//! Committed partition facts for diagnostics. Observation only, never
//! authority: no ticket fences the read, so a follower answers too.

use super::{JournalError, OwnedJournal};
use ozzy_core::state::{AssignmentState, CoordinatorGroup};
use ozzy_journal::operation::ProgressOwner;
use ozzy_proto::{ConsumerGroupId, Offset, PartitionIncarnation};
use std::sync::Arc;

/// One partition's committed state, summarized.
#[derive(Debug, Clone)]
pub struct PartitionFacts {
    /// Earliest retained record offset.
    pub retained_from: Offset,
    /// Offset the next record gets.
    pub next_offset: Offset,
    /// Open producer sessions.
    pub producers: usize,
    /// Consumer groups this partition coordinates.
    pub groups: Vec<Arc<CoordinatorGroup>>,
    /// Consumer groups reading this partition.
    pub sources: Vec<GroupSource>,
}

/// One consumer group on a source partition it reads.
#[derive(Debug, Clone, Copy)]
pub struct GroupSource {
    /// Group incarnation.
    pub group: ConsumerGroupId,
    /// Current assignment, absent before the first plan reached it.
    pub assignment: Option<AssignmentState>,
    /// Exclusive committed cursor, absent before the first progress.
    pub progress: Option<Offset>,
}

impl OwnedJournal {
    /// Summarize the committed state of `partition` without I/O.
    pub fn facts(
        &self,
        partition: PartitionIncarnation,
    ) -> Result<Option<PartitionFacts>, JournalError> {
        let state = self.images()?.committed();
        let Some(log) = state.partition(partition) else {
            return Ok(None);
        };
        let groups = state
            .coordinator_groups()
            .filter(|group| group.coordinator == partition)
            .cloned()
            .collect();
        let sources = state
            .source_groups(partition)
            .into_iter()
            .map(|group| GroupSource {
                group,
                assignment: state.assignment(group, partition),
                progress: state.progress(ProgressOwner::ConsumerGroup(group), partition),
            })
            .collect();
        Ok(Some(PartitionFacts {
            retained_from: log.retained_from,
            next_offset: log.next_offset,
            producers: log.producers().len(),
            groups,
            sources,
        }))
    }
}

//! Partition-local writer state and exact retry coordinates.

use std::collections::VecDeque;

use ozzy_proto::{Offset, ProducerEpoch, ProducerSequence};

use super::StateError;

/// One contiguous sequence/offset mapping. Adjacent assignments coalesce only
/// when both coordinate ranges are contiguous; another writer may occupy gaps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerResultSpan {
    pub first_sequence: ProducerSequence,
    pub first_offset: Offset,
    pub records: u64,
}

/// Independent session and retained retry metadata for one writer in a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalProducer {
    pub producer_epoch: ProducerEpoch,
    pub next_producer_sequence: ProducerSequence,
    pub producer_result_floor: ProducerSequence,
    pub(super) results: VecDeque<ProducerResultSpan>,
}

impl CanonicalProducer {
    /// Empty session. Opening/fencing authority is enforced by canonical state.
    pub fn new(producer_epoch: ProducerEpoch) -> Self {
        Self {
            producer_epoch,
            next_producer_sequence: ProducerSequence::ZERO,
            producer_result_floor: ProducerSequence::ZERO,
            results: VecDeque::new(),
        }
    }

    /// Exact original offset, independent of current SDK grouping boundaries.
    pub fn result_offset(&self, sequence: ProducerSequence) -> Option<Offset> {
        let index = self
            .results
            .partition_point(|span| span.first_sequence <= sequence);
        let span = self.results.get(index.checked_sub(1)?)?;
        let delta = sequence.get().checked_sub(span.first_sequence.get())?;
        (delta < span.records)
            .then(|| span.first_offset.get().checked_add(delta).map(Offset::new))
            .flatten()
    }

    /// Retained contiguous ranges, in increasing sequence and offset order.
    pub fn result_spans(&self) -> impl ExactSizeIterator<Item = &ProducerResultSpan> {
        self.results.iter()
    }

    /// Earliest retained retry result. Empty writers do not block partition trim.
    pub fn result_offset_floor(&self) -> Option<Offset> {
        self.results.front().map(|span| span.first_offset)
    }

    /// Whether this fresh assignment needs another bounded mapping entry.
    pub fn needs_result_span(&self, first_offset: Offset) -> bool {
        self.results.back().is_none_or(|span| {
            span.first_offset.get().checked_add(span.records) != Some(first_offset.get())
        })
    }

    /// Add one resolved assignment to a state image or a private preparation copy.
    /// This records coordinates only; it does not establish confirmation authority.
    pub fn record_assignment(
        &mut self,
        first_sequence: ProducerSequence,
        first_offset: Offset,
        records: u64,
        max_spans: usize,
    ) -> Result<(), StateError> {
        if first_sequence != self.next_producer_sequence {
            return Err(StateError::ProducerSequenceMismatch);
        }
        if records == 0 {
            return Err(StateError::MalformedBody("empty producer assignment"));
        }
        let next_sequence = first_sequence
            .get()
            .checked_add(records)
            .ok_or(StateError::PositionExhausted)?;
        first_offset
            .get()
            .checked_add(records)
            .ok_or(StateError::PositionExhausted)?;
        if self.results.back().is_some_and(|span| {
            span.first_offset
                .get()
                .checked_add(span.records)
                .is_none_or(|end| end > first_offset.get())
        }) {
            return Err(StateError::OffsetMismatch);
        }
        if self.needs_result_span(first_offset) {
            if self.results.len() >= max_spans {
                return Err(StateError::LimitExceeded("producer retry spans"));
            }
            self.results.push_back(ProducerResultSpan {
                first_sequence,
                first_offset,
                records,
            });
        } else {
            // Sum is bounded by the checked sequence and offset ends above.
            self.results
                .back_mut()
                .expect("adjacent result span")
                .records += records;
        }
        self.next_producer_sequence = ProducerSequence::new(next_sequence);
        Ok(())
    }

    pub(super) fn expire_results(&mut self, floor: ProducerSequence) {
        while self
            .results
            .front()
            .is_some_and(|span| span.first_sequence.get() + span.records <= floor.get())
        {
            self.results.pop_front();
        }
        if let Some(span) = self.results.front_mut() {
            let expired = floor.get() - span.first_sequence.get();
            span.first_sequence = floor;
            span.first_offset = Offset::new(span.first_offset.get() + expired);
            span.records -= expired;
        }
        self.producer_result_floor = floor;
    }
}

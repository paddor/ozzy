//! Partition-local writer state and exact retry coordinates.

use std::collections::VecDeque;

use ozzy_journal::operation::AppendResults;
use ozzy_proto::append::{AppendDisposition, AppendOutcome};
use ozzy_proto::{Offset, OperationId, ProducerEpoch, ProducerSequence};
use smallvec::SmallVec;

use super::StateError;

/// Current open/fence identity survives retirement of its canonical operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerTransition {
    /// Exact idempotence identity of the latest session transition.
    pub operation_id: OperationId,
    /// Original conditional fence, absent for the first session.
    pub expected_epoch: Option<ProducerEpoch>,
}

/// One contiguous sequence/offset mapping. Adjacent assignments coalesce only
/// when both coordinate ranges are contiguous; another writer may occupy gaps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerResultSpan {
    /// First producer-local sequence in this span.
    pub first_sequence: ProducerSequence,
    /// Partition-global offset assigned to that sequence.
    pub first_offset: Offset,
    /// Number of consecutive sequence and offset coordinates.
    pub records: u64,
    /// Evidence needed to verify the original attempted record.
    pub source: ProducerResultSource,
}

/// The retry proof survives independently of an original duplicate's payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProducerResultSource {
    /// Exact record bytes remain at the assigned offsets.
    Stored,
    /// Fingerprints remain in this partition's original canonical operation.
    Duplicate {
        /// Operation containing the attempted bytes' fingerprints.
        operation: u64,
    },
}

impl ProducerResultSpan {
    /// Stored or duplicate, without implying a confirmation boundary.
    pub const fn disposition(self) -> AppendDisposition {
        match self.source {
            ProducerResultSource::Stored => AppendDisposition::Stored,
            ProducerResultSource::Duplicate { .. } => AppendDisposition::Duplicate,
        }
    }

    pub(super) fn can_extend(self, next: Self) -> bool {
        self.source == next.source
            && self.first_sequence.get().checked_add(self.records)
                == Some(next.first_sequence.get())
            && self.first_offset.get().checked_add(self.records) == Some(next.first_offset.get())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PreparedProducerResults {
    pub(super) spans: SmallVec<[ProducerResultSpan; 4]>,
    pub(super) additional: usize,
    pub(super) next_sequence: ProducerSequence,
    pub(super) next_offset: Offset,
    pub(super) last_stored_end: Offset,
}

/// Independent session and retained retry metadata for one writer in a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalProducer {
    /// Current producer-session fence.
    pub producer_epoch: ProducerEpoch,
    /// Next admissible producer-local sequence.
    pub next_producer_sequence: ProducerSequence,
    /// Earliest sequence whose original retry result remains retained.
    pub producer_result_floor: ProducerSequence,
    pub(super) transition: Option<ProducerTransition>,
    pub(super) results: VecDeque<ProducerResultSpan>,
    pub(super) last_stored_end: Offset,
}

impl CanonicalProducer {
    /// Empty session. Opening/fencing authority is enforced by canonical state.
    pub fn new(producer_epoch: ProducerEpoch) -> Self {
        Self {
            producer_epoch,
            next_producer_sequence: ProducerSequence::ZERO,
            producer_result_floor: ProducerSequence::ZERO,
            transition: None,
            results: VecDeque::new(),
            last_stored_end: Offset::ZERO,
        }
    }

    /// Latest confirmed or speculative transition in this state image.
    pub fn transition(&self) -> Option<ProducerTransition> {
        self.transition
    }

    /// Exact original offset, independent of current SDK grouping boundaries.
    pub fn result_offset(&self, sequence: ProducerSequence) -> Option<Offset> {
        self.result_outcome(sequence).map(|outcome| outcome.offset)
    }

    /// The original disposition and offset of one retained attempt.
    pub fn result_outcome(&self, sequence: ProducerSequence) -> Option<AppendOutcome> {
        let span = self.result_span(sequence)?;
        let delta = sequence.get() - span.first_sequence.get();
        Some(AppendOutcome {
            disposition: span.disposition(),
            offset: Offset::new(span.first_offset.get().checked_add(delta)?),
        })
    }

    /// Locate retained coordinates and the operation needed for exact retry proof.
    pub fn result_span(&self, sequence: ProducerSequence) -> Option<&ProducerResultSpan> {
        let index = self
            .results
            .partition_point(|span| span.first_sequence <= sequence);
        let span = self.results.get(index.checked_sub(1)?)?;
        let delta = sequence.get().checked_sub(span.first_sequence.get())?;
        (delta < span.records).then_some(span)
    }

    /// Retained contiguous ranges, in increasing sequence order.
    pub fn result_spans(&self) -> impl ExactSizeIterator<Item = &ProducerResultSpan> {
        self.results.iter()
    }

    /// Earliest retained retry result. Empty writers do not block partition trim.
    pub fn result_offset_floor(&self) -> Option<Offset> {
        self.results
            .iter()
            .filter(|span| span.source == ProducerResultSource::Stored)
            .map(|span| span.first_offset)
            .min()
    }

    /// Oldest operation carrying a retained duplicate retry fingerprint.
    pub fn result_operation_floor(&self) -> Option<u64> {
        self.results
            .iter()
            .filter_map(|span| match span.source {
                ProducerResultSource::Duplicate { operation } => Some(operation),
                ProducerResultSource::Stored => None,
            })
            .min()
    }

    /// Expire exactly the retry coordinates whose assigned offsets precede a
    /// partition trim. Other producers may occupy gaps between this one's spans.
    pub fn result_floor_for_offset(&self, floor: Offset) -> ProducerSequence {
        self.result_floor_for_retention(floor, 0)
    }

    /// Retire only a contiguous prefix whose stored bytes and duplicate proof
    /// operations are below the proposed record and exclusive operation floors.
    pub fn result_floor_for_retention(
        &self,
        floor: Offset,
        operation_floor: u64,
    ) -> ProducerSequence {
        for span in &self.results {
            if let ProducerResultSource::Duplicate { operation } = span.source {
                if operation >= operation_floor {
                    return span.first_sequence;
                }
                continue;
            }
            let expired = floor.get().saturating_sub(span.first_offset.get());
            if expired < span.records {
                return ProducerSequence::new(span.first_sequence.get() + expired);
            }
        }
        self.next_producer_sequence
    }

    /// Whether this fresh assignment needs another bounded mapping entry.
    pub fn needs_result_span(&self, first_offset: Offset) -> bool {
        self.results.back().is_none_or(|span| {
            span.source != ProducerResultSource::Stored
                || span.first_offset.get().checked_add(span.records) != Some(first_offset.get())
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
        let stored_end = first_offset
            .get()
            .checked_add(records)
            .ok_or(StateError::PositionExhausted)?;
        if first_offset < self.last_stored_end {
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
                source: ProducerResultSource::Stored,
            });
        } else {
            // Sum is bounded by the checked sequence and offset ends above.
            self.results
                .back_mut()
                .expect("adjacent result span")
                .records += records;
        }
        self.next_producer_sequence = ProducerSequence::new(next_sequence);
        self.last_stored_end = Offset::new(stored_end);
        Ok(())
    }

    pub(super) fn prepare_results(
        &self,
        first_sequence: ProducerSequence,
        first_offset: Offset,
        results: AppendResults<'_>,
        operation: u64,
    ) -> Result<PreparedProducerResults, StateError> {
        if first_sequence != self.next_producer_sequence {
            return Err(StateError::ProducerSequenceMismatch);
        }
        if operation == 0 {
            return Err(StateError::OperationNumberMismatch);
        }
        if first_offset < self.last_stored_end {
            return Err(StateError::OffsetMismatch);
        }
        let (next_sequence, next_offset) = results
            .validate_positions(first_sequence, first_offset, results.stored() as usize)
            .map_err(|_| StateError::MalformedBody("invalid producer outcomes"))?;
        let mut spans = SmallVec::<[ProducerResultSpan; 4]>::new();
        let mut sequence = first_sequence.get();
        for run in results.runs() {
            let source = match run.disposition {
                AppendDisposition::Stored => ProducerResultSource::Stored,
                AppendDisposition::Duplicate => ProducerResultSource::Duplicate { operation },
                _ => return Err(StateError::MalformedBody("unknown producer outcome")),
            };
            let next = ProducerResultSpan {
                first_sequence: ProducerSequence::new(sequence),
                first_offset: run.first_offset,
                records: u64::from(run.records),
                source,
            };
            if let Some(last) = spans.last_mut()
                && last.can_extend(next)
            {
                last.records += next.records;
            } else {
                spans.push(next);
            }
            sequence += u64::from(run.records);
        }
        let additional = spans.len()
            - usize::from(
                self.results
                    .back()
                    .is_some_and(|last| last.can_extend(spans[0])),
            );
        Ok(PreparedProducerResults {
            spans,
            additional,
            next_sequence,
            next_offset,
            last_stored_end: if results.stored() == 0 {
                self.last_stored_end
            } else {
                next_offset
            },
        })
    }

    pub(super) fn install_results(&mut self, prepared: PreparedProducerResults) {
        for next in prepared.spans {
            if let Some(last) = self.results.back_mut()
                && last.can_extend(next)
            {
                last.records += next.records;
            } else {
                self.results.push_back(next);
            }
        }
        self.next_producer_sequence = prepared.next_sequence;
        self.last_stored_end = prepared.last_stored_end;
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

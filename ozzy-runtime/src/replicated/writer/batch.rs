//! Bounded SDK request selection. No caller batches and no transport scheduling.

use omq_tokio::message::Payload;
use ozzy_proto::Envelope;
use ozzy_proto::append::{self, Append, Authority, DataLimits, PayloadEncoding, Record};
use smallvec::SmallVec;
use std::{collections::VecDeque, ops::Range};

use super::{AppendKey, Error, Shared, state::Queued};

mod payload;
mod selection;
pub(super) use payload::PayloadPool;
use selection::Selection;

pub(super) struct Batch {
    pub(super) records: SelectedRecords,
    pub(super) bytes: usize,
    pub(super) waiting_for_payload: bool,
    payload: Option<Payload>,
    /// A partial selection waiting for an in-flight APPEND. Records queued
    /// later extend it, so each record is sized once per request.
    partial: Option<Partial>,
}

/// A partial selection and the inputs it was sized with.
struct Partial {
    sizing: Sizing,
    selection: Selection,
}

/// What a selection is sized with: its first sequence, record cap, and
/// request limits. A partial selection resumes only under the same inputs.
#[derive(Clone, Copy, PartialEq)]
struct Sizing {
    next: u64,
    cap: usize,
    limits: DataLimits,
}

impl Sizing {
    /// Extend `partial` when it was sized with these inputs and its records
    /// are still queued from `index`, else size afresh from `index`. Only
    /// records queued since are sized.
    fn extend(
        self,
        partial: Option<Partial>,
        records: &VecDeque<Queued>,
        index: usize,
        target_bytes: usize,
    ) -> Result<Selection, Error> {
        let mut selection = partial
            .filter(|partial| {
                partial.sizing == self && index + partial.selection.records <= records.len()
            })
            .map_or_else(
                || Selection::new(append::IDENTITY_METADATA_BYTES + 10),
                |partial| partial.selection,
            );
        selection.extend(
            records.range(index + selection.records..).map(|record| {
                (
                    record.body.len(),
                    record.parts(),
                    record.encoding,
                    record.due_millis.is_some(),
                )
            }),
            self.limits,
            target_bytes,
            self.cap,
        )?;
        Ok(selection)
    }
}

/// One APPEND request to encode: its envelope, the partition authority it
/// writes under, and its packed body.
#[derive(Clone, Copy)]
pub(super) struct RequestView<'a> {
    pub(super) envelope: Envelope,
    pub(super) authority: Authority,
    pub(super) payload: PayloadView<'a>,
}

/// Borrowed admission range. Selection and encoding run synchronously on the
/// SDK owner, so duplicating every record descriptor would only add work.
pub(super) struct SelectedRecords {
    start: usize,
    count: usize,
}

impl SelectedRecords {
    pub(super) fn len(&self) -> usize {
        self.count
    }

    pub(super) fn clear(&mut self) {
        self.count = 0;
    }
}

#[derive(Clone, Copy)]
pub(super) struct PayloadView<'a> {
    pub(super) decoded: &'a [u8],
    pub(super) encoding: PayloadEncoding,
    pub(super) encoded_bytes: usize,
}

impl Batch {
    pub(super) fn new() -> Self {
        Self {
            records: SelectedRecords { start: 0, count: 0 },
            bytes: 0,
            waiting_for_payload: false,
            payload: None,
            partial: None,
        }
    }

    /// Select the next APPEND. While an earlier APPEND of this writer is
    /// unconfirmed (`in_flight`), a partial batch keeps collecting until that
    /// confirmation arrives. Full batches and flushes still pipeline. With
    /// nothing in flight, a single ready record is sent at once.
    pub(super) fn select(
        &mut self,
        next: u64,
        limits: DataLimits,
        in_flight: bool,
        shared: &mut super::state::Driver,
    ) -> Result<bool, Error> {
        self.records.clear();
        self.payload = None;
        self.bytes = 0;
        self.waiting_for_payload = false;
        let partial = self.partial.take();
        shared.drain();
        let force_through = shared
            .flush_through
            .load(std::sync::atomic::Ordering::Acquire);
        let sealed = shared.sealed();
        let target_bytes = shared.config.batch_target_bytes;
        let records = &shared.admission.records;
        let Some(first) = records.front() else {
            return Ok(false);
        };
        let index = usize::try_from(next - first.sequence).map_err(|_| Error::Response)?;
        if records.get(index).is_none() {
            return Ok(false);
        }
        let force = next < force_through || sealed;
        let mut cap = limits.max_records.min(super::MAX_APPEND_RECORDS);
        if next < force_through {
            cap = cap.min((force_through - next) as usize);
        }
        let sizing = Sizing { next, cap, limits };
        let selection = sizing.extend(partial, records, index, target_bytes)?;
        let count = selection.records;
        if count == 0 {
            return Ok(false);
        }
        self.bytes = selection.bytes;
        if !force && !selection.full && in_flight {
            self.partial = Some(Partial { sizing, selection });
            return Ok(false);
        }
        Ok(self.pack(shared, index..index + count))
    }

    /// Pack the selected `range` into one transport body. `false` while sent
    /// frames still hold every packing buffer of this route.
    fn pack(&mut self, shared: &mut super::state::Driver, range: Range<usize>) -> bool {
        let admission = &mut shared.admission;
        let Some(payload) = shared.batches.pack(admission, range.clone()) else {
            self.waiting_for_payload = true;
            return false;
        };
        self.payload = Some(payload);
        admission.profile_queue(range.clone());
        self.records = SelectedRecords {
            start: range.start,
            count: range.len(),
        };
        shared
            .groups_formed
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        true
    }

    /// Whether the selected records are scheduled. Their payload stays raw.
    pub(super) fn scheduled(&self, driver: &super::state::Driver) -> bool {
        self.selected(driver)
            .next()
            .is_some_and(|record| record.due_millis.is_some())
    }

    /// Due times of a scheduled selection. Selection never mixes scheduled
    /// and immediate records.
    fn due_times(&self, driver: &super::state::Driver) -> Option<Vec<u64>> {
        self.scheduled(driver).then(|| {
            self.selected(driver)
                .filter_map(|record| record.due_millis)
                .collect()
        })
    }

    /// Transfer the prepared frame once; descriptor scratch retains no payloads.
    pub(super) fn take_payload(&mut self) -> Option<Payload> {
        self.payload.take()
    }

    pub(super) fn encode(
        &self,
        driver: &super::state::Driver,
        request: RequestView<'_>,
        metadata: &mut Vec<u8>,
        limits: DataLimits,
    ) -> Result<[u8; 64], Error> {
        // Borrow the already packed transport body once, not each record's
        // original owner. Its order and part boundaries match this selection.
        let shared: &Shared = driver;
        let payload = request.payload;
        let selected = self.selected(driver);
        // An empty selection is an APPEND without records.
        let first_sequence = selected
            .clone()
            .next()
            .ok_or(Error::Append(append::CodecError::Limit))?
            .sequence;
        let body = payload.decoded;
        debug_assert_eq!(body.len(), self.bytes);
        let mut parts: SmallVec<[&[u8]; 1024]> = SmallVec::new();
        let mut offset = 0;
        for record in selected.clone() {
            for length in record.lengths() {
                parts.push(&body[offset..offset + length]);
                offset += length;
            }
        }
        debug_assert_eq!(offset, body.len());
        let mut start = 0;
        let records: SmallVec<[Record<'_>; 1024]> = self
            .selected(driver)
            .map(|record| {
                let end = start + record.parts();
                let view = Record {
                    encoding: record.encoding,
                    message_id: record.message_id(),
                    parts: &parts[start..end],
                };
                start = end;
                view
            })
            .collect();
        let due_millis = self.due_times(driver);
        let key = AppendKey {
            producer_id: shared.config.producer_id,
            producer_epoch: shared.config.producer_epoch,
            first_sequence,
        };
        Ok(append::encode_prepared_append_metadata(
            request.envelope,
            Append {
                authority: request.authority,
                partition: shared.config.partition,
                owner_epoch: shared.config.owner_epoch,
                key,
                policy: shared.config.policy,
                due_millis: due_millis.as_deref(),
                records: &records,
            },
            metadata,
            append::PreparedPayload {
                encoding: payload.encoding,
                encoded_bytes: payload.encoded_bytes,
            },
            limits,
        )?)
    }

    fn selected<'a>(
        &self,
        driver: &'a super::state::Driver,
    ) -> impl Clone + Iterator<Item = &'a super::state::Queued> {
        driver
            .admission
            .records
            .range(self.records.start..self.records.start + self.records.count)
    }
}

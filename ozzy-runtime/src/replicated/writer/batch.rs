//! Bounded SDK request selection. No caller batches and no transport scheduling.

use omq_tokio::message::Payload;
use ozzy_proto::Envelope;
use ozzy_proto::append::{self, Append, Authority, DataLimits, PayloadEncoding, Record};
use smallvec::SmallVec;
use tokio::time::Instant;

use super::{AppendKey, Error, PartitionTarget, Shared};

mod payload;
mod selection;
pub(super) use payload::PayloadPool;

pub(super) struct Batch {
    pub(super) records: SelectedRecords,
    pub(super) bytes: usize,
    pub(super) deadline: Option<Instant>,
    pub(super) waiting_for_payload: bool,
    payload: Option<Payload>,
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
    pub(super) fn new(_records: usize) -> Self {
        Self {
            records: SelectedRecords { start: 0, count: 0 },
            bytes: 0,
            deadline: None,
            waiting_for_payload: false,
            payload: None,
        }
    }

    pub(super) fn select(
        &mut self,
        next: u64,
        limits: DataLimits,
        record_credit: usize,
        byte_credit: usize,
        shared: &mut super::state::Driver,
    ) -> Result<bool, Error> {
        self.records.clear();
        self.payload = None;
        self.bytes = 0;
        self.deadline = None;
        self.waiting_for_payload = false;
        if record_credit == 0 {
            return Ok(false);
        }
        shared.drain();
        let force_through = shared
            .flush_through
            .load(std::sync::atomic::Ordering::Acquire);
        let sealed = shared.sealed();
        let fixed = shared.config.partition.metadata_bytes() + 10;
        let target_bytes = shared.config.batch_target_bytes;
        let linger = shared.config.linger;
        let admission = &mut shared.admission;
        let Some(first) = admission.records.front() else {
            return Ok(false);
        };
        let index = usize::try_from(next - first.sequence).map_err(|_| Error::Response)?;
        let Some(oldest) = admission.records.get(index) else {
            return Ok(false);
        };
        let force = next < force_through || sealed;
        let mut cap = limits
            .max_records
            .min(record_credit)
            .min(super::MAX_APPEND_RECORDS);
        if next < force_through {
            cap = cap.min((force_through - next) as usize);
        }
        let selection = selection::Selection::collect(
            admission
                .records
                .range(index..)
                .map(|record| (record.body.len(), record.parts(), record.encoding)),
            limits,
            target_bytes,
            cap,
            byte_credit,
            fixed,
        )?;
        let count = selection.records;
        if count == 0 {
            return Ok(false);
        }
        self.bytes = selection.bytes;
        if !force
            && !selection.full
            && let Some(deadline) = oldest.linger_deadline(linger)
            && Instant::now() < deadline
        {
            self.deadline = Some(deadline);
            return Ok(false);
        }
        let Some(payload) = shared.batches.pack(admission, index..index + count) else {
            self.waiting_for_payload = true;
            return Ok(false);
        };
        self.payload = Some(payload);
        admission.profile_queue(index..index + count);
        self.records = SelectedRecords {
            start: index,
            count,
        };
        shared
            .groups_formed
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        Ok(true)
    }

    /// Transfer the prepared frame once; descriptor scratch retains no payloads.
    pub(super) fn take_payload(&mut self) -> Option<Payload> {
        self.payload.take()
    }

    pub(super) fn encode(
        &self,
        driver: &super::state::Driver,
        authority: Option<Authority>,
        envelope: Envelope,
        payload: PayloadView<'_>,
        metadata: &mut Vec<u8>,
        limits: DataLimits,
    ) -> Result<[u8; 64], Error> {
        // Borrow the already packed transport body once, not each record's
        // original owner. Its order and part boundaries match this selection.
        let shared: &Shared = driver;
        let selected = driver
            .admission
            .records
            .range(self.records.start..self.records.start + self.records.count);
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
        let key = AppendKey {
            producer_id: shared.config.producer_id,
            producer_epoch: shared.config.producer_epoch,
            first_sequence: self
                .selected(driver)
                .next()
                .expect("nonempty selected batch")
                .sequence,
        };
        Ok(match &shared.config.partition {
            PartitionTarget::Group(partition) => append::encode_prepared_append_metadata(
                envelope,
                Append {
                    authority: authority.expect("group route"),
                    partition: *partition,
                    owner_epoch: shared.config.owner_epoch,
                    key,
                    policy: shared.config.policy,
                    records: &records,
                },
                metadata,
                payload.encoding,
                payload.encoded_bytes,
                limits,
            ),
        }?)
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

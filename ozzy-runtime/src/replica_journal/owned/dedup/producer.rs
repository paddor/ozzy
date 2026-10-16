use super::{JournalError, OwnedJournal};
use crate::replica_journal::{AppendBuffer, append::dedup::Rewrite};
use ozzy_core::dedup::AttemptOperation;
use ozzy_replication::{JournalGeneration, Scope, driver::ValidationTicket};
use tokio::time::Instant;

#[derive(Debug)]
pub(super) struct Clock {
    scope: Scope,
    generation: JournalGeneration,
    base: u64,
    start: Instant,
}

impl Clock {
    pub(super) fn time(
        clock: &mut Option<Self>,
        ticket: ValidationTicket,
        accepted_time: u64,
    ) -> Result<u64, JournalError> {
        let now = Instant::now();
        let clock = clock.get_or_insert_with(|| Self {
            scope: ticket.scope(),
            generation: ticket.generation(),
            base: accepted_time,
            start: now,
        });
        if clock.scope != ticket.scope() || clock.generation != ticket.generation() {
            *clock = Self {
                scope: ticket.scope(),
                generation: ticket.generation(),
                base: accepted_time,
                start: now,
            };
        }
        let elapsed = u64::try_from(now.duration_since(clock.start).as_millis())
            .map_err(|_| JournalError::AppendCapacity)?;
        let time = clock
            .base
            .checked_add(elapsed)
            .ok_or(JournalError::AppendCapacity)?;
        if time < accepted_time {
            clock.base = accepted_time;
            clock.start = now;
        }
        Ok(time.max(accepted_time))
    }
}

impl OwnedJournal {
    pub(in crate::replica_journal::owned) fn resolve_producer_ids(
        &mut self,
        ticket: ValidationTicket,
        buffer: &AppendBuffer,
    ) -> Result<Rewrite, JournalError> {
        let limits = self.dedup.limits.ok_or(JournalError::Configuration)?;
        let record_bound = limits
            .batch_records
            .checked_mul(buffer.len())
            .ok_or(JournalError::AppendCapacity)?
            .min(buffer.body_bytes() / 16);
        let bytes = record_bound
            .checked_mul(256)
            .and_then(|bytes| bytes.checked_add(buffer.len() * 256 + 1024))
            .ok_or(JournalError::AppendCapacity)?;
        let charge = self.charge_validation(bytes)?;
        let image = self
            .dedup
            .image
            .as_mut()
            .ok_or(JournalError::AppendMismatch)?;
        let batches = buffer.producer_batches(self.limits.operations)?;
        image.index.expire(record_bound, limits.bytes);
        let time = Clock::time(&mut self.dedup.clock, ticket, image.time)?;
        let first = ticket
            .accepted()
            .op
            .0
            .checked_add(1)
            .ok_or(JournalError::AppendMismatch)?;
        let mut ids = Vec::new();
        ids.try_reserve_exact(batches.len())
            .map_err(std::io::Error::other)?;
        for batch in &batches {
            if Some(batch.summary.partition) != image.partition
                || batch.summary.record_count > limits.batch_records
            {
                return Err(JournalError::AppendMismatch);
            }
            let mut rows = Vec::new();
            rows.try_reserve_exact(batch.summary.record_count)
                .map_err(std::io::Error::other)?;
            rows.extend(batch.descriptors().map(|record| record.message_id));
            ids.push(rows);
        }
        let operations: smallvec::SmallVec<[_; 4]> = ids
            .iter()
            .enumerate()
            .map(|(index, ids)| {
                Ok(AttemptOperation {
                    operation: first
                        .checked_add(index as u64)
                        .ok_or(JournalError::AppendMismatch)?,
                    logical_time: time,
                    ids,
                })
            })
            .collect::<Result<_, JournalError>>()?;
        let prepared = image.index.prepare_group(&operations, image.end)?;
        if let Some(original) = prepared.pending_original() {
            return Err(ozzy_core::dedup::Error::PendingOriginal(original).into());
        }
        let codec_budget = limits.bytes.saturating_sub(image.index.reserved_bytes());
        buffer.resolve_dedup(
            &prepared,
            &mut self.dedup.codec,
            codec_budget,
            self.limits.operations,
            charge,
        )
    }
}

//! Writer assignment uses pure shared policy; cold retry reads are file futures.

use super::{AppendBuffer, JournalError, OwnedJournal, prefix};
use crate::replica_journal::{
    AppendAdmissionError, ProposalBuffer, ProposalValidation, ValidatedAppend,
    append::ProducerRetryResults,
    commands::read_fault,
    producer::{self, Assignment, Plan},
};
use ozzy_journal::operation::{OperationBody, OperationKind, decode_operation_body};
use ozzy_journal_segment::{DecodedBatches, JournalIndexError};
use ozzy_replication::{Prefix, driver::ValidationTicket};

enum Prepared {
    Fresh(
        ozzy_core::state::PreparedCanonicalGroup,
        Vec<ozzy_journal_segment::PreparedOperationRecords>,
    ),
    Retry(Prefix),
}

// A protocol APPEND is one canonical operation. Read all its records together
// so a cold retry does not reread and decode that operation for each small
// verification chunk. ReadLimits still bounds the returned payload bytes.
const RETRY_READ_RECORDS: usize = 2048;

impl OwnedJournal {
    /// Cold retries and control identities may read files while holding the
    /// journal owner. Let earlier writes finish before starting that read.
    pub(in crate::replica_journal) fn validation_may_read(&self, buffer: &AppendBuffer) -> bool {
        if !buffer.is_producer() {
            return buffer.producer_session.is_some()
                || buffer.operations().any(|operation| {
                    operation.kind != OperationKind::Append
                        && decode_operation_body(
                            operation.kind,
                            operation.body,
                            self.limits.operations,
                        )
                        .map_or(true, |body| body.operation_id().is_some())
                });
        }
        let Ok(images) = self.images() else {
            return true;
        };
        (0..buffer.len()).any(|index| {
            !matches!(
                producer::plan(
                    images.speculative(),
                    buffer,
                    index,
                    self.buffer_generation,
                    self.limits.operations,
                ),
                Ok(Plan::Fresh(_))
            )
        })
    }

    /// Lease a body-only request arena from this owner's existing bounded pool.
    pub fn lease_proposal_buffer(
        &self,
    ) -> Result<ProposalBuffer, crate::replica_journal::SubmitError> {
        self.lease_append_buffer().map(ProposalBuffer)
    }

    /// Assign shared partition offsets and verify per-writer retries without
    /// accepting data. The caller supplies its observed timestamp and must
    /// recheck the returned driver image before admission or replying.
    /// SDK-compressed payloads remain unchanged; no broker recompression occurs.
    pub async fn propose_append(
        &mut self,
        ticket: ValidationTicket,
        ProposalBuffer(mut buffer): ProposalBuffer,
        timestamp_millis: u64,
    ) -> ProposalValidation {
        let mut positions = smallvec::SmallVec::<[[u8; 16]; 8]>::new();
        let result = self
            .prepare_proposal(ticket, &mut buffer, timestamp_millis, &mut positions)
            .await;
        self.faulted |= result.as_ref().err().is_some_and(read_fault);
        match result {
            Ok(Prepared::Fresh(plan, records)) => ProposalValidation::Ready(ValidatedAppend {
                validation: ticket,
                buffer,
                plan,
                records,
            }),
            Ok(Prepared::Retry(through)) => ProposalValidation::Resolved {
                validation: ticket,
                through,
                buffer: ProposalBuffer(buffer),
            },
            Err(reason) => {
                for (index, position) in positions.into_iter().enumerate() {
                    buffer
                        .replace_append_position(index, position)
                        .expect("private proposal backing is already reserved");
                }
                ProposalValidation::Rejected {
                    reason,
                    buffer: ProposalBuffer(buffer),
                }
            }
        }
    }

    async fn prepare_proposal(
        &mut self,
        ticket: ValidationTicket,
        buffer: &mut AppendBuffer,
        timestamp_millis: u64,
        positions: &mut smallvec::SmallVec<[[u8; 16]; 8]>,
    ) -> Result<Prepared, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        if buffer.owner_generation() != self.buffer_generation
            || buffer
                .retry_results
                .as_ref()
                .is_some_and(|results| results.trimmed)
            || self.configuration.primary(ticket.scope().view)
                != self.journal.readable()?.manifest().identity.replica_node_id
        {
            return Err(JournalError::AppendMismatch);
        }
        buffer.retry_results = None;
        if buffer
            .proposal_policy
            .is_some_and(|policy| policy != self.configuration.append_policy())
        {
            return Err(AppendAdmissionError::Policy.into());
        }
        if buffer.proposal_authority.is_some_and(|authority| {
            authority.group_id != ticket.scope().group_id
                || authority.config_epoch != ticket.scope().configuration_epoch
                || authority.view != ticket.scope().view
        }) {
            return Err(AppendAdmissionError::Authority.into());
        }
        if buffer.producer_session.is_some()
            && let Some(through) = Box::pin(self.prepare_producer_open(ticket, buffer)).await?
        {
            return Ok(Prepared::Retry(through));
        }
        if let Some(through) = crate::replica_journal::proposal::partition_retry(
            self.images()?,
            buffer,
            ticket,
            self.buffer_generation,
            self.limits.operations,
        )? {
            return Ok(Prepared::Retry(through));
        }
        if buffer.is_producer() {
            let grouped = buffer.len() > 1;
            for index in 0..buffer.len() {
                let Assignment {
                    position,
                    retry,
                    trim,
                    results,
                } = self
                    .assign_producer(buffer, index, timestamp_millis)
                    .await?;
                if grouped && (retry.is_some() || trim != 0) {
                    return Err(JournalError::AppendMismatch);
                }
                if trim != 0 {
                    buffer.trim_producer_prefix(trim, self.limits.operations)?;
                }
                buffer.retry_results = (!results.is_empty()).then(|| {
                    Box::new(ProducerRetryResults {
                        ranges: results,
                        trimmed: trim != 0,
                    })
                });
                positions.push(buffer.replace_append_position(index, position)?);
                if let Some(through) = retry {
                    return Ok(Prepared::Retry(through));
                }
            }
        }
        let (plan, records) = self.prepare_append(ticket, buffer, true).await?;
        Ok(Prepared::Fresh(plan, records))
    }

    async fn assign_producer(
        &mut self,
        buffer: &AppendBuffer,
        index: usize,
        now: u64,
    ) -> Result<Assignment, JournalError> {
        let plan = producer::plan(
            self.images()?.speculative(),
            buffer,
            index,
            self.buffer_generation,
            self.limits.operations,
        )?;
        let Plan::Retry(retry) = plan else {
            let Plan::Fresh(offset) = plan else {
                unreachable!()
            };
            return Ok(Assignment::fresh(offset, now));
        };
        let operation = buffer
            .operations()
            .nth(index)
            .ok_or(JournalError::AppendMismatch)?;
        let OperationBody::Append(body) =
            decode_operation_body(operation.kind, operation.body, self.limits.operations)?
        else {
            return Err(JournalError::AppendMismatch);
        };
        let [expected] = body.batches.as_slice() else {
            return Err(JournalError::AppendMismatch);
        };
        let mut snapshot = None;
        let mut decoded = DecodedBatches::default();
        let mut timestamp = None;
        let mut through = Prefix::GENESIS;
        let mut expected_records = expected.records.iter();
        let mut index = 0;
        while index < retry.retained {
            let offset = retry.offset(index)?;
            if let Some((actual, position)) =
                self.writeback
                    .record(retry.batch.partition, offset, &mut decoded)
            {
                retry.verify(
                    index,
                    &actual,
                    expected_records
                        .next()
                        .ok_or(JournalError::AppendMismatch)?,
                )?;
                timestamp.get_or_insert(actual.append_timestamp_millis);
                through = position;
                index += 1;
                continue;
            }
            let offsets = self.cold_retry_offsets(&retry, index, &mut decoded)?;
            let limits = ozzy_journal::ReadLimits {
                max_records: RETRY_READ_RECORDS,
                max_bytes: self.limits.operations.max_payload_bytes,
            };
            let recent = self
                .reader
                .as_ref()
                .ok_or(JournalError::AppendMismatch)?
                .read_offsets_with_positions(
                    self.journal.readable()?,
                    retry.batch.partition,
                    &offsets,
                    limits,
                )
                .await?;
            let records = if let Some(records) = recent {
                records
            } else {
                if snapshot.is_none() {
                    snapshot = Some(self.journal.retry_snapshot(self.recovery.index).await?);
                }
                snapshot
                    .as_ref()
                    .expect("retry snapshot")
                    .read_offsets_with_positions(retry.batch.partition, &offsets, limits)
                    .await?
            };
            if records.is_empty() {
                return Err(JournalIndexError::MissingOffset(offset).into());
            }
            for (actual, position) in records {
                retry.verify(
                    index,
                    &actual,
                    expected_records
                        .next()
                        .ok_or(JournalError::AppendMismatch)?,
                )?;
                timestamp.get_or_insert(actual.append_timestamp_millis);
                through = prefix(position);
                index += 1;
            }
        }
        Ok(retry.finish(timestamp.expect("nonempty retry"), through, now))
    }

    fn cold_retry_offsets(
        &self,
        retry: &producer::Retry,
        index: usize,
        decoded: &mut DecodedBatches,
    ) -> Result<Vec<ozzy_proto::Offset>, JournalError> {
        let end = (index + RETRY_READ_RECORDS).min(retry.retained);
        let mut offsets = Vec::with_capacity(end - index);
        for next in index..end {
            let offset = retry.offset(next)?;
            // A later retry range can still be queued for its physical write.
            // Its authoritative bytes remain in writeback, outside this snapshot.
            if next != index
                && self
                    .writeback
                    .record(retry.batch.partition, offset, decoded)
                    .is_some()
            {
                break;
            }
            offsets.push(offset);
        }
        Ok(offsets)
    }
}

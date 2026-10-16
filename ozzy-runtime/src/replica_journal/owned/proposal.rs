//! Writer assignment uses pure shared policy; cold retry reads are file futures.

use super::{AppendBuffer, JournalError, OwnedJournal, prefix};
use crate::replica_journal::{
    AppendAdmissionError, ProposalBuffer, ProposalDeferral, ProposalValidation, ValidatedAppend,
    append::ProducerRetryResults,
    commands::read_fault,
    producer::{self, Assignment, Plan},
};
use ozzy_core::state::ProducerResultSource;
use ozzy_journal::operation::{OperationBody, decode_operation_body};
use ozzy_journal_segment::{DecodedBatches, JournalIndexError};
use ozzy_replication::{Prefix, driver::ValidationTicket};

enum Prepared {
    Fresh(
        ozzy_core::state::PreparedCanonicalGroup,
        Vec<ozzy_journal_segment::PreparedOperationRecords>,
        Option<Box<super::dedup::Prepared>>,
    ),
    Retry(Prefix),
}

// A protocol APPEND is one canonical operation. Read all its records together
// so a cold retry does not reread and decode that operation for each small
// verification chunk. ReadLimits still bounds the returned payload bytes.
const RETRY_READ_RECORDS: usize = 2048;

#[derive(Clone, Copy)]
struct DuplicateEvidence {
    number: u64,
    through: Prefix,
    proof: producer::DuplicateProof,
}

impl OwnedJournal {
    /// Cold retries and control identities may read files while holding the
    /// journal owner. Let earlier writes finish before starting that read.
    pub(in crate::replica_journal) fn validation_may_read(&self, buffer: &AppendBuffer) -> bool {
        if let Some(session) = &buffer.group_assignment
            && self.images().is_ok_and(|images| {
                images
                    .speculative()
                    .assignment(session.request.group, session.request.partition)
                    .is_some_and(|current| current.plan_revision == session.request.plan_revision)
            })
        {
            return false;
        }
        if let Some(session) = &buffer.group_progress {
            let Some(cursor) = session.request.cursor else {
                return false;
            };
            if self.images().is_ok_and(|images| {
                images
                    .speculative()
                    .progress(
                        ozzy_journal::operation::ProgressOwner::ConsumerGroup(
                            session.request.assignment.group,
                        ),
                        session.request.assignment.partition,
                    )
                    .is_some_and(|prior| cursor <= prior)
            }) {
                return false;
            }
        }
        if !buffer.is_producer() {
            // Bodies without control identities never resolve them, and
            // invalid bodies are rejected without a read.
            return buffer.producer_session.is_some()
                || buffer
                    .operations()
                    .any(|operation| operation.kind.has_operation_id());
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
    /// All-unique SDK-compressed payloads stay unchanged. Deduplication selects
    /// and repacks mixed batches while retaining exact retry evidence.
    pub async fn propose_append(
        &mut self,
        ticket: ValidationTicket,
        ProposalBuffer(mut buffer): ProposalBuffer,
        timestamp_millis: u64,
    ) -> ProposalValidation {
        self.begin_validation_memory();
        let mut positions = smallvec::SmallVec::<[[u8; 16]; 8]>::new();
        let result = self
            .prepare_proposal(ticket, &mut buffer, timestamp_millis, &mut positions)
            .await;
        self.faulted |= result.as_ref().err().is_some_and(read_fault);
        match result {
            Ok(Prepared::Fresh(plan, records, dedup)) => {
                ProposalValidation::Ready(ValidatedAppend {
                    validation: ticket,
                    buffer,
                    plan,
                    records,
                    dedup,
                })
            }
            Ok(Prepared::Retry(through)) => ProposalValidation::Resolved {
                validation: ticket,
                through,
                buffer: ProposalBuffer(buffer),
            },
            Err(reason) => {
                buffer.restore_dedup_original();
                for (index, position) in positions.into_iter().enumerate() {
                    buffer
                        .replace_append_position(index, position)
                        .expect("private proposal backing is already reserved");
                }
                let deferral = match &reason {
                    JournalError::Dedup(ozzy_core::dedup::Error::PendingOriginal(original)) => {
                        Some(ProposalDeferral::Original(ozzy_replication::OpNumber(
                            *original,
                        )))
                    }
                    JournalError::Dedup(
                        ozzy_core::dedup::Error::Capacity
                        | ozzy_core::dedup::Error::PendingCapacity
                        | ozzy_core::dedup::Error::SnapshotCapacity,
                    ) => Some(ProposalDeferral::IdentityCapacity),
                    JournalError::Io(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if let Err(reason) = self.retain_validation_pressure() {
                            return ProposalValidation::Rejected {
                                reason,
                                buffer: ProposalBuffer(buffer),
                            };
                        }
                        Some(ProposalDeferral::MemoryCapacity)
                    }
                    _ => None,
                };
                if let Some(reason) = deferral {
                    return ProposalValidation::Deferred {
                        validation: ticket,
                        reason,
                        buffer: ProposalBuffer(buffer),
                    };
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
        self.validate_proposal_fences(ticket, buffer)?;
        if buffer.producer_session.is_some()
            && let Some(through) = Box::pin(self.prepare_producer_open(ticket, buffer)).await?
        {
            return Ok(Prepared::Retry(through));
        }
        if buffer.group_progress.is_some()
            && let Some(through) = self.prepare_group_progress(ticket, buffer)?
        {
            return Ok(Prepared::Retry(through));
        }
        if buffer.group_assignment.is_some()
            && let Some(through) = self.prepare_group_assignment(ticket, buffer)?
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
                    if self.dedup.limits.is_some() {
                        let charge = self.charge_validation(1024)?;
                        buffer.preserve_dedup_original(charge)?;
                    }
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
        if buffer.is_producer() && self.dedup.limits.is_some() {
            let rewrite = self.resolve_producer_ids(ticket, buffer)?;
            let mut staged = buffer.stage_dedup(rewrite);
            let (plan, records) = self.prepare_append(ticket, staged.buffer(), true).await?;
            let dedup = self.prepare_dedup(ticket, staged.buffer())?;
            staged.commit();
            return Ok(Prepared::Fresh(plan, records, dedup));
        }
        let (plan, records) = self.prepare_append(ticket, buffer, true).await?;
        let dedup = self.prepare_dedup(ticket, buffer)?;
        Ok(Prepared::Fresh(plan, records, dedup))
    }

    fn validate_proposal_fences(
        &self,
        ticket: ValidationTicket,
        buffer: &AppendBuffer,
    ) -> Result<(), JournalError> {
        if buffer
            .proposal_policy
            .is_some_and(|policy| policy != self.configuration.append_policy())
        {
            if buffer.group_progress.is_some() {
                return Err(crate::replica_journal::GroupProgressError::Policy.into());
            }
            if buffer.group_assignment.is_some() {
                return Err(crate::replica_journal::GroupAssignError::Policy.into());
            }
            return Err(AppendAdmissionError::Policy.into());
        }
        if buffer.proposal_authority.is_some_and(|authority| {
            authority.group_id != ticket.scope().group_id
                || authority.config_epoch != ticket.scope().configuration_epoch
                || authority.view != ticket.scope().view
        }) {
            if buffer.group_progress.is_some() {
                return Err(crate::replica_journal::GroupProgressError::Authority.into());
            }
            if buffer.group_assignment.is_some() {
                return Err(crate::replica_journal::GroupAssignError::Authority.into());
            }
            return Err(AppendAdmissionError::Authority.into());
        }
        Ok(())
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
        let mut duplicate = None::<DuplicateEvidence>;
        let mut index = 0;
        while index < retry.retained {
            match retry.source(index)? {
                ProducerResultSource::Stored => {}
                ProducerResultSource::Duplicate { operation: number } => {
                    let evidence = if let Some(evidence) =
                        duplicate.filter(|evidence| evidence.number == number)
                    {
                        evidence
                    } else {
                        self.duplicate_evidence(&retry, number, &expected.records, &mut snapshot)
                            .await?
                    };
                    if !evidence
                        .proof
                        .contains(retry.batch.first_sequence.get() + index as u64)
                    {
                        return Err(JournalIndexError::Read(
                            ozzy_journal_segment::IndexedReadError::RecordMismatch,
                        )
                        .into());
                    }
                    duplicate = Some(evidence);
                    expected_records
                        .next()
                        .ok_or(JournalError::AppendMismatch)?;
                    timestamp.get_or_insert(evidence.proof.timestamp);
                    through = evidence.through;
                    index += 1;
                    continue;
                }
                _ => return Err(JournalError::AppendMismatch),
            }
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
            let records = self
                .cold_retry_records(&retry, index, &mut decoded, &mut snapshot)
                .await?;
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

    async fn duplicate_evidence(
        &mut self,
        retry: &producer::Retry,
        number: u64,
        expected: &ozzy_journal::operation::AppendRecordList<'_>,
        snapshot: &mut Option<ozzy_journal_segment::AsyncJournalIndexSnapshot>,
    ) -> Result<DuplicateEvidence, JournalError> {
        if let Some((operation, through)) = self.writeback.operation(number) {
            return Ok(DuplicateEvidence {
                number,
                through,
                proof: retry
                    .verify_duplicates(
                        number,
                        operation.kind,
                        operation.body,
                        expected,
                        self.limits.operations,
                    )
                    .await?,
            });
        }
        if snapshot.is_none() {
            *snapshot = Some(self.journal.retry_snapshot(self.recovery.index).await?);
        }
        let operation = snapshot
            .as_ref()
            .expect("retry snapshot")
            .read_operation_number(number)
            .await?
            .ok_or(JournalIndexError::MissingOperationNumber(number))?;
        Ok(DuplicateEvidence {
            number,
            through: Prefix {
                op: ozzy_replication::OpNumber(number),
                digest: operation.digest,
            },
            proof: retry
                .verify_duplicates(
                    number,
                    operation.kind,
                    operation.body.as_ref(),
                    expected,
                    self.limits.operations,
                )
                .await?,
        })
    }

    async fn cold_retry_records(
        &mut self,
        retry: &producer::Retry,
        index: usize,
        decoded: &mut DecodedBatches,
        snapshot: &mut Option<ozzy_journal_segment::AsyncJournalIndexSnapshot>,
    ) -> Result<
        Vec<(
            ozzy_journal_segment::IndexedRecord,
            ozzy_journal_segment::LogPosition,
        )>,
        JournalError,
    > {
        let end = (index + RETRY_READ_RECORDS).min(retry.retained);
        let mut offsets = Vec::with_capacity(end - index);
        for next in index..end {
            if retry.source(next)? != ProducerResultSource::Stored {
                break;
            }
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
                *snapshot = Some(self.journal.retry_snapshot(self.recovery.index).await?);
            }
            snapshot
                .as_ref()
                .expect("retry snapshot")
                .read_offsets_with_positions(retry.batch.partition, &offsets, limits)
                .await?
        };
        if records.is_empty() {
            return Err(JournalIndexError::MissingOffset(retry.offset(index)?).into());
        }
        Ok(records)
    }
}

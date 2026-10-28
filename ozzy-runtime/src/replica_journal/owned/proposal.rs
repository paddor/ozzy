//! Writer assignment uses pure shared policy; cold retry reads are file futures.

mod retry;
mod schedule;

use super::{AppendBuffer, JournalError, OwnedJournal};
use crate::replica_journal::{
    AppendAdmissionError, ProposalBuffer, ProposalDeferral, ProposalValidation, ValidatedAppend,
    append::ProducerRetryResults,
    commands::read_fault,
    producer::{self, Assignment, Plan},
};
use ozzy_replication::{Prefix, driver::ValidationTicket};

enum Prepared {
    Fresh(
        ozzy_core::state::PreparedCanonicalGroup,
        Vec<ozzy_journal_segment::PreparedOperationRecords>,
        Option<Box<super::dedup::Prepared>>,
    ),
    Retry(Prefix),
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
            Err(reason) => self.refuse_proposal(ticket, buffer, positions, reason),
        }
    }

    /// Restore the original bytes and positions of a refused proposal. Defer
    /// it when only capacity or a pending original stood in its way.
    fn refuse_proposal(
        &self,
        ticket: ValidationTicket,
        mut buffer: AppendBuffer,
        positions: smallvec::SmallVec<[[u8; 16]; 8]>,
        reason: JournalError,
    ) -> ProposalValidation {
        buffer.restore_dedup_original();
        let restored = buffer.revert_schedule().and_then(|()| {
            positions
                .into_iter()
                .enumerate()
                .try_for_each(|(index, position)| {
                    buffer.replace_append_position(index, position).map(drop)
                })
        });
        let deferral = match restored.and_then(|()| self.deferral(&reason)) {
            Ok(deferral) => deferral,
            Err(reason) => {
                return ProposalValidation::Rejected {
                    reason,
                    buffer: ProposalBuffer(buffer),
                };
            }
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

    /// Why a refused proposal may retry later, if it may. Memory pressure is
    /// retained here so the retry waits for room.
    fn deferral(&self, reason: &JournalError) -> Result<Option<ProposalDeferral>, JournalError> {
        Ok(match reason {
            JournalError::Dedup(ozzy_core::dedup::Error::PendingOriginal(original)) => Some(
                ProposalDeferral::Original(ozzy_replication::OpNumber(*original)),
            ),
            JournalError::Dedup(
                ozzy_core::dedup::Error::Capacity
                | ozzy_core::dedup::Error::PendingCapacity
                | ozzy_core::dedup::Error::SnapshotCapacity,
            ) => Some(ProposalDeferral::IdentityCapacity),
            JournalError::Io(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                self.retain_validation_pressure()?;
                Some(ProposalDeferral::MemoryCapacity)
            }
            _ => None,
        })
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
        if let Some(through) = self.resolve_control(ticket, buffer).await? {
            return Ok(Prepared::Retry(through));
        }
        if buffer.is_producer()
            && let Some(through) = self
                .assign_producers(buffer, timestamp_millis, positions)
                .await?
        {
            return Ok(Prepared::Retry(through));
        }
        if buffer.scheduled().is_some() {
            self.schedule_proposal(buffer, timestamp_millis)?;
        }
        if buffer.is_release() {
            self.release_proposal(buffer, timestamp_millis)?;
        }
        if buffer.is_carry() {
            self.carry_proposal(buffer, timestamp_millis)?;
        }
        self.check_room(buffer)?;
        if buffer.is_producer() && self.dedup.limits.is_some() {
            let rewrite = self.resolve_producer_ids(ticket, buffer)?;
            let mut staged = buffer.stage_dedup(rewrite);
            let (plan, records) = self.prepare_append(ticket, staged.buffer(), true).await?;
            self.prepare_held(staged.buffer()).await?;
            let dedup = self.prepare_dedup(ticket, staged.buffer())?;
            staged.commit();
            return Ok(Prepared::Fresh(plan, records, dedup));
        }
        let (plan, records) = self.prepare_append(ticket, buffer, true).await?;
        self.prepare_held(buffer).await?;
        let dedup = self.prepare_dedup(ticket, buffer)?;
        Ok(Prepared::Fresh(plan, records, dedup))
    }

    /// A writer open, consumer control request, or partition operation that
    /// retries an earlier one resolves to that one's position.
    async fn resolve_control(
        &mut self,
        ticket: ValidationTicket,
        buffer: &mut AppendBuffer,
    ) -> Result<Option<Prefix>, JournalError> {
        if buffer.producer_session.is_some()
            && let Some(through) = Box::pin(self.prepare_producer_open(ticket, buffer)).await?
        {
            return Ok(Some(through));
        }
        if buffer.group_progress.is_some()
            && let Some(through) = self.prepare_group_progress(ticket, buffer)?
        {
            return Ok(Some(through));
        }
        if buffer.group_assignment.is_some()
            && let Some(through) = self.prepare_group_assignment(ticket, buffer)?
        {
            return Ok(Some(through));
        }
        crate::replica_journal::proposal::partition_retry(
            self.images()?,
            buffer,
            ticket,
            self.buffer_generation,
            self.limits.operations,
        )
    }

    /// Assign every producer batch its position. A retry resolves to the
    /// position its original reached; only a lone batch may retry.
    async fn assign_producers(
        &mut self,
        buffer: &mut AppendBuffer,
        timestamp_millis: u64,
        positions: &mut smallvec::SmallVec<[[u8; 16]; 8]>,
    ) -> Result<Option<Prefix>, JournalError> {
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
                buffer.trim_schedule(trim);
            }
            buffer.retry_results = (!results.is_empty()).then(|| {
                Box::new(ProducerRetryResults {
                    ranges: results,
                    trimmed: trim != 0,
                })
            });
            positions.push(buffer.replace_append_position(index, position)?);
            if retry.is_some() {
                return Ok(retry);
            }
        }
        Ok(None)
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
}

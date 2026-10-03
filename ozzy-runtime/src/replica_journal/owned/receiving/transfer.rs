use super::{
    AppendBuffer, JournalError, PublishedRecovery, QuorumPolicy, RecoveringJournal, RecoveryError,
    RecoveryPlan, RecoveryTicket, State, SuffixStreamLimits, authority,
};
use crate::replica_journal::owned::prefix;
use crate::replica_journal::{InstallationConfig, ReceivedChunk, authority::position};
use ozzy_journal_segment::{RecoveryPublication, SealedRepairLimits, SuffixReplacement};
use ozzy_replication::{FrozenLog, Prefix, PreparedOperation, RecoveredState, Scope};

impl RecoveringJournal {
    /// Start exactly one fresh-group-authorized transfer. A mismatching donor
    /// boundary selects full replacement before any sealed repair is published.
    pub async fn begin_recovery(
        &mut self,
        ticket: RecoveryTicket,
        physical: InstallationConfig,
    ) -> Result<RecoveryPlan, JournalError> {
        self.healthy()?;
        self.faulted = true;
        if self.attempt.is_some() {
            return Err(RecoveryError::StaleTransfer.into());
        }
        authority::recovery_ticket(
            self.config.configuration.configuration(),
            self.config.identity.replica_node_id,
            self.generations.attempt,
            ticket,
        )?;
        if physical.segment_capacity > self.config.limits.io.max_segment_bytes
            || !physical.body_encoding.is_supported()
            || physical.max_staged_bytes < physical.segment_capacity
            || physical.max_orphan_probes == 0
        {
            return Err(JournalError::Configuration);
        }
        if matches!(self.state, State::Directory(_))
            && let Some(plan) = self.begin_repair(ticket, physical).await?
        {
            self.attempt = Some(ticket);
            self.faulted = false;
            return Ok(plan);
        }
        let State::Journal(journal) = self.take_state() else {
            return Err(JournalError::Faulted);
        };
        let replacement = SuffixReplacement {
            expected_current: journal.current(),
            protected_committed: position(Prefix::GENESIS),
            promised_view: ticket.scope().view,
            last_normal_view: ticket.scope().view,
            committed: position(ticket.committed()),
            writer_generation: ticket.generation(),
            segment_capacity: physical.segment_capacity,
            body_encoding: physical.body_encoding,
        };
        let limits = SuffixStreamLimits {
            max_group_operations: self
                .config
                .append_limits
                .max_operations
                .min(self.config.limits.decode.max_entries),
            max_group_body_bytes: self
                .config
                .append_limits
                .max_body_bytes
                .min(self.config.limits.decode.max_group_decoded_body_bytes),
            max_segments: self.config.limits.metadata.max_segments,
            max_staged_bytes: physical.max_staged_bytes,
            max_source_segment_bytes: self.config.limits.io.max_segment_bytes,
            max_orphan_probes: physical.max_orphan_probes,
        };
        let staging = journal
            .begin_recovery_replacement(
                &self.config.configuration.encode(),
                replacement,
                position(ticket.source().accepted),
                limits,
            )
            .await?;
        self.state = State::Installing(Box::new((staging, limits)));
        self.attempt = Some(ticket);
        self.faulted = false;
        Ok(RecoveryPlan::Full)
    }

    async fn begin_repair(
        &mut self,
        ticket: RecoveryTicket,
        physical: InstallationConfig,
    ) -> Result<Option<RecoveryPlan>, JournalError> {
        let State::Directory(directory) = self.take_state() else {
            return Err(JournalError::Faulted);
        };
        let boundary = directory
            .manifest()
            .segments
            .last()
            .expect("active segment")
            .first_chain;
        let source = ticket.source().accepted;
        let sealed_end = boundary.next_op_number() - 1;
        if sealed_end > source.op.0
            || (sealed_end == source.op.0 && boundary.previous_digest() != source.digest)
        {
            self.state = State::Journal(Box::new(
                directory
                    .recover_nonvoting(
                        &self.config.configuration.encode(),
                        self.generations.temporary,
                        self.config.limits.directory_entries,
                    )
                    .await?,
            ));
            return Ok(None);
        }
        let repair = directory
            .begin_sealed_repair(
                &self.config.configuration.encode(),
                self.generations.attempt,
                ticket.scope().view,
                position(source),
                SealedRepairLimits {
                    max_segment_bytes: self.config.limits.io.max_segment_bytes,
                    max_chunk_operations: self
                        .config
                        .append_limits
                        .max_operations
                        .min(self.config.limits.decode.max_entries),
                    max_chunk_body_bytes: self
                        .config
                        .append_limits
                        .max_body_bytes
                        .min(self.config.limits.decode.max_group_decoded_body_bytes),
                    max_staged_bytes: physical.max_staged_bytes,
                    max_orphan_probes: physical.max_orphan_probes,
                    body_encoding: physical.body_encoding,
                },
            )
            .await?;
        let plan = RecoveryPlan::Repair(repair.pending()?);
        self.state = State::Repair(Box::new(repair));
        Ok(Some(plan))
    }

    /// Stage bounded bytes and return verified descriptors, not publication or
    /// voting evidence. Feed full-transfer descriptors to the live recovery core.
    /// `RetryFull` requires abandoning this repair and a fresh authorized attempt.
    pub async fn receive_chunk(
        &mut self,
        ticket: RecoveryTicket,
        mut buffer: AppendBuffer,
    ) -> Result<ReceivedChunk, JournalError> {
        self.healthy()?;
        self.faulted = true;
        self.require_ticket(ticket)?;
        if buffer.is_empty() || buffer.owner_generation() != self.generations.attempt {
            return Err(RecoveryError::StaleTransfer.into());
        }
        let mut prepared = std::mem::take(&mut buffer.prepared);
        prepared.clear();
        let operations: Vec<_> = buffer.operations().collect();
        let plan = match &mut self.state {
            State::Repair(repair) => match repair.append(&operations).await {
                Ok(()) => RecoveryPlan::Repair(repair.pending()?),
                Err(ozzy_journal_segment::DirectoryError::CurrentMismatch) => {
                    RecoveryPlan::RetryFull
                }
                Err(error) => return Err(error.into()),
            },
            State::Installing(pending) => {
                let (staging, limits) = &mut **pending;
                let mut first = 0;
                let mut bytes = 0;
                for (index, operation) in operations.iter().enumerate() {
                    if operation.body.len() > limits.max_group_body_bytes {
                        return Err(JournalError::AppendCapacity);
                    }
                    if index - first == limits.max_group_operations
                        || bytes > limits.max_group_body_bytes - operation.body.len()
                    {
                        staging.append_chunk(&operations[first..index]).await?;
                        first = index;
                        bytes = 0;
                    }
                    bytes += operation.body.len();
                }
                staging.append_chunk(&operations[first..]).await?;
                RecoveryPlan::Full
            }
            _ => return Err(RecoveryError::StaleTransfer.into()),
        };
        // A retry-full result contains no verified transfer progress. Preserve
        // its arena for disposal/reuse, but never expose unvalidated descriptors.
        if plan != RecoveryPlan::RetryFull {
            for operation in &operations {
                prepared.push(PreparedOperation::from_verified(
                    operation,
                    ozzy_journal::operation::canonical_body_digest(operation.body),
                ));
            }
        }
        let end = prepared
            .last()
            .map_or(Prefix::GENESIS, |operation| operation.prefix());
        drop(operations);
        buffer.prepared = prepared;
        self.faulted = false;
        Ok(ReceivedChunk {
            ticket,
            end,
            buffer,
            plan,
        })
    }

    /// Publish only complete, privately replayed history. Exact retries return
    /// the same evidence. Publication still grants no normal or bootstrap role.
    pub async fn finish_recovery(
        &mut self,
        ticket: RecoveryTicket,
    ) -> Result<PublishedRecovery, JournalError> {
        self.healthy()?;
        self.faulted = true;
        self.require_ticket(ticket)?;
        if let Some(published) = self.published {
            self.faulted = false;
            return Ok(published);
        }
        let (mut journal, published) = match self.take_state() {
            State::Repair(repair) => {
                let journal = Box::pin(
                    repair.finish(&self.config.configuration.encode(), self.config.recovery),
                )
                .await?;
                let manifest = journal.manifest();
                let recovered = RecoveredState {
                    scope: Scope {
                        view: manifest.promised_view,
                        ..ticket.scope()
                    },
                    log: FrozenLog {
                        last_normal_view: manifest.last_normal_view,
                        accepted: prefix(manifest.accepted),
                        committed: prefix(manifest.committed),
                    },
                };
                (
                    journal,
                    PublishedRecovery {
                        ticket,
                        applied: recovered.log.committed,
                        repaired: Some(recovered),
                    },
                )
            }
            State::Installing(staging) => {
                let mut journal = Box::pin(staging.0.finish()).await?;
                let publication = RecoveryPublication {
                    current: journal.current(),
                    generation: self.generations.attempt,
                    view: ticket.scope().view,
                    accepted: position(ticket.source().accepted),
                    committed: position(ticket.committed()),
                };
                let candidate = journal
                    .publish_recovered_configuration(
                        &self.config.configuration.encode(),
                        publication,
                        self.config.recovery,
                    )
                    .await?;
                if candidate.committed_images().committed().revision() != ticket.committed().op.0 {
                    return Err(JournalError::CompletionMismatch);
                }
                drop(candidate);
                (
                    journal,
                    PublishedRecovery {
                        ticket,
                        applied: ticket.committed(),
                        repaired: None,
                    },
                )
            }
            _ => return Err(RecoveryError::StaleTransfer.into()),
        };
        if self.config.configuration.configuration().policy() == QuorumPolicy::Replicated {
            journal.publish_drained_memory_history().await?;
        }
        self.state = State::Journal(Box::new(journal));
        self.published = Some(published);
        self.faulted = false;
        Ok(published)
    }
}

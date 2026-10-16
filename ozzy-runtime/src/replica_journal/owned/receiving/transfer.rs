use super::{
    AppendBuffer, GroupJournal, JournalError, PublishedRecovery, RecoveringJournal, RecoveryError,
    RecoveryPlan, RecoveryTicket, SealedRepair, State, SuffixInstaller, SuffixStreamLimits,
    recover_nonvoting,
};
use crate::replica_journal::owned::{install::stage_chunks, prefix};
use crate::replica_journal::{InstallationConfig, ReceivedChunk, authority, authority::position};
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
            &ticket,
        )?;
        let limits = physical.stream_limits(&self.config.limits, self.config.append_limits)?;
        if ticket.checkpoint().is_some() {
            self.state = match self.take_state() {
                State::Directory(directory) => {
                    recover_nonvoting(*directory, &self.config, self.generations.temporary).await?
                }
                state => state,
            };
        }
        self.prepare_checkpoint(&ticket).await?;
        if matches!(self.state, State::Directory(_))
            && let Some(plan) = self.begin_repair(&ticket, physical, limits).await?
        {
            self.attempt = Some(ticket);
            self.faulted = false;
            return Ok(plan);
        }
        let State::Journal(journal) = self.take_state() else {
            return Err(JournalError::Faulted);
        };
        let staging = Box::pin(self.begin_replacement(journal, &ticket, physical, limits)).await?;
        self.state = State::Installing(Box::new((staging, limits)));
        self.attempt = Some(ticket);
        self.faulted = false;
        Ok(RecoveryPlan::Full)
    }

    /// Stage a full replacement of `journal` from the donor's source, from
    /// its checkpoint when the ticket names one.
    async fn begin_replacement(
        &self,
        journal: Box<GroupJournal>,
        ticket: &RecoveryTicket,
        physical: InstallationConfig,
        limits: SuffixStreamLimits,
    ) -> Result<SuffixInstaller, JournalError> {
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
        let configuration = self.config.configuration.encode();
        let request = ozzy_journal_segment::SuffixRequest {
            replacement,
            accepted: position(ticket.source().accepted),
            limits,
        };
        let staging = if let Some(anchor) = ticket.checkpoint() {
            journal
                .begin_checkpoint_replacement(
                    &configuration,
                    request,
                    ozzy_journal_segment::CheckpointRecovery {
                        predecessor: position(anchor.predecessor),
                        position: position(anchor.position),
                    },
                )
                .await?
        } else {
            journal
                .begin_recovery_replacement(&configuration, request)
                .await?
        };
        Ok(staging)
    }

    async fn begin_repair(
        &mut self,
        ticket: &RecoveryTicket,
        physical: InstallationConfig,
        limits: SuffixStreamLimits,
    ) -> Result<Option<RecoveryPlan>, JournalError> {
        let State::Directory(directory) = self.take_state() else {
            return Err(JournalError::Faulted);
        };
        let boundary = directory
            .manifest()
            .segments
            .last()
            .ok_or(ozzy_journal_segment::DirectoryError::MissingActiveSegment)?
            .first_chain;
        let source = ticket.source().accepted;
        let sealed_end = boundary.next_op_number() - 1;
        if sealed_end > source.op.0
            || (sealed_end == source.op.0 && boundary.previous_digest() != source.digest)
        {
            self.state =
                recover_nonvoting(*directory, &self.config, self.generations.temporary).await?;
            return Ok(None);
        }
        let repair = directory
            .begin_sealed_repair(
                &self.config.configuration.encode(),
                self.generations.attempt,
                ozzy_journal_segment::RepairSource {
                    view: ticket.scope().view,
                    accepted: position(source),
                },
                SealedRepairLimits {
                    max_segment_bytes: self.config.limits.io.max_segment_bytes,
                    max_chunk_operations: limits.max_group_operations,
                    max_chunk_body_bytes: limits.max_group_body_bytes,
                    max_staged_bytes: limits.max_staged_bytes,
                    max_orphan_probes: limits.max_orphan_probes,
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
        self.require_ticket(&ticket)?;
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
                stage_chunks(staging, *limits, &operations).await?;
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
        self.require_ticket(&ticket)?;
        if let Some(published) = self.published {
            self.faulted = false;
            return Ok(published);
        }
        let (journal, published) = match self.take_state() {
            State::Repair(repair) => self.finish_repair(repair, &ticket).await?,
            State::Installing(staging) => Box::pin(self.finish_transfer(staging, &ticket)).await?,
            _ => return Err(RecoveryError::StaleTransfer.into()),
        };
        self.state = State::Journal(Box::new(journal));
        self.published = Some(published);
        self.faulted = false;
        Ok(published)
    }

    /// Publish repaired sealed files; their manifest is the recovered state.
    async fn finish_repair(
        &self,
        repair: Box<SealedRepair>,
        ticket: &RecoveryTicket,
    ) -> Result<(GroupJournal, PublishedRecovery), JournalError> {
        let journal =
            Box::pin(repair.finish(&self.config.configuration.encode(), self.config.recovery))
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
        let published = PublishedRecovery {
            ticket: *ticket,
            applied: recovered.log.committed,
            repaired: Some(recovered),
        };
        Ok((journal, published))
    }

    /// Publish a fully transferred replacement once its replayed state
    /// commits exactly through the ticket.
    async fn finish_transfer(
        &mut self,
        staging: Box<(SuffixInstaller, SuffixStreamLimits)>,
        ticket: &RecoveryTicket,
    ) -> Result<(GroupJournal, PublishedRecovery), JournalError> {
        let mut journal = if let Some(anchor) = ticket.checkpoint() {
            let checkpoint = self
                .checkpoint
                .take()
                .ok_or(JournalError::HistorySourceMismatch)?;
            checkpoint
                .state
                .as_ref()
                .ok_or(JournalError::HistorySourceMismatch)?;
            let journal = Box::pin(staging.0.finish_with_checkpoint(
                ozzy_journal_segment::CanonicalCheckpointImport {
                    id: ozzy_proto::CheckpointId::from_bytes(*ticket.nonce().as_bytes()),
                    position: position(anchor.position),
                    image: &checkpoint.inbox,
                    state_limits: self.config.recovery.state,
                    snapshot_limits: self.config.recovery.snapshot,
                },
            ))
            .await?;
            checkpoint.inbox.discard().await?;
            journal
        } else {
            Box::pin(staging.0.finish()).await?
        };
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
        let published = PublishedRecovery {
            ticket: *ticket,
            applied: ticket.committed(),
            repaired: None,
        };
        Ok((journal, published))
    }
}

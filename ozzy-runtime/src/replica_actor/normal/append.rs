//! Fresh suffix validation, write ownership, and application-gated replies.

use super::{
    ActorError, Control, DriverError, Duration, Live, Message, NodeId, Prefix, ProposalOutcome,
    ReplicaActor, Scope, Submission,
};
use crate::replica_actor::HistoryReason;
use crate::replica_actor::io::Completed;
use crate::replica_actor::{Bytes, MAX_TRANSFER_OPERATIONS, wire};
use crate::replica_journal::{JournalError, ProposalValidation, ReceivedValidation};
use ozzy_replication::wire::{Operation, Prepare, PrepareBatch};
use ozzy_replication::{Admission, Commit, NormalReplica, ReplicationError};

impl ReplicaActor {
    /// False leaves a valid packet waiting for predecessors or admission space.
    pub(in crate::replica_actor) fn receive_prepare(
        &mut self,
        from: NodeId,
        batch: PrepareBatch<'_>,
        now: Duration,
    ) -> Result<bool, ActorError> {
        // A future primary's packet may fence old authority, never bypass installation.
        if from != self.configuration.primary(batch.scope().view) {
            return Ok(true);
        }
        self.receive_control(
            from,
            Control::Commit(Commit {
                scope: batch.scope(),
                committed: batch.committed(),
            }),
            now,
        )?;
        let Some(snapshot) = self.driver.normal().map(NormalReplica::snapshot) else {
            return Ok(true);
        };
        // Until the ledger restarts for this core, repair supplies the suffix.
        if batch.scope() != snapshot.scope
            || !self.application_ready()
            || !self.receipt_current(&snapshot)
        {
            return Ok(true);
        }
        let consumed = self
            .work
            .receive
            .stage(batch, &snapshot, self.config.pipeline)?;
        // Staging is neither admission nor durability. ACK only the core prefix.
        if consumed {
            self.ack_at = now;
        }
        Ok(consumed)
    }

    pub(in crate::replica_actor) fn complete_normal(
        &mut self,
        completed: Completed,
        now: Duration,
    ) -> Result<(), ActorError> {
        match completed {
            Completed::Sync(ticket) => {
                self.driver.complete_sync(ticket, now)?;
                // Later writes own the next batch's age/accounting. This older
                // captured completion cannot clear them or acknowledge them.
                self.ack_at = now;
            }
            Completed::Apply(ticket) => self.complete_applied(ticket)?,
            Completed::Turn(turn) => {
                // Admission first: the proposal was validated against the
                // image it leaves. Application last: the proposal's ticket
                // still names the applied prefix it saw.
                if let Some(admitted) = turn.admitted {
                    self.complete_admitted(admitted)?;
                }
                if let Some(proposal) = turn.proposal {
                    self.complete_proposal(proposal, now)?;
                }
                if let Some(validated) = turn.validated {
                    self.complete_validation(validated, now)?;
                }
                if let Some(ticket) = turn.applied {
                    self.complete_applied(ticket)?;
                }
                if let Some(deferred) = turn.deferred {
                    self.submit_turn(*deferred)?;
                } else {
                    self.send_ready_turn(now)?;
                }
            }
            Completed::Replay(fetched, to) => self.complete_replay(fetched, to, now)?,
            // Election completions go to the election path, never here.
            _ => return Err(JournalError::CompletionMismatch.into()),
        }
        Ok(())
    }

    /// Settle a follower's received-suffix validation. A suffix that waits or
    /// splits stays only while its validation is current.
    fn complete_validation(
        &mut self,
        validated: ReceivedValidation,
        now: Duration,
    ) -> Result<(), ActorError> {
        match validated {
            ReceivedValidation::Ready(validated) => self.complete_received(validated, now),
            ReceivedValidation::Deferred { validation, buffer }
                if self.validation_current(validation) =>
            {
                self.work.receive.defer(buffer);
                Ok(())
            }
            ReceivedValidation::Split { validation, buffer }
                if self.validation_current(validation) =>
            {
                self.split_received(buffer)
            }
            ReceivedValidation::Deferred { buffer, .. }
            | ReceivedValidation::Split { buffer, .. } => self.recycle(buffer),
            ReceivedValidation::Rejected(error) => Err(error.into()),
        }
    }

    /// Whether a deferred validation still belongs to the current scope and
    /// journal generation.
    fn validation_current(&self, validation: ozzy_replication::driver::ValidationTicket) -> bool {
        self.driver.normal().is_some_and(|normal| {
            let state = normal.snapshot();
            state.scope == validation.scope() && state.journal.generation == validation.generation()
        })
    }

    /// The write is queued on the dedicated writer. The vote waits for its
    /// `O_DSYNC` completion.
    fn complete_admitted(
        &mut self,
        admitted: crate::replica_journal::AdmittedAppend,
    ) -> Result<(), ActorError> {
        let (ticket, buffer, persisted) = admitted.into_parts();
        assert!(self.pending_persistence.len() < self.config.pipeline.max_operations);
        self.pending_persistence.push_back(persisted);
        self.work.persisting_bytes.push_back(buffer.body_bytes());
        self.return_admitted(ticket, buffer)
    }

    /// One queued write completed on the dedicated writer, in admission order.
    pub(in crate::replica_actor) fn complete_persisted(
        &mut self,
        ticket: ozzy_replication::WriteTicket,
        now: Duration,
    ) -> Result<(), ActorError> {
        let body_bytes = self
            .work
            .persisting_bytes
            .pop_front()
            .ok_or(JournalError::CompletionMismatch)?;
        // `O_DSYNC` made the write durable, and later seals mark it, so the
        // vote goes now. The group still joins the next `DURABLE` publication,
        // which a due batch starts now.
        self.driver.complete_durable_write(ticket, now)?;
        self.ack_at = now;
        self.work.last_persisted = Some(now);
        self.work.needs_sync = true;
        // A write ticket spans at most one pipeline of operations.
        self.work.sync_batch_operations += (ticket.through().0 - ticket.first().0 + 1) as usize;
        self.work.sync_batch_body_bytes += body_bytes;
        if self.pending_sync.is_none()
            && self.journal.available_command_slots() != 0
            && self.driver.normal().is_some()
            && self.sync_due(now)
        {
            self.schedule_sync()?;
            // As in scheduling: the captured group syncs while the next fills.
            self.ingress.resume();
        }
        Ok(())
    }

    fn return_admitted(
        &mut self,
        ticket: ozzy_replication::WriteTicket,
        buffer: crate::replica_journal::AppendBuffer,
    ) -> Result<(), ActorError> {
        if let Some(live) =
            self.work.live.iter_mut().find(|live| {
                live.generation == ticket.generation() && live.end.op == ticket.through()
            })
        {
            assert!(live.buffer.replace(buffer).is_none());
            live.reply.admitted();
            Ok(())
        } else {
            self.recycle(buffer)
        }
    }

    fn complete_applied(
        &mut self,
        ticket: ozzy_replication::driver::ValidationTicket,
    ) -> Result<(), ActorError> {
        if self.driver.normal().is_some_and(|normal| {
            let state = normal.snapshot();
            state.scope == ticket.scope() && state.journal.generation == ticket.generation()
        }) {
            self.driver.apply_through(ticket.committed())?;
        }
        Ok(())
    }

    /// Admit a validated received suffix into the core. Its install rides on
    /// the next journal turn, which may validate the following suffix.
    fn complete_received(
        &mut self,
        validated: crate::replica_journal::ValidatedAppend,
        now: Duration,
    ) -> Result<(), ActorError> {
        let from = self
            .configuration
            .primary(validated.validation().scope().view);
        match self
            .driver
            .prepare_validated(from, validated.validation(), validated.prepared(), now)
        {
            Ok(Admission::Write {
                ticket,
                first_new: 0,
            }) => {
                self.work.receive.admitted();
                assert!(self.work.ready.replace((ticket, validated)).is_none());
                self.observe_window();
            }
            Err(DriverError::StaleValidation) => {
                crate::profiling::event(crate::profiling::Event::ReplicaStaleValidation);
                self.retract_received(now)?;
                self.recycle(validated.into_buffer())?;
            }
            Err(error) => return Err(error.into()),
            _ => return Err(ActorError::history(HistoryReason::ProposalWindow)),
        }
        Ok(())
    }

    fn complete_proposal(
        &mut self,
        result: ProposalValidation,
        now: Duration,
    ) -> Result<(), ActorError> {
        let reply = self
            .work
            .proposals
            .validated()
            .ok_or(JournalError::CompletionMismatch)?;
        let validated = match result {
            ProposalValidation::Deferred {
                validation,
                reason,
                buffer,
            } => {
                self.defer_proposal(
                    super::ProposalWait { validation, reason },
                    Submission { buffer, reply },
                );
                return Ok(());
            }
            ProposalValidation::Ready(validated) => validated,
            ProposalValidation::Resolved {
                validation,
                through,
                buffer,
            } => {
                self.complete_retry(validation, through, Submission { buffer, reply });
                return Ok(());
            }
            ProposalValidation::Rejected { reason, buffer } => {
                reply.finish(buffer, ProposalOutcome::Invalid(reason));
                return Ok(());
            }
        };
        self.admit_proposal(validated, reply, now)
    }

    fn admit_proposal(
        &mut self,
        validated: crate::replica_journal::ValidatedAppend,
        reply: super::Reply,
        now: Duration,
    ) -> Result<(), ActorError> {
        if reply.fenced() {
            reply.finish(validated.into_buffer().into(), ProposalOutcome::NotAdmitted);
            return Ok(());
        }
        let validation = validated.validation();
        let ticket =
            match self
                .driver
                .prepare_validated(self.local, validation, validated.prepared(), now)
            {
                Ok(Admission::Write {
                    ticket,
                    first_new: 0,
                }) => ticket,
                Err(DriverError::StaleValidation) => {
                    crate::profiling::event(crate::profiling::Event::ReplicaStaleValidation);
                    let buffer =
                        crate::replica_journal::ProposalBuffer::from(validated.into_buffer());
                    if !reply.is_maintenance()
                        && !buffer.has_trimmed_retry()
                        && self.application_ready()
                        && self.driver.scope() == validation.scope()
                    {
                        self.work.proposals.requeue(Submission { buffer, reply });
                    } else {
                        reply.finish(buffer, ProposalOutcome::NotAdmitted);
                    }
                    return Ok(());
                }
                Err(DriverError::Replication(ReplicationError::Capacity)) => {
                    reply.finish(validated.into_buffer().into(), ProposalOutcome::NotAdmitted);
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
                _ => return Err(ActorError::history(HistoryReason::ProposalWindow)),
            };
        self.enter_live(ticket, validated, reply)
    }

    /// Cache the admitted proposal's PREPARE templates in the live window.
    fn enter_live(
        &mut self,
        ticket: ozzy_replication::WriteTicket,
        mut validated: crate::replica_journal::ValidatedAppend,
        reply: super::Reply,
    ) -> Result<(), ActorError> {
        let validation = validated.validation();
        let payload = validated.shared_bodies();
        let operations: smallvec::SmallVec<[_; MAX_TRANSFER_OPERATIONS]> =
            validated.wire_operations().collect();
        let packets = self.prepare_packets(
            validation.scope(),
            validation.committed(),
            &operations,
            &payload,
        )?;
        let end = validated
            .prepared()
            .last()
            .ok_or(JournalError::AppendMismatch)?
            .prefix();
        for operation in &operations {
            self.work.flow.remember(super::flow::operation(*operation));
        }
        drop(operations);
        assert!(self.work.live.len() < self.config.proposal_capacity);
        self.work.live.push_back(Live {
            retry: false,
            published: false,
            scope: validation.scope(),
            generation: validation.generation(),
            predecessor: validation.accepted(),
            end,
            buffer: None,
            reply,
            packets,
        });
        // The worker and network proceed independently after the same core
        // admission. The install rides on the next journal turn, which is
        // sent as soon as this completion is handled.
        assert!(self.work.ready.replace((ticket, validated)).is_none());
        self.observe_window();
        Ok(())
    }

    fn defer_proposal(&mut self, original: super::ProposalWait, submission: Submission) {
        crate::profiling::event(match original.reason {
            crate::replica_journal::ProposalDeferral::Original(_) => {
                crate::profiling::Event::OriginalWait
            }
            _ => crate::profiling::Event::CapacityDeferral,
        });
        if !submission.reply.fenced()
            && self.application_ready()
            && self.driver.scope() == original.validation.scope()
        {
            self.work.proposals.defer(submission, original);
        } else {
            submission
                .reply
                .finish(submission.buffer, ProposalOutcome::NotAdmitted);
        }
    }

    fn complete_retry(
        &mut self,
        validation: ozzy_replication::driver::ValidationTicket,
        through: Prefix,
        Submission { buffer, reply }: Submission,
    ) {
        if self.driver.begin_validation().ok() != Some(validation) {
            if self.application_ready() && self.driver.scope() == validation.scope() {
                self.work.proposals.requeue(Submission { buffer, reply });
            } else {
                reply.finish(buffer, ProposalOutcome::NotAdmitted);
            }
        } else if validation.applied().op >= through.op {
            reply.finish(
                buffer,
                ProposalOutcome::Committed {
                    scope: validation.scope(),
                    through,
                },
            );
        } else {
            // Duplicate waiters consume the same global ingress permits, not new
            // operations, flow metadata, PREPARE packets, or retransmission cache.
            assert!(self.work.live.len() < self.config.proposal_capacity);
            self.work.live.push_back(Live {
                retry: true,
                published: true,
                scope: validation.scope(),
                generation: validation.generation(),
                predecessor: through,
                end: through,
                buffer: Some(buffer.0),
                reply,
                packets: [None, None, None],
            });
        }
    }

    pub(super) fn prepare_packets(
        &mut self,
        scope: Scope,
        committed: Prefix,
        operations: &[Operation<'_>],
        payload: &Bytes,
    ) -> Result<[Option<Message>; 3], ActorError> {
        let encoded = wire::encode_prepare_unbound(
            self.local,
            Prepare {
                scope,
                committed,
                operations,
            },
            &mut self.metadata,
            self.wire_limits,
        )?;
        let metadata = Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]);
        if payload.len() != encoded.payload_bytes {
            return Err(ActorError::history(HistoryReason::ProposalWindow));
        }
        let mut packets = [None, None, None];
        for (index, to) in self.configuration.voters().iter().enumerate() {
            if *to == self.local {
                continue;
            }
            // An empty header marks an internal unsendable template. Bind the
            // destination's current session and receive epoch on every send.
            packets[index] = Some(Message::multipart([
                Bytes::new(),
                metadata.clone(),
                payload.clone(),
            ]));
        }
        Ok(packets)
    }
}

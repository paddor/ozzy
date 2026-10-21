//! Resolve cold control identities asynchronously, then enter the shared pure validator.

use super::{AppendBuffer, JournalError, OwnedJournal};
use crate::replica_journal::{ValidatedAppend, canonical, commands::read_fault};
use ozzy_core::state::{CanonicalImagesError, IdentityKey, StateError};
use ozzy_journal::operation::decode_operation_body;
use ozzy_journal_segment::IdentityResolveError;
use ozzy_replication::{Scope, driver::ValidationTicket};

impl OwnedJournal {
    pub(super) fn active(&self, ticket: ValidationTicket) -> Result<(), JournalError> {
        self.active_scope(ticket.scope(), ticket.generation())
    }

    pub(super) fn active_scope(
        &self,
        scope: Scope,
        generation: ozzy_replication::JournalGeneration,
    ) -> Result<(), JournalError> {
        let journal = self.journal.readable()?;
        let manifest = journal.manifest();
        self.images()?;
        if scope
            != (Scope {
                view: scope.view,
                ..self.scope
            })
            || scope.view != manifest.last_normal_view
            || scope.view != manifest.promised_view
            || generation != journal.writer().durable_position().generation()
        {
            return Err(JournalError::AppendMismatch);
        }
        Ok(())
    }

    pub(in crate::replica_journal) fn accepted(&self) -> ozzy_replication::Prefix {
        self.pending.back().copied().unwrap_or(self.applied)
    }

    pub(super) fn contains(&self, prefix: ozzy_replication::Prefix) -> bool {
        prefix == self.applied || self.pending.contains(&prefix)
    }

    /// Maintenance that rewrites settled state needs every admitted write
    /// persisted and the accepted, committed, and applied prefixes equal.
    pub(super) fn require_settled(&self, ticket: ValidationTicket) -> Result<(), JournalError> {
        self.writeback.require_idle()?;
        if !self.pending.is_empty()
            || ticket.accepted() != self.applied
            || ticket.committed() != self.applied
        {
            return Err(JournalError::CompletionMismatch);
        }
        Ok(())
    }

    pub(super) fn validate_image(&self, ticket: ValidationTicket) -> Result<(), JournalError> {
        self.active(ticket)?;
        let images = self.images()?;
        if ticket.accepted() != self.accepted()
            || ticket.applied() != self.applied
            || images.speculative().revision() != ticket.accepted().op.0
            || images.committed().revision() != self.applied.op.0
            || ticket.committed().op < self.applied.op
            || !self.contains(ticket.committed())
        {
            return Err(JournalError::AppendMismatch);
        }
        Ok(())
    }

    /// Validate an exact driver image without changing journal or application
    /// authority. APPEND payload validation shares the existing fast path.
    /// Control identity reads suspend only this future. Cancellation admits
    /// nothing; invalid input remains nonfatal unless storage is corrupt.
    pub async fn validate_append(
        &mut self,
        ticket: ValidationTicket,
        buffer: AppendBuffer,
    ) -> Result<ValidatedAppend, JournalError> {
        match self.validate_received(ticket, buffer).await {
            super::super::turn::ReceivedValidation::Ready(validated) => Ok(validated),
            super::super::turn::ReceivedValidation::Deferred { .. }
            | super::super::turn::ReceivedValidation::Split { .. } => {
                Err(std::io::Error::from(std::io::ErrorKind::WouldBlock).into())
            }
            super::super::turn::ReceivedValidation::Rejected(error) => Err(error),
        }
    }

    pub(in crate::replica_journal) async fn validate_received(
        &mut self,
        ticket: ValidationTicket,
        mut buffer: AppendBuffer,
    ) -> super::super::turn::ReceivedValidation {
        use super::super::turn::ReceivedValidation;
        self.begin_validation_memory();
        self.dedup.apply_wait.set(None);
        let result = async {
            self.healthy()?;
            let (plan, records) = self.prepare_append(ticket, &mut buffer, false).await?;
            let dedup = self.prepare_dedup(ticket, &buffer)?;
            Ok::<_, JournalError>((plan, records, dedup))
        }
        .await;
        self.faulted |= result.as_ref().err().is_some_and(read_fault);
        match result {
            Ok((plan, records, dedup)) => ReceivedValidation::Ready(ValidatedAppend {
                validation: ticket,
                buffer,
                plan,
                records,
                dedup,
            }),
            Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if let Err(error) = self.retain_validation_pressure() {
                    return ReceivedValidation::Rejected(error);
                }
                ReceivedValidation::Deferred {
                    validation: ticket,
                    buffer,
                }
            }
            // The leader admitted this suffix after a commit freed identity
            // capacity. A follower that has not applied that commit, or not
            // yet learned of it, still holds rows its expiry will drop. Retry
            // once it applies more.
            Err(JournalError::Dedup(
                ozzy_core::dedup::Error::Capacity
                | ozzy_core::dedup::Error::PendingCapacity
                | ozzy_core::dedup::Error::SnapshotCapacity,
            )) if !self.pending.is_empty() => {
                self.dedup.apply_wait.set(Some(self.applied.op.0));
                ReceivedValidation::Deferred {
                    validation: ticket,
                    buffer,
                }
            }
            // With nothing left to apply, the commit may be inside this
            // suffix. The leader admitted its first operation against a
            // commit this replica has applied, so it fits alone; the rest
            // waits for it. A single operation that does not fit is invalid.
            Err(JournalError::Dedup(
                ozzy_core::dedup::Error::Capacity
                | ozzy_core::dedup::Error::PendingCapacity
                | ozzy_core::dedup::Error::SnapshotCapacity,
            )) if buffer.len() > 1 => ReceivedValidation::Split {
                validation: ticket,
                buffer,
            },
            Err(error) => ReceivedValidation::Rejected(error),
        }
    }

    pub(super) async fn prepare_append(
        &self,
        ticket: ValidationTicket,
        buffer: &mut AppendBuffer,
        assign: bool,
    ) -> Result<
        (
            ozzy_core::state::PreparedCanonicalGroup,
            Vec<ozzy_journal_segment::PreparedOperationRecords>,
        ),
        JournalError,
    > {
        self.validate_image(ticket)?;
        buffer.set_validated(false);
        if buffer.owner_generation() != self.buffer_generation || buffer.is_empty() {
            return Err(JournalError::AppendMismatch);
        }
        canonical::physical_capacity(
            buffer,
            self.journal.readable()?.writer().header().capacity(),
            self.limits.decode,
        )?;
        buffer.prepare_metadata_async(ticket, assign).await?;
        let images = self.images()?;
        let mut keys = smallvec::SmallVec::<[IdentityKey; 4]>::new();
        for operation in buffer.operations() {
            // Canonical preparation below validates the other kinds' bodies.
            if operation.kind.has_operation_id() {
                let body =
                    decode_operation_body(operation.kind, operation.body, self.limits.operations)?;
                if let Some(id) = body.operation_id() {
                    keys.push(IdentityKey::operation(id));
                }
            }
        }
        if !keys.is_empty() {
            images
                .speculative_identities()
                .resolve(&keys)
                .await
                .map_err(|error| match error {
                    IdentityResolveError::Lookup(error) => JournalError::Index(error),
                    IdentityResolveError::Identity(error) => JournalError::Images(
                        CanonicalImagesError::State(StateError::IdentityIndex(error)),
                    ),
                })?;
        }
        let primary_payloads_validated = buffer.primary_payloads_validated();
        let prepared = canonical::prepare(
            images,
            buffer,
            self.limits.operations,
            primary_payloads_validated,
        )?;
        for operation in buffer.operations() {
            if matches!(
                operation.kind,
                ozzy_journal::operation::OperationKind::Append
                    | ozzy_journal::operation::OperationKind::ResolvedAppend
            ) {
                crate::profiling::event(crate::profiling::Event::PreparedOperation);
                crate::profiling::peak(
                    crate::profiling::Event::PreparedPayloadBytesMax,
                    operation.body.len() as u64,
                );
            }
        }
        Ok(prepared)
    }
}

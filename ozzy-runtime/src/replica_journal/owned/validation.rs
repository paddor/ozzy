//! Resolve cold control identities asynchronously, then enter the shared pure validator.

use super::{AppendBuffer, JournalError, OwnedJournal};
use crate::replica_journal::{ValidatedAppend, canonical, commands::read_fault};
use ozzy_core::state::{CanonicalImagesError, IdentityKey, StateError};
use ozzy_journal::operation::{OperationKind, decode_operation_body};
use ozzy_journal_segment::AsyncIdentityResolveError;
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
        mut buffer: AppendBuffer,
    ) -> Result<ValidatedAppend, JournalError> {
        self.healthy()?;
        let result = self.prepare_append(ticket, &mut buffer, false).await;
        self.faulted |= result.as_ref().err().is_some_and(read_fault);
        let (plan, records) = result?;
        Ok(ValidatedAppend {
            validation: ticket,
            buffer,
            plan,
            records,
        })
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
            if operation.kind != OperationKind::Append {
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
                    AsyncIdentityResolveError::Lookup(error) => JournalError::Index(error),
                    AsyncIdentityResolveError::Identity(error) => JournalError::Images(
                        CanonicalImagesError::State(StateError::IdentityIndex(error)),
                    ),
                })?;
        }
        let prepared = canonical::prepare(images, buffer, self.limits.operations, false)?;
        #[cfg(feature = "storage-metrics")]
        for operation in buffer.operations() {
            if operation.kind == OperationKind::Append {
                crate::storage_metrics::prepared_operation(operation.body.len());
            }
        }
        Ok(prepared)
    }
}

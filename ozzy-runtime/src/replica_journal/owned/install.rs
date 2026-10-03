//! Selected-history staging and activation on the owning application shard.

use super::{AppendBuffer, JournalError, JournalOwner, OwnedJournal, history, prefix};
use crate::replica_journal::{
    InstallationConfig, InstalledChunk, InstalledJournal, authority::position,
};
use ozzy_journal::operation::canonical_body_digest;
use ozzy_journal_segment::{AsyncSuffixInstaller, SuffixReplacement, SuffixStreamLimits};
use ozzy_replication::{InstallTicket, PreparedOperation, driver::ActivationTicket};

#[derive(Debug)]
pub(super) struct PendingInstallation {
    pub(super) ticket: InstallTicket,
    staging: AsyncSuffixInstaller,
    limits: SuffixStreamLimits,
}

impl OwnedJournal {
    /// Stage a selected replacement after this writer's exact durable promise.
    /// The old selection and its protected prefix remain intact until finish.
    /// Cancellation fences this owner, even if a backend job later succeeds.
    pub async fn begin_installation(
        &mut self,
        ticket: InstallTicket,
        config: InstallationConfig,
    ) -> Result<InstallTicket, JournalError> {
        self.healthy()?;
        self.configuration.replicated()?;
        self.writeback.require_idle()?;
        self.faulted = true;
        let journal = self.journal.ready()?;
        let manifest = journal.manifest();
        if self.images.is_some()
            || self.selected.is_some()
            || ticket.scope() != self.scope
            || ticket.scope().view != manifest.promised_view
            || ticket.previous_generation() != journal.writer().durable_position().generation()
            || ticket.generation() == ticket.previous_generation()
            || ticket.protected_committed() != prefix(manifest.committed)
        {
            return Err(JournalError::InstallationMismatch);
        }
        if config.segment_capacity > self.limits.io.max_segment_bytes
            || !config.body_encoding.is_supported()
            || config.max_staged_bytes < config.segment_capacity
            || config.max_orphan_probes == 0
        {
            return Err(JournalError::Configuration);
        }
        if ticket.source().voter == manifest.identity.replica_node_id
            && self.history.as_ref().map(history::source) != Some(ticket.source())
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        let replacement = SuffixReplacement {
            expected_current: journal.current(),
            protected_committed: position(ticket.protected_committed()),
            promised_view: ticket.scope().view,
            last_normal_view: ticket.scope().view,
            committed: position(ticket.committed()),
            writer_generation: ticket.generation(),
            segment_capacity: config.segment_capacity,
            body_encoding: config.body_encoding,
        };
        let limits = SuffixStreamLimits {
            max_group_operations: self
                .append_limits
                .max_operations
                .min(self.limits.decode.max_entries),
            max_group_body_bytes: self
                .append_limits
                .max_body_bytes
                .min(self.limits.decode.max_group_decoded_body_bytes),
            max_segments: self.limits.metadata.max_segments,
            max_staged_bytes: config.max_staged_bytes,
            max_source_segment_bytes: self.limits.io.max_segment_bytes,
            max_orphan_probes: config.max_orphan_probes,
        };
        let journal = self.journal.take_ready()?;
        let staging = journal
            .begin_suffix_replacement(replacement, position(ticket.accepted()), limits)
            .await?;
        self.journal = JournalOwner::Installing(Box::new(PendingInstallation {
            ticket,
            staging,
            limits,
        }));
        self.faulted = false;
        Ok(ticket)
    }

    /// Validate and stage a bounded canonical chunk. The result is not a vote,
    /// durable installation, or application completion. Reuse its leased arena.
    pub async fn install_chunk(
        &mut self,
        ticket: InstallTicket,
        mut buffer: AppendBuffer,
    ) -> Result<InstalledChunk, JournalError> {
        self.healthy()?;
        self.faulted = true;
        let installing = self.journal.installing_mut()?;
        if installing.ticket != ticket
            || buffer.is_empty()
            || buffer.owner_generation() != self.buffer_generation
        {
            return Err(JournalError::InstallationMismatch);
        }
        // Bounded descriptors only. Payloads stay in the existing arena and file
        // jobs retain their backing independently of this future's observation.
        let mut prepared = std::mem::take(&mut buffer.prepared);
        prepared.clear();
        let operations: Vec<_> = buffer.operations().collect();
        if operations
            .iter()
            .any(|operation| operation.original_view >= ticket.scope().view)
        {
            return Err(JournalError::InstallationMismatch);
        }
        let mut first = 0;
        let mut bytes = 0;
        for (index, operation) in operations.iter().enumerate() {
            if operation.body.len() > installing.limits.max_group_body_bytes {
                return Err(JournalError::AppendCapacity);
            }
            if index - first == installing.limits.max_group_operations
                || bytes > installing.limits.max_group_body_bytes - operation.body.len()
            {
                installing
                    .staging
                    .append_chunk(&operations[first..index])
                    .await?;
                first = index;
                bytes = 0;
            }
            bytes += operation.body.len();
        }
        installing
            .staging
            .append_chunk(&operations[first..])
            .await?;
        for operation in &operations {
            prepared.push(PreparedOperation::from_verified(
                operation,
                canonical_body_digest(operation.body),
            ));
        }
        let end = prepared.last().expect("nonempty staged chunk").prefix();
        drop(operations);
        buffer.prepared = prepared;
        self.faulted = false;
        Ok(InstalledChunk {
            ticket,
            end,
            buffer,
        })
    }

    /// Remove this healthy unpublished attempt without changing old authority.
    /// Exact ticket matching prevents a stale abandonment from aborting another.
    pub async fn abort_installation(
        &mut self,
        ticket: InstallTicket,
    ) -> Result<InstallTicket, JournalError> {
        self.healthy()?;
        self.faulted = true;
        if self.journal.installing_mut()?.ticket != ticket {
            return Err(JournalError::InstallationMismatch);
        }
        let pending = self.journal.take_installing()?;
        self.journal = JournalOwner::Ready(Box::new(pending.staging.abort().await?));
        self.faulted = false;
        Ok(ticket)
    }

    /// Publish selected bytes and rebuild canonical state privately. The existing
    /// replica driver must still establish new-view agreement before activation.
    pub async fn finish_installation(
        &mut self,
        ticket: InstallTicket,
    ) -> Result<InstalledJournal, JournalError> {
        self.healthy()?;
        self.faulted = true;
        if self.journal.installing_mut()?.ticket != ticket {
            return Err(JournalError::InstallationMismatch);
        }
        let pending = self.journal.take_installing()?;
        let mut journal = Box::pin(pending.staging.finish()).await?;
        let candidate = journal.recover_canonical_candidate(self.recovery).await?;
        if candidate.accepted_position() != position(ticket.accepted())
            || candidate.committed_images().committed().revision() != ticket.committed().op.0
        {
            return Err(JournalError::InstallationMismatch);
        }
        self.applied = ticket.committed();
        self.selected = Some(candidate);
        self.journal = JournalOwner::Ready(Box::new(journal));
        self.faulted = false;
        Ok(InstalledJournal {
            ticket,
            applied: self.applied,
        })
    }

    /// Publish the whole-tail commit before exposing its canonical state. Recheck
    /// returned evidence with the driver; I/O completion alone grants no role.
    pub async fn activate_installed(
        &mut self,
        ticket: ActivationTicket,
    ) -> Result<ActivationTicket, JournalError> {
        self.healthy()?;
        self.faulted = true;
        let journal = self.journal.ready()?;
        let manifest = journal.manifest();
        let candidate = self
            .selected
            .as_ref()
            .ok_or(JournalError::InstallationMismatch)?;
        if ticket.local() != manifest.identity.replica_node_id
            || ticket.scope() != self.scope
            || ticket.scope().view != manifest.last_normal_view
            || ticket.scope().view != manifest.promised_view
            || ticket.generation() != journal.writer().durable_position().generation()
            || ticket.through() != prefix(candidate.accepted_position())
            || ticket.applied() != self.applied
        {
            return Err(JournalError::InstallationMismatch);
        }
        let mut next = manifest.clone();
        next.parent_generation = next.generation;
        next.generation = next
            .generation
            .checked_add(1)
            .ok_or(JournalError::InstallationMismatch)?;
        next.accepted = position(ticket.through());
        next.committed = next.accepted;
        self.journal.ready_mut()?.install_metadata(next).await?;
        let candidate = self.selected.take().expect("selected candidate checked");
        self.reader = Some(
            ozzy_journal_segment::AsyncJournalPartitionIndex::open(
                self.journal.ready()?,
                self.read_limits,
            )
            .await?,
        );
        self.images = Some(candidate.activate(self.journal.ready()?)?);
        self.applied = ticket.through();
        self.pending.clear();
        self.faulted = false;
        Ok(ticket)
    }
}

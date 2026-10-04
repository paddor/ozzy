//! Nonvoting recovery donations, isolated from election and normal replay state.

use super::{
    AppendBuffer, AsyncJournalHistory, JournalError, JournalOwner, OwnedJournal, history, prefix,
};
use crate::replica_journal::{
    FetchedHistory, PinnedRecovery, authority::position, commands::read_fault,
};
use ozzy_proto::NodeId;
use ozzy_replication::{
    recovery::RecoveryResponse,
    wire::{CheckpointRequest, FetchOps},
};
use std::{cell::Cell, rc::Rc};

#[derive(Debug)]
pub(super) struct Source {
    pin: PinnedRecovery,
    metadata: ozzy_journal_segment::AsyncJournalHistoryMetadata,
    cached: Option<AsyncJournalHistory>,
    busy: Rc<Cell<bool>>,
    checkpoint: Option<Rc<ozzy_journal_segment::AsyncCheckpointFiles>>,
}

#[derive(Debug)]
struct Slot(Rc<Cell<bool>>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

/// One bounded recovery transfer read. Its donor retains metadata protection
/// independently, so dropping this work never implicitly releases the source.
#[derive(Debug)]
pub struct PreparedRecoveryRead {
    key: Rc<()>,
    pin: PinnedRecovery,
    history: AsyncJournalHistory,
    request: FetchOps,
    buffer: AppendBuffer,
    slot: Slot,
}

/// Exact-source bytes pending owner observation, not a normal replication vote.
#[derive(Debug)]
pub struct CompletedRecoveryRead {
    key: Rc<()>,
    pin: PinnedRecovery,
    history: AsyncJournalHistory,
    result: Result<FetchedHistory, JournalError>,
    slot: Slot,
}

impl PreparedRecoveryRead {
    /// Execute through shared file backends without borrowing the donor owner.
    pub async fn read(self) -> CompletedRecoveryRead {
        let Self {
            key,
            pin,
            mut history,
            request,
            buffer,
            slot,
        } = self;
        let result = history::fetch(&mut history, request, buffer, false).await;
        CompletedRecoveryRead {
            key,
            pin,
            history,
            result,
            slot,
        }
    }
}

impl OwnedJournal {
    fn donor_slot(&self, requester: NodeId) -> Result<usize, JournalError> {
        let local = self.journal.readable()?.manifest().identity.replica_node_id;
        if requester == local {
            return Err(JournalError::HistorySourceMismatch);
        }
        self.configuration
            .replicated()?
            .voters()
            .iter()
            .position(|&voter| voter == requester)
            .ok_or(JournalError::HistorySourceMismatch)
    }

    /// Synchronize and retain this exact recovery response before offering its
    /// history. The barrier grants no normal write/sync ticket or quorum vote.
    /// At most one nonce-bound source exists for each of the other two brokers.
    pub async fn pin_recovery(
        &mut self,
        requester: NodeId,
        response: RecoveryResponse,
    ) -> Result<PinnedRecovery, JournalError> {
        self.healthy()?;
        let at = self.donor_slot(requester)?;
        let journal = self.journal.readable()?;
        let donor = journal.manifest().identity.replica_node_id;
        let log = response
            .primary
            .ok_or(JournalError::HistorySourceMismatch)?;
        if response.scope != self.scope
            || donor != self.configuration.primary(self.scope.view)
            || self.images.is_none()
            || response.nonce.as_bytes() == &[0; 16]
            || log.generation != journal.writer().written_position().generation()
            || log.accepted.op > prefix(journal.written_position()?).op
            || log.committed.op > log.accepted.op
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        let pin = PinnedRecovery {
            requester,
            response,
            donor,
        };
        if let Some(existing) = &self.donors[at] {
            let mut original = existing.pin;
            if let Some(log) = original.response.primary.as_mut() {
                log.checkpoint = None;
            }
            return if existing.pin == pin || original == pin {
                Ok(existing.pin)
            } else {
                Err(JournalError::HistorySourceMismatch)
            };
        }
        if matches!(self.journal, JournalOwner::Rolling(_))
            || matches!(&self.journal, JournalOwner::Writing(pipeline) if pipeline.sync_pending())
        {
            return Err(JournalError::CompletionMismatch);
        }
        // A canceled barrier or validation leaves this owner fenced, even if
        // its backend-held file work completes after the observer disappears.
        self.faulted = true;
        let result = self.capture_donor(&pin).await;
        self.faulted = result.as_ref().err().is_some_and(read_fault);
        let source = result?;
        let pin = source.pin;
        self.donors[at] = Some(source);
        Ok(pin)
    }

    async fn capture_donor(&mut self, pin: &PinnedRecovery) -> Result<Source, JournalError> {
        let mut pin = *pin;
        match &mut self.journal {
            JournalOwner::Ready(journal) => {
                journal
                    .sync_through(journal.writer().written_position())
                    .await?;
            }
            JournalOwner::Writing(pipeline) => {
                pipeline.sync_installed().await?;
            }
            _ => return Err(JournalError::CompletionMismatch),
        }
        let log = pin.response.primary.expect("validated donor response");
        let mut history = self
            .journal
            .readable()?
            .freeze_history_through(
                position(log.accepted),
                self.limits.io.max_segment_bytes as usize,
            )
            .await?;
        if history.position(log.committed.op.0).await? != Some(position(log.committed)) {
            return Err(JournalError::HistorySourceMismatch);
        }
        let journal = self.journal.readable()?;
        let checkpoint = if journal.manifest().checkpoint.is_some() {
            let files = Rc::new(journal.capture_checkpoint()?);
            let manifest = files.manifest();
            let anchor = ozzy_replication::recovery::CheckpointAnchor {
                predecessor: prefix(history.predecessor()),
                position: prefix(manifest.position),
                schema: manifest.state_schema_digest,
                state_digest: manifest.state_digest,
                state_bytes: manifest.state_bytes,
                chunk_bytes: manifest.chunk_bytes,
            };
            anchor.validate(log.committed)?;
            pin.response
                .primary
                .as_mut()
                .expect("primary donor")
                .checkpoint = Some(anchor);
            Some(files)
        } else {
            None
        };
        Ok(Source {
            pin,
            checkpoint,
            metadata: history.metadata(),
            cached: Some(history),
            busy: Rc::new(Cell::new(false)),
        })
    }

    /// Release only the exact source. Detached reads keep their own protection;
    /// their late completion cannot reinstall a released or replaced donation.
    pub fn release_recovery(
        &mut self,
        pin: PinnedRecovery,
    ) -> Result<PinnedRecovery, JournalError> {
        self.healthy()?;
        let at = self.donor_slot(pin.requester)?;
        if self.donors[at]
            .as_ref()
            .is_some_and(|source| source.pin != pin)
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        self.donors[at] = None;
        Ok(pin)
    }

    /// Capture one read per recovering broker. Normal append/replay and the
    /// other recovering broker remain independent while this read executes.
    pub fn prepare_recovery_read(
        &mut self,
        pin: PinnedRecovery,
        request: FetchOps,
        buffer: AppendBuffer,
    ) -> Result<PreparedRecoveryRead, JournalError> {
        self.healthy()?;
        if request.scope != self.scope
            || request.scope != pin.response.scope
            || request.source != pin.source()
            || buffer.owner_generation() != self.buffer_generation
            || !buffer.is_empty()
            || request.max_operations == 0
            || request.max_body_bytes == 0
            || request.max_operations as usize > buffer.limits().max_operations
            || request.max_body_bytes as usize > buffer.limits().max_body_bytes
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        let at = self.donor_slot(pin.requester)?;
        let source = self.donors[at]
            .as_mut()
            .filter(|source| source.pin == pin)
            .ok_or(JournalError::HistorySourceMismatch)?;
        if source.busy.get() {
            return Err(JournalError::AppendCapacity);
        }
        let history = match source.cached.take() {
            Some(history) => history,
            None => source.metadata.reader()?,
        };
        source.busy.set(true);
        Ok(PreparedRecoveryRead {
            key: self.read_key.clone(),
            pin,
            history,
            request,
            buffer,
            slot: Slot(source.busy.clone()),
        })
    }

    /// Observe only a still-retained exact source. Restore its verified cache;
    /// storage faults fence this owner, stale completions provide no authority.
    pub fn complete_recovery_read(
        &mut self,
        done: CompletedRecoveryRead,
    ) -> Result<FetchedHistory, JournalError> {
        self.healthy()?;
        if !Rc::ptr_eq(&done.key, &self.read_key) || done.pin.response.scope != self.scope {
            return Err(JournalError::HistorySourceMismatch);
        }
        let at = self.donor_slot(done.pin.requester)?;
        let source = self.donors[at]
            .as_mut()
            .filter(|source| source.pin == done.pin && Rc::ptr_eq(&source.busy, &done.slot.0))
            .ok_or(JournalError::HistorySourceMismatch)?;
        self.faulted |= done.result.as_ref().err().is_some_and(read_fault);
        if !self.faulted {
            source.cached = Some(done.history);
        }
        done.result
    }
}

/// One detached, bounded immutable checkpoint chunk read.
#[derive(Debug)]
pub struct PreparedCheckpointRead {
    key: Rc<()>,
    pin: PinnedRecovery,
    request: CheckpointRequest,
    files: Rc<ozzy_journal_segment::AsyncCheckpointFiles>,
    slot: Slot,
}

/// Checkpoint bytes still requiring exact owner/pin observation.
#[derive(Debug)]
pub struct CompletedCheckpointRead {
    key: Rc<()>,
    pin: PinnedRecovery,
    request: CheckpointRequest,
    result: Result<bytes::Bytes, JournalError>,
    slot: Slot,
}

impl PreparedCheckpointRead {
    /// Execute file work without borrowing the journal owner.
    pub async fn read(self) -> CompletedCheckpointRead {
        let result = self
            .files
            .read_range(self.request.offset, self.request.max_bytes as usize)
            .await
            .map(bytes::Bytes::from)
            .map_err(JournalError::from);
        CompletedCheckpointRead {
            key: self.key,
            pin: self.pin,
            request: self.request,
            result,
            slot: self.slot,
        }
    }
}

impl OwnedJournal {
    /// Capture at most one detached read per nonce-bound recovery donor.
    pub fn prepare_checkpoint_read(
        &mut self,
        pin: PinnedRecovery,
        request: CheckpointRequest,
    ) -> Result<PreparedCheckpointRead, JournalError> {
        self.healthy()?;
        let anchor = pin
            .response
            .primary
            .and_then(|log| log.checkpoint)
            .ok_or(JournalError::HistorySourceMismatch)?;
        if request.scope != self.scope
            || request.scope != pin.response.scope
            || request.source != pin.source()
            || request.nonce != pin.response.nonce
            || request.max_bytes == 0
            || request.max_bytes as usize > self.append_limits.max_body_bytes
            || request.offset >= anchor.state_bytes
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        let at = self.donor_slot(pin.requester)?;
        let source = self.donors[at]
            .as_ref()
            .filter(|source| source.pin == pin)
            .ok_or(JournalError::HistorySourceMismatch)?;
        if source.busy.get() {
            return Err(JournalError::AppendCapacity);
        }
        let files = source
            .checkpoint
            .clone()
            .ok_or(JournalError::HistorySourceMismatch)?;
        source.busy.set(true);
        Ok(PreparedCheckpointRead {
            key: self.read_key.clone(),
            pin,
            request,
            files,
            slot: Slot(source.busy.clone()),
        })
    }

    /// Observe only matching nonce/view/source bytes; stale reads grant no authority.
    pub fn complete_checkpoint_read(
        &mut self,
        done: CompletedCheckpointRead,
    ) -> Result<super::super::recovery::RecoveryCheckpointRead, JournalError> {
        self.healthy()?;
        let at = self.donor_slot(done.pin.requester)?;
        if !Rc::ptr_eq(&done.key, &self.read_key)
            || done.pin.response.scope != self.scope
            || !self.donors[at].as_ref().is_some_and(|source| {
                source.pin == done.pin && Rc::ptr_eq(&source.busy, &done.slot.0)
            })
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        self.faulted |= done.result.as_ref().err().is_some_and(read_fault);
        Ok(super::super::recovery::RecoveryCheckpointRead {
            request: done.request,
            bytes: done.result?,
        })
    }
}

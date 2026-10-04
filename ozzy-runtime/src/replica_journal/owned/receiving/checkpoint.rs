//! Private bounded checkpoint assembly, checked before any history is adopted.

use super::{JournalError, RecoveringJournal, RecoveryTicket};
use crate::replica_journal::receiving::CheckpointProgress;
use ozzy_core::state::{CanonicalState, canonical_state_schema_digest};
use ozzy_replication::recovery::CheckpointAnchor;

#[derive(Debug)]
enum Storage {
    Leased(crate::memory::Buffer),
    Model(Vec<u8>),
}
impl Storage {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Leased(bytes) => bytes,
            Self::Model(bytes) => bytes,
        }
    }
    fn bytes_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Leased(bytes) => bytes,
            Self::Model(bytes) => bytes,
        }
    }
}

#[derive(Debug)]
pub(super) struct ReceivingCheckpoint {
    anchor: CheckpointAnchor,
    bytes: Storage,
    through: usize,
    pub(super) state: Option<CanonicalState>,
}

impl RecoveringJournal {
    pub(super) fn prepare_checkpoint(
        &mut self,
        ticket: &RecoveryTicket,
    ) -> Result<(), JournalError> {
        let Some(anchor) = ticket.checkpoint() else {
            return Ok(());
        };
        let length =
            usize::try_from(anchor.state_bytes).map_err(|_| JournalError::AppendCapacity)?;
        if length == 0
            || length > self.config.recovery.snapshot.max_snapshot_bytes
            || anchor.state_bytes > self.config.limits.checkpoint.max_state_bytes
            || anchor.schema != canonical_state_schema_digest()
        {
            return Err(JournalError::Configuration);
        }
        let bytes = match &self.append_memory {
            Some(owner) => Storage::Leased(owner.try_lease(length)?),
            None => Storage::Model(vec![0; length]),
        };
        self.checkpoint = Some(ReceivingCheckpoint {
            anchor,
            bytes,
            through: 0,
            state: None,
        });
        Ok(())
    }

    /// Copy one exact next range, then validate the complete private state.
    /// No partial state or repeated range can advance recovery authority.
    pub async fn receive_checkpoint(
        &mut self,
        ticket: RecoveryTicket,
        offset: u64,
        bytes: &[u8],
    ) -> Result<CheckpointProgress, JournalError> {
        self.healthy()?;
        self.require_ticket(&ticket)?;
        self.faulted = true;
        let checkpoint = self
            .checkpoint
            .as_mut()
            .ok_or(JournalError::HistorySourceMismatch)?;
        let start = usize::try_from(offset).map_err(|_| JournalError::AppendCapacity)?;
        let end = start
            .checked_add(bytes.len())
            .ok_or(JournalError::AppendCapacity)?;
        if start != checkpoint.through
            || bytes.is_empty()
            || bytes.len() > self.config.append_limits.max_body_bytes
            || end > checkpoint.bytes.bytes().len()
            || checkpoint.state.is_some()
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        checkpoint.bytes.bytes_mut()[start..end].copy_from_slice(bytes);
        checkpoint.through = end;
        let revision = if end == checkpoint.bytes.bytes().len() {
            ozzy_journal_segment::verify_checkpoint_state(
                checkpoint.bytes.bytes(),
                checkpoint.anchor.state_digest,
                async |_| tokio::task::yield_now().await,
            )
            .await?;
            let state = CanonicalState::decode_snapshot_cooperative(
                checkpoint.bytes.bytes(),
                self.config.recovery.state,
                self.config.recovery.snapshot,
                async |_| tokio::task::yield_now().await,
            )
            .await
            .map_err(ozzy_journal_segment::CanonicalCheckpointError::from)?;
            if state.revision() != checkpoint.anchor.position.op.0 {
                return Err(JournalError::HistorySourceMismatch);
            }
            checkpoint.state = Some(state);
            Some(checkpoint.anchor.position.op.0)
        } else {
            None
        };
        self.faulted = false;
        Ok(CheckpointProgress {
            through: end as u64,
            revision,
        })
    }
}

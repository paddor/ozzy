//! Private bounded file assembly and the same identity recovery checks as local open.

use super::{JournalError, RecoveringJournal, RecoveryTicket, State};
use crate::replica_journal::{
    owned::dedup::checkpoint::recovery::Restoring, receiving::CheckpointProgress, shard::yield_turn,
};
use ozzy_core::state::{CanonicalState, canonical_state_schema_digest};
use ozzy_journal_segment::AsyncCheckpointInbox;

#[derive(Debug)]
pub(super) struct ReceivingCheckpoint {
    pub(super) inbox: AsyncCheckpointInbox,
    parser: Option<Restoring>,
    pub(super) state: Option<CanonicalState>,
    _metadata: Option<crate::memory::Charge>,
    core_charge: Option<crate::memory::Charge>,
}

impl RecoveringJournal {
    pub(super) async fn prepare_checkpoint(
        &mut self,
        ticket: &RecoveryTicket,
    ) -> Result<(), JournalError> {
        let Some(anchor) = ticket.checkpoint() else {
            return Ok(());
        };
        let rows = self
            .config
            .deduplication
            .map_or(Some(0), |limits| {
                (limits.entries as u64).checked_mul(ozzy_core::dedup::SNAPSHOT_ROW_BYTES as u64)
            })
            .ok_or(JournalError::Configuration)?;
        let maximum = (self.config.recovery.snapshot.max_snapshot_bytes as u64)
            .checked_add(ozzy_core::checkpoint::HEADER_BYTES as u64)
            .and_then(|bytes| bytes.checked_add(rows))
            .ok_or(JournalError::Configuration)?;
        if anchor.state_bytes == 0
            || anchor.state_bytes > maximum
            || anchor.state_bytes > self.config.limits.checkpoint.max_state_bytes
            || anchor.schema != canonical_state_schema_digest()
        {
            return Err(JournalError::Configuration);
        }
        let State::Journal(journal) = &self.state else {
            return Err(JournalError::Faulted);
        };
        let inbox = journal
            .begin_checkpoint_receive(
                ozzy_proto::CheckpointId::from_bytes(*ticket.nonce().as_bytes()),
                anchor.state_bytes,
                anchor.state_digest,
            )
            .await?;
        let parser = Restoring::new(
            anchor.state_bytes,
            self.config.recovery,
            self.config.deduplication,
            self.append_memory.clone(),
        );
        let metadata = self
            .append_memory
            .as_ref()
            .map(|owner| {
                owner.try_charge(
                    inbox.retained_bytes()
                        + size_of::<ReceivingCheckpoint>()
                        + size_of::<Restoring>(),
                )
            })
            .transpose()?;
        self.checkpoint = Some(ReceivingCheckpoint {
            inbox,
            parser: Some(parser),
            state: None,
            _metadata: metadata,
            core_charge: None,
        });
        Ok(())
    }

    /// Write the exact next bounded range and validate into private owner state.
    /// Receipt cannot authorize history or voting before complete image adoption.
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
        let end = offset
            .checked_add(bytes.len() as u64)
            .ok_or(JournalError::AppendCapacity)?;
        if offset != checkpoint.inbox.through()
            || bytes.is_empty()
            || bytes.len() > self.config.append_limits.max_body_bytes
            || end > checkpoint.inbox.state_bytes()
            || checkpoint.state.is_some()
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        for fragment in bytes.chunks(checkpoint.inbox.fragment_bytes()) {
            checkpoint
                .inbox
                .push(checkpoint.inbox.through(), fragment)
                .await?;
            checkpoint
                .parser
                .as_mut()
                .ok_or(JournalError::HistorySourceMismatch)?
                .push(fragment)?;
            yield_turn().await;
        }
        let revision = if checkpoint.inbox.is_complete() {
            let decoded = checkpoint
                .parser
                .take()
                .ok_or(JournalError::HistorySourceMismatch)?
                .finish(
                    ticket
                        .checkpoint()
                        .ok_or(JournalError::HistorySourceMismatch)?
                        .position
                        .op
                        .0,
                )
                .await?;
            // Keep only bounded metadata here. Normal activation restores the verified
            // identity file; no second long-lived index is retained during adoption.
            checkpoint.state = Some(decoded.state);
            checkpoint.core_charge = decoded.charge;
            Some(
                checkpoint
                    .state
                    .as_ref()
                    .expect("complete state")
                    .revision(),
            )
        } else {
            None
        };
        self.faulted = false;
        Ok(CheckpointProgress {
            through: end,
            revision,
        })
    }
}

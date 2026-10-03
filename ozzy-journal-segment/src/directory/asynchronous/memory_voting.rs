use super::{DirectoryError, Journal};
use crate::directory::memory_voting::{self, NAME, TEMPORARY, VoteState};

impl Journal {
    /// Exact drained evidence is necessary, not sufficient, for intact restart.
    /// Missing, damaged, running or stale evidence requires nonvoting recovery.
    pub async fn require_drained_memory_history(&self) -> Result<(), DirectoryError> {
        self.healthy()?;
        let expected = self.memory_evidence(VoteState::Drained)?;
        let bytes = self
            .access
            .read_file(
                self.root().join(NAME),
                expected.len(),
                self.limits.io.chunk_bytes,
            )
            .await
            .map_err(|_| DirectoryError::MemoryHistoryUnproven)?;
        if bytes != expected {
            return Err(DirectoryError::MemoryHistoryUnproven);
        }
        Ok(())
    }

    /// Caller must prove fresh bootstrap, drained restart or installed recovery
    /// first. Enable memory voting only after this durable publication completes.
    pub async fn mark_memory_voting_running(&mut self) -> Result<(), DirectoryError> {
        self.publish_memory_evidence(VoteState::Running).await
    }

    /// Caller has stopped voting/admission and delivered all memory-only work.
    /// Freeze synchronized progress before publishing the exact drained marker.
    /// This cannot recover acknowledged records lost from every volatile copy.
    pub async fn publish_drained_memory_history(&mut self) -> Result<(), DirectoryError> {
        self.healthy()?;
        self.sync_through(self.writer.written_position()).await?;
        self.publish_progress().await?;
        self.publish_memory_evidence(VoteState::Drained).await
    }

    async fn publish_memory_evidence(&mut self, state: VoteState) -> Result<(), DirectoryError> {
        self.healthy()?;
        let bytes = self.memory_evidence(state)?;
        self.interrupted = true;
        self.directory.replace(NAME, TEMPORARY, &bytes).await?;
        self.interrupted = false;
        Ok(())
    }

    fn memory_evidence(
        &self,
        state: VoteState,
    ) -> Result<[u8; memory_voting::BYTES], DirectoryError> {
        let configuration = self
            .configuration()
            .ok_or(DirectoryError::ConfigurationMismatch)?;
        memory_voting::encode(
            &self.manifest,
            configuration,
            self.accepted_position()?,
            state,
        )
    }
}

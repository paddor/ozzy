use super::Journal;
use crate::{AsyncJournalHistory, AsyncStorageValidation, HistoryError, LogPosition};

impl Journal {
    /// Capture settled history without file access. The snapshot protects exact
    /// file generations and reserves one segment arena plus operation locations.
    pub fn freeze_history(
        &self,
        max_segment_bytes: usize,
    ) -> Result<AsyncJournalHistory, HistoryError> {
        if self.writer.written_position() != self.writer.durable_position() {
            return Err(HistoryError::Unsettled);
        }
        self.freeze_written_history(max_segment_bytes)
    }

    /// Capture only installed complete writes. Unsynchronized bytes grant no
    /// restart, election or durability authority.
    pub fn freeze_written_history(
        &self,
        max_segment_bytes: usize,
    ) -> Result<AsyncJournalHistory, HistoryError> {
        if self.is_faulted() {
            return Err(HistoryError::Unsettled);
        }
        AsyncJournalHistory::capture(
            self.access.clone(),
            self.root().join("segments"),
            &self.manifest,
            (
                self.writer.written_position(),
                self.writer.state().structural_digest(),
            ),
            &self.pins,
            self.limits,
            max_segment_bytes,
        )
    }

    /// Validate an exact earlier boundary without exposing later operations in
    /// its enclosing physical group. The whole captured group is still checked.
    pub async fn freeze_history_through(
        &self,
        through: LogPosition,
        max_segment_bytes: usize,
    ) -> Result<AsyncJournalHistory, HistoryError> {
        self.freeze_history(max_segment_bytes)?
            .restrict(through)
            .await
    }

    pub async fn begin_storage_validation(
        &mut self,
        max_segment_bytes: usize,
    ) -> Result<AsyncStorageValidation, HistoryError> {
        self.validate_authority_files().await?;
        Ok(AsyncStorageValidation::new(
            self.freeze_history(max_segment_bytes)?,
            self.current,
        ))
    }

    pub async fn begin_written_storage_validation(
        &mut self,
        max_segment_bytes: usize,
    ) -> Result<AsyncStorageValidation, HistoryError> {
        self.validate_authority_files().await?;
        Ok(AsyncStorageValidation::new(
            self.freeze_written_history(max_segment_bytes)?,
            self.current,
        ))
    }
}

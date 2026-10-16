use super::Journal;
use crate::{HistoryError, JournalHistory, LogPosition, StorageValidation};

impl Journal {
    /// Capture settled history without file access. The snapshot protects exact
    /// file generations and reserves one segment arena plus operation locations.
    pub fn freeze_history(&self, max_segment_bytes: usize) -> Result<JournalHistory, HistoryError> {
        if self.writer.written_position() != self.writer.durable_position() {
            return Err(HistoryError::Unsettled);
        }
        self.freeze_written_history(max_segment_bytes)
    }

    /// Capture only installed complete writes. Unsynchronized bytes grant no
    /// restart, election or durability authority.
    pub(crate) fn freeze_written_history(
        &self,
        max_segment_bytes: usize,
    ) -> Result<JournalHistory, HistoryError> {
        if self.is_faulted() {
            return Err(HistoryError::Unsettled);
        }
        JournalHistory::capture(
            self.access.clone(),
            self.root().join("segments"),
            crate::history::asynchronous::Capture {
                manifest: &self.manifest,
                written: (
                    self.writer.written_position(),
                    self.writer.state().structural_digest(),
                ),
                pins: &self.pins,
            },
            self.limits,
            max_segment_bytes,
        )
    }

    /// Validate an exact durable boundary while later writes remain unsynchronized.
    /// Later operations stay outside the response; their physical source group
    /// is still checked. Capture does not advance durability evidence.
    pub async fn freeze_history_through(
        &self,
        through: LogPosition,
        max_segment_bytes: usize,
    ) -> Result<JournalHistory, HistoryError> {
        if through.op_number >= self.writer.durable_position().next_chain().next_op_number() {
            return Err(HistoryError::Unsettled);
        }
        self.freeze_written_history(max_segment_bytes)?
            .restrict(through)
            .await
    }

    /// Capture selected confirmed history for a fresh physical and canonical scrub.
    pub async fn begin_storage_validation(
        &mut self,
        max_segment_bytes: usize,
    ) -> Result<StorageValidation, HistoryError> {
        self.validate_authority_files().await?;
        Ok(StorageValidation::new(
            self.freeze_history(max_segment_bytes)?,
            self.current,
        ))
    }
}

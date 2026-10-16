use super::{Image, Index, Installed, JournalError, Offset, OwnedJournal, StoredOperation};
use ozzy_journal::operation::CanonicalOperation;
use ozzy_journal_segment::{
    CanonicalRecoveryLimits, DecodedOperation, GroupJournal, JournalIndexBoundary,
    JournalIndexError,
};

impl OwnedJournal {
    /// Rebuild only independently validated canonical history before admission.
    /// Missing identities are never replaced with an empty recovered index.
    pub(in crate::replica_journal) async fn restore_dedup(&mut self) -> Result<(), JournalError> {
        let Some(limits) = self.dedup.limits else {
            return self.refuse_dedup_windows();
        };
        let journal = self.journal.ready_mut()?;
        let accepted = journal.accepted_position()?.op_number;
        let (mut image, first) =
            Image::restore_base(journal, self.applied.op.0, limits, self.recovery).await?;
        if first <= accepted {
            let snapshot = journal
                .build_index_snapshot(JournalIndexBoundary::Accepted, self.recovery.index)
                .await?;
            for number in first..=accepted {
                let decoded = snapshot
                    .read_operation_number(number)
                    .await?
                    .ok_or(JournalIndexError::MissingOperationNumber(number))?;
                let operation = canonical(&decoded);
                let (partition, row) = image.row(operation, self.limits.operations, limits)?;
                let prepared = image.index.prepare_stored(
                    &[StoredOperation {
                        operation: number,
                        logical_time: row.time,
                        first_offset: row.first,
                        ids: &row.ids,
                    }],
                    self.applied.op.0,
                )?;
                image.index.install(prepared)?;
                image.partition = partition;
                image.time = row.time;
                image.end = row
                    .first
                    .get()
                    .checked_add(row.ids.len() as u64)
                    .map(Offset::new)
                    .ok_or(JournalError::AppendCapacity)?;
                if number <= self.applied.op.0 {
                    image.index.commit_through(number)?;
                    image.index.expire(limits.batch_records, limits.bytes);
                }
            }
        }
        self.validate_dedup_image(&image, accepted, limits)?;
        image.reservation = Installed::count(image.index.reserved_bytes(), limits.entries);
        self.dedup.image = Some(image);
        Ok(())
    }

    /// Without identity limits, no recovered partition may enable a window.
    fn refuse_dedup_windows(&self) -> Result<(), JournalError> {
        if self.images.as_ref().is_some_and(|images| {
            images
                .speculative()
                .partitions()
                .any(|(_, partition)| partition.dedup_window_millis.is_some())
        }) {
            return Err(JournalError::Configuration);
        }
        Ok(())
    }

    fn validate_dedup_image(
        &self,
        image: &Image,
        accepted: u64,
        limits: ozzy_core::dedup::Limits,
    ) -> Result<(), JournalError> {
        let core = self
            .images
            .as_ref()
            .ok_or(JournalError::InstallationMismatch)?;
        if core.speculative().revision() != accepted
            || core.committed().revision() != self.applied.op.0
            || core.speculative().partitions().len() != usize::from(image.partition.is_some())
            || image.partition.is_some_and(|partition| {
                core.speculative().partition(partition).is_none_or(|state| {
                    state.next_offset != image.end
                        || state.dedup_time != image.time
                        || state.dedup_window_millis.map(std::num::NonZeroU64::get)
                            != Some(limits.window_millis)
                })
            })
        {
            return Err(JournalError::InstallationMismatch);
        }
        Ok(())
    }
}

impl Image {
    /// The image at the selected checkpoint, or an empty one without it, and
    /// the first operation replay adds to it.
    async fn restore_base(
        journal: &GroupJournal,
        applied: u64,
        limits: ozzy_core::dedup::Limits,
        recovery: CanonicalRecoveryLimits,
    ) -> Result<(Self, u64), JournalError> {
        let Some(selected) = journal.manifest().checkpoint else {
            let empty = Self {
                index: Index::new(limits, Offset::ZERO, 0)?,
                partition: None,
                time: 0,
                end: Offset::ZERO,
                reservation: None,
            };
            return Ok((empty, 1));
        };
        if selected.position.op_number > applied {
            return Err(JournalError::CompletionMismatch);
        }
        let files = journal.capture_checkpoint()?;
        if files.reference() != selected {
            return Err(JournalError::HistorySourceMismatch);
        }
        let image = super::checkpoint::restore(&files, limits, recovery).await?;
        let first = selected
            .position
            .op_number
            .checked_add(1)
            .ok_or(JournalError::CompletionMismatch)?;
        Ok((image, first))
    }
}

/// The canonical fields of one indexed operation.
fn canonical<'a>(decoded: &'a DecodedOperation<'_>) -> CanonicalOperation<'a> {
    CanonicalOperation {
        group_id: decoded.group_id,
        configuration_epoch: decoded.configuration_epoch,
        original_view: decoded.original_view,
        op_number: decoded.op_number,
        previous_digest: decoded.previous_digest,
        kind: decoded.kind,
        body: &decoded.body,
    }
}

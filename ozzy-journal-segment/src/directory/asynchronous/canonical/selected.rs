use super::{
    Journal,
    replay::{Resolved, finish_replay},
};
use crate::{
    AsyncJournalIdentityIndex, CanonicalRecoveryLimits, CanonicalStateRecoveryError as Error,
    CurrentReference, LogPosition,
};
use ozzy_core::state::{CanonicalImages, CanonicalImagesError, CanonicalState};
use ozzy_journal::{operation::decode_operation_body, progress::JournalGeneration};

/// Validated selected history, private until its exact whole-tail confirmation
/// is published. No consensus authority or live admission is supplied here.
#[derive(Debug)]
pub struct Candidate {
    committed: CanonicalImages<AsyncJournalIdentityIndex>,
    selected: CanonicalState,
    identities: AsyncJournalIdentityIndex,
    source: CurrentReference,
    generation: JournalGeneration,
    configuration_epoch: u64,
    promised_view: u64,
    last_normal_view: u64,
    pending_capacity: usize,
}

impl Journal {
    /// Validate the selected tail privately without retaining historical plans.
    /// The caller must establish group agreement and publish that exact tail
    /// before activation. Application visibility stays at the old commit floor.
    pub async fn recover_canonical_candidate(
        &mut self,
        limits: CanonicalRecoveryLimits,
    ) -> Result<Candidate, Error> {
        if self.is_faulted() || self.writer.written_position() != self.writer.durable_position() {
            return Err(Error::SelectionChanged);
        }
        let mut selected = self
            .selected_canonical_checkpoint(limits.state, limits.snapshot, limits.checkpoint)
            .await?
            .unwrap_or_else(|| CanonicalState::new(limits.state));
        let committed_position = self.committed_position()?;
        let accepted_position = self.accepted_position()?;
        let (committed_snapshot, accepted_snapshot) =
            self.build_recovery_index_snapshots(limits.index).await?;
        let mut committed =
            (selected.revision() == committed_position.op_number).then(|| selected.clone());
        finish_replay(
            self.replay_accepted_async(async |item| {
                let op = item.operation;
                let body = decode_operation_body(op.kind, &op.body, self.limits.operations)?;
                let mut replay =
                    Resolved::new(&accepted_snapshot, &body, selected.revision()).await?;
                // Historical progress can refer to the preceding private prefix,
                // never future operations. This does not make that prefix visible.
                let plan = selected
                    .prepare(op.op_number, &body, &replay, &selected)
                    .map_err(CanonicalImagesError::from)?;
                selected
                    .apply(plan, &mut replay)
                    .map_err(CanonicalImagesError::from)?;
                if op.op_number == committed_position.op_number {
                    committed = Some(selected.clone());
                }
                Ok(())
            })
            .await,
        )?;
        if selected.revision() != accepted_position.op_number {
            return Err(Error::PositionMismatch);
        }
        let committed = committed.ok_or(Error::PositionMismatch)?;
        let identities = AsyncJournalIdentityIndex::new(
            committed_snapshot,
            limits.retained_identities,
            limits.retained_identities,
        );
        let committed = CanonicalImages::from_recovered(
            committed.clone(),
            committed,
            identities.clone(),
            identities,
            Vec::new(),
            limits.accepted_transitions,
        )?;
        Ok(Candidate {
            committed,
            selected,
            identities: AsyncJournalIdentityIndex::new(
                accepted_snapshot,
                limits.retained_identities,
                limits.retained_identities,
            ),
            source: self.current,
            generation: self.writer.durable_position().generation(),
            configuration_epoch: self.manifest.configuration_epoch,
            promised_view: self.manifest.promised_view,
            last_normal_view: self.manifest.last_normal_view,
            pending_capacity: limits.accepted_transitions,
        })
    }
}

impl Candidate {
    /// Canonical application images through the selected committed prefix.
    pub const fn committed_images(&self) -> &CanonicalImages<AsyncJournalIdentityIndex> {
        &self.committed
    }
    /// Canonical prefix accepted under this journal mode.
    pub fn accepted_position(&self) -> LogPosition {
        self.identities.snapshot().through()
    }

    /// Pure activation after the consensus adapter publishes the whole-tail
    /// commit floor. A different writer, view, selection or tail rejects.
    pub fn activate(
        self,
        journal: &Journal,
    ) -> Result<CanonicalImages<AsyncJournalIdentityIndex>, Error> {
        let manifest = &journal.manifest;
        let current = journal.current;
        let accepted = self.accepted_position();
        if manifest.identity != self.identities.snapshot().identity()
            || journal.writer.durable_position().generation() != self.generation
            || journal.is_faulted()
            || journal.writer.written_position() != journal.writer.durable_position()
            || manifest.configuration_epoch != self.configuration_epoch
            || manifest.promised_view != self.promised_view
            || manifest.last_normal_view != self.last_normal_view
            || current.generation < self.source.generation
            || (current.generation == self.source.generation && current != self.source)
            || journal.accepted_position()? != accepted
        {
            return Err(Error::SelectionChanged);
        }
        if journal.committed_position()? != accepted || manifest.committed != accepted {
            return Err(Error::CommitNotPublished);
        }
        Ok(CanonicalImages::from_recovered(
            self.selected.clone(),
            self.selected,
            self.identities.clone(),
            self.identities,
            Vec::new(),
            self.pending_capacity,
        )?)
    }
}

use super::{
    Journal,
    replay::{Resolved, finish_replay, identities, settled_images},
};
use crate::{
    CanonicalRecoveryLimits, CanonicalStateRecoveryError as Error, CurrentReference,
    JournalIdentityIndex, LogPosition,
};
use ozzy_core::state::{CanonicalImages, CanonicalImagesError, CanonicalState};
use ozzy_journal::{operation::decode_operation_body, progress::JournalGeneration};

/// Validated selected history, private until its exact whole-tail confirmation
/// is published. No consensus authority or live admission is supplied here.
#[derive(Debug)]
pub struct Candidate {
    committed: CanonicalImages<JournalIdentityIndex>,
    selected: CanonicalState,
    identities: JournalIdentityIndex,
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
    pub fn recover_canonical_candidate(
        &mut self,
        limits: CanonicalRecoveryLimits,
    ) -> impl Future<Output = Result<Candidate, Error>> + '_ {
        // No wrapping future: callers sit deep in startup futures, and debug
        // builds give every poll frame its own copy.
        self.recover_candidate(limits, None)
    }

    /// As `recover_canonical_candidate`, but replay only after `base`, the
    /// caller's canonical state through `at`. `at` must be in this selected
    /// history at or after its checkpoint, and `base` must be the state that
    /// history gives through `at`; replay checks the position, not the state.
    pub async fn recover_canonical_candidate_after(
        &mut self,
        limits: CanonicalRecoveryLimits,
        at: LogPosition,
        base: CanonicalState,
    ) -> Result<Candidate, Error> {
        if base.revision() != at.op_number {
            return Err(Error::PositionMismatch);
        }
        Box::pin(self.recover_candidate(limits, Some((at, base)))).await
    }

    async fn recover_candidate(
        &mut self,
        limits: CanonicalRecoveryLimits,
        base: Option<(LogPosition, CanonicalState)>,
    ) -> Result<Candidate, Error> {
        if self.is_faulted() || self.writer.written_position() != self.writer.durable_position() {
            return Err(Error::SelectionChanged);
        }
        let checkpoint = self
            .manifest
            .checkpoint
            .map_or(LogPosition::GENESIS, |value| value.position);
        let (start, mut selected) = match base {
            Some((at, base)) if at.op_number >= checkpoint.op_number => (at, base),
            _ => (checkpoint, self.initial_canonical_state(limits).await?),
        };
        let committed_position = self.committed_position()?;
        let accepted_position = self.accepted_position()?;
        let (committed_snapshot, accepted_snapshot) =
            self.build_recovery_index_snapshots(limits.index).await?;
        let mut committed =
            (selected.revision() == committed_position.op_number).then(|| selected.clone());
        finish_replay(
            self.replay_accepted_after_async(start, async |item| {
                let op = item.operation;
                let body = decode_operation_body(op.kind, &op.body, self.limits.operations)?;
                let mut replay =
                    Resolved::new(&accepted_snapshot, &body, selected.revision()).await?;
                // Historical progress can refer to the preceding private prefix,
                // never future operations. This does not make that prefix visible.
                let plan = selected
                    .prepare(op.op_number, &body, &replay)
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
        let committed = settled_images(
            committed,
            identities(committed_snapshot, limits),
            limits.accepted_transitions,
        )?;
        Ok(Candidate {
            committed,
            selected,
            identities: identities(accepted_snapshot, limits),
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
    pub const fn committed_images(&self) -> &CanonicalImages<JournalIdentityIndex> {
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
    ) -> Result<CanonicalImages<JournalIdentityIndex>, Error> {
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
        Ok(settled_images(
            self.selected,
            self.identities,
            self.pending_capacity,
        )?)
    }
}

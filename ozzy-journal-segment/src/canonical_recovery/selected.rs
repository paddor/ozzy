//! Private selected-tail validation, separate from the bounded live pipeline.

use ozzy_core::state::{CanonicalImages, CanonicalState};
use ozzy_journal::{operation::decode_operation_body, progress::JournalGeneration};

use super::{
    CanonicalRecoveryLimits, CanonicalReplayError, CanonicalStateRecoveryError as Error,
    ReplayIdentityIndex, flatten_replay,
};
use crate::{CurrentReference, JournalIdentityIndex, LogPosition, OpenGroupJournal};

/// Validated selected history which has not acquired application visibility.
///
/// Only the already committed image is accessible before activation. The private
/// tail contains bounded application state and source-bound indexes, not one
/// transition plan per historical operation. Keeping this object pins its files.
/// It supplies no consensus authority and must not be used for live admission.
#[derive(Debug)]
pub struct CanonicalRecoveryCandidate {
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

impl OpenGroupJournal {
    /// Validate a quiescent selected journal without retaining its pending plans.
    ///
    /// Runs blocking replay/index work on a dedicated storage worker. Each plan
    /// is validated, applied privately, and discarded. Only the persisted commit
    /// floor remains visible. Historical progress may depend on earlier records
    /// whose real commit preceded the last persisted commit marker; validation
    /// therefore checks it against the private preceding prefix, never against
    /// future records. This is not permission to expose that prefix as committed.
    ///
    /// The caller must establish whole-tail quorum, publish that exact commit
    /// floor, and call `CanonicalRecoveryCandidate::activate` before live use.
    pub fn recover_canonical_candidate(
        &self,
        limits: CanonicalRecoveryLimits,
    ) -> Result<CanonicalRecoveryCandidate, Error> {
        if self.writer().is_faulted()
            || self.writer().written_position() != self.writer().durable_position()
        {
            return Err(Error::SelectionChanged);
        }
        let checkpoint =
            self.selected_canonical_checkpoint(limits.state, limits.snapshot, limits.checkpoint)?;
        let mut selected = checkpoint.unwrap_or_else(|| CanonicalState::new(limits.state));
        let checkpoint_revision = selected.revision();
        let committed_position = self.committed_position()?;
        let accepted_position = self.accepted_position()?;
        let (committed_snapshot, accepted_snapshot) =
            self.build_recovery_index_snapshots(limits.index)?;
        let mut replay = ReplayIdentityIndex::new(&accepted_snapshot, checkpoint_revision);
        let mut committed =
            (checkpoint_revision == committed_position.op_number).then(|| selected.clone());
        flatten_replay(self.replay_accepted(|item| {
            let operation = item.operation;
            let body = decode_operation_body(
                operation.kind,
                operation.body.as_ref(),
                self.operation_limits(),
            )?;
            let plan = selected.prepare(operation.op_number, &body, &replay, &selected)?;
            selected.apply(plan, &mut replay)?;
            replay.advance(operation.op_number);
            if operation.op_number == committed_position.op_number {
                committed = Some(selected.clone());
            }
            Ok::<_, CanonicalReplayError>(())
        }))?;
        if selected.revision() != accepted_position.op_number {
            return Err(Error::PositionMismatch);
        }
        let committed = committed.ok_or(Error::PositionMismatch)?;
        let identities = JournalIdentityIndex::new(committed_snapshot, limits.retained_identities);
        let committed = CanonicalImages::from_recovered(
            committed.clone(),
            committed,
            identities.clone(),
            identities,
            Vec::new(),
            limits.accepted_transitions,
        )?;
        let manifest = self.directory().manifest();
        Ok(CanonicalRecoveryCandidate {
            committed,
            selected,
            identities: JournalIdentityIndex::new(accepted_snapshot, limits.retained_identities),
            source: self.directory().current(),
            generation: self.writer().durable_position().generation(),
            configuration_epoch: manifest.configuration_epoch,
            promised_view: manifest.promised_view,
            last_normal_view: manifest.last_normal_view,
            pending_capacity: limits.accepted_transitions,
        })
    }
}

impl CanonicalRecoveryCandidate {
    /// Already committed state and identities, with no accepted-only live plans.
    ///
    /// Its speculative image equals its committed image. The selected tail stays
    /// private; a replica must block new admission until activation completes.
    pub const fn committed_images(&self) -> &CanonicalImages<JournalIdentityIndex> {
        &self.committed
    }

    /// Exact accepted operation/digest validated by this candidate.
    pub fn accepted_position(&self) -> LogPosition {
        self.identities.snapshot().through()
    }

    /// Consume the private candidate after the whole-tail commit is published.
    ///
    /// The consensus adapter, not this object, authorizes the metadata update.
    /// It must also fence stale callbacks if a newer view arrives during disk I/O.
    /// Require the same store, writer, configuration epoch, view, and exact tail.
    /// A different selection or incomplete commit rejects; errors consume this candidate, so rebuild
    /// rather than resuming stale activation. This adds no per-record barrier.
    pub fn activate(
        self,
        journal: &OpenGroupJournal,
    ) -> Result<CanonicalImages<JournalIdentityIndex>, Error> {
        let manifest = journal.directory().manifest();
        let current = journal.directory().current();
        let accepted = self.accepted_position();
        if journal.directory().identity() != self.identities.snapshot().identity()
            || journal.writer().durable_position().generation() != self.generation
            || journal.writer().is_faulted()
            || journal.writer().written_position() != journal.writer().durable_position()
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

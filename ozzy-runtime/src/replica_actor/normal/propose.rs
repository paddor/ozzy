//! Leader proposals: the next one that fits the live window, and the
//! install turn of an admitted batch.

use super::{ActorError, Duration, NextProposal, PendingIo, ReplicaActor};

impl ReplicaActor {
    /// Take the waiting or next queued proposal when the live window has room,
    /// with a validation ticket for the image the journal will then hold.
    pub(super) fn take_proposal(
        &mut self,
        snapshot: &ozzy_replication::ReplicaSnapshot,
    ) -> Result<
        Option<(
            ozzy_replication::driver::ValidationTicket,
            crate::replica_journal::ProposalBuffer,
        )>,
        ActorError,
    > {
        // Maintenance waits for room like any proposal, ahead of queued ones.
        // It holds no ingress permit, so it also waits for a live slot.
        if let Some(buffer) = self.work.proposals.maintenance() {
            let slot = self.work.live.len() < self.config.proposal_capacity;
            let room = room(&self.journal, self.config.pipeline, buffer, snapshot)
                .map(|fits| fits && slot);
            if !self.note_room(room) {
                return Ok(None);
            }
            let Some(buffer) = self.work.proposals.start_maintenance() else {
                return Ok(None);
            };
            return Ok(Some((self.driver.begin_validation()?, buffer)));
        }
        let driver = &self.driver;
        let submission = match self
            .work
            .proposals
            .next(&mut self.ingress, || driver.begin_validation())?
        {
            NextProposal::Idle | NextProposal::Blocked => return Ok(None),
            // As a backup rejection: the next round runs what it deferred.
            NextProposal::Finished => {
                self.more_work = true;
                return Ok(None);
            }
            NextProposal::Ready(submission) => submission,
        };
        let room = room(
            &self.journal,
            self.config.pipeline,
            &submission.buffer,
            snapshot,
        );
        if !self.note_room(room) {
            return Ok(None);
        }
        let Some(buffer) = self.work.proposals.start() else {
            return Ok(None);
        };
        let ticket = self.driver.begin_validation()?;
        Ok(Some((ticket, buffer)))
    }

    /// Whether `room` admits a proposal. Reports the live window filling.
    fn note_room(&mut self, room: Option<bool>) -> bool {
        match room {
            None => false,
            Some(false) => {
                if !std::mem::replace(&mut self.work.window_full, true) {
                    crate::profiling::event(crate::profiling::Event::LiveWindowFull);
                }
                false
            }
            Some(true) => {
                self.work.window_full = false;
                true
            }
        }
    }

    /// Install the accepted batch directly. The next validation observes its
    /// installed image in the next bounded actor turn.
    pub(in crate::replica_actor) fn send_ready_turn(
        &mut self,
        _now: Duration,
    ) -> Result<(), ActorError> {
        let Some(admit) = self.work.ready.take() else {
            return Ok(());
        };
        let apply = if self.driver.normal().is_some_and(|normal| {
            let snapshot = normal.snapshot();
            snapshot.applied != snapshot.committed
        }) {
            Some(self.driver.begin_validation()?)
        } else {
            None
        };
        let turn = Box::new(crate::replica_journal::Turn {
            admit: Some(admit),
            apply,
            ..Default::default()
        });
        assert!(self.pending.is_none());
        match self.journal.turn(turn) {
            Ok(completion) => self.pending = Some(PendingIo::Turn(completion)),
            Err(rejected) if rejected.reason == crate::replica_journal::SubmitError::Full => {
                // Retained backing can fill the physical backlog before the
                // core's operation/body window fills. Keep the same validated
                // batch in its existing slot until physical progress frees it.
                self.work.ready = rejected.value.admit;
            }
            Err(rejected) => return Err(rejected.reason.into()),
        }
        Ok(())
    }

    /// Record live-window occupancy after a core admission.
    pub(in crate::replica_actor) fn observe_window(&self) {
        if let Some(normal) = self.driver.normal() {
            let snapshot = normal.snapshot();
            crate::profiling::peak(
                crate::profiling::Event::LiveWindowPeakOperations,
                snapshot.pending_operations as u64,
            );
            crate::profiling::peak(
                crate::profiling::Event::LiveWindowPeakBytes,
                snapshot.pending_body_bytes as u64,
            );
        }
    }
}

/// Whether `buffer` fits beside the operations `snapshot` holds: `None`
/// when validation is full, `Some(false)` when the live window is.
fn room(
    journal: &crate::replica_journal::ReplicaJournal,
    limits: ozzy_replication::PipelineLimits,
    buffer: &crate::replica_journal::ProposalBuffer,
    snapshot: &ozzy_replication::ReplicaSnapshot,
) -> Option<bool> {
    if !journal.validation_has_capacity(&buffer.0, snapshot.accepted.op) {
        return None;
    }
    Some(
        buffer.len() <= limits.max_operations - snapshot.pending_operations
            && buffer.admitted_body_bytes() <= limits.max_body_bytes - snapshot.pending_body_bytes,
    )
}

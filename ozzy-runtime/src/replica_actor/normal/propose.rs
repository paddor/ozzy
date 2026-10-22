//! Primary proposals: the next one that fits the live window, and the
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
        if let Some(buffer) = self.work.proposals.start_maintenance() {
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
        if !self
            .journal
            .validation_has_capacity(&submission.buffer.0, snapshot.accepted.op)
        {
            return Ok(None);
        }
        if submission.buffer.len()
            > self.config.pipeline.max_operations - snapshot.pending_operations
            || submission.buffer.body_bytes()
                > self.config.pipeline.max_body_bytes - snapshot.pending_body_bytes
        {
            if !std::mem::replace(&mut self.work.window_full, true) {
                crate::profiling::event(crate::profiling::Event::LiveWindowFull);
            }
            return Ok(None);
        }
        self.work.window_full = false;
        let Some(buffer) = self.work.proposals.start() else {
            return Ok(None);
        };
        let ticket = self.driver.begin_validation()?;
        Ok(Some((ticket, buffer)))
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

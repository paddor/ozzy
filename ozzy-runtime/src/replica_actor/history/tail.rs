//! A normal backup takes the end of a resumed primary's log.
//!
//! A primary that resumed its view with operations no backup confirmed
//! sends nothing until a backup holds its whole log. A backup that lacks
//! the end of that log fetches it from the primary's pinned source, as an
//! installing backup would, and admits it as it admits prepares.

use super::{ActorError, Duration, FetchOps, OpsBatch, ReplicaActor, TransferPurpose};
use ozzy_replication::NormalReplica;

impl ReplicaActor {
    /// Start, or give up, the fetch of what this backup lacks of its
    /// resumed primary's log. One chunk at a time: the next starts once
    /// the core admitted the last.
    pub(in crate::replica_actor) fn schedule_tail(&mut self) -> Result<(), ActorError> {
        let fetching = self
            .transfer
            .filter(|transfer| matches!(transfer.purpose, TransferPurpose::Tail));
        let wanted = self.tail_source();
        if let Some(transfer) = fetching {
            if wanted != Some(transfer.request.source) {
                self.transfer = None;
            }
            return Ok(());
        }
        let (Some(source), None) = (wanted, self.transfer) else {
            return Ok(());
        };
        let Some(snapshot) = self.driver.normal().map(NormalReplica::snapshot) else {
            return Ok(());
        };
        self.start_transfer(
            snapshot.scope,
            source,
            snapshot.accepted,
            TransferPurpose::Tail,
        )
    }

    /// The source to fetch from now, if any. Not while the primary sends
    /// in this backup's receive epoch, and not while received operations
    /// wait for the core: the fetch continues from the accepted prefix.
    fn tail_source(&self) -> Option<ozzy_replication::LogSource> {
        let normal = self.driver.normal()?;
        let source = normal.missing_tail()?;
        (self.application_ready()
            && self.receipt_current(&normal.snapshot())
            && self.tail_refused != Some(source)
            && !self.work.flow.receiving()
            && !self.work.receive_has_work())
        .then_some(source)
    }

    /// Admit one fetched chunk. A fresh receive epoch goes first: whatever
    /// the primary learned of the old one no longer counts, so its own
    /// sends cannot meet operations it never sent there.
    pub(super) fn receive_tail(
        &mut self,
        batch: &OpsBatch<'_>,
        request: FetchOps,
        now: Duration,
    ) -> Result<(), ActorError> {
        if self.tail_source() != Some(request.source) {
            return Ok(());
        }
        let Some(snapshot) = self.driver.normal().map(NormalReplica::snapshot) else {
            return Ok(());
        };
        if batch.predecessor() != snapshot.accepted {
            self.transfer = None;
            return Ok(());
        }
        self.retract_received(now)?;
        let prepare = batch.as_prepare(snapshot.committed);
        if self.receive_prepare(request.source.voter, prepare, now)? {
            self.transfer = None;
        }
        Ok(())
    }
}

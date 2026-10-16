//! Confirmed floors precede checkpoint selection and physical retirement.

mod cleanup;
mod plan;
mod retirement;

use super::{JournalError, OwnedJournal};
use crate::replica_journal::RetentionTurn;
use cleanup::{Cleaned, Cleaning};
use ozzy_core::retention::{Plan, Segment};
use ozzy_journal_segment::{RetirementBudget, RetirementProof};
use ozzy_proto::{CheckpointId, OperationId, PartitionIncarnation};
use ozzy_replication::driver::ValidationTicket;
use retirement::Retiring;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
pub(super) struct Retention {
    /// Summaries of selected sealed segments, from a roll or a scan.
    pub(super) sealed: BTreeMap<u64, Segment>,
    /// Record ends of segments this owner rolled, so retirement need not
    /// scan them.
    pub(super) proofs: Vec<RetirementProof>,
    /// The last settled turn spent its one proposal on an idle-time
    /// watermark; the next one proposes record retention first.
    records_owed: bool,
    /// Selection whose unselected objects are all reclaimed or pinned.
    cleaned: Option<Cleaned>,
    /// Retirement whose checkpoint write and prefix scan run beside writes.
    retiring: Retiring,
    /// Unselected-storage cleanup that runs beside writes.
    cleaning: Cleaning,
    /// A complete exclusive sweep ran since this owner opened.
    swept: bool,
    /// Retention jobs started, so a stale completion is recognized.
    attempts: u64,
}

impl Retention {
    /// Forget cached summaries and roll proofs of segments `keep` rejects.
    fn retain_segments(&mut self, keep: impl Fn(u64) -> bool) {
        self.sealed.retain(|id, _| keep(*id));
        self.proofs.retain(|proof| keep(proof.segment_id()));
    }
}

impl RetentionTurn {
    /// A turn that proposes nothing and owes no further step now.
    const fn idle(enabled: bool) -> Self {
        Self {
            enabled,
            expired_identities: 0,
            more_work: false,
            proposal: None,
            released: None,
        }
    }
}

impl OwnedJournal {
    fn charge_retention(&self) -> Result<Option<crate::memory::Charge>, JournalError> {
        if self.append_memory.is_none() {
            return Ok(None);
        }
        let bytes = self.journal.ready()?.retention_scratch_bytes()?;
        charge(self.append_memory.as_ref(), bytes)
    }

    pub(in crate::replica_journal) async fn retention_turn(
        &mut self,
        ticket: ValidationTicket,
        now_millis: u64,
        seed: OperationId,
        leader: bool,
    ) -> Result<RetentionTurn, JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        if ticket.accepted() != ticket.applied() || ticket.committed() != ticket.applied() {
            return Ok(deferred());
        }
        // Bootstrap may activate the empty log before CreatePartition commits.
        // No partition state has selected an unlimited retention policy yet.
        if self.images()?.committed().partitions().next().is_none() {
            return Ok(deferred());
        }
        // One bounded expiry step, then record retention in the same turn.
        // Under steady load expiry always has more work, so waiting for it
        // would starve record retention. A turn carries one proposal.
        let owed = std::mem::take(&mut self.retention.records_owed);
        let identities = self.retain_identities(ticket, leader && !owed)?;
        if identities.proposal.is_some() {
            self.retention.records_owed = true;
            return Ok(RetentionTurn {
                more_work: identities.more_work,
                expired_identities: identities.expired,
                proposal: identities.proposal,
                ..RetentionTurn::idle(true)
            });
        }
        let mut turn = self
            .record_retention_turn(ticket, now_millis, seed, leader)
            .await?;
        turn.enabled |= self.dedup.limits.is_some();
        turn.more_work |= identities.more_work;
        turn.expired_identities = identities.expired;
        Ok(turn)
    }

    /// Whether a retirement or cleanup job is captured, running, or awaits
    /// installation.
    pub(in crate::replica_journal) fn retention_busy(&self) -> bool {
        self.retiring() || self.cleaning()
    }

    async fn record_retention_turn(
        &mut self,
        ticket: ValidationTicket,
        now_millis: u64,
        seed: OperationId,
        leader: bool,
    ) -> Result<RetentionTurn, JournalError> {
        // One retention job at a time; it runs beside writes, and planning
        // resumes once it is installed.
        if self.retention_busy() {
            crate::profiling::event(crate::profiling::Event::RetentionRetiring);
            return Ok(RetentionTurn::idle(true));
        }
        let planned = match self.plan_retention(ticket, now_millis, 1).await {
            Err(JournalError::AppendCapacity) => return Ok(deferred()),
            result => result?,
        };
        let Some((partition, plan)) = planned else {
            return Ok(RetentionTurn::idle(false));
        };
        self.clean_unselected(ticket).await?;
        if self.floors_pending(partition, &plan)? {
            return self.propose_floors(partition, &plan, seed, leader);
        }
        if !plan.retire.is_empty() {
            if self.cleaning() {
                // The prefix retires once the cleanup just captured finishes.
                return Ok(RetentionTurn {
                    more_work: true,
                    ..RetentionTurn::idle(true)
                });
            }
            return self.start_retirement(ticket, seed).await;
        }
        if leader && plan.roll_active {
            let work = self.begin_roll(16)?;
            let done = work.publish().await;
            self.complete_roll(done)?;
            return Ok(RetentionTurn {
                more_work: true,
                ..RetentionTurn::idle(true)
            });
        }
        Ok(RetentionTurn::idle(true))
    }

    /// Whether the plan's record floor or a producer's retry floor must
    /// advance before its prefix can retire.
    fn floors_pending(
        &self,
        partition: PartitionIncarnation,
        plan: &Plan,
    ) -> Result<bool, JournalError> {
        let state = self
            .images()?
            .committed()
            .partition(partition)
            .ok_or(JournalError::Configuration)?;
        let floor = plan.record_floor.max(state.retained_from);
        Ok(plan.record_floor > state.retained_from
            || state.producers().any(|(_, producer)| {
                plan::raised_retry_floor(producer, floor, plan.operation_floor).is_some()
            }))
    }

    /// A leader proposes the floors; a follower waits for its proposal.
    fn propose_floors(
        &self,
        partition: PartitionIncarnation,
        plan: &Plan,
        seed: OperationId,
        leader: bool,
    ) -> Result<RetentionTurn, JournalError> {
        let proposal = if leader {
            match self.retention_proposal(partition, plan.record_floor, plan.operation_floor, seed)
            {
                Err(JournalError::AppendCapacity) => return Ok(deferred()),
                result => result?,
            }
        } else {
            None
        };
        if proposal.is_some() {
            crate::profiling::event(crate::profiling::Event::RetentionFloor);
        }
        Ok(RetentionTurn {
            more_work: proposal.is_some(),
            proposal,
            ..RetentionTurn::idle(true)
        })
    }

    /// Capture one retirement; it releases the previous election source.
    async fn start_retirement(
        &mut self,
        ticket: ValidationTicket,
        seed: OperationId,
    ) -> Result<RetentionTurn, JournalError> {
        let previous = self.history.take();
        let released = previous.as_ref().map(super::history::source);
        let budget = RetirementBudget {
            max_segments: 1,
            max_read_bytes: self.limits.io.max_segment_bytes as usize,
        };
        let id = CheckpointId::from_bytes(*seed.as_bytes());
        let retirement = self.begin_retirement(ticket, id, budget).await;
        if matches!(retirement, Err(JournalError::AppendCapacity)) {
            self.history = previous;
            return Ok(deferred());
        }
        retirement?;
        crate::profiling::event(crate::profiling::Event::RetirementStarted);
        // Eligible debt continues once this retirement is installed.
        Ok(RetentionTurn {
            more_work: true,
            released,
            ..RetentionTurn::idle(true)
        })
    }
}

fn charge(
    memory: Option<&crate::memory::Owner>,
    bytes: usize,
) -> Result<Option<crate::memory::Charge>, JournalError> {
    let Some(owner) = memory else {
        return Ok(None);
    };
    owner.try_charge(bytes).map(Some).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            JournalError::AppendCapacity
        } else {
            error.into()
        }
    })
}

fn deferred() -> RetentionTurn {
    crate::profiling::event(crate::profiling::Event::RetentionDeferred);
    RetentionTurn::idle(true)
}

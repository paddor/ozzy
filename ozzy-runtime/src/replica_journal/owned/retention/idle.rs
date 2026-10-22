//! Primary-local idle observations of producer sessions, and the proposal
//! that retires sessions quiet for the configured expiry.

use super::super::{JournalError, OwnedJournal};
use super::plan::push;
use crate::replica_journal::ProposalBuffer;
use ahash::AHashMap;
use ozzy_core::state::CanonicalPartition;
use ozzy_journal::operation::{OperationBody, RetireProducer};
use ozzy_proto::{ProducerEpoch, ProducerId, ProducerSequence};
use std::time::Duration;

/// When each session last changed, as this primary saw it. Nothing here is
/// authority: a retirement commits only while the session's epoch and next
/// sequence still match.
#[derive(Debug, Default)]
pub(in crate::replica_journal::owned) struct IdleProducers {
    expiry: Option<Duration>,
    /// The view these observations belong to.
    view: u64,
    seen: AHashMap<ProducerId, Seen>,
    /// Broker time of the last complete sweep.
    swept: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct Seen {
    epoch: ProducerEpoch,
    next: ProducerSequence,
    since: u64,
}

impl IdleProducers {
    /// `None` keeps every session.
    pub(in crate::replica_journal::owned) fn new(expiry: Option<Duration>) -> Self {
        Self {
            expiry,
            ..Self::default()
        }
    }

    pub(in crate::replica_journal::owned) const fn expiry(&self) -> Option<Duration> {
        self.expiry
    }

    /// An open shows its writer is live; the next sweep sees it fresh.
    pub(in crate::replica_journal::owned) fn forget(&mut self, producer: ProducerId) {
        self.seen.remove(&producer);
    }

    /// Sessions unchanged for the whole expiry, at most `limit`, in ID order.
    /// A sweep runs once per sixteenth of the expiry, or again at once while
    /// idle sessions remain.
    fn sweep(
        &mut self,
        state: &CanonicalPartition,
        now: u64,
        limit: usize,
    ) -> (Vec<(ProducerId, Seen)>, bool) {
        let Some(expiry) = self.expiry.map(millis) else {
            return (Vec::new(), false);
        };
        if self
            .swept
            .is_some_and(|at| now.saturating_sub(at) < (expiry / 16).max(1))
        {
            return (Vec::new(), false);
        }
        self.seen.retain(|id, _| state.producer(*id).is_some());
        let mut idle = Vec::new();
        let mut more = false;
        for (id, producer) in state.producers() {
            let current = Seen {
                epoch: producer.producer_epoch,
                next: producer.next_producer_sequence,
                since: now,
            };
            let seen = self.seen.entry(id).or_insert(current);
            if (seen.epoch, seen.next) != (current.epoch, current.next) {
                *seen = current;
            } else if now.saturating_sub(seen.since) >= expiry {
                if idle.len() < limit {
                    idle.push((id, *seen));
                } else {
                    more = true;
                }
            }
        }
        if !more {
            self.swept = Some(now);
        }
        idle.sort_unstable_by_key(|(id, _)| *id);
        (idle, more)
    }
}

impl OwnedJournal {
    /// Propose retiring the sessions idle past the expiry, and whether more
    /// remain. Observations last one view: opens served by another primary
    /// change no state, so a new primary starts every session fresh.
    pub(in crate::replica_journal::owned) fn idle_producer_proposal(
        &mut self,
        view: u64,
        now_millis: u64,
        leader: bool,
    ) -> Result<(Option<ProposalBuffer>, bool), JournalError> {
        let idle = &mut self.retention.idle;
        if !leader || idle.view != view {
            *idle = IdleProducers {
                view,
                ..IdleProducers::new(idle.expiry)
            };
        }
        if !leader {
            return Ok((None, false));
        }
        self.healthy()?;
        let limit = self.append_limits.max_operations;
        // Field access keeps the image borrow apart from the observations.
        let images = self
            .images
            .as_ref()
            .ok_or(JournalError::InstallationMismatch)?;
        let mut partitions = images.committed().partitions();
        let Some((partition, state)) = partitions.next() else {
            return Ok((None, false));
        };
        if partitions.next().is_some() {
            return Err(JournalError::Configuration);
        }
        let (idle, more) = self.retention.idle.sweep(state, now_millis, limit);
        if idle.is_empty() {
            return Ok((None, more));
        }
        let mut buffer = self
            .lease_proposal_buffer()
            .map_err(|_| JournalError::AppendCapacity)?;
        for (producer_id, seen) in idle {
            push(
                &mut buffer,
                &OperationBody::RetireProducer(RetireProducer {
                    partition,
                    producer_id,
                    producer_epoch: seen.epoch,
                    next_sequence: seen.next,
                }),
                self.limits.operations,
            )?;
        }
        Ok((Some(buffer), more))
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

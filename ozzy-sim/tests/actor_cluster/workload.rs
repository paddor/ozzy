//! Clients propose one proposal of barrier operations at a time through
//! production proposal lanes, one lane per broker. A refused proposal moves
//! to another broker; an unknown outcome resends the same bytes there later.
//! A resend that finds its operation identity taken learns that an earlier
//! attempt committed. A journal too full for new operations refuses them; the
//! client backs off and resends.

use crate::{
    broker::Lane,
    check::Checker,
    network::BROKERS,
    seeds::{Rng, Trace},
};
use ozzy_core::state::{CanonicalImagesError, StateError};
use ozzy_journal::operation::OperationKind;
use ozzy_replication::Prefix;
use ozzy_runtime::{
    replica_actor::{PendingProposal, ProposalOutcome, ProposalSubmitError, UnsubmittedProposal},
    replica_journal::JournalError,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

/// Ticks before a resend after an unknown outcome, and after a refusal.
const RESEND: u64 = 200;
const REDIRECT: u64 = 10;

#[derive(Debug)]
struct Client {
    id: u8,
    seq: u32,
    target: usize,
    /// One lane per broker; `None` after that broker's actor was replaced
    /// and before its new lanes arrive.
    lanes: Vec<Option<Lane>>,
    pending: Option<(usize, Pin<Box<PendingProposal>>)>,
    next_at: u64,
    /// The current operation went out before with an unknown outcome.
    resent: bool,
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Counts {
    pub submitted: u64,
    pub not_admitted: u64,
    pub unknown: u64,
    pub stopped: u64,
    /// Refusals by a journal too full for new operations.
    pub full: u64,
    /// Resends whose earlier attempt had committed.
    pub duplicates: u64,
}

#[derive(Debug)]
pub(crate) struct Workload {
    clients: Vec<Client>,
    /// Barrier operations per proposal.
    batch: usize,
    rng: Rng,
    /// Every confirmed prefix, in confirmation order.
    pub confirmed: Vec<Prefix>,
    pub counts: Counts,
}

impl Workload {
    /// `lanes[broker][client]`.
    pub(crate) fn new(lanes: Vec<Vec<Lane>>, seed: u64, batch: usize) -> Self {
        let count = lanes[0].len();
        let mut by_broker: Vec<_> = lanes.into_iter().map(Vec::into_iter).collect();
        let clients = (0..count)
            .map(|client| Client {
                id: client as u8,
                seq: 0,
                target: client % BROKERS,
                lanes: by_broker
                    .iter_mut()
                    .map(|lanes| Some(lanes.next().unwrap()))
                    .collect(),
                pending: None,
                next_at: 0,
                resent: false,
            })
            .collect();
        Self {
            clients,
            batch,
            rng: Rng::new(seed),
            confirmed: Vec::new(),
            counts: Counts::default(),
        }
    }

    /// Fresh lanes of a broker whose partition actor was replaced.
    pub(crate) fn replace(&mut self, broker: usize, lanes: Vec<Lane>) {
        for (client, lane) in self.clients.iter_mut().zip(lanes) {
            client.lanes[broker] = Some(lane);
        }
    }

    /// No proposal is outstanding.
    pub(crate) fn idle(&self) -> bool {
        self.clients.iter().all(|client| client.pending.is_none())
    }

    /// Observe replies, then submit. `submit` false only observes.
    pub(crate) fn step(
        &mut self,
        tick: u64,
        submit: bool,
        checker: &mut Checker,
        trace: &mut Trace,
    ) -> Result<(), String> {
        for client in &mut self.clients {
            if let Some((broker, pending)) = &mut client.pending {
                let broker = *broker;
                let Poll::Ready(reply) = pending
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                else {
                    continue;
                };
                client.pending = None;
                trace.word(tick);
                trace.word(u64::from(client.id));
                let Ok(reply) = reply else {
                    // The actor was replaced by recovery; its lanes closed and
                    // the arena went with them. The outcome is unknown.
                    self.counts.stopped += 1;
                    client.lanes[broker] = None;
                    client.resent = true;
                    client.target = other(&mut self.rng, broker);
                    client.next_at = tick + RESEND;
                    continue;
                };
                if let Some(lane) = &mut client.lanes[broker] {
                    lane.buffer = Some(reply.buffer);
                }
                match reply.outcome {
                    ProposalOutcome::Committed { through, .. } => {
                        trace.word(through.op.0);
                        checker.confirm(through)?;
                        self.confirmed.push(through);
                        client.next(tick + self.rng.below(3));
                    }
                    ProposalOutcome::NotAdmitted => {
                        self.counts.not_admitted += 1;
                        client.target = other(&mut self.rng, broker);
                        client.next_at = tick + REDIRECT;
                    }
                    ProposalOutcome::Unknown => {
                        self.counts.unknown += 1;
                        client.resent = true;
                        client.target = other(&mut self.rng, broker);
                        client.next_at = tick + RESEND;
                    }
                    ProposalOutcome::Invalid(JournalError::RollCapacity) => {
                        // Nothing was written; the same operation waits for room.
                        self.counts.full += 1;
                        client.next_at = tick + RESEND;
                    }
                    ProposalOutcome::Invalid(JournalError::Images(
                        CanonicalImagesError::State(StateError::IdentityConflict),
                    )) if client.resent => {
                        self.counts.duplicates += 1;
                        client.next(tick + self.rng.below(3));
                    }
                    ProposalOutcome::Invalid(error) => {
                        return Err(format!("client {}: barrier rejected: {error}", client.id));
                    }
                }
            }
            if !submit || tick < client.next_at {
                continue;
            }
            let target = client.target;
            let Some(lane) = &mut client.lanes[target] else {
                client.target = other(&mut self.rng, target);
                continue;
            };
            let mut buffer = lane.buffer.take().expect("idle lane keeps its arena");
            buffer.clear();
            // Barrier operation IDs must be nonzero.
            let mut body = [0; 16];
            body[0] = client.id + 1;
            body[1..5].copy_from_slice(&client.seq.to_be_bytes());
            for index in 0..self.batch {
                body[5] = index as u8;
                buffer
                    .push(OperationKind::Barrier, &body)
                    .map_err(|error| format!("client {}: {error}", client.id))?;
            }
            match lane.submitter.try_submit(buffer) {
                Ok(pending) => {
                    self.counts.submitted += 1;
                    client.pending = Some((target, Box::pin(pending)));
                }
                Err(UnsubmittedProposal {
                    reason: ProposalSubmitError::Full,
                    buffer,
                }) => {
                    lane.buffer = Some(buffer);
                    client.next_at = tick + 1;
                }
                Err(UnsubmittedProposal {
                    reason: ProposalSubmitError::Stopped,
                    ..
                }) => {
                    client.lanes[target] = None;
                    client.target = other(&mut self.rng, target);
                }
                Err(rejected) => {
                    return Err(format!(
                        "client {}: proposal refused: {:?}",
                        client.id, rejected.reason
                    ));
                }
            }
        }
        Ok(())
    }
}

impl Client {
    /// Move on to a fresh operation.
    fn next(&mut self, at: u64) {
        self.seq += 1;
        self.resent = false;
        self.next_at = at;
    }
}

/// A random broker other than `broker`.
fn other(rng: &mut Rng, broker: usize) -> usize {
    (broker + 1 + rng.below(BROKERS as u64 - 1) as usize) % BROKERS
}

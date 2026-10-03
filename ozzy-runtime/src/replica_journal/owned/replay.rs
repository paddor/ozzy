//! One detached normal replication-history read, separate from election history.

use super::{
    AppendBuffer, AsyncJournalHistory, JournalError, OwnedJournal, PipelineLimits, Scope, history,
};
use crate::replica_journal::{FetchedHistory, ReplicationPositions, commands::read_fault};
use ozzy_proto::RequestId;
use ozzy_replication::{OpNumber, Prefix, driver::ValidationTicket, wire::FetchOps};
use std::{cell::Cell, rc::Rc};

#[derive(Debug, Default)]
pub(super) struct Replay {
    cached: Option<(Scope, AsyncJournalHistory)>,
    busy: Rc<Cell<bool>>,
}
impl Replay {
    pub(super) fn clear(&mut self) {
        self.cached = None;
    }
}

#[derive(Debug)]
struct Slot(Rc<Cell<bool>>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

/// Captured installed history. This job borrows no application/journal owner;
/// writes may continue beyond its immutable prefix while file jobs execute.
#[derive(Debug)]
pub struct PreparedReplay {
    key: Rc<()>,
    history: AsyncJournalHistory,
    request: FetchOps,
    buffer: AppendBuffer,
    slot: Slot,
}

/// Read result and reusable source cache awaiting its originating owner.
/// Cancellation discards the cache and releases the single replay slot.
#[derive(Debug)]
pub struct CompletedReplay {
    key: Rc<()>,
    history: AsyncJournalHistory,
    scope: Scope,
    result: Result<FetchedHistory, JournalError>,
    _slot: Slot,
}

impl PreparedReplay {
    /// Fetch exact-source bytes with digest proofs, not replication authority.
    pub async fn read(self) -> CompletedReplay {
        let Self {
            key,
            mut history,
            request,
            buffer,
            slot,
        } = self;
        let result = history::fetch(&mut history, request, buffer, true).await;
        CompletedReplay {
            key,
            history,
            scope: request.scope,
            result,
            _slot: slot,
        }
    }
}

impl OwnedJournal {
    fn normal_history(&self) -> Result<AsyncJournalHistory, JournalError> {
        let journal = self.journal.readable()?;
        Ok(if self.configuration.memory_voting() {
            journal.freeze_written_history(self.limits.io.max_segment_bytes as usize)?
        } else {
            journal.freeze_history(self.limits.io.max_segment_bytes as usize)?
        })
    }

    fn primary_history(&self, ticket: ValidationTicket) -> Result<(), JournalError> {
        self.healthy()?;
        self.validate_image(ticket)?;
        if self.configuration.primary(ticket.scope().view)
            != self.journal.readable()?.manifest().identity.replica_node_id
        {
            return Err(JournalError::AppendMismatch);
        }
        Ok(())
    }

    /// Reserve the single replay job, independently of election-history capture.
    /// Correlation IDs come from the actor so deterministic simulation controls
    /// them. Insufficient body credit returns a size hint, never an empty packet.
    pub fn prepare_replay(
        &mut self,
        ticket: ValidationTicket,
        predecessor: Prefix,
        limits: PipelineLimits,
        buffer: AppendBuffer,
        request_id: RequestId,
    ) -> Result<PreparedReplay, JournalError> {
        self.primary_history(ticket)?;
        if self.replay.busy.get() {
            return Err(JournalError::AppendCapacity);
        }
        if buffer.owner_generation() != self.buffer_generation
            || !buffer.is_empty()
            || limits.max_operations == 0
            || limits.max_operations > buffer.limits().max_operations
            || limits.max_body_bytes == 0
            || limits.max_body_bytes > buffer.limits().max_body_bytes
            || request_id.as_bytes() == &[0; 16]
        {
            return Err(JournalError::AppendMismatch);
        }
        let max_body_bytes =
            u32::try_from(limits.max_body_bytes).map_err(|_| JournalError::AppendCapacity)?;
        let reusable = self.replay.cached.as_ref().is_some_and(|(scope, history)| {
            *scope == ticket.scope()
                && history.generation() == ticket.generation()
                && predecessor.op.0 >= history.predecessor().op_number
                && predecessor.op.0 < history.through().op_number
        });
        let history = if reusable {
            self.replay.cached.take().expect("checked source").1
        } else {
            self.replay.clear();
            self.normal_history()?
        };
        let request = FetchOps {
            scope: ticket.scope(),
            request_id,
            source: history::source(&history),
            predecessor,
            max_operations: limits.max_operations as u32,
            max_body_bytes,
        };
        self.replay.busy.set(true);
        Ok(PreparedReplay {
            key: self.read_key.clone(),
            history,
            request,
            buffer,
            slot: Slot(self.replay.busy.clone()),
        })
    }

    /// Install only a live-source result. An older completion cannot replace a
    /// newer cache populated by a flow-position lookup. Caller correlation and
    /// replication-core checks still precede sending these bytes to a follower.
    pub fn complete_replay(
        &mut self,
        done: CompletedReplay,
    ) -> Result<FetchedHistory, JournalError> {
        self.healthy()?;
        if !Rc::ptr_eq(&done.key, &self.read_key) {
            return Err(JournalError::HistorySourceMismatch);
        }
        self.active_scope(done.scope, done.history.generation())?;
        self.faulted |= done.result.as_ref().err().is_some_and(read_fault);
        if !self.faulted
            && self.replay.cached.as_ref().is_none_or(|(_, newer)| {
                newer.through().op_number <= done.history.through().op_number
            })
        {
            self.replay.cached = Some((done.scope, done.history));
        }
        done.result
    }

    /// Resolve bounded normal-history flow hints. The ticket is echoed, never
    /// extended to a newer image. Payload reads and digest verification are async.
    pub async fn replication_positions(
        &mut self,
        ticket: ValidationTicket,
        requested: [OpNumber; 2],
    ) -> Result<ReplicationPositions, JournalError> {
        self.primary_history(ticket)?;
        let reusable = self.replay.cached.as_ref().is_some_and(|(scope, history)| {
            *scope == ticket.scope()
                && history.generation() == ticket.generation()
                && requested.iter().all(|op| {
                    op.0 >= history.predecessor().op_number && op.0 <= history.through().op_number
                })
        });
        if !reusable {
            self.replay.cached = Some((ticket.scope(), self.normal_history()?));
        }
        let result = async {
            let (_, history) = self.replay.cached.as_mut().expect("captured source");
            let positions = [
                history.position(requested[0].0).await?.map(super::prefix),
                history.position(requested[1].0).await?.map(super::prefix),
            ];
            Ok(ReplicationPositions {
                ticket,
                requested,
                positions,
            })
        }
        .await;
        self.faulted |= result.as_ref().err().is_some_and(read_fault);
        result
    }
}

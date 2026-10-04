//! Source-bound election history with asynchronous reads and deletion protection.

use super::{AppendBuffer, AsyncJournalHistory, JournalError, OwnedJournal, prefix};
use crate::replica_journal::{FetchedHistory, HistoryPosition, authority::position};
use ozzy_replication::{LogSource, OpNumber, wire::FetchOps};

pub(super) fn source(history: &AsyncJournalHistory) -> LogSource {
    LogSource {
        voter: history.identity().replica_node_id,
        generation: history.generation(),
        accepted: prefix(history.through()),
    }
}

impl OwnedJournal {
    /// Capture settled history before publishing its election descriptor. This
    /// is memory-only. Exact files stay protected through later installation.
    pub fn capture_history(&mut self, requested: LogSource) -> Result<LogSource, JournalError> {
        self.healthy()?;
        if let Some(history) = &self.history {
            return if source(history) == requested {
                Ok(requested)
            } else {
                Err(JournalError::HistorySourceMismatch)
            };
        }
        let journal = self.journal.ready()?;
        if requested.voter != journal.manifest().identity.replica_node_id
            || requested.generation != journal.writer().durable_position().generation()
            || requested.accepted != prefix(journal.accepted_position()?)
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        self.history = Some(journal.freeze_history(self.limits.io.max_segment_bytes as usize)?);
        Ok(requested)
    }

    /// A stale release cannot remove another source's deletion protection.
    pub fn release_history(&mut self, requested: LogSource) -> Result<LogSource, JournalError> {
        self.healthy()?;
        if self
            .history
            .as_ref()
            .is_some_and(|history| source(history) != requested)
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        self.history = None;
        Ok(requested)
    }

    fn history(&mut self, requested: LogSource) -> Result<&mut AsyncJournalHistory, JournalError> {
        self.history
            .as_mut()
            .filter(|history| source(history) == requested)
            .ok_or(JournalError::HistorySourceMismatch)
    }

    /// Read a bounded source-scoped digest. A stale response supplies no authority.
    /// Dropped reads do not mutate the journal. Corrupt/unreadable sources fence
    /// further use; request errors do not.
    pub async fn history_position(
        &mut self,
        source: LogSource,
        op: OpNumber,
    ) -> Result<HistoryPosition, JournalError> {
        self.healthy()?;
        let retained_predecessor = prefix(self.history(source)?.predecessor());
        let result = self
            .history(source)?
            .position(op.0)
            .await
            .map_err(JournalError::from);
        self.faulted |= result
            .as_ref()
            .err()
            .is_some_and(crate::replica_journal::commands::read_fault);
        Ok(HistoryPosition {
            source,
            op,
            retained_predecessor,
            position: result?.map(prefix),
        })
    }

    /// Read one exact-source chunk into a leased arena. Metadata checks precede
    /// file jobs; a canceled read releases no installation or voting authority.
    pub async fn fetch_history(
        &mut self,
        request: FetchOps,
        buffer: AppendBuffer,
    ) -> Result<FetchedHistory, JournalError> {
        self.healthy()?;
        if request.scope != self.scope
            || buffer.owner_generation() != self.buffer_generation
            || !buffer.is_empty()
            || request.max_operations == 0
            || request.max_body_bytes == 0
            || request.max_operations as usize > buffer.limits().max_operations
            || request.max_body_bytes as usize > buffer.limits().max_body_bytes
        {
            return Err(JournalError::HistorySourceMismatch);
        }
        let result = if self
            .history
            .as_ref()
            .is_some_and(|history| source(history) == request.source)
        {
            fetch(self.history(request.source)?, request, buffer, false).await
        } else {
            let journal = self.journal.readable()?;
            if request.source.voter != journal.manifest().identity.replica_node_id
                || request.source.generation != journal.writer().written_position().generation()
            {
                return Err(JournalError::HistorySourceMismatch);
            }
            let mut history = journal
                .freeze_history_through(
                    position(request.source.accepted),
                    self.limits.io.max_segment_bytes as usize,
                )
                .await?;
            fetch(&mut history, request, buffer, false).await
        };
        self.faulted |= result
            .as_ref()
            .err()
            .is_some_and(crate::replica_journal::commands::read_fault);
        result
    }
}

pub(super) async fn fetch(
    history: &mut AsyncJournalHistory,
    request: FetchOps,
    mut buffer: AppendBuffer,
    backpressure: bool,
) -> Result<FetchedHistory, JournalError> {
    if request.predecessor.op.0 < history.predecessor().op_number {
        return Ok(FetchedHistory {
            request,
            end: request.predecessor,
            buffer,
            minimum_body_bytes: None,
            retired_predecessor: Some(prefix(history.predecessor())),
        });
    }
    let chunk = match history
        .read_after(
            position(request.predecessor),
            request.max_operations as usize,
            request.max_body_bytes as usize,
        )
        .await
    {
        Ok(chunk) => chunk,
        Err(ozzy_journal_segment::HistoryError::BodyBudget { required, .. }) if backpressure => {
            return Ok(FetchedHistory {
                request,
                end: request.predecessor,
                buffer,
                minimum_body_bytes: Some(required),
                retired_predecessor: None,
            });
        }
        Err(error) => {
            return Err(error.into());
        }
    };
    let bytes = chunk
        .operations()
        .map(|operation| operation.body.len())
        .sum();
    buffer.reserve_history_payload(bytes).await?;
    for (operation, digest) in chunk.verified_operations() {
        buffer.push(operation)?;
        buffer.body_digests.push(digest);
    }
    Ok(FetchedHistory {
        request,
        end: prefix(chunk.end()),
        buffer,
        minimum_body_bytes: None,
        retired_predecessor: None,
    })
}

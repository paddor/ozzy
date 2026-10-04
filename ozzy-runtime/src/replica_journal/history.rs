//! Source-bound selection lookup and replica history export on the journal worker.

use ozzy_replication::wire::{FetchOps, Operation};
use ozzy_replication::{LogSource, OpNumber, Prefix};

use super::commands::Action;
use super::{AppendBuffer, JournalCompletion, Rejected, ReplicaJournal, SubmitError};

/// Result for an exact source/op selection-cache key. No vote or nonexistence proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryPosition {
    /// Frozen voter, writer incarnation, and authoritative tail.
    pub source: LogSource,
    /// Requested operation number.
    pub op: OpNumber,
    /// Exact predecessor of the frozen retained range. Missing older positions
    /// supply no ancestry evidence and must not fault an intact requester.
    pub retained_predecessor: Prefix,
    /// Exact position, or `None` only outside this captured retained range.
    pub position: Option<Prefix>,
}

/// Two exact normal-history lookups, retaining the authority captured before I/O.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationPositions {
    /// Physical retained-chain predecessor, not a claim of remote history.
    pub retained_predecessor: Prefix,
    /// Scope, writer generation, and image against which this read was submitted.
    pub ticket: ozzy_replication::driver::ValidationTicket,
    /// Base and receipt positions requested by the protocol actor.
    pub requested: [OpNumber; 2],
    /// Independently read canonical prefixes; missing positions are not evidence.
    pub positions: [Option<Prefix>; 2],
}

/// One bounded response retaining its request correlation and leased payload arena.
#[derive(Debug)]
pub struct FetchedHistory {
    pub(super) request: FetchOps,
    pub(super) end: Prefix,
    pub(super) buffer: AppendBuffer,
    pub(super) minimum_body_bytes: Option<usize>,
    pub(super) retired_predecessor: Option<Prefix>,
}

impl FetchedHistory {
    pub(crate) fn retired_predecessor(&self) -> Option<Prefix> {
        self.retired_predecessor
    }

    pub(crate) fn shared_bodies(&mut self) -> bytes::Bytes {
        self.buffer.shared_bodies()
    }

    /// Normal replay found a verified next operation larger than current credit.
    /// Such a response has no operations and leaves the end at the predecessor.
    /// It is local backpressure and must never be encoded as an empty OPS packet.
    pub const fn minimum_body_bytes(&self) -> Option<usize> {
        self.minimum_body_bytes
    }

    /// Exact request/session-exchange ID, scope, predecessor, and source echoed by OPS.
    pub const fn request(&self) -> FetchOps {
        self.request
    }

    /// Last complete operation returned. A segment boundary may end a chunk early.
    pub const fn end(&self) -> Prefix {
        self.end
    }

    /// Encode OPS with worker-computed body digests; never rehash payload on the actor.
    pub fn wire_operations(&self) -> impl ExactSizeIterator<Item = Operation<'_>> {
        self.buffer.wire_operations()
    }

    /// Borrow canonical bytes without mutating the verified response.
    pub const fn buffer(&self) -> &AppendBuffer {
        &self.buffer
    }

    /// Recover the arena after transport releases the response; clear before reuse.
    pub fn into_buffer(self) -> AppendBuffer {
        self.buffer
    }
}

impl ReplicaJournal {
    /// Verify base/receipt hints on the disk worker, independent of election pins.
    /// Requires synchronized normal history and reuses its bounded frozen reader.
    /// The actor must recheck generation and live flow correlation after completion.
    pub fn replication_positions(
        &mut self,
        ticket: ozzy_replication::driver::ValidationTicket,
        requested: [OpNumber; 2],
    ) -> Result<JournalCompletion<ReplicationPositions>, SubmitError> {
        self.submit(
            (ticket, requested),
            |(ticket, requested), done| Action::ReplicationPositions {
                ticket,
                requested,
                done,
            },
            |action| match action {
                Action::ReplicationPositions {
                    ticket, requested, ..
                } => (ticket, requested),
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Read stable normal history for one lagging replica without replacing the
    /// separately pinned election source. Capture includes only completed physical
    /// writes; background reservations may continue beyond that stable prefix.
    /// The predecessor is a hint until checked against the actual local journal.
    /// One detached job runs on the shard reader. The owner restores its verified
    /// segment/index for subsequent chunks until the tail or authority changes.
    /// The returned source may precede the current journal tail.
    /// One encoded segment and its operation index stay cached; refreshing a source
    /// and decoding response bodies still allocate bounded storage.
    #[expect(
        clippy::result_large_err,
        reason = "return the leased arena on backpressure"
    )]
    pub fn fetch_replication(
        &mut self,
        ticket: ozzy_replication::driver::ValidationTicket,
        predecessor: Prefix,
        limits: ozzy_replication::PipelineLimits,
        buffer: AppendBuffer,
    ) -> Result<JournalCompletion<FetchedHistory>, Rejected<AppendBuffer>> {
        self.submit(
            buffer,
            |buffer, done| Action::FetchReplication {
                ticket,
                predecessor,
                limits,
                buffer,
                done,
            },
            |action| match action {
                Action::FetchReplication { buffer, .. } => buffer,
                _ => unreachable!("submission preserves command kind"),
            },
        )
    }

    /// Pin the exact stable source before publishing its `DO_VIEW_CHANGE`/`START_VIEW`
    /// descriptor. One captured source is allowed; identical captures are idempotent.
    /// Release it explicitly before capturing a different one. No files leave the worker.
    pub fn capture_history(
        &mut self,
        source: LogSource,
    ) -> Result<JournalCompletion<LogSource>, SubmitError> {
        self.submit(
            source,
            |source, done| Action::CaptureHistory { source, done },
            |action| match action {
                Action::CaptureHistory { source, .. } => source,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Retire the exact source after all consumers no longer require its files.
    /// A stale release cannot unpin a different source. Already released is harmless.
    pub fn release_history(
        &mut self,
        source: LogSource,
    ) -> Result<JournalCompletion<LogSource>, SubmitError> {
        self.submit(
            source,
            |source, done| Action::ReleaseHistory { source, done },
            |action| match action {
                Action::ReleaseHistory { source, .. } => source,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Populate a bounded actor-owned digest cache asynchronously for core selection.
    /// Every cache key must include the full returned source, not only the op number.
    pub fn history_position(
        &mut self,
        source: LogSource,
        op: OpNumber,
    ) -> Result<JournalCompletion<HistoryPosition>, SubmitError> {
        self.submit(
            (source, op),
            |(source, op), done| Action::HistoryPosition { source, op, done },
            |action| match action {
                Action::HistoryPosition { source, op, .. } => (source, op),
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Fetch into an empty leased arena. Scope/source/predecessor and request bounds
    /// are checked on the worker. Read corruption fences the worker; invalid requests
    /// do not. Queue backpressure returns the unchanged arena for retry.
    #[expect(
        clippy::result_large_err,
        reason = "return arena ownership without allocating on backpressure"
    )]
    pub fn fetch_history(
        &mut self,
        request: FetchOps,
        buffer: AppendBuffer,
    ) -> Result<JournalCompletion<FetchedHistory>, Rejected<AppendBuffer>> {
        self.submit(
            buffer,
            |buffer, done| Action::FetchHistory {
                request,
                buffer,
                done,
            },
            |action| match action {
                Action::FetchHistory { buffer, .. } => buffer,
                _ => unreachable!("submission preserves command kind"),
            },
        )
    }
}

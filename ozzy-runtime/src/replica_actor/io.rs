//! Owned write/image actions and one independently progressing data barrier.

mod observe;

use ozzy_proto::NodeId;
use ozzy_replication::driver::{ActivationTicket, ValidationTicket};
use ozzy_replication::{InstallTicket, LogSource, PromiseTicket, SyncTicket};

use crate::replica_journal::{
    FetchedHistory, HistoryPosition, InstalledChunk, InstalledJournal, JournalCompletion,
    JournalError, PinnedRecovery, ReadyJournalSync, ReplicationPositions,
};

#[derive(Debug)]
pub(super) enum PendingSync {
    Barrier(JournalCompletion<ReadyJournalSync>),
    Install(JournalCompletion<SyncTicket>),
}

#[derive(Debug)]
pub(super) enum SyncEvent {
    Ready(ReadyJournalSync),
    Installed(SyncTicket),
}

#[derive(Debug)]
pub(super) struct PendingReplay {
    pub completion: JournalCompletion<FetchedHistory>,
    pub to: NodeId,
}

impl PendingReplay {
    pub(super) async fn wait(&mut self) -> Result<Completed, JournalError> {
        Ok(Completed::Replay((&mut self.completion).await?, self.to))
    }
}

impl PendingSync {
    pub(super) async fn wait(&mut self) -> Result<SyncEvent, JournalError> {
        Ok(match self {
            Self::Barrier(future) => SyncEvent::Ready(future.await?),
            Self::Install(future) => SyncEvent::Installed(future.await?),
        })
    }
}

impl super::ReplicaActor {
    pub(super) fn complete_sync_event(
        &mut self,
        event: SyncEvent,
        now: super::Duration,
    ) -> Result<(), super::ActorError> {
        match event {
            SyncEvent::Ready(ready) => {
                self.pending_sync = Some(PendingSync::Install(
                    self.journal
                        .finish_pipelined_sync(ready)
                        .map_err(|rejected| rejected.reason)?,
                ));
            }
            SyncEvent::Installed(ticket) => self.complete_normal(Completed::Sync(ticket), now)?,
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
pub(super) enum FetchPurpose {
    Recovery(NodeId, ozzy_proto::RequestId),
    Serve(NodeId),
    Install,
}

#[derive(Debug)]
pub(super) enum PendingIo {
    RetiredLookup(
        JournalCompletion<ReplicationPositions>,
        NodeId,
        ozzy_replication::wire::FetchOps,
    ),
    Retention(JournalCompletion<crate::replica_journal::RetentionTurn>),
    OrphanCleanup(JournalCompletion<crate::replica_journal::OwnedCleanedStorage>),
    MetadataCleanup(JournalCompletion<crate::replica_journal::OwnedCleanedStorage>),
    StorageValidation(JournalCompletion<crate::replica_journal::ValidatedStorage>),
    RecoveryCheckpoint(
        JournalCompletion<crate::replica_journal::RecoveryCheckpointRead>,
        NodeId,
    ),
    RecoveryPin(JournalCompletion<PinnedRecovery>),
    RecoveryRelease(JournalCompletion<PinnedRecovery>),
    Promise(JournalCompletion<PromiseTicket>),
    Capture(JournalCompletion<LogSource>),
    Release(JournalCompletion<LogSource>),
    Position(JournalCompletion<HistoryPosition>),
    Fetch(JournalCompletion<FetchedHistory>, FetchPurpose),
    Begin(JournalCompletion<InstallTicket>),
    Chunk(JournalCompletion<InstalledChunk>),
    Finish(JournalCompletion<InstalledJournal>),
    Abort(JournalCompletion<InstallTicket>),
    Activate(JournalCompletion<ActivationTicket>),
    Sync(JournalCompletion<SyncTicket>),
    Apply(JournalCompletion<ValidationTicket>),
    Turn(JournalCompletion<Box<crate::replica_journal::TurnResult>>),
    FlowPositions(
        JournalCompletion<ReplicationPositions>,
        usize,
        ozzy_replication::flow::OpenRequest,
    ),
}

#[derive(Debug)]
pub(super) enum Completed {
    RetiredLookup(
        ReplicationPositions,
        NodeId,
        ozzy_replication::wire::FetchOps,
    ),
    Retention(crate::replica_journal::RetentionTurn),
    OrphanCleanup(crate::replica_journal::OwnedCleanedStorage),
    MetadataCleanup(crate::replica_journal::OwnedCleanedStorage),
    StorageValidation(crate::replica_journal::ValidatedStorage),
    RecoveryCheckpoint(crate::replica_journal::RecoveryCheckpointRead, NodeId),
    RecoveryPin(PinnedRecovery),
    RecoveryRelease(PinnedRecovery),
    Promise(PromiseTicket),
    Capture(LogSource),
    Release(LogSource),
    Position(HistoryPosition),
    Fetch(FetchedHistory, FetchPurpose),
    Begin(InstallTicket),
    Chunk(InstalledChunk),
    Finish(InstalledJournal),
    Abort(InstallTicket),
    Activate(ActivationTicket),
    Sync(SyncTicket),
    Apply(ValidationTicket),
    Turn(Box<crate::replica_journal::TurnResult>),
    Replay(FetchedHistory, NodeId),
    FlowPositions(
        ReplicationPositions,
        usize,
        ozzy_replication::flow::OpenRequest,
    ),
}

impl PendingIo {
    pub(super) async fn wait(&mut self) -> Result<Completed, JournalError> {
        Ok(match self {
            Self::RetiredLookup(future, to, request) => {
                Completed::RetiredLookup(future.await?, *to, *request)
            }
            Self::Retention(future) => Completed::Retention(future.await?),
            Self::OrphanCleanup(future) => Completed::OrphanCleanup(future.await?),
            Self::MetadataCleanup(future) => Completed::MetadataCleanup(future.await?),
            Self::StorageValidation(future) => Completed::StorageValidation(future.await?),
            Self::RecoveryCheckpoint(future, to) => {
                Completed::RecoveryCheckpoint(future.await?, *to)
            }
            Self::RecoveryPin(future) => Completed::RecoveryPin(future.await?),
            Self::RecoveryRelease(future) => Completed::RecoveryRelease(future.await?),
            Self::Promise(future) => Completed::Promise(future.await?),
            Self::Capture(future) => Completed::Capture(future.await?),
            Self::Release(future) => Completed::Release(future.await?),
            Self::Position(future) => Completed::Position(future.await?),
            Self::Fetch(future, purpose) => Completed::Fetch(future.await?, *purpose),
            Self::Begin(future) => Completed::Begin(future.await?),
            Self::Chunk(future) => Completed::Chunk(future.await?),
            Self::Finish(future) => Completed::Finish(future.await?),
            Self::Abort(future) => Completed::Abort(future.await?),
            Self::Activate(future) => Completed::Activate(future.await?),
            Self::Sync(future) => Completed::Sync(future.await?),
            Self::Apply(future) => Completed::Apply(future.await?),
            Self::Turn(future) => Completed::Turn(future.await?),
            Self::FlowPositions(future, voter, request) => {
                Completed::FlowPositions(future.await?, *voter, *request)
            }
        })
    }
}

//! Recovery ownership used by both legacy and shard-local protocol actors.

mod local;
pub use local::ShardRecoveringJournal;

use super::{
    AppendBuffer, InstallationConfig, JournalError, JournalGeneration, PublishedRecovery,
    ReceivedChunk, Recovery, RecoveryPlan, RecoveryStartup, RecoveryTicket,
};
use crate::replica_journal::{
    JournalCompletion, JournalExecution, JournalStartup, OwnedRecoveryGenerations, Rejected,
    ReplicaJournal, SubmitError,
};
use std::{
    future::Future,
    task::{Context, Poll},
};

/// Bounded nonvoting storage placement. Only publication adoption may produce
/// a normal journal, and that journal still requires election/activation.
pub trait RecoveryStorage: std::fmt::Debug + Sized {
    /// Execution placement retained when recovery reopens for election.
    type Normal: JournalExecution;
    /// Lease a bounded transfer arena from this exact incarnation.
    fn lease_append_buffer(&self) -> Result<AppendBuffer, SubmitError>;
    /// Queue one authorized transfer attempt.
    fn begin_recovery(
        &mut self,
        ticket: RecoveryTicket,
        config: InstallationConfig,
    ) -> Result<JournalCompletion<RecoveryPlan>, SubmitError>;
    /// Queue a correlated bounded chunk, retaining its arena on backpressure.
    #[expect(
        clippy::result_large_err,
        reason = "return bounded transfer arena intact"
    )]
    fn receive_chunk(
        &mut self,
        ticket: RecoveryTicket,
        buffer: AppendBuffer,
    ) -> Result<JournalCompletion<ReceivedChunk>, Rejected<AppendBuffer>>;
    /// Validate and publish the complete selected history.
    fn finish_recovery(
        &mut self,
        ticket: RecoveryTicket,
    ) -> Result<JournalCompletion<PublishedRecovery>, SubmitError>;
    /// Remove only this attempt's unpublished staging.
    fn abort_recovery(
        &mut self,
        ticket: RecoveryTicket,
    ) -> Result<JournalCompletion<RecoveryTicket>, SubmitError>;
    /// Poll execution beside transport/timers. No blocking file work is allowed.
    fn poll_stopped(&mut self, cx: &mut Context<'_>) -> Poll<JournalError>;
    /// Close admission and drain. Canceling this wait retains execution state.
    fn shutdown(&mut self) -> impl Future<Output = Result<(), JournalError>>;
    /// Reopen a marker-backed attempt with new identities on the same backend.
    fn restart(
        self,
        full: bool,
        generations: OwnedRecoveryGenerations,
    ) -> impl Future<Output = Result<(Self, RecoveryStartup), JournalError>>;
    /// Adopt exact publication or, after newer authority, reopen intact without
    /// completing the stale recovery core. Never return same-view voting state.
    fn adopt(
        self,
        recovery: &mut Recovery,
        publication: PublishedRecovery,
        abandoned: bool,
        generation: JournalGeneration,
    ) -> impl Future<Output = Result<(ReplicaJournal<Self::Normal>, JournalStartup), JournalError>>;
}

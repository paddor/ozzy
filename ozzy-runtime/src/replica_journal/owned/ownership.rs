//! Exclusive journal ownership across consuming asynchronous transitions.

use super::{GroupJournal, JournalError, install::PendingInstallation};

#[derive(Debug)]
pub(super) enum JournalOwner {
    Ready(Box<GroupJournal>),
    Writing(Box<ozzy_journal_segment::JournalWritePipeline>),
    Rolling(Box<ozzy_journal_segment::PendingJournalRoll>),
    Installing(Box<PendingInstallation>),
    Fenced,
}

impl JournalOwner {
    pub(super) fn is_faulted(&self) -> bool {
        match self {
            Self::Ready(journal) => journal.is_faulted(),
            Self::Writing(pipeline) => pipeline.is_faulted(),
            Self::Rolling(pending) => pending.journal().is_faulted(),
            Self::Installing(_) => false,
            Self::Fenced => true,
        }
    }

    pub(super) fn readable(&self) -> Result<&GroupJournal, JournalError> {
        match self {
            Self::Ready(journal) => Ok(journal),
            Self::Writing(pipeline) => Ok(pipeline.journal()),
            Self::Rolling(pending) => Ok(pending.journal()),
            _ => Err(JournalError::InstallationMismatch),
        }
    }

    pub(super) async fn retry_snapshot(
        &mut self,
        limits: ozzy_journal_segment::IndexBuildLimits,
    ) -> Result<ozzy_journal_segment::JournalIndexSnapshot, JournalError> {
        use ozzy_journal_segment::JournalIndexBoundary::Written;
        Ok(match self {
            Self::Ready(journal) => journal.build_index_snapshot(Written, limits).await?,
            Self::Writing(pipeline) => pipeline.build_index_snapshot(Written, limits).await?,
            Self::Rolling(pending) => pending.build_index_snapshot(Written, limits).await?,
            _ => return Err(JournalError::InstallationMismatch),
        })
    }

    pub(super) fn pipeline(
        &mut self,
        capacity: usize,
    ) -> Result<&mut ozzy_journal_segment::JournalWritePipeline, JournalError> {
        if matches!(self, Self::Ready(_)) {
            *self = Self::Writing(Box::new(self.take_ready()?.begin_write_pipeline(capacity)?));
        }
        match self {
            Self::Writing(pipeline) => Ok(pipeline),
            _ => Err(JournalError::CompletionMismatch),
        }
    }

    pub(super) fn finish_writes(&mut self) -> Result<(), JournalError> {
        let Self::Writing(pipeline) = self else {
            return Ok(());
        };
        if pipeline.pending() != 0 || pipeline.sync_pending() {
            return Err(JournalError::CompletionMismatch);
        }
        let pipeline = self.take(JournalError::CompletionMismatch, |state| match state {
            Self::Writing(pipeline) => Ok(pipeline),
            state => Err(state),
        })?;
        *self = Self::Ready(Box::new(pipeline.finish()?));
        Ok(())
    }

    /// Take the state `pick` accepts, leaving `Fenced`; any other state stays.
    fn take<T>(
        &mut self,
        mismatch: JournalError,
        pick: impl FnOnce(Self) -> Result<T, Self>,
    ) -> Result<T, JournalError> {
        pick(std::mem::replace(self, Self::Fenced)).map_err(|state| {
            *self = state;
            mismatch
        })
    }

    pub(super) fn take_rolling(
        &mut self,
    ) -> Result<ozzy_journal_segment::PendingJournalRoll, JournalError> {
        match std::mem::replace(self, Self::Fenced) {
            Self::Rolling(pending) => Ok(*pending),
            _ => Err(JournalError::CompletionMismatch),
        }
    }

    pub(super) fn ready(&self) -> Result<&GroupJournal, JournalError> {
        match self {
            Self::Ready(journal) => Ok(journal),
            _ => Err(JournalError::InstallationMismatch),
        }
    }

    pub(super) fn ready_mut(&mut self) -> Result<&mut GroupJournal, JournalError> {
        match self {
            Self::Ready(journal) => Ok(journal),
            _ => Err(JournalError::InstallationMismatch),
        }
    }

    pub(super) fn take_ready(&mut self) -> Result<GroupJournal, JournalError> {
        self.take(JournalError::InstallationMismatch, |state| match state {
            Self::Ready(journal) => Ok(*journal),
            state => Err(state),
        })
    }

    pub(super) fn installing_mut(&mut self) -> Result<&mut PendingInstallation, JournalError> {
        match self {
            Self::Installing(pending) => Ok(pending),
            _ => Err(JournalError::InstallationMismatch),
        }
    }

    pub(super) fn take_installing(&mut self) -> Result<PendingInstallation, JournalError> {
        self.take(JournalError::InstallationMismatch, |state| match state {
            Self::Installing(pending) => Ok(*pending),
            state => Err(state),
        })
    }
}

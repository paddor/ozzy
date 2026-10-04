use super::{Install, Job, State};
use crate::replica_journal::{
    self as journal, JournalError, OwnedSemaphorePermit, ReadyJournalSync,
    commands::{finish, finish_read},
    completion,
    read::delivery::ReadReply,
};
use futures::FutureExt;

type Reply<T> = completion::Sender<Result<T, JournalError>>;

pub(super) fn seek(
    prepared: journal::owned::seek::PreparedSeek,
    done: Reply<journal::PartitionReadCursor>,
    permit: OwnedSemaphorePermit,
) -> Job {
    async move {
        let result = prepared.execute().await;
        Box::new(move |state: &mut State| {
            finish_read(done, state.owner.complete_seek(result), permit)
        }) as Install
    }
    .boxed_local()
}

pub(super) fn sync(
    state: &mut State,
    ticket: ozzy_replication::SyncTicket,
    done: Reply<ReadyJournalSync>,
    permit: OwnedSemaphorePermit,
) -> Result<Job, ()> {
    let captured = (|| {
        if state.sync.is_some() {
            return Err(JournalError::CompletionMismatch);
        }
        state.sequence = state
            .sequence
            .checked_add(1)
            .ok_or(JournalError::CompletionMismatch)?;
        let work = state.owner.begin_sync(ticket)?;
        let key = journal::sync::Key {
            owner: ticket.generation(),
            sequence: state.sequence,
            ticket,
        };
        state.sync = Some(key);
        state.sync_busy = true;
        Ok((key, work))
    })();
    let (key, work) = match captured {
        Ok(captured) => captured,
        Err(error) => {
            finish(done, Err(error), permit);
            return Err(());
        }
    };
    Ok(async move {
        let started_at = crate::profiling::start();
        let result = work.publish().await;
        crate::profiling::finish(crate::profiling::Stage::JournalPhysicalSync, started_at);
        Box::new(move |state: &mut State| {
            let result = if state.sync == Some(key) {
                state
                    .owner
                    .complete_sync(result)
                    .map(|_| ReadyJournalSync(key))
            } else {
                Err(JournalError::CompletionMismatch)
            };
            state.sync_busy = false;
            finish(done, result, permit)
        }) as Install
    }
    .boxed_local())
}

pub(super) fn read(
    work: journal::OwnedPreparedRead,
    done: ReadReply,
    permit: OwnedSemaphorePermit,
    read_permit: OwnedSemaphorePermit,
) -> Job {
    async move {
        match done {
            ReadReply::Copied(done) => {
                let result = work.read().await;
                Box::new(move |state: &mut State| {
                    let result = state.owner.complete_read(result);
                    drop(read_permit);
                    finish_read(done, result, permit)
                }) as Install
            }
            ReadReply::Direct(done) => {
                let result = work.read_delivery().await;
                Box::new(move |state: &mut State| {
                    let result = state
                        .owner
                        .complete_delivery(result)
                        .map(journal::OwnedPartitionDelivery::into_native);
                    drop(read_permit);
                    finish_read(done, result, permit)
                }) as Install
            }
        }
    }
    .boxed_local()
}

pub(super) fn replay(
    work: journal::OwnedPreparedReplay,
    done: Reply<journal::FetchedHistory>,
    permit: OwnedSemaphorePermit,
) -> Job {
    async move {
        let result = work.read().await;
        Box::new(move |state: &mut State| {
            finish_read(done, state.owner.complete_replay(result), permit)
        }) as Install
    }
    .boxed_local()
}

pub(super) fn donation(
    work: journal::OwnedPreparedRecoveryRead,
    done: Reply<journal::FetchedHistory>,
    permit: OwnedSemaphorePermit,
) -> Job {
    async move {
        let result = work.read().await;
        Box::new(move |state: &mut State| {
            finish_read(done, state.owner.complete_recovery_read(result), permit)
        }) as Install
    }
    .boxed_local()
}

pub(super) fn validate(
    work: journal::OwnedPreparedStorageValidation,
    done: Reply<journal::ValidatedStorage>,
    permit: OwnedSemaphorePermit,
) -> Job {
    async move {
        let result = work.validate().await;
        Box::new(move |state: &mut State| {
            finish_read(
                done,
                state.owner.complete_storage_validation(result),
                permit,
            )
        }) as Install
    }
    .boxed_local()
}

pub(super) async fn cleanup(
    state: &mut State,
    ticket: ozzy_replication::driver::ValidationTicket,
    budget: ozzy_journal_segment::MaintenanceBudget,
    metadata: bool,
) -> Result<journal::OwnedCleanedStorage, JournalError> {
    use journal::OwnedStorageCleanup as Class;
    if budget.max_entries == 0 {
        return Err(JournalError::Configuration);
    }
    let kind = if metadata {
        Class::Metadata
    } else {
        let kind = [Class::Indexes, Class::Segments, Class::Checkpoints][state.cleanup_class];
        state.cleanup_class = (state.cleanup_class + 1) % 3;
        kind
    };
    state
        .owner
        .cleanup_storage(ticket, kind, budget.max_entries)
        .await
}

pub(super) fn checkpoint(
    work: journal::owned::PreparedCheckpointRead,
    done: Reply<journal::recovery::RecoveryCheckpointRead>,
    permit: OwnedSemaphorePermit,
) -> Job {
    async move {
        let result = work.read().await;
        Box::new(move |state: &mut State| {
            finish_read(done, state.owner.complete_checkpoint_read(result), permit)
        }) as Install
    }
    .boxed_local()
}

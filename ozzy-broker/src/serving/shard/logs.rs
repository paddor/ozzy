//! Runtime logs of one shard: create and delete commands from the broker
//! handle, their storage jobs, and the logs opened again at start.

use super::install::{self, Reopened};
use super::{Binding, State, failure};
use crate::StartupError;
use crate::logs::{
    Command, CreateError, DeleteError, LogSetup, LogTable, group, registry, stops::Stops,
};
use crate::replacement::Replacement;
use futures::{StreamExt, stream::FuturesUnordered};
use ozzy_config::{LogRecord, LogState};
use ozzy_io::Local;
use ozzy_proto::GroupId;
use ozzy_replication::driver::CleanStop;
use ozzy_runtime::{frontend::RuntimeRoutes, log::Log, replica_actor::PartitionActor};
use std::{
    collections::BTreeMap,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

/// Commands one turn takes before it yields to the partitions.
const TURN_COMMANDS: usize = 16;
/// Logs one shard opens at once at start.
const OPENS: usize = 8;

/// A finished storage job, applied to the shard on its own thread.
enum Done {
    /// The lane for log stores, and the records found at start.
    Loaded(Result<Loaded, StartupError>),
    /// A ready log opened again at start.
    Opened {
        record: LogRecord,
        opened: Result<Reopened, StartupError>,
    },
    /// What a creation left behind at a crash is gone.
    Discarded {
        id: GroupId,
        discarded: Result<(), StartupError>,
    },
    Created {
        record: LogRecord,
        opened: Result<Reopened, StartupError>,
        reply: oneshot::Sender<Result<(), CreateError>>,
    },
    Deleted {
        id: GroupId,
        removed: Result<(), StartupError>,
        /// Absent for a deletion resumed at start.
        reply: Option<oneshot::Sender<Result<(), DeleteError>>>,
    },
}

/// The guarded lane, the records, and the clean stops of the last run.
type Loaded = (Local, Arc<[LogRecord]>, Arc<Stops>);

pub(super) struct RuntimeLogs {
    pub(super) setup: Arc<LogSetup>,
    /// Where this shard's device keeps log records and stores.
    directory: PathBuf,
    /// The lane for log stores. From the load on, the lock of the device's
    /// log directory guards them as a whole.
    io: Local,
    commands: mpsc::Receiver<Command>,
    /// Every log of this broker and its state, for the broker handle.
    table: LogTable,
    pub(super) routes: RuntimeRoutes,
    /// Whether this broker replaces one that lost its stores.
    replacement: Replacement,
    /// Every log of this shard, tombstones included.
    records: BTreeMap<GroupId, LogRecord>,
    jobs: FuturesUnordered<Pin<Box<dyn Future<Output = Done>>>>,
    /// Ready logs found at start that wait for their turn to open.
    unopened: Vec<LogRecord>,
    /// Start jobs in flight: the load, opens, and resumed removals.
    starting: usize,
    /// Logs opened at start that still look for their leader.
    pub(super) settling: super::waves::Settling,
    /// Logs this broker led when it last stopped cleanly.
    stops: Arc<Stops>,
}

impl RuntimeLogs {
    pub(super) fn new(
        setup: Arc<LogSetup>,
        device: &str,
        commands: mpsc::Receiver<Command>,
        config: &super::Config,
        io: &Local,
    ) -> Result<Self, StartupError> {
        let directory = setup.directory(device)?;
        let jobs = FuturesUnordered::<Pin<Box<dyn Future<Output = Done>>>>::new();
        // The shard reads its records once it runs: its partitions hold
        // device budget that only their own turns release.
        let (lane, listed, locks) = (io.clone(), directory.clone(), setup.clone());
        jobs.push(Box::pin(async move {
            Done::Loaded(load(&lane, &locks, &listed).await)
        }));
        Ok(Self {
            io: io.clone(),
            directory,
            setup,
            commands,
            table: config.logs.table.clone(),
            routes: config.logs.routes.clone(),
            replacement: config.logs.replacement.clone(),
            records: BTreeMap::new(),
            jobs,
            unopened: Vec::new(),
            starting: 1,
            settling: super::waves::Settling::new(),
            stops: Arc::default(),
        })
    }

    /// Whether every log recorded at start is open again or gone.
    pub(super) fn settled(&self) -> bool {
        self.starting == 0 && self.unopened.is_empty()
    }

    /// Move log `id` to `state`, for the broker handle and in the shard's
    /// own record of it. `log` is its handle while it serves.
    pub(super) fn mark(&mut self, id: GroupId, state: LogState, log: Option<Log>) {
        self.table.set(id, state, log);
        if let Some(record) = self.records.get_mut(&id) {
            record.state = state;
        }
    }

    /// What a storage job of this shard works with: the lane for log
    /// stores, the setup, and the directory of records and stores.
    fn job(&self) -> (Local, Arc<LogSetup>, PathBuf) {
        (self.io.clone(), self.setup.clone(), self.directory.clone())
    }

    /// Keep the clean stops of this shard's logs for the next start.
    pub(super) async fn keep_stops(
        &self,
        shard: u32,
        stops: Vec<(GroupId, CleanStop)>,
    ) -> Result<(), StartupError> {
        let known = |(id, _): &(GroupId, CleanStop)| self.records.contains_key(id);
        let kept: Vec<_> = stops.into_iter().filter(known).collect();
        if kept.is_empty() {
            return Ok(());
        }
        crate::logs::stops::publish(&self.io, &self.directory, shard, &kept).await
    }
}

/// Lock the device's log directory, then read its records.
async fn load(io: &Local, setup: &LogSetup, directory: &Path) -> Result<Loaded, StartupError> {
    crate::format::initialize_directory(io, directory).await?;
    let guarded = setup.guarded(io, directory).await?;
    let stops = setup.stops(&guarded, directory).await?;
    let records = setup.records(io, directory).await?;
    Ok((guarded, records, stops))
}

/// The store directory of a log, where its placement puts it, and where
/// it waits for removal.
fn stores(directory: &Path, record: &LogRecord) -> (PathBuf, PathBuf) {
    let label = ozzy_config::log_label(record.log);
    (
        directory.join(&label),
        directory.join(format!("{label}.trash")),
    )
}

/// Record, format, and mark ready. The journal comes back open. A broker
/// that replaces a lost one makes a replicated log's store as a nonvoting
/// copy instead: it cannot know that it never voted in the log.
async fn create(
    (io, setup, directory): &(Local, Arc<LogSetup>, PathBuf),
    replacement: &Replacement,
    record: &LogRecord,
) -> Result<Reopened, StartupError> {
    crate::format::initialize_directory(io, directory).await?;
    registry::publish(io, directory, record).await?;
    let opened = if replacement.on() && record.members.len() == 3 {
        install::replace(io, setup, record).await?
    } else {
        let generation = crate::journals::generation();
        let journal = setup.journal(record)?;
        Reopened::Serving(Box::new(journal.format(io.clone(), generation).await?))
    };
    let ready = record.with_state(LogState::Ready);
    registry::publish(io, directory, &ready).await?;
    Ok(opened)
}

/// Remove what a log left on disk and keep its record as a tombstone.
async fn erase(io: &Local, directory: &Path, record: &LogRecord) -> Result<(), StartupError> {
    let (store, trash) = stores(directory, record);
    let rename = ozzy_io::Operation::Rename {
        source: store.clone(),
        destination: trash.clone(),
    };
    match io.execute(ozzy_io::Class::Progress, rename).await {
        Ok(_) => {}
        // A crash after the rename left only the trash, or nothing.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(crate::io_error(store, error)),
    }
    registry::remove_tree(io, directory, &trash).await?;
    registry::publish(io, directory, &record.with_state(LogState::Deleted)).await
}

/// Mark deleting, drain the actor, then erase.
async fn delete(
    io: &Local,
    directory: &Path,
    record: &LogRecord,
    actor: PartitionActor,
) -> Result<(), StartupError> {
    let published = registry::publish(io, directory, record).await;
    // The actor drains whatever happened to the record.
    let drained = actor.shutdown().await.map_err(failure);
    published?;
    drained?;
    erase(io, directory, record).await
}

impl State {
    /// Sort the records found at start: ready logs wait to open, an
    /// interrupted deletion resumes, and a creation that never finished is
    /// discarded with whatever it left.
    fn recorded(
        &mut self,
        runtime: &mut RuntimeLogs,
        records: &[LogRecord],
    ) -> Result<(), StartupError> {
        for record in records {
            let id = group(record);
            if runtime.setup.shard(id)? != self.shard {
                continue;
            }
            let record = record.clone();
            runtime.setup.check(&record)?;
            runtime.table.set(id, record.state, None);
            let (io, _, directory) = runtime.job();
            match record.state {
                LogState::Creating => {
                    // It never served or voted: nothing depends on it.
                    let stale = record.clone();
                    runtime.starting += 1;
                    runtime.jobs.push(Box::pin(async move {
                        let (store, _) = stores(&directory, &stale);
                        let removed = registry::remove_tree(&io, &directory, &store).await;
                        let discarded = match removed {
                            Ok(()) => registry::discard(&io, &directory, &stale).await,
                            Err(error) => Err(error),
                        };
                        Done::Discarded { id, discarded }
                    }));
                }
                LogState::Ready => runtime.unopened.push(record.clone()),
                LogState::Deleting => {
                    let resumed = record.clone();
                    runtime.starting += 1;
                    runtime.jobs.push(Box::pin(async move {
                        let removed = erase(&io, &directory, &resumed).await;
                        Done::Deleted {
                            id,
                            removed,
                            reply: None,
                        }
                    }));
                }
                LogState::Deleted => {}
            }
            runtime.records.insert(id, record);
        }
        Ok(())
    }

    /// Open waiting logs, at most `OPENS` at once and within the window of
    /// logs that settle.
    fn open_waiting(runtime: &mut RuntimeLogs) {
        while runtime.starting < OPENS
            && runtime.settling.room(runtime.starting)
            && let Some(record) = runtime.unopened.pop()
        {
            let (io, setup, _) = runtime.job();
            let stop = runtime.stops.get(&group(&record)).copied();
            runtime.starting += 1;
            runtime.jobs.push(Box::pin(async move {
                let opened = install::reopen(&io, &setup, &record).await;
                // The evidence left the disk before this log opened.
                let opened = opened.map(|opened| match opened {
                    Reopened::Serving(opened) => {
                        Reopened::Serving(Box::new(opened.stopped_cleanly(stop)))
                    }
                    recovering @ Reopened::Recovering(_) => recovering,
                });
                Done::Opened { record, opened }
            }));
        }
    }

    /// Whether every runtime log recorded at start is open again.
    pub(super) fn logs_settled(&self) -> bool {
        self.runtime.as_ref().is_none_or(RuntimeLogs::settled)
    }

    /// Take queued commands and apply finished storage jobs.
    pub(super) fn poll_logs(
        &mut self,
        cx: &mut Context<'_>,
        binding: &Binding,
        now: Duration,
    ) -> Result<(), StartupError> {
        let Some(mut runtime) = self.runtime.take() else {
            return Ok(());
        };
        let result = self.poll_runtime(&mut runtime, cx, binding, now);
        self.runtime = Some(runtime);
        result
    }

    fn poll_runtime(
        &mut self,
        runtime: &mut RuntimeLogs,
        cx: &mut Context<'_>,
        binding: &Binding,
        now: Duration,
    ) -> Result<(), StartupError> {
        runtime.settling.observe(now, |log| self.log_ready(log));
        Self::open_waiting(runtime);
        self.poll_jobs(runtime, cx, binding, now)?;
        // Commands wait until every log recorded at start is back.
        if !runtime.settled() {
            return Ok(());
        }
        for taken in 0..=TURN_COMMANDS {
            if taken == TURN_COMMANDS {
                cx.waker().wake_by_ref();
                break;
            }
            match runtime.commands.poll_recv(cx) {
                Poll::Ready(Some(Command::Create { spec, reply })) => {
                    Self::start_create(runtime, &spec, reply);
                }
                Poll::Ready(Some(Command::Delete { id, reply })) => {
                    self.start_delete(runtime, id, reply);
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }
        // Poll the jobs these commands started, so they can wake the shard.
        self.poll_jobs(runtime, cx, binding, now)
    }

    fn poll_jobs(
        &mut self,
        runtime: &mut RuntimeLogs,
        cx: &mut Context<'_>,
        binding: &Binding,
        now: Duration,
    ) -> Result<(), StartupError> {
        while let Poll::Ready(Some(done)) = runtime.jobs.poll_next_unpin(cx) {
            self.finish(runtime, done, binding, now)?;
            Self::open_waiting(runtime);
        }
        Ok(())
    }

    fn start_create(
        runtime: &mut RuntimeLogs,
        spec: &crate::LogSpec,
        reply: oneshot::Sender<Result<(), CreateError>>,
    ) {
        let record = match runtime.setup.record(spec, uuid::Uuid::now_v7()) {
            Ok(record) => record,
            Err(error) => {
                let _ = reply.send(Err(error));
                return;
            }
        };
        let refused = match runtime.records.get(&spec.id) {
            Some(known) if !known.same_log(&record) => Some(Err(CreateError::Exists)),
            Some(known) => Some(match known.state {
                LogState::Ready => Ok(()),
                LogState::Creating => Err(CreateError::Busy),
                LogState::Deleting | LogState::Deleted => Err(CreateError::Deleted),
            }),
            None if runtime.table.live() >= runtime.setup.class.max_logs => {
                Some(Err(CreateError::Full))
            }
            None => None,
        };
        if let Some(refused) = refused {
            let _ = reply.send(refused);
            return;
        }
        runtime.records.insert(spec.id, record.clone());
        runtime.mark(spec.id, LogState::Creating, None);
        let (job, replacement) = (runtime.job(), runtime.replacement.clone());
        runtime.jobs.push(Box::pin(async move {
            let opened = Box::pin(create(&job, &replacement, &record)).await;
            Done::Created {
                record,
                opened,
                reply,
            }
        }));
    }

    /// Withdraw the routes, take the actor out of the shard, then remove
    /// the store off the turn.
    fn start_delete(
        &mut self,
        runtime: &mut RuntimeLogs,
        id: GroupId,
        reply: oneshot::Sender<Result<(), DeleteError>>,
    ) {
        let refused = match runtime.records.get(&id).map(|record| record.state) {
            None => Some(Err(DeleteError::Unknown)),
            Some(LogState::Deleted) => Some(Ok(())),
            Some(LogState::Creating | LogState::Deleting) => Some(Err(DeleteError::Busy)),
            Some(LogState::Ready) => None,
        };
        if let Some(refused) = refused {
            let _ = reply.send(refused);
            return;
        }
        runtime.routes.remove(id);
        let actor = match self.remove_log(id) {
            Ok(actor) => actor,
            Err(error) => {
                let _ = reply.send(Err(DeleteError::Failed(error.to_string())));
                return;
            }
        };
        runtime.mark(id, LogState::Deleting, None);
        let Some(record) = runtime.records.get(&id).cloned() else {
            return;
        };
        let (io, _, directory) = runtime.job();
        runtime.jobs.push(Box::pin(async move {
            let removed = delete(&io, &directory, &record, actor).await;
            Done::Deleted {
                id,
                removed,
                reply: Some(reply),
            }
        }));
    }

    /// Apply a finished job. A log recorded at start that cannot come back
    /// stops the shard: it never serves without a log it may have voted in.
    fn finish(
        &mut self,
        runtime: &mut RuntimeLogs,
        done: Done,
        binding: &Binding,
        now: Duration,
    ) -> Result<(), StartupError> {
        match done {
            Done::Loaded(loaded) => {
                runtime.starting -= 1;
                let (io, records, stops) = loaded?;
                (runtime.io, runtime.stops) = (io, stops);
                self.recorded(runtime, &records)?;
            }
            Done::Opened { record, opened } => {
                runtime.starting -= 1;
                self.install(runtime, &record, opened?, (binding, now))?;
                runtime.settling.opened(group(&record), now);
            }
            Done::Discarded { id, discarded } => {
                runtime.starting -= 1;
                discarded?;
                runtime.records.remove(&id);
                runtime.table.forget(id);
            }
            Done::Created {
                record,
                opened,
                reply,
            } => {
                let installed = opened
                    .and_then(|opened| self.install(runtime, &record, opened, (binding, now)));
                // A failed log stays recorded as creating until a restart
                // removes it, so its ID is not formatted twice while it runs.
                let _ =
                    reply.send(installed.map_err(|error| CreateError::Failed(error.to_string())));
            }
            Done::Deleted { id, removed, reply } => {
                if reply.is_none() {
                    runtime.starting -= 1;
                }
                if removed.is_ok() {
                    runtime.mark(id, LogState::Deleted, None);
                }
                match reply {
                    Some(reply) => {
                        let failed = |error: StartupError| DeleteError::Failed(error.to_string());
                        let _ = reply.send(removed.map_err(failed));
                    }
                    // A deletion resumed at start must finish.
                    None => removed?,
                }
            }
        }
        Ok(())
    }
}

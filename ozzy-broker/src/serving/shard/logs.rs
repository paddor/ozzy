//! Runtime logs of one shard: create and delete commands from the broker
//! handle, their storage jobs, and the logs opened again at start.

use super::{Binding, Bootstrap, State, failure};
use crate::logs::{Command, CreateError, DeleteError, LogSetup, LogTable, registry};
use crate::{JournalConfig, OpenedPartition, StartupError};
use futures::{StreamExt, stream::FuturesUnordered};
use ozzy_config::{LogRecord, LogState};
use ozzy_io::Local;
use ozzy_proto::GroupId;
use ozzy_replication::JournalGeneration;
use ozzy_runtime::{
    frontend::RuntimeRoutes,
    replica_actor::{ActorIds, LogLane, PartitionActor},
};
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
    /// The records found at start.
    Loaded(Result<Vec<LogRecord>, StartupError>),
    /// A ready log opened again at start.
    Opened {
        record: LogRecord,
        opened: Result<OpenedPartition, StartupError>,
    },
    /// What a creation left behind at a crash is gone.
    Discarded {
        id: GroupId,
        discarded: Result<(), StartupError>,
    },
    Created {
        record: LogRecord,
        opened: Result<OpenedPartition, StartupError>,
        reply: oneshot::Sender<Result<(), CreateError>>,
    },
    Deleted {
        id: GroupId,
        removed: Result<(), StartupError>,
        /// Absent for a deletion resumed at start.
        reply: Option<oneshot::Sender<Result<(), DeleteError>>>,
    },
}

pub(super) struct RuntimeLogs {
    setup: Arc<LogSetup>,
    /// Where this shard's device keeps log records and stores.
    directory: PathBuf,
    commands: mpsc::Receiver<Command>,
    table: LogTable,
    routes: RuntimeRoutes,
    /// Every log of this shard, tombstones included.
    records: BTreeMap<GroupId, LogRecord>,
    jobs: FuturesUnordered<Pin<Box<dyn Future<Output = Done>>>>,
    /// Ready logs found at start that wait for their turn to open.
    unopened: Vec<LogRecord>,
    /// Start jobs in flight: the load, opens, and resumed removals.
    starting: usize,
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
        let (io, listed, maximum) = (io.clone(), directory.clone(), setup.class.max_logs);
        jobs.push(Box::pin(async move {
            Done::Loaded(registry::load(&io, &listed, maximum).await)
        }));
        Ok(Self {
            directory,
            setup,
            commands,
            table: config.log_table.clone(),
            routes: config.runtime_routes.clone(),
            records: BTreeMap::new(),
            jobs,
            unopened: Vec::new(),
            starting: 1,
        })
    }

    /// Whether every log recorded at start is open again or gone.
    pub(super) fn settled(&self) -> bool {
        self.starting == 0 && self.unopened.is_empty()
    }
}

fn group(record: &LogRecord) -> GroupId {
    GroupId::from_bytes(*record.log.as_bytes())
}

/// The store directory of a log, and where it waits for removal.
fn stores(directory: &Path, record: &LogRecord) -> (PathBuf, PathBuf) {
    let label = record.log.simple().to_string();
    (
        directory.join(&label),
        directory.join(format!("{label}.trash")),
    )
}

/// Record, format, and mark ready. The journal comes back open.
async fn create(
    io: &Local,
    setup: &LogSetup,
    directory: &Path,
    record: &LogRecord,
) -> Result<OpenedPartition, StartupError> {
    crate::format::initialize_directory(io, directory).await?;
    registry::publish(io, directory, record).await?;
    let generation = JournalGeneration(uuid::Uuid::now_v7().as_u128());
    let opened = setup
        .journal(record)?
        .format(io.clone(), generation)
        .await?;
    let ready = LogRecord {
        state: LogState::Ready,
        ..record.clone()
    };
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
    let deleted = LogRecord {
        state: LogState::Deleted,
        ..record.clone()
    };
    registry::publish(io, directory, &deleted).await
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
        records: Vec<LogRecord>,
    ) -> Result<(), StartupError> {
        for record in records {
            let id = group(&record);
            if runtime.setup.shard(id)? != self.shard {
                continue;
            }
            runtime.setup.check(&record)?;
            runtime.table.set(id, record.state, None);
            let (io, directory) = (self.io.clone(), runtime.directory.clone());
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

    /// Open waiting logs, at most `OPENS` at once.
    fn open_waiting(&self, runtime: &mut RuntimeLogs) {
        while runtime.starting < OPENS
            && let Some(record) = runtime.unopened.pop()
        {
            let (io, setup) = (self.io.clone(), runtime.setup.clone());
            runtime.starting += 1;
            runtime.jobs.push(Box::pin(async move {
                let generation = JournalGeneration(uuid::Uuid::now_v7().as_u128());
                let opened = match setup.journal(&record) {
                    Ok(journal) => journal.open(io, generation).await,
                    Err(error) => Err(error),
                };
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
                    self.start_create(runtime, &spec, reply);
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
            self.open_waiting(runtime);
        }
        Ok(())
    }

    fn start_create(
        &self,
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
        runtime.table.set(spec.id, LogState::Creating, None);
        let (io, setup, directory) = (
            self.io.clone(),
            runtime.setup.clone(),
            runtime.directory.clone(),
        );
        runtime.jobs.push(Box::pin(async move {
            let opened = Box::pin(create(&io, &setup, &directory, &record)).await;
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
        runtime.table.set(id, LogState::Deleting, None);
        let Some(record) = runtime.records.get_mut(&id) else {
            return;
        };
        record.state = LogState::Deleting;
        let (io, directory, record) = (self.io.clone(), runtime.directory.clone(), record.clone());
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
            Done::Loaded(records) => {
                runtime.starting -= 1;
                self.recorded(runtime, records?)?;
            }
            Done::Opened { record, opened } => {
                runtime.starting -= 1;
                self.install_log(runtime, &record, opened?, Some((binding, now)))?;
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
                let installed = opened.and_then(|opened| {
                    self.install_log(runtime, &record, opened, Some((binding, now)))
                });
                // A failed log stays recorded as creating until a restart
                // removes it, so its ID is not formatted twice while it runs.
                let _ =
                    reply.send(installed.map_err(|error| CreateError::Failed(error.to_string())));
            }
            Done::Deleted { id, removed, reply } => {
                let Some(reply) = reply else {
                    runtime.starting -= 1;
                    removed?;
                    runtime.table.set(id, LogState::Deleted, None);
                    if let Some(record) = runtime.records.get_mut(&id) {
                        record.state = LogState::Deleted;
                    }
                    return Ok(());
                };
                if removed.is_ok() {
                    runtime.table.set(id, LogState::Deleted, None);
                    if let Some(record) = runtime.records.get_mut(&id) {
                        record.state = LogState::Deleted;
                    }
                }
                let _ = reply.send(removed.map_err(|error| DeleteError::Failed(error.to_string())));
            }
        }
        Ok(())
    }

    /// Start the actor of an open log on this shard with its log service
    /// and bootstrap, and publish its broker route and handle.
    fn install_log(
        &mut self,
        runtime: &mut RuntimeLogs,
        record: &LogRecord,
        opened: OpenedPartition,
        live: Option<(&Binding, Duration)>,
    ) -> Result<(), StartupError> {
        let id = group(record);
        let plan = runtime.setup.journal(record)?;
        let route = runtime.setup.route(record)?;
        let body_bytes = match &plan.config {
            JournalConfig::Local(config) => config.append_limits.max_body_bytes,
            JournalConfig::Replicated(config) => config.append_limits.max_body_bytes,
        };
        let limits = super::super::config::log_limits(body_bytes);
        let (log, port) = ozzy_runtime::log::log_channel(plan.incarnation, limits)
            .ok_or_else(|| failure(format!("invalid log limits for log {id}")))?;
        let mut started = opened.into_actor(
            &self.data_memory,
            &BTreeMap::new(),
            ActorIds::random(self.config.receipt_handles.clone()),
            self.config.journal_clock(),
        )?;
        started
            .actor
            .bind_receive_owner(&self.replica_memory)
            .map_err(failure)?;
        started.actor.enable_publication();
        started.prepare_partition()?;
        let lane = LogLane::new(&mut started.actor, limits).map_err(failure)?;
        self.actors
            .insert(started.actor)
            .map_err(|refused| failure(refused.error))?;
        self.actors
            .install_log(id, port, Some(lane))
            .map_err(failure)?;
        self.indices.insert(id, self.identities.len());
        self.identities.push((id, plan.incarnation));
        self.bootstrap.push(Some(Bootstrap {
            group: id,
            submitter: started.proposal,
            buffer: Some(started.buffer),
            pending: None,
            done: false,
        }));
        if let Some(route) = route {
            self.recoveries.insert(id, plan);
            if !runtime.routes.insert(route) {
                return Err(failure(format!("log {id} already has a route")));
            }
            if let Some((binding, now)) = live {
                self.sync_group(id, binding, now)?;
            }
        }
        runtime.table.set(id, LogState::Ready, Some(log));
        if let Some(known) = runtime.records.get_mut(&id) {
            known.state = LogState::Ready;
        }
        Ok(())
    }

    /// Take a log's actor and every index entry out of the shard.
    fn remove_log(&mut self, id: GroupId) -> Result<PartitionActor, StartupError> {
        let actor = self.actors.remove(id).map_err(failure)?;
        if let Some(index) = self.indices.remove(&id) {
            self.identities.remove(index);
            self.bootstrap.remove(index);
            for later in self.indices.values_mut() {
                if *later > index {
                    *later -= 1;
                }
            }
        }
        self.recoveries.remove(&id);
        self.sessions.retain(|(group, _), _| *group != id);
        self.unsynced = self.unsynced.min(self.identities.len());
        Ok(actor)
    }
}

//! A runtime log's journal, open to serve or to recover, and its actor on
//! the running shard.

use super::logs::RuntimeLogs;
use super::parts::{log_channel, prepare_serving, recovering_actor, serving_actor};
use super::{Binding, Bootstrap, State, failure};
use crate::journals::{OpenedRecovery, generation, recovery_generations};
use crate::logs::{LogSetup, group, registry};
use crate::{JournalConfig, OpenedPartition, PartitionJournal, RecoveryIntent, StartupError};
use ozzy_config::{LogRecord, LogState};
use ozzy_io::Local;
use ozzy_proto::GroupId;
use ozzy_runtime::{
    frontend::RuntimeRoute,
    log::{Log, LogPort},
    replica_actor::{LogLane, PartitionActor},
};
use std::time::Duration;

/// The journal of a recorded log.
pub(super) enum Reopened {
    /// An intact store. A replicated copy votes.
    Serving(Box<OpenedPartition>),
    /// A store that is new or unfinished. The copy stays nonvoting until
    /// it holds the log of the other two.
    Recovering(Box<OpenedRecovery>),
}

/// Open `plan` as a nonvoting copy.
async fn recover(
    io: &Local,
    plan: PartitionJournal,
    intent: RecoveryIntent,
) -> Result<Reopened, StartupError> {
    let opened = Box::pin(plan.recover(io.clone(), recovery_generations(), intent)).await?;
    Ok(Reopened::Recovering(Box::new(opened)))
}

/// Make the store of a replicated log as a nonvoting copy: this broker
/// may have voted in the log on a store it lost.
pub(super) async fn replace(
    io: &Local,
    setup: &LogSetup,
    record: &LogRecord,
) -> Result<Reopened, StartupError> {
    recover(io, setup.journal(record)?, RecoveryIntent::Replace).await
}

/// Open the store of a ready log. A replicated copy whose store is gone,
/// or was left in the middle of a recovery, comes back nonvoting. Any
/// other store that does not open fails, and so does a lost local log:
/// nothing else holds its records.
pub(super) async fn reopen(
    io: &Local,
    setup: &LogSetup,
    record: &LogRecord,
) -> Result<Reopened, StartupError> {
    let plan = setup.journal(record)?;
    let refused = match Box::pin(plan.clone().open(io.clone(), generation())).await {
        Ok(opened) => return Ok(Reopened::Serving(Box::new(opened))),
        Err(refused) => refused,
    };
    if !matches!(plan.config, JournalConfig::Replicated(_)) {
        return Err(refused);
    }
    if !registry::holds_directory(io, &plan.placement.directory).await? {
        return recover(io, plan, RecoveryIntent::Replace).await;
    }
    // Only a store with the marker of an unfinished recovery resumes.
    recover(io, plan, RecoveryIntent::Resume)
        .await
        .map_err(|_| refused)
}

/// What a log's actor needs beside its journal.
struct Fitting {
    id: GroupId,
    plan: PartitionJournal,
    route: Option<RuntimeRoute>,
    log: Log,
    port: LogPort,
}

impl State {
    /// Start the actor of an open log on this shard, and publish its
    /// broker route and handle.
    pub(super) fn install(
        &mut self,
        runtime: &mut RuntimeLogs,
        record: &LogRecord,
        opened: Reopened,
        live: (&Binding, Duration),
    ) -> Result<(), StartupError> {
        let id = group(record);
        let plan = runtime.setup.journal(record)?;
        let route = runtime.setup.route(record)?;
        let (log, port) = log_channel(&plan)?;
        let fitting = Fitting {
            id,
            plan,
            route,
            log,
            port,
        };
        match opened {
            Reopened::Serving(opened) => self.install_serving(runtime, *opened, fitting, live),
            Reopened::Recovering(opened) => {
                self.install_recovering(runtime, *opened, fitting, live)
            }
        }
    }

    /// A serving log with its log service and bootstrap.
    fn install_serving(
        &mut self,
        runtime: &mut RuntimeLogs,
        opened: OpenedPartition,
        fitting: Fitting,
        live: (&Binding, Duration),
    ) -> Result<(), StartupError> {
        let mut started = serving_actor(opened, &self.memory, &self.config)?;
        prepare_serving(&mut started, &self.memory)?;
        // Most logs idle: each keeps only what it uses.
        started.actor.shrink();
        started.buffer.shrink();
        let lane = LogLane::new(&mut started.actor, fitting.port.limits()).map_err(failure)?;
        let bootstrap = Bootstrap::new(fitting.id, started.proposal, started.buffer);
        let parts = (started.actor, Some(lane), Some(bootstrap));
        self.publish_log(runtime, parts, fitting, live)
    }

    /// A nonvoting copy. Its log answers without a lane until the copy
    /// rejoins, which gives it the services of a serving log.
    fn install_recovering(
        &mut self,
        runtime: &mut RuntimeLogs,
        opened: OpenedRecovery,
        fitting: Fitting,
        live: (&Binding, Duration),
    ) -> Result<(), StartupError> {
        let mut actor = recovering_actor(opened, &self.memory, &self.config)?;
        actor
            .bind_receive_owner(&self.memory.replica)
            .map_err(failure)?;
        self.publish_log(runtime, (actor, None, None), fitting, live)
    }

    fn publish_log(
        &mut self,
        runtime: &mut RuntimeLogs,
        (actor, lane, bootstrap): (PartitionActor, Option<LogLane>, Option<Bootstrap>),
        fitting: Fitting,
        (binding, now): (&Binding, Duration),
    ) -> Result<(), StartupError> {
        let id = fitting.id;
        self.actors
            .insert(actor)
            .map_err(|refused| failure(refused.error))?;
        self.actors
            .install_log(id, fitting.port, lane)
            .map_err(failure)?;
        self.indices.insert(id, self.identities.len());
        self.identities.push((id, fitting.plan.incarnation));
        self.bootstrap.push(bootstrap);
        if let Some(route) = fitting.route {
            self.recoveries.insert(id, fitting.plan);
            if !runtime.routes.insert(route) {
                return Err(failure(format!("log {id} already has a route")));
            }
            self.sync_group(id, binding, now)?;
        }
        runtime.mark(id, LogState::Ready, Some(fitting.log));
        Ok(())
    }

    /// Take a log's actor and every index entry out of the shard.
    pub(super) fn remove_log(&mut self, id: GroupId) -> Result<PartitionActor, StartupError> {
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
        self.synced.sessions.retain(|(group, _), _| *group != id);
        self.synced.unsynced = self.synced.unsynced.min(self.identities.len());
        Ok(actor)
    }
}

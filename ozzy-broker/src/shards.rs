//! Symmetric application runtimes. Transport and device workers remain external.

pub(crate) mod lifecycle;
mod memory;
#[cfg(test)]
mod tests;

use crate::signal::CloseSignal;
use crate::{ShardIo, StartupError};
use futures::{StreamExt, stream::FuturesUnordered};
use lifecycle::{Registration, State};
pub use memory::ShardMemory;
use ozzy_config::{BrokerPlan, PartitionPlacement, ShardPlan};
use ozzy_io::{Backend, Local, Ready};
use std::{collections::BTreeMap, future::Future, panic::AssertUnwindSafe, sync::Arc};

/// One OS thread/current-thread Tokio runtime per configured application shard.
/// Dropping requests shutdown. Worker drain does not depend on the caller runtime.
#[derive(Debug)]
pub struct ApplicationShards {
    state: Arc<State>,
}

/// Constructed on the pinned application thread. The startup factory creates
/// partition actors here, after placement, using this shared local I/O lane.
#[derive(Debug)]
pub struct ShardContext {
    /// Validated broker-local resources and partition placement.
    pub plan: ShardPlan,
    /// Broker-local partitions in the validated construction plan.
    pub partitions: Vec<PartitionPlacement>,
    /// Shard-local asynchronous submission access to the shared backend.
    pub io: Local,
    /// Shard-owned data and control allocation domains.
    pub memory: ShardMemory,
    /// Shared shutdown request and observation handle.
    pub shutdown: CloseSignal,
    ready: Option<Ready<()>>,
}

impl ShardContext {
    /// Publish successful actor initialization. Once ready, keep driving until
    /// shutdown is requested, then drain accepted journal work before returning.
    pub fn ready(&mut self) -> Result<(), StartupError> {
        self.ready
            .take()
            .ok_or_else(|| StartupError::Shard {
                shard: self.plan.id,
                reason: "shard readiness already published".into(),
            })?
            .report(())
            .then_some(())
            .ok_or_else(|| StartupError::Shard {
                shard: self.plan.id,
                reason: "shard startup observer disappeared".into(),
            })
    }
}

impl ApplicationShards {
    /// Run the same factory on every shard, including shard zero. Factories and
    /// their futures execute only on the assigned thread; futures may be !Send.
    /// Factories must observe shutdown during initialization as well as serving.
    pub async fn start<B, F, Fut>(
        plan: &BrokerPlan,
        lanes: Vec<ShardIo<B>>,
        factory: F,
    ) -> Result<Self, StartupError>
    where
        B: Backend + 'static,
        F: Fn(ShardContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), StartupError>> + 'static,
    {
        let startup = Registration::new(Arc::new(State::new(plan.shards.len())));
        Self::start_registered(plan, lanes, factory, startup, false).await
    }

    pub(crate) async fn start_registered<B, F, Fut>(
        plan: &BrokerPlan,
        lanes: Vec<ShardIo<B>>,
        factory: F,
        startup: Registration,
        follower_progress: bool,
    ) -> Result<Self, StartupError>
    where
        B: Backend + 'static,
        F: Fn(ShardContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), StartupError>> + 'static,
    {
        let mut lanes = validate_lanes(plan, lanes)?;
        let domains = memory::Domains::new(plan)?;
        let state = startup.state();
        let owner = Self {
            state: state.clone(),
        };
        let factory = Arc::new(factory);
        // Prevent early worker completion from closing the whole startup group.
        let mut readiness = FuturesUnordered::new();
        for shard in &plan.shards {
            let assignment = Assignment {
                memory: domains.plan(shard, follower_progress)?,
                lane: lanes.remove(&shard.id).ok_or_else(|| StartupError::Shard {
                    shard: shard.id,
                    reason: "missing device lane".into(),
                })?,
                partitions: plan
                    .partitions
                    .iter()
                    .filter(|part| part.shard == shard.id)
                    .cloned()
                    .collect(),
                plan: shard.clone(),
            };
            match spawn_shard(&state, &factory, assignment) {
                Ok(receiver) => {
                    readiness.push(observe_readiness(state.clone(), shard.id, receiver));
                }
                Err(error) => {
                    state.fail(error);
                    break;
                }
            }
        }
        drop(startup);
        let result = tokio::select! {
            biased;
            () = state.stop.closed() => state.result().and(Err(StartupError::Runtime("application startup stopped".into()))),
            result = async { while let Some(result) = readiness.next().await { result?; } Ok(()) } => result,
        };
        if let Err(error) = result {
            state.fail(error);
            owner.closed().await?;
        }
        state.result()?;
        Ok(owner)
    }

    /// Number of application-shard owner threads.
    pub fn thread_count(&self) -> usize {
        self.state.threads
    }

    /// Observe all worker runtimes being destroyed, including failure drain.
    pub async fn closed(&self) -> Result<(), StartupError> {
        self.state.finished.closed().await;
        self.state.result()
    }

    /// Cancellation loses only this observation, never the shutdown request.
    pub async fn shutdown(&self) -> Result<(), StartupError> {
        self.request_shutdown();
        self.closed().await
    }

    pub(crate) fn request_shutdown(&self) {
        self.state.stop.close();
    }
}

impl Drop for ApplicationShards {
    fn drop(&mut self) {
        self.state.stop.close();
    }
}

fn validate_lanes<B: Backend>(
    plan: &BrokerPlan,
    lanes: Vec<ShardIo<B>>,
) -> Result<BTreeMap<u32, ShardIo<B>>, StartupError> {
    let count = lanes.len();
    let lanes: BTreeMap<_, _> = lanes.into_iter().map(|lane| (lane.shard, lane)).collect();
    let unique: std::collections::BTreeSet<_> = plan.shards.iter().map(|shard| shard.id).collect();
    if count == 0
        || count != lanes.len()
        || count != plan.shards.len()
        || unique.len() != count
        || unique.iter().any(|id| !lanes.contains_key(id))
        || plan
            .partitions
            .iter()
            .any(|part| !unique.contains(&part.shard))
    {
        return Err(StartupError::Runtime(
            "application shards require exactly one I/O lane each".into(),
        ));
    }
    let mut assigned = std::collections::BTreeSet::new();
    let mut controllers = std::collections::BTreeSet::new();
    for controller in &plan.controllers {
        if controller.shards.is_empty() || !controllers.insert(&controller.name) {
            return Err(StartupError::Runtime(
                "empty or duplicate storage controller".into(),
            ));
        }
        for (index, id) in controller.shards.iter().enumerate() {
            if lanes.get(id).is_none_or(|lane| {
                lane.controller != controller.name
                    || lane.client.shard() != index
                    || lane.client.admission().limits().shards != controller.shards.len()
            }) || !assigned.insert(*id)
            {
                return Err(StartupError::Runtime(
                    "application I/O lane belongs to another controller".into(),
                ));
            }
        }
    }
    if assigned != unique {
        return Err(StartupError::Runtime(
            "storage controllers do not cover all shards".into(),
        ));
    }
    Ok(lanes)
}

/// Everything one application shard thread owns from startup.
struct Assignment<B: Backend> {
    plan: ShardPlan,
    partitions: Vec<PartitionPlacement>,
    lane: ShardIo<B>,
    memory: memory::Plan,
}

/// Start the owner thread of one shard. Its result fails the whole group,
/// and so does an exit before shutdown was requested.
fn spawn_shard<B, F, Fut>(
    state: &Arc<State>,
    factory: &Arc<F>,
    assignment: Assignment<B>,
) -> Result<ozzy_io::Readiness<()>, StartupError>
where
    B: Backend + 'static,
    F: Fn(ShardContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<(), StartupError>> + 'static,
{
    let registration = Registration::new(state.clone());
    let worker = state.clone();
    let id = assignment.plan.id;
    let factory = factory.clone();
    let spawned = ozzy_io::spawn_ready(format!("ozzy/shard/{id}"), move |ready| {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            run(assignment, ready, &worker.stop, &*factory)
        }))
        .unwrap_or_else(|_| {
            Err(StartupError::Shard {
                shard: id,
                reason: "application runtime panicked".into(),
            })
        });
        if let Err(error) = result {
            worker.fail(error);
        } else if !worker.stop.is_closed() {
            worker.fail(StartupError::Shard {
                shard: id,
                reason: "application runtime exited before shutdown".into(),
            });
        }
        // Runtime and every local task are gone before observers see
        // completion. Observers never need a blocking join. The
        // broker drain thread joins to report exited threads.
        drop(registration);
    });
    spawned
        .map_err(|error| StartupError::Shard {
            shard: id,
            reason: error.to_string(),
        })
        .and_then(|(thread, readiness)| state.own(thread).map(|()| readiness))
}

/// Shard `id` reported ready, or the startup error its thread recorded.
async fn observe_readiness(
    state: Arc<State>,
    id: u32,
    receiver: ozzy_io::Readiness<()>,
) -> Result<(), StartupError> {
    if receiver.await.is_none() {
        // The sender drops as the shard unwinds, before its thread
        // records the actual startup error. Wait for that report.
        state.stop.closed().await;
        return state.result().and(Err(StartupError::Shard {
            shard: id,
            reason: "application startup exited before readiness".into(),
        }));
    }
    Ok(())
}

fn run<B, F, Fut>(
    assignment: Assignment<B>,
    ready: Ready<()>,
    shutdown: &CloseSignal,
    factory: &F,
) -> Result<(), StartupError>
where
    B: Backend + 'static,
    F: Fn(ShardContext) -> Fut,
    Fut: Future<Output = Result<(), StartupError>> + 'static,
{
    let Assignment {
        plan,
        partitions,
        lane,
        memory,
    } = assignment;
    let error = |error: std::io::Error| StartupError::Shard {
        shard: plan.id,
        reason: error.to_string(),
    };
    crate::placement::pin(plan.affinity.cpu).map_err(error)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(error)?;
    let local = tokio::task::LocalSet::new();
    let context = ShardContext {
        memory: memory.allocate().map_err(error)?,
        plan,
        partitions,
        io: Local::new(lane.client),
        shutdown: shutdown.clone(),
        ready: Some(ready),
    };
    runtime.block_on(local.run_until(async { factory(context).await }))
}

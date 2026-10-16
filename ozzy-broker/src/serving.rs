//! Broker lifecycle assembled from the production shard, journal, and frontend owners.

mod config;
mod context;
pub use context::ServingContext;
mod drain;
mod outbound;
mod shard;

use crate::shards::lifecycle::startup_groups;
use crate::{
    ApplicationShards, CheckedConfig, DevicePools, Frontend, JournalPlan, RecoverySelection,
    StartupError,
};
pub(crate) use config::native_arenas;
use drain::DeviceDrain;
pub use drain::StorageOwner;
use omq_tokio::Context;
use ozzy_config::BrokerIdentity;
use ozzy_runtime::frontend::{DataSender, Links, Port};
use std::{collections::BTreeMap, sync::Arc};
use tokio::sync::{mpsc, oneshot};

/// Running broker. Dropping requests worker shutdown; explicit shutdown observes
/// journal and socket drain, followed by device completion.
#[derive(Debug)]
pub struct Broker {
    application: ApplicationShards,
    frontend: Frontend,
    devices: DeviceDrain,
    serving: Vec<tokio::sync::watch::Receiver<bool>>,
}

struct Registration {
    id: u32,
    serving: tokio::sync::watch::Receiver<bool>,
    sender: DataSender,
    broker_control: DataSender,
    data: DataSender,
    replica: DataSender,
    routes: Vec<ozzy_proto::directory::RouteState>,
    handoff: oneshot::Sender<Binding>,
}

struct Binding {
    links: Links,
    port: Port,
}

/// Storage a broker serves on: one backend submission lane per application
/// shard, and the owner that drains its devices after both owners stop.
#[derive(Debug)]
pub struct ShardStorage<B, S> {
    /// Submission lane of each application shard.
    pub lanes: Vec<crate::ShardIo<B>>,
    /// Device lifetime owner, drained once after shutdown.
    pub owner: S,
}

impl Broker {
    /// Serve a deployment in an explicitly trusted transport domain. Native
    /// client IDs are bounded admission identities, not authentication claims.
    /// Established stores are opened; this never formats missing history.
    pub async fn start_trusted(
        checked: CheckedConfig,
        local: BrokerIdentity,
    ) -> Result<Self, StartupError> {
        Self::start(checked, local, None, &[]).await
    }

    /// Reuse an owned OMQ context for actual inproc SDK/broker integration.
    /// Broker identity, dispatcher, shards, and storage remain independent.
    pub async fn start_trusted_with_context(
        checked: CheckedConfig,
        local: BrokerIdentity,
        context: Context,
    ) -> Result<Self, StartupError> {
        Self::start(checked, local, Some(context), &[]).await
    }

    /// Serve with explicitly selected nonvoting partition recovery. Selection
    /// is validated before workers or mutations. Other partitions open normally.
    pub async fn start_recovering_trusted(
        checked: CheckedConfig,
        local: BrokerIdentity,
        selections: &[RecoverySelection],
    ) -> Result<Self, StartupError> {
        Self::start(checked, local, None, selections).await
    }

    /// Explicit recovery using the same shared OMQ context as SDKs in a harness.
    pub async fn start_recovering_trusted_with_context(
        checked: CheckedConfig,
        local: BrokerIdentity,
        selections: &[RecoverySelection],
        context: Context,
    ) -> Result<Self, StartupError> {
        Self::start(checked, local, Some(context), selections).await
    }

    async fn start(
        checked: CheckedConfig,
        local: BrokerIdentity,
        context: Option<Context>,
        selections: &[RecoverySelection],
    ) -> Result<Self, StartupError> {
        let journals = Arc::new(
            JournalPlan::from_trusted_deployment(&checked, &local)?.select_recovery(selections)?,
        );
        crate::check_volumes(&checked, &local)?;
        journals.check_recovery_intents()?;
        let omq = context.unwrap_or_else(|| {
            Context::with_config_and_name(
                omq_tokio::ContextConfig {
                    io_threads: checked.plan.omq_io_threads,
                },
                "ozzy/omq",
            )
        });
        let (owner, lanes) = DevicePools::start(&checked.plan)?;
        let storage = ShardStorage { lanes, owner };
        Self::start_on_storage(checked, journals, omq.into(), storage).await
    }

    /// Serve using an externally owned file backend. The caller must provision
    /// and validate its volume bindings before this call. Partition identity and
    /// recovery preflight use the supplied backend and never format missing data.
    /// Explicit selections use the same nonvoting recovery as ordinary startup.
    /// Storage remains owned through canceled startup and shutdown observations.
    pub async fn start_trusted_with_storage<B: ozzy_io::Backend + 'static>(
        checked: CheckedConfig,
        local: BrokerIdentity,
        selections: &[RecoverySelection],
        context: impl Into<ServingContext>,
        storage: ShardStorage<B, impl StorageOwner>,
    ) -> Result<Self, StartupError> {
        let journals = Arc::new(
            JournalPlan::from_trusted_deployment(&checked, &local)?.select_recovery(selections)?,
        );
        Self::start_on_storage(checked, journals, context.into(), storage).await
    }

    async fn start_on_storage<B: ozzy_io::Backend + 'static>(
        checked: CheckedConfig,
        journals: Arc<JournalPlan>,
        context: ServingContext,
        storage: ShardStorage<B, impl StorageOwner>,
    ) -> Result<Self, StartupError> {
        let mut config = config::Config::new(&checked, context)?;
        let admin = crate::admin::Admin::bind(&checked, &config.omq, config.local, config.clients)?;
        config.admin = admin.as_ref().map(crate::admin::Admin::hooks);
        let config = Arc::new(config);
        let checked = Arc::new(checked);
        let shards = checked.plan.shards.len();
        let (application_start, frontend_start) = startup_groups(shards);
        let devices = DeviceDrain::new(
            storage.owner,
            application_start.state(),
            frontend_start.state(),
        );
        devices.start_admin(admin).await?;
        let (send, registrations) = mpsc::channel(shards);
        let preflight = Arc::new(tokio::sync::Barrier::new(shards));
        let application = ApplicationShards::start_registered(
            &checked.plan,
            storage.lanes,
            {
                let config = config.clone();
                move |shard| {
                    shard::run(
                        shard,
                        journals.clone(),
                        config.clone(),
                        send.clone(),
                        preflight.clone(),
                    )
                }
            },
            application_start,
            !config.remote.control.is_empty(),
        );
        let frontend = start_frontend(&checked, &config, registrations, frontend_start);
        // Keep both startup observations alive: neither owner publishes ready
        // until the bounded two-way shard/frontend handoff has completed.
        let error = match futures::future::join(application, frontend).await {
            (Ok(application), Ok((frontend, serving))) => {
                return Ok(Self {
                    application,
                    frontend,
                    devices,
                    serving,
                });
            }
            (Err(error), Err(_)) => error,
            (Ok(application), Err(error)) => {
                let _ = application.shutdown().await;
                error
            }
            (Err(error), Ok((frontend, _))) => {
                let _ = frontend.shutdown().await;
                error
            }
        };
        devices.0.request();
        let _ = devices.0.closed().await;
        Err(error)
    }

    /// Number of application-shard owner threads.
    pub fn application_threads(&self) -> usize {
        self.application.thread_count()
    }
    /// Number of broker-owned dispatcher threads.
    pub fn dispatcher_threads(&self) -> usize {
        self.frontend.dispatcher_threads()
    }
    /// Number of OMQ-owned transport threads.
    pub fn io_threads(&self) -> usize {
        self.frontend.io_threads()
    }

    /// Resolve once every local partition can serve clients: it finished
    /// recovery and either bootstrapped as leader or observed a remote leader.
    /// Startup returns before that point. The observation is latched; later
    /// leader changes use ordinary client retries.
    pub fn ready(
        &self,
    ) -> impl std::future::Future<Output = Result<(), StartupError>> + Send + 'static + use<> {
        let mut shards = self.serving.clone();
        async move {
            for shard in &mut shards {
                shard
                    .wait_for(|serving| *serving)
                    .await
                    .map_err(|_| failure("shard stopped before serving"))?;
            }
            Ok(())
        }
    }

    /// Report an unexpected worker exit, then drain the remaining owners.
    pub fn closed(
        &self,
    ) -> impl std::future::Future<Output = Result<(), StartupError>> + Send + 'static + use<> {
        let drain = self.devices.0.clone();
        async move {
            tokio::select! {
                () = drain.application.finished.closed() => {},
                () = drain.frontend.finished.closed() => {},
            }
            drain.request();
            drain.closed().await
        }
    }

    /// Canceling this observation retains both owners' shutdown requests.
    pub fn shutdown(
        &self,
    ) -> impl std::future::Future<Output = Result<(), StartupError>> + Send + 'static + use<> {
        let drain = self.devices.0.clone();
        drain.request();
        async move { drain.closed().await }
    }
}

/// Wait for every shard to register, then start the dispatcher. Its service
/// hands each shard its links and port before serving.
async fn start_frontend(
    checked: &CheckedConfig,
    config: &Arc<config::Config>,
    mut registrations: mpsc::Receiver<Registration>,
    startup: crate::shards::lifecycle::Registration,
) -> Result<(Frontend, Vec<tokio::sync::watch::Receiver<bool>>), StartupError> {
    let mut registered = BTreeMap::new();
    for _ in &checked.plan.shards {
        let registration: Registration = registrations
            .recv()
            .await
            .ok_or_else(|| failure("shard registration ended"))?;
        if registered.insert(registration.id, registration).is_some() {
            return Err(failure("duplicate shard registration"));
        }
    }
    let serving = registered
        .values()
        .map(|registration| registration.serving.clone())
        .collect::<Vec<_>>();
    let serving_config = config.clone();
    let factory = move |frontend: crate::FrontendContext| async move {
        let (mut service, handoffs) = serving_config.service(registered.into_values().collect())?;
        serving_config.handoff(&mut service, handoffs)?;
        frontend
            .serve(
                service,
                serving_config.remote.clone(),
                serving_config.buffers,
                std::time::Duration::from_millis(10),
            )
            .await
    };
    let plan = crate::FrontendPlan {
        broker: &checked.plan,
        local: config.local,
        endpoints: &checked.local_broker()?.endpoints,
        limits: config.transport,
    };
    Frontend::start_registered(plan, Some(config.omq.clone()), factory, startup)
        .await
        .map(|frontend| (frontend, serving))
}

fn failure(error: impl std::fmt::Display) -> StartupError {
    StartupError::Runtime(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn either_broker_owner_preserves_the_first_failure_for_both_observers() {
        for frontend_first in [false, true] {
            let (application, frontend) = startup_groups(2);
            let application = application.state();
            let frontend = frontend.state();
            let (first, later) = if frontend_first {
                (&frontend, &application)
            } else {
                (&application, &frontend)
            };
            first.fail(StartupError::Frontend("original startup failure".into()));
            later.fail(StartupError::Runtime("secondary stopped observer".into()));
            for state in [&application, &frontend] {
                assert!(matches!(
                    state.result(),
                    Err(StartupError::Frontend(reason)) if reason == "original startup failure"
                ));
            }
        }
    }

    #[test]
    fn either_broker_owner_requests_shutdown_before_the_other_can_observe_closed_queues() {
        for frontend_first in [false, true] {
            let (application, frontend) = startup_groups(2);
            let application_state = application.state();
            let frontend_state = frontend.state();
            let stopped = if frontend_first {
                &frontend_state
            } else {
                &application_state
            };
            stopped.stop.close();
            assert!(application_state.stop.is_closed());
            assert!(frontend_state.stop.is_closed());
            drop(application);
            assert!(application_state.finished.is_closed());
            assert!(!frontend_state.finished.is_closed());
            drop(frontend);
            assert!(frontend_state.finished.is_closed());
        }
    }
}

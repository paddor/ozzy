//! Broker lifecycle assembled from the production shard, journal, and frontend owners.

mod config;
mod drain;
mod outbound;
mod shard;

use crate::shards::lifecycle::{Registration as StartupRegistration, State as Lifecycle};
use crate::{
    ApplicationShards, CheckedConfig, DevicePools, Frontend, JournalPlan, RecoverySelection,
    StartupError,
};
pub(crate) use config::NATIVE_ARENAS;
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
}

struct Registration {
    id: u32,
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
                format!("ozzy_omq-{}", checked.plan.name),
            )
        });
        let (devices, lanes) = DevicePools::start(&checked.plan)?;
        Self::start_on_lanes(checked, journals, omq, lanes, devices).await
    }

    /// Serve using an externally owned file backend. The caller must provision
    /// and validate its volume bindings before this call. Partition identity and
    /// recovery preflight use the supplied backend and never format missing data.
    /// Storage remains owned through canceled startup and shutdown observations.
    pub async fn start_trusted_with_storage<B: ozzy_io::Backend + 'static>(
        checked: CheckedConfig,
        local: BrokerIdentity,
        context: Context,
        lanes: Vec<crate::ShardIo<B>>,
        storage: impl StorageOwner,
    ) -> Result<Self, StartupError> {
        let journals = Arc::new(JournalPlan::from_trusted_deployment(&checked, &local)?);
        Self::start_on_lanes(checked, journals, context, lanes, storage).await
    }

    async fn start_on_lanes<B: ozzy_io::Backend + 'static>(
        checked: CheckedConfig,
        journals: Arc<JournalPlan>,
        omq: Context,
        lanes: Vec<crate::ShardIo<B>>,
        devices: impl StorageOwner,
    ) -> Result<Self, StartupError> {
        let config = Arc::new(config::Config::new(&checked, omq.clone())?);
        let checked = Arc::new(checked);
        let application_start =
            StartupRegistration::new(Arc::new(Lifecycle::new(checked.plan.shards.len())));
        let frontend_start = StartupRegistration::new(Arc::new(Lifecycle::new(1)));
        let devices = DeviceDrain::new(devices, application_start.state(), frontend_start.state());
        let (send, mut registrations) = mpsc::channel(checked.plan.shards.len());
        let preflight = Arc::new(tokio::sync::Barrier::new(checked.plan.shards.len()));
        let application = ApplicationShards::start_registered(
            &checked.plan,
            lanes,
            {
                let journals = journals.clone();
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
            !config.brokers.is_empty(),
        );
        let frontend = async {
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
            let serving_config = config.clone();
            let factory = move |frontend: crate::FrontendContext| async move {
                let (mut service, handoffs) =
                    serving_config.service(registered.into_values().collect())?;
                serving_config.handoff(&mut service, handoffs)?;
                frontend
                    .serve(
                        service,
                        serving_config.brokers.clone(),
                        serving_config.followers.clone(),
                        serving_config.buffers,
                        std::time::Duration::from_millis(10),
                    )
                    .await
            };
            let endpoints = &checked.deployment.deployment().brokers[&checked.plan.name].endpoints;
            Frontend::start_registered(
                &checked.plan,
                config.local,
                endpoints,
                config.transport,
                Some(omq),
                factory,
                frontend_start,
            )
            .await
        };
        // Keep both startup observations alive: neither owner publishes ready
        // until the bounded two-way shard/frontend handoff has completed.
        let result = futures::future::join(application, frontend).await;
        match result {
            (Ok(application), Ok(frontend)) => Ok(Self {
                application,
                frontend,
                devices,
            }),
            (application, frontend) => {
                let error = match (application, frontend) {
                    (Err(error), Err(_)) => error,
                    (Ok(application), Err(error)) => {
                        let _ = application.shutdown().await;
                        error
                    }
                    (Err(error), Ok(frontend)) => {
                        let _ = frontend.shutdown().await;
                        error
                    }
                    (Ok(_), Ok(_)) => unreachable!(),
                };
                devices.0.request();
                let _ = devices.0.closed().await;
                Err(error)
            }
        }
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

    /// Report an unexpected worker exit, then drain the remaining owners.
    pub fn closed(
        &self,
    ) -> impl std::future::Future<Output = Result<(), StartupError>> + Send + 'static + use<> {
        let drain = self.devices.0.clone();
        async move {
            tokio::select! {
                () = drain.application.finished.requested() => {},
                () = drain.frontend.finished.requested() => {},
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

fn failure(error: impl std::fmt::Display) -> StartupError {
    StartupError::Runtime(error.to_string())
}

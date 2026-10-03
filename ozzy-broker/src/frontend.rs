//! One owned dispatcher runtime and one shared set of broker sockets.

use std::{future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use bytes::Bytes;
use futures::FutureExt;
use omq_tokio::{Context, ContextConfig, Endpoint, IdentitySocket, Socket, SocketType};
use ozzy_config::{BrokerPlan, Endpoints};
use ozzy_proto::NodeId;
use tokio::sync::oneshot;

use crate::shards::lifecycle::{Registration, State};
use crate::{Shutdown, StartupError};

mod followers;
mod serving;
pub use followers::FollowerRoutes;

/// Directional OMQ queue/frame bounds. These transport limits do not replace
/// owner-local accounting for retained backing allocations.
#[derive(Clone, Copy, Debug)]
pub struct TransportLimits {
    /// Maximum queued messages per outbound OMQ connection.
    pub send_messages: u32,
    /// Maximum queued messages per inbound OMQ connection.
    pub receive_messages: u32,
    /// Maximum accepted native message bytes.
    pub message_bytes: usize,
    /// Positive finite drain interval. OMQ zero-linger close detaches its socket
    /// task; a positive interval also waits for endpoint destruction.
    pub close_linger: Duration,
}

/// Owned dispatcher thread. OMQ owns its separate transport workers. Drop only
/// requests shutdown; thread and socket destruction happen on the worker.
#[derive(Debug)]
pub struct Frontend {
    state: Arc<State>,
    io_threads: usize,
}

/// Constructed on the dispatcher after affinity and all binds succeed. The
/// factory owns bounded session/routing work here, never partition actors.
#[derive(Debug)]
pub struct FrontendContext {
    /// Persistent local broker routing identity.
    pub local: NodeId,
    /// Shared addressed control socket for SDKs and broker peers.
    pub peer: IdentitySocket,
    /// Shared addressed bulk socket for APPENDs and replay.
    pub data: IdentitySocket,
    /// PUB socket for confirmed live consumer records.
    pub reader_pub: Socket,
    /// Optional PUB socket for live canonical follower replication.
    pub follower_pub: Option<Socket>,
    /// Shared shutdown request and observation handle.
    pub shutdown: Shutdown,
    context: Context,
    monitor: omq_tokio::MonitorStream,
    data_monitor: omq_tokio::MonitorStream,
    endpoint: Endpoint,
    limits: TransportLimits,
    ready: Option<oneshot::Sender<()>>,
}

impl FrontendContext {
    /// Existing broker-owned OMQ context for local shard command sockets.
    pub fn omq_context(&self) -> &Context {
        &self.context
    }

    /// Publish readiness after route/session queues are installed. The factory
    /// must observe shutdown during startup and while serving. It may use !Send
    /// futures on this current-thread runtime and `LocalSet`.
    pub fn ready(&mut self) -> Result<(), StartupError> {
        self.ready
            .take()
            .ok_or_else(|| error("frontend readiness already published"))?
            .send(())
            .map_err(|()| error("frontend startup observer disappeared"))
    }
}

impl Frontend {
    /// Start the sole dispatcher and bind each configured endpoint exactly once.
    /// The same lifecycle supports inproc harnesses and deployment transports.
    /// Factories build their local routing state after dispatcher CPU placement.
    pub async fn start<F, Fut>(
        plan: &BrokerPlan,
        local: NodeId,
        endpoints: &Endpoints,
        limits: TransportLimits,
        factory: F,
    ) -> Result<Self, StartupError>
    where
        F: FnOnce(FrontendContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), StartupError>> + 'static,
    {
        let startup = Registration::new(Arc::new(State::new(1)));
        Self::start_registered(plan, local, endpoints, limits, None, factory, startup).await
    }

    /// Inject an owned-I/O context for inproc integration harnesses. Clones share
    /// OMQ workers and its inproc namespace, never dispatcher or partition state.
    /// The harness must create it before applying dispatcher affinity. Contexts
    /// borrowing an application runtime are rejected. Reported I/O thread count
    /// belongs to the shared context, not to each broker using it.
    pub async fn start_with_context<F, Fut>(
        plan: &BrokerPlan,
        local: NodeId,
        endpoints: &Endpoints,
        limits: TransportLimits,
        context: Context,
        factory: F,
    ) -> Result<Self, StartupError>
    where
        F: FnOnce(FrontendContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), StartupError>> + 'static,
    {
        let startup = Registration::new(Arc::new(State::new(1)));
        Self::start_registered(
            plan,
            local,
            endpoints,
            limits,
            Some(context),
            factory,
            startup,
        )
        .await
    }

    pub(crate) async fn start_registered<F, Fut>(
        plan: &BrokerPlan,
        local: NodeId,
        endpoints: &Endpoints,
        limits: TransportLimits,
        context: Option<Context>,
        factory: F,
        startup: Registration,
    ) -> Result<Self, StartupError>
    where
        F: FnOnce(FrontendContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), StartupError>> + 'static,
    {
        validate(plan, local, limits)?;
        let bindings = Bindings::parse(endpoints)?;
        // Create owned OMQ workers before applying dispatcher affinity.
        let context = context.unwrap_or_else(|| {
            Context::with_config_and_name(
                ContextConfig {
                    io_threads: plan.omq_io_threads,
                },
                format!("ozzy_omq-{local}"),
            )
        });
        if context.io_threads() != plan.omq_io_threads {
            return Err(error("OMQ context worker count differs from broker plan"));
        }
        let state = startup.state();
        let owner = Self {
            state: state.clone(),
            io_threads: plan.omq_io_threads,
        };
        let worker = state.clone();
        let registration = Registration::new(state.clone());
        let (ready, readiness) = oneshot::channel();
        let cpu = plan.dispatcher.cpu;
        let thread = std::thread::Builder::new()
            .name("ozzy_dispatch".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    crate::placement::pin(cpu).map_err(|failure| error(failure.to_string()))?;
                    let runtime = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .map_err(|failure| error(failure.to_string()))?;
                    tokio::task::LocalSet::new().block_on(&runtime, async {
                        let endpoint = bindings.peer.clone();
                        let mut sockets = Sockets::bind(&context, local, bindings, limits).await?;
                        let serving = AssertUnwindSafe(async {
                            factory(FrontendContext {
                                local,
                                context: context.clone(),
                                peer: sockets.peer.clone(),
                                data: sockets.data.clone(),
                                reader_pub: sockets.reader.clone(),
                                follower_pub: sockets.follower.clone(),
                                shutdown: worker.stop.clone(),
                                monitor: sockets.monitor.take().expect("one frontend monitor"),
                                data_monitor: sockets
                                    .data_monitor
                                    .take()
                                    .expect("one data monitor"),
                                endpoint,
                                limits,
                                ready: Some(ready),
                            })
                            .await
                        })
                        .catch_unwind();
                        let result = serving
                            .await
                            .unwrap_or_else(|_| Err(error("dispatcher task panicked")));
                        let closed = sockets.close().await;
                        result.and(closed)
                    })
                }))
                .unwrap_or_else(|_| Err(error("dispatcher runtime panicked")));
                if let Err(failure) = result {
                    worker.fail(failure);
                } else if !worker.stop.is_requested() {
                    worker.fail(error("dispatcher exited before shutdown"));
                }
                // Dropping the owned OMQ context can join its workers. Do that here,
                // after sockets close, before publishing terminal completion.
                drop(context);
                drop(registration);
            })
            .map_err(|failure| error(failure.to_string()))?;
        state.own(thread);
        drop(startup);
        tokio::select! {
            biased;
            () = state.stop.requested() => {
                owner.closed().await?;
                return Err(error("frontend startup stopped"));
            }
            result = readiness => {
                if result.is_err() {
                    state.stop.request();
                    owner.closed().await?;
                    return Err(error("dispatcher exited before readiness"));
                }
            }
        }
        state.result()?;
        Ok(owner)
    }

    /// Number of broker-owned dispatcher threads.
    pub fn dispatcher_threads(&self) -> usize {
        self.state.threads
    }
    /// Number of OMQ-owned transport threads.
    pub fn io_threads(&self) -> usize {
        self.io_threads
    }

    /// Observe socket closure and worker destruction without blocking a caller.
    pub async fn closed(&self) -> Result<(), StartupError> {
        self.state.finished.requested().await;
        self.state.result()
    }

    /// Cancellation abandons only this observation; shutdown remains requested.
    pub async fn shutdown(&self) -> Result<(), StartupError> {
        self.request_shutdown();
        self.closed().await
    }

    pub(crate) fn request_shutdown(&self) {
        self.state.stop.request();
    }
}

impl Drop for Frontend {
    fn drop(&mut self) {
        self.state.stop.request();
    }
}

struct Bindings {
    peer: Endpoint,
    data: Endpoint,
    reader: Endpoint,
    follower: Option<Endpoint>,
}

impl Bindings {
    fn parse(endpoints: &Endpoints) -> Result<Self, StartupError> {
        if endpoints.data_peer == endpoints.peer
            || endpoints.data_peer == endpoints.reader_pub
            || endpoints.follower_pub.as_ref() == Some(&endpoints.data_peer)
            || endpoints.peer == endpoints.reader_pub
            || endpoints.follower_pub.as_ref().is_some_and(|endpoint| {
                endpoint == &endpoints.peer || endpoint == &endpoints.reader_pub
            })
        {
            return Err(error("frontend endpoints must be distinct"));
        }
        let parse = |endpoint: &str| {
            endpoint
                .parse()
                .map_err(|failure: omq_tokio::Error| error(failure.to_string()))
        };
        Ok(Self {
            peer: parse(&endpoints.peer)?,
            data: parse(&endpoints.data_peer)?,
            reader: parse(&endpoints.reader_pub)?,
            follower: endpoints.follower_pub.as_deref().map(parse).transpose()?,
        })
    }
}

struct Sockets {
    peer: IdentitySocket,
    data: IdentitySocket,
    reader: Socket,
    follower: Option<Socket>,
    monitor: Option<omq_tokio::MonitorStream>,
    data_monitor: Option<omq_tokio::MonitorStream>,
}

impl Sockets {
    async fn bind(
        context: &Context,
        local: NodeId,
        bindings: Bindings,
        limits: TransportLimits,
    ) -> Result<Self, StartupError> {
        let options = ozzy_runtime::transport::socket_options()
            .identity(Bytes::copy_from_slice(local.as_bytes()))
            .router_mandatory(true)
            .send_hwm(limits.send_messages)
            .recv_hwm(limits.receive_messages)
            .max_message_size(limits.message_bytes)
            .linger(limits.close_linger);
        let peer = context
            .socket(SocketType::Peer, options.clone())
            .identity_routing()
            .map_err(|reason| error(reason.to_string()))?;
        let monitor = Some(peer.monitor());
        let data = context
            .socket(SocketType::Peer, options.clone())
            .identity_routing()
            .map_err(|reason| error(reason.to_string()))?;
        let data_monitor = Some(data.monitor());
        let sockets = Self {
            data,
            data_monitor,
            peer,
            monitor,
            reader: context.socket(SocketType::Pub, options.clone()),
            follower: bindings
                .follower
                .as_ref()
                .map(|_| context.socket(SocketType::Pub, options)),
        };
        let result = async {
            sockets.peer.bind(bindings.peer).await?;
            sockets.data.bind(bindings.data).await?;
            sockets.reader.bind(bindings.reader).await?;
            if let (Some(socket), Some(endpoint)) = (&sockets.follower, bindings.follower) {
                socket.bind(endpoint).await?;
            }
            Ok::<(), omq_tokio::Error>(())
        }
        .await;
        if let Err(failure) = result {
            let _ = sockets.close().await;
            return Err(error(failure.to_string()));
        }
        Ok(sockets)
    }

    async fn close(self) -> Result<(), StartupError> {
        let peer = self.peer.into_inner().close().await;
        let data = self.data.into_inner().close().await;
        let reader = self.reader.close().await;
        let follower = match self.follower {
            Some(socket) => socket.close().await,
            None => Ok(()),
        };
        peer.and(data)
            .and(reader)
            .and(follower)
            .map_err(|failure| error(failure.to_string()))
    }
}

fn error(reason: impl Into<String>) -> StartupError {
    StartupError::Frontend(reason.into())
}

fn validate(plan: &BrokerPlan, local: NodeId, limits: TransportLimits) -> Result<(), StartupError> {
    if !(1..=256).contains(&plan.omq_io_threads)
        || local.as_bytes() == &[0; 16]
        || limits.send_messages == 0
        || limits.receive_messages == 0
        || limits.message_bytes < 1024
        || limits.close_linger.is_zero()
    {
        return Err(error("invalid frontend transport limits or identity"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

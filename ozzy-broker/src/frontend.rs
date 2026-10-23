//! One owned dispatcher runtime and one shared set of broker sockets.

use std::{future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration};

#[cfg(test)]
use bytes::Bytes;
use futures::FutureExt;
use omq_tokio::{Context, ContextConfig, Endpoint, IdentitySocket, Socket, SocketType};
use ozzy_config::{BrokerPlan, Endpoints};
use ozzy_proto::NodeId;

use crate::StartupError;
use crate::shards::lifecycle::{Registration, State};
use crate::signal::CloseSignal;

mod followers;
mod serving;
pub use followers::FollowerRoutes;
pub use serving::RemoteBrokers;

/// Directional OMQ queue/frame bounds. These transport limits do not replace
/// owner-local accounting for retained backing allocations.
#[derive(Clone, Copy, Debug)]
pub struct TransportLimits {
    /// Maximum queued messages per outbound PEER connection and per sending
    /// handle. OMQ caps the queued bytes of each connection at 16 MiB or
    /// `message_bytes`, whichever is larger.
    pub send_messages: u32,
    /// Maximum queued and retained messages per inbound PEER connection,
    /// under the same byte cap.
    pub receive_messages: u32,
    /// Maximum connections sending into one PEER socket. OMQ counts a
    /// replaced connection until its queue drains and refuses new ones while
    /// full.
    pub peers: usize,
    /// Maximum queued messages per PUB subscriber or SUB source. SUB has no
    /// byte cap, so this count bounds its memory.
    pub publication_messages: u32,
    /// Maximum accepted native message bytes.
    pub message_bytes: usize,
    /// Positive finite drain interval. OMQ zero-linger close detaches its socket
    /// task; a positive interval also waits for endpoint destruction.
    pub close_linger: Duration,
}

impl TransportLimits {
    /// PEER options with these queue, frame, and close bounds.
    fn peer(self) -> omq_tokio::Options {
        ozzy_runtime::transport::socket_options()
            .router_mandatory(true)
            .send_hwm(self.send_messages)
            .peer_send_queue_capacity(self.send_messages as usize)
            .recv_hwm(self.receive_messages)
            .peer_recv_max_messages(self.receive_messages as usize)
            .peer_max_peers(self.peers)
            .max_message_size(self.message_bytes)
            .linger(self.close_linger)
    }

    /// PEER `options` with the PUB/SUB queue bound.
    fn publications(self, options: &omq_tokio::Options) -> omq_tokio::Options {
        options
            .clone()
            .send_hwm(self.publication_messages)
            .recv_hwm(self.publication_messages)
    }
}

/// Install limits before any broker socket exists. An active shared context
/// must already match, so one embedded broker cannot alter another's budget.
pub(crate) fn configure_network(context: &Context, plan: &BrokerPlan) -> Result<(), StartupError> {
    let limit = |value: ozzy_config::ByteRateLimit| {
        omq_tokio::ByteRateLimit::new(value.bytes_per_second, value.burst_bytes)
    };
    let limits = omq_tokio::NetworkRateLimits {
        ingress: plan.omq_ingress.map(limit),
        egress: plan.omq_egress.map(limit),
    };
    if context.network_rate_limits() != limits {
        context
            .set_network_rate_limits(limits)
            .map_err(|reason| error(format!("OMQ network limits: {reason}")))?;
    }
    Ok(())
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
    pub shutdown: CloseSignal,
    context: Context,
    endpoint: Endpoint,
    limits: TransportLimits,
    ready: Option<ozzy_io::Ready<()>>,
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
            .report(())
            .then_some(())
            .ok_or_else(|| error("frontend startup observer disappeared"))
    }
}

/// What one frontend binds and how it bounds its transport.
#[derive(Clone, Copy, Debug)]
pub struct FrontendPlan<'a> {
    /// Dispatcher placement and OMQ worker count.
    pub broker: &'a BrokerPlan,
    /// Persistent local broker routing identity.
    pub local: NodeId,
    /// Endpoints to bind, each exactly once.
    pub endpoints: &'a Endpoints,
    /// Directional OMQ queue and frame bounds.
    pub limits: TransportLimits,
    /// Who may subscribe to each publication socket.
    pub admission: &'a PublicationAdmission,
}

/// Who may subscribe to this broker's publications. A subscriber is its SUB's
/// routing identity, a claim the transport does not authenticate.
#[derive(Clone, Debug, Default)]
pub struct PublicationAdmission {
    /// Clients holding a subscription to a partition admit its live topic.
    pub readers: ozzy_runtime::frontend::ReaderGrants,
    /// Brokers that may follow this one's canonical stream.
    pub followers: std::collections::BTreeSet<NodeId>,
}

impl Frontend {
    /// Start the sole dispatcher and bind each configured endpoint exactly once.
    /// The same lifecycle supports inproc harnesses and deployment transports.
    /// Factories build their local routing state after dispatcher CPU placement.
    pub async fn start<F, Fut>(plan: FrontendPlan<'_>, factory: F) -> Result<Self, StartupError>
    where
        F: FnOnce(FrontendContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), StartupError>> + 'static,
    {
        let startup = Registration::new(Arc::new(State::new(1)));
        Self::start_registered(plan, None, factory, startup).await
    }

    /// Inject an owned-I/O context for inproc integration harnesses. Clones share
    /// OMQ workers and its inproc namespace, never dispatcher or partition state.
    /// The harness must create it before applying dispatcher affinity. Contexts
    /// borrowing an application runtime are rejected. Reported I/O thread count
    /// belongs to the shared context, not to each broker using it.
    /// Network limits share that context's allowance and must already match
    /// the broker plan if any socket was created on it.
    pub async fn start_with_context<F, Fut>(
        plan: FrontendPlan<'_>,
        context: Context,
        factory: F,
    ) -> Result<Self, StartupError>
    where
        F: FnOnce(FrontendContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), StartupError>> + 'static,
    {
        let startup = Registration::new(Arc::new(State::new(1)));
        Self::start_registered(plan, Some(context), factory, startup).await
    }

    pub(crate) async fn start_registered<F, Fut>(
        plan: FrontendPlan<'_>,
        context: Option<Context>,
        factory: F,
        startup: Registration,
    ) -> Result<Self, StartupError>
    where
        F: FnOnce(FrontendContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), StartupError>> + 'static,
    {
        validate(plan)?;
        let bindings = Bindings::parse(plan)?;
        let io_threads = plan.broker.omq_io_threads;
        // Create owned OMQ workers before applying dispatcher affinity.
        let context = context.unwrap_or_else(|| {
            Context::with_config_and_name(ContextConfig { io_threads }, "ozzy/omq")
        });
        if context.io_threads() != io_threads {
            return Err(error("OMQ context worker count differs from broker plan"));
        }
        configure_network(&context, plan.broker)?;
        let state = startup.state();
        let owner = Self {
            state: state.clone(),
            io_threads,
        };
        let cpu = plan.broker.dispatcher.cpu;
        let readiness = spawn_dispatcher(&state, cpu, context, bindings, factory)?;
        drop(startup);
        owner.started(readiness).await
    }

    /// Wait for dispatcher readiness. A stop request or a dispatcher exit
    /// first waits for the dispatcher to finish and reports startup failure.
    async fn started(self, readiness: ozzy_io::Readiness<()>) -> Result<Self, StartupError> {
        tokio::select! {
            biased;
            () = self.state.stop.closed() => {
                self.closed().await?;
                return Err(error("frontend startup stopped"));
            }
            result = readiness => {
                if result.is_none() {
                    // The ready sender drops before its worker publishes the
                    // failure. Do not replace that cause with a stop request.
                    self.state.stop.closed().await;
                    self.closed().await?;
                    return Err(error("dispatcher exited before readiness"));
                }
            }
        }
        self.state.result()?;
        Ok(self)
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
        self.state.finished.closed().await;
        self.state.result()
    }

    /// Cancellation abandons only this observation; shutdown remains requested.
    pub async fn shutdown(&self) -> Result<(), StartupError> {
        self.request_shutdown();
        self.closed().await
    }

    pub(crate) fn request_shutdown(&self) {
        self.state.stop.close();
    }
}

impl Drop for Frontend {
    fn drop(&mut self) {
        self.state.stop.close();
    }
}

/// Parsed endpoints with the identity and limits of the sockets bound to them.
struct Bindings {
    local: NodeId,
    limits: TransportLimits,
    admission: PublicationAdmission,
    peer: Endpoint,
    data: Endpoint,
    reader: Endpoint,
    follower: Option<Endpoint>,
}

impl Bindings {
    /// Parse the distinct endpoints a validated plan binds.
    fn parse(plan: FrontendPlan<'_>) -> Result<Self, StartupError> {
        let endpoints = plan.endpoints;
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
            local: plan.local,
            limits: plan.limits,
            admission: plan.admission.clone(),
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
}

impl Sockets {
    async fn bind(context: &Context, bindings: Bindings) -> Result<Self, StartupError> {
        let limits = bindings.limits;
        let options = limits
            .peer()
            .identity(ozzy_runtime::transport::peer_identity(bindings.local));
        let publications = limits.publications(&options);
        let peer = context
            .socket(SocketType::Peer, options.clone())
            .identity_routing()
            .map_err(|reason| error(reason.to_string()))?;
        let data = context
            .socket(SocketType::Peer, options.clone())
            .identity_routing()
            .map_err(|reason| error(reason.to_string()))?;
        let sockets = Self {
            data,
            peer,
            reader: context.socket(
                SocketType::Pub,
                (publications.clone())
                    .subscription_admission(bindings.admission.readers.admission()),
            ),
            follower: bindings.follower.as_ref().map(|_| {
                let followers = bindings.admission.followers.clone();
                // A follower subscribes to every group it may host.
                let admission = move |request: &omq_tokio::SubscriptionRequest<'_>| {
                    (request.peer.identity.as_deref())
                        .and_then(ozzy_runtime::transport::decode_peer_identity)
                        .is_some_and(|node| followers.contains(&node))
                };
                context.socket(
                    SocketType::Pub,
                    publications.subscription_admission(admission),
                )
            }),
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

/// Spawn the dispatcher thread: pin it, run the factory on a current-thread
/// runtime, and publish its exit cause before its OMQ context joins workers.
fn spawn_dispatcher<F, Fut>(
    state: &Arc<State>,
    cpu: Option<u32>,
    context: Context,
    bindings: Bindings,
    factory: F,
) -> Result<ozzy_io::Readiness<()>, StartupError>
where
    F: FnOnce(FrontendContext) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), StartupError>> + 'static,
{
    let worker = state.clone();
    let registration = Registration::new(state.clone());
    let (thread, readiness) = ozzy_io::spawn_ready("ozzy/dispatch".into(), move |ready| {
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            crate::placement::pin(cpu).map_err(|failure| error(failure.to_string()))?;
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|failure| error(failure.to_string()))?;
            tokio::task::LocalSet::new().block_on(
                &runtime,
                run_dispatcher(&context, bindings, factory, ready, &worker),
            )
        }))
        .unwrap_or_else(|_| Err(error("dispatcher runtime panicked")));
        if let Err(failure) = result {
            worker.fail(failure);
        } else if !worker.stop.is_closed() {
            worker.fail(error("dispatcher exited before shutdown"));
        }
        // Dropping the owned OMQ context can join its workers. Do that here,
        // after sockets close, before publishing terminal completion.
        drop(context);
        drop(registration);
    })
    .map_err(|failure| error(failure.to_string()))?;
    state.own(thread)?;
    Ok(readiness)
}

/// Bind the sockets and serve the factory on them. Publish its outcome and
/// stop both owners before the sockets close.
async fn run_dispatcher<F, Fut>(
    context: &Context,
    bindings: Bindings,
    factory: F,
    ready: ozzy_io::Ready<()>,
    worker: &State,
) -> Result<(), StartupError>
where
    F: FnOnce(FrontendContext) -> Fut,
    Fut: Future<Output = Result<(), StartupError>>,
{
    let local = bindings.local;
    let limits = bindings.limits;
    let endpoint = bindings.peer.clone();
    let sockets = Sockets::bind(context, bindings).await?;
    let serving = AssertUnwindSafe(async {
        factory(FrontendContext {
            local,
            context: context.clone(),
            peer: sockets.peer.clone(),
            data: sockets.data.clone(),
            reader_pub: sockets.reader.clone(),
            follower_pub: sockets.follower.clone(),
            shutdown: worker.stop.clone(),
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
    // Publish the cause and stop both owners before dropping
    // their serving queues or closing transport sockets.
    match result {
        Err(failure) => worker.fail(failure),
        Ok(()) if !worker.stop.is_closed() => {
            worker.fail(error("dispatcher exited before shutdown"));
        }
        Ok(()) => {}
    }
    let closed = sockets.close().await;
    worker.result().and(closed)
}

fn error(reason: impl Into<String>) -> StartupError {
    StartupError::Frontend(reason.into())
}

fn validate(plan: FrontendPlan<'_>) -> Result<(), StartupError> {
    let limits = plan.limits;
    if !(1..=256).contains(&plan.broker.omq_io_threads)
        || plan.local.as_bytes() == &[0; 16]
        || limits.send_messages == 0
        || limits.receive_messages == 0
        || limits.peers == 0
        || limits.publication_messages == 0
        || limits.message_bytes < 1024
        || limits.close_linger.is_zero()
    {
        return Err(error("invalid frontend transport limits or identity"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

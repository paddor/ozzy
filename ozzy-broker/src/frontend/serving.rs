//! Ordinary receive and transport lifecycle. Partition work stays on shards.

use omq_tokio::Endpoint;
use ozzy_proto::NodeId;
use ozzy_runtime::frontend::{
    DataPressure, Kind, ReceiveBuffers, ReceiveError, Rejection, Service,
};
use std::{collections::BTreeMap, time::Duration};

use super::{FollowerRoutes, FrontendContext, StartupError, error, followers::Followers};

mod ingress;
mod publishers;
use ingress::Ingress;
use publishers::Publishers;

/// Remote brokers a frontend serves: control endpoints and follower routes.
#[derive(Clone, Debug, Default)]
pub struct RemoteBrokers {
    /// Control PEER endpoint of each remote broker.
    pub control: BTreeMap<NodeId, Endpoint>,
    /// Follower publication and repair routes between the brokers.
    pub followers: FollowerRoutes,
}

/// What the serving loop reads but never changes.
#[derive(Clone, Copy)]
struct Serving<'a> {
    /// Remote broker control endpoints; each tick retries their handshakes.
    brokers: &'a BTreeMap<NodeId, Endpoint>,
    /// Follower publication and repair transport.
    followers: &'a Followers,
    /// Receive backing and bounds of all inbound messages.
    buffers: &'a ReceiveBuffers,
    /// Handshake retry interval.
    retry: Duration,
}

impl FrontendContext {
    /// Serve an already assembled bounded frontend on its owned dispatcher.
    /// Broker control uses one connection per pair; repair uses one per remote
    /// destination shard. Readiness
    /// does not wait for every broker. The service owns no partition authority.
    /// Input rejection drops this attempt; native record retry identity remains
    /// with the sender. Accounting failures stop the frontend; lost monitor
    /// events resynchronize from the socket's connection table.
    pub async fn serve(
        self,
        mut service: Service,
        brokers: RemoteBrokers,
        buffers: ReceiveBuffers,
        retry: Duration,
    ) -> Result<(), StartupError> {
        self.check_serving(&service, &brokers, &buffers, retry)?;
        for (&peer, endpoint) in &brokers.control {
            if self.local < peer {
                tokio::select! {
                    () = self.shutdown.closed() => return Ok(()),
                    result = self.peer.connect(endpoint.clone()) => result.map_err(failure)?,
                }
            }
            service.start(peer).map_err(failure)?;
        }
        let followers = Followers::open(&self, brokers.followers).await?;
        let serving = Serving {
            brokers: &brokers.control,
            followers: &followers,
            buffers: &buffers,
            retry,
        };
        // Dispatch consumes this context before followers close. The dispatcher
        // thread still owns the bound sockets and OMQ context until we return.
        let result = self.dispatch(&mut service, serving).await;
        let closed = followers.close().await;
        result.and(closed.map_err(failure))
    }

    /// Check identity, retry interval, transport sizes, broker authorization,
    /// and follower routes before connecting anywhere.
    fn check_serving(
        &self,
        service: &Service,
        brokers: &RemoteBrokers,
        buffers: &ReceiveBuffers,
        retry: Duration,
    ) -> Result<(), StartupError> {
        if service.local() != self.local || retry.is_zero() {
            return Err(error(
                "invalid serving identity or handshake retry interval",
            ));
        }
        buffers
            .check_transport(&self.endpoint, self.limits.message_bytes)
            .map_err(failure)?;
        for (&peer, endpoint) in &brokers.control {
            buffers
                .check_transport(endpoint, self.limits.message_bytes)
                .map_err(failure)?;
            if service
                .access(peer)
                .is_none_or(|access| access.kind != Kind::Broker)
            {
                return Err(error("outbound broker lacks independent authorization"));
            }
        }
        check_followers(self, brokers, buffers)
    }

    /// Publish readiness, then serve until shutdown: admit PEER input on both
    /// lanes, forward publications, progress replies, and retry handshakes.
    async fn dispatch(
        mut self,
        service: &mut Service,
        serving: Serving<'_>,
    ) -> Result<(), StartupError> {
        let mut publications = serving.followers.publications();
        self.ready()?;
        let mut control = Ingress::new(&self.peer, self.monitor, false, &self.shutdown, serving);
        let mut data = Ingress::new(&self.data, self.data_monitor, true, &self.shutdown, serving);
        let mut publishers = Publishers::new(self.follower_pub.as_ref(), &self.reader_pub);
        let mut tick = tokio::time::interval(serving.retry);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut retrying = false;
        loop {
            // Process lifecycle changes before dequeuing native input. Once
            // received, admit synchronously before observing another cutover.
            let control_ready = control.prepare(service).await?;
            let data_ready = data.prepare(service).await?;
            publishers.publish(service)?;
            // Only an unfinished handshake needs the retry timer; idle links
            // do not wake the dispatcher.
            let pending = serving.brokers.keys().any(|&peer| service.retrying(peer));
            if pending && !retrying {
                tick.reset();
            }
            retrying = pending;
            tokio::select! {
                () = self.shutdown.closed() => return Ok(()),
                event = control.monitor.recv() => control.observe(event, service).await?,
                message = control.socket.recv_with_receipt(None), if control_ready => {
                    let (receipt, body) = message.map_err(failure)?;
                    control.receive(service, receipt, body)?;
                }
                event = data.monitor.recv() => data.observe(event, service).await?,
                message = data.socket.recv_with_receipt(None), if data_ready => {
                    let (receipt, body) = message.map_err(failure)?;
                    data.receive(service, receipt, body)?;
                }
                (publisher, received) = publications.receive() => {
                    receive_publication(service, serving.buffers, publisher, received)?;
                }
                socket = publishers.ready() => publishers.retry(socket)?,
                Some(lane) = control.paused.writable(), if control_ready => {
                    control.retry(lane, service)?;
                }
                Some(lane) = data.paused.writable(), if data_ready => {
                    data.retry(lane, service)?;
                }
                result = service.progress_with(|message| serving.followers.send(outbound_socket(&self.peer, &self.data, &message), message), |probe| serving.followers.wait(outbound_socket(&self.peer, &self.data, &probe), probe)) => {
                    if !self.shutdown.is_closed() {
                        result.map_err(failure)?;
                    }
                },
                _ = tick.tick(), if retrying => {
                    for &peer in serving.brokers.keys() {
                        service.retry(peer).map_err(failure)?;
                    }
                }
            }
        }
    }
}

/// Admit one follower publication. PUB receive is lossy; it never holds a
/// source on capacity, and only fatal admission failures stop the frontend.
fn receive_publication(
    service: &mut Service,
    buffers: &ReceiveBuffers,
    publisher: NodeId,
    received: Result<omq_tokio::Message, omq_tokio::Error>,
) -> Result<(), StartupError> {
    let message = received.map_err(failure)?;
    if let Ok((message, retained)) = buffers.prepare_borrowed(&message)
        && let Err(reason) = service.receive_publication(publisher, &message, retained)
        && fatal(&reason)
    {
        return Err(failure(reason));
    }
    Ok(())
}

fn outbound_socket<'a>(
    control: &'a omq_tokio::IdentitySocket,
    data: &'a omq_tokio::IdentitySocket,
    message: &omq_tokio::Message,
) -> &'a omq_tokio::IdentitySocket {
    if message.part_slice(1).is_some_and(|header| {
        header.get(5).is_some_and(|opcode| {
            [
                ozzy_proto::Opcode::Records as u8,
                ozzy_proto::Opcode::PrepareFlow as u8,
                ozzy_proto::Opcode::Ops as u8,
                ozzy_proto::Opcode::SnapshotChunk as u8,
            ]
            .contains(opcode)
        })
    }) {
        data
    } else {
        control
    }
}

fn check_followers(
    frontend: &FrontendContext,
    remote: &RemoteBrokers,
    buffers: &ReceiveBuffers,
) -> Result<(), StartupError> {
    let brokers = &remote.control;
    let routes = &remote.followers;
    for (&publisher, endpoint) in &routes.publications {
        if !brokers.contains_key(&publisher) {
            return Err(error("unknown follower publisher"));
        }
        buffers
            .check_transport(endpoint, frontend.limits.message_bytes)
            .map_err(failure)?;
    }
    for (&broker, endpoint) in &routes.repairs {
        if brokers
            .get(&broker)
            .is_none_or(|control| control == endpoint)
        {
            return Err(error("invalid follower data endpoint"));
        }
        buffers
            .check_transport(endpoint, frontend.limits.message_bytes)
            .map_err(failure)?;
    }
    for (&alias, &(sender, shard)) in &routes.incoming {
        if !brokers.contains_key(&sender)
            || alias != FollowerRoutes::identity(sender, frontend.local, shard)
            || alias == frontend.local
            || brokers.contains_key(&alias)
        {
            return Err(error("invalid follower repair alias"));
        }
    }
    for &(broker, group) in routes.destinations.keys() {
        if !brokers.contains_key(&broker)
            || !routes.repairs.contains_key(&broker)
            || !routes.local.contains_key(&group)
        {
            return Err(error("unmapped follower repair destination"));
        }
    }
    if !routes.destinations.is_empty() && frontend.follower_pub.is_none() {
        return Err(error("follower PUB missing"));
    }
    Ok(())
}

fn fatal(error: &ReceiveError) -> bool {
    matches!(
        error,
        ReceiveError::Setup(_)
            | ReceiveError::DirectoryReply(_)
            | ReceiveError::Dispatch(
                Rejection::Charge
                    | Rejection::Data(
                        DataPressure::Closed | DataPressure::Charge | DataPressure::Invalid
                    )
            )
    )
}

fn failure(reason: impl std::fmt::Display) -> StartupError {
    error(reason.to_string())
}

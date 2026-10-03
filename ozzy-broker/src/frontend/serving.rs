//! Ordinary receive and transport lifecycle. Partition work stays on shards.

use omq_tokio::{Endpoint, TrySendError};
use ozzy_proto::NodeId;
use ozzy_runtime::frontend::{
    DataPressure, Kind, ReceiveBuffers, ReceiveError, Rejection, Service,
};
use std::{collections::BTreeMap, time::Duration};

use super::{FollowerRoutes, FrontendContext, StartupError, error, followers::Followers};

mod ingress;
use ingress::Ingress;

impl FrontendContext {
    /// Serve an already assembled bounded frontend on its owned dispatcher.
    /// Broker control uses one connection per pair; repair uses one per remote
    /// destination shard. Readiness
    /// does not wait for every broker. The service owns no partition authority.
    /// Input rejection drops this attempt; native record retry identity remains
    /// with the sender. Accounting failures and monitor loss stop the frontend.
    pub async fn serve(
        mut self,
        mut service: Service,
        brokers: BTreeMap<NodeId, Endpoint>,
        follower_routes: FollowerRoutes,
        buffers: ReceiveBuffers,
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
        for (&peer, endpoint) in &brokers {
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
        check_followers(&self, &brokers, &follower_routes, &buffers)?;
        for (&peer, endpoint) in &brokers {
            if self.local < peer {
                tokio::select! {
                    () = self.shutdown.requested() => return Ok(()),
                    result = self.peer.connect(endpoint.clone()) => result.map_err(failure)?,
                }
            }
            service.start(peer).map_err(failure)?;
        }
        let followers = Followers::open(&self, &brokers, follower_routes).await?;
        let mut publications = followers.publications();
        let result = async {
            self.ready()?;
            let mut control = Ingress::new(&self.peer, self.monitor, false);
            let mut data = Ingress::new(&self.data, self.data_monitor, true);
            let mut tick = tokio::time::interval(retry);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                if let Some(message) = service.take_publication() {
                    publish(&self.reader_pub, self.follower_pub.as_ref(), &service, message)?;
                }
                tokio::select! {
                    () = self.shutdown.requested() => return Ok(()),
                    event = control.monitor.recv() => {
                        control.observe(event.map_err(failure)?, &mut service, &followers.routes)?;
                    }
                    message = control.socket.recv_from_source(None) => {
                        let (receipt, body) = message.map_err(failure)?;
                        control.receive(&mut service, &buffers, &followers.routes, receipt, body)?;
                    }
                    event = data.monitor.recv() => {
                        data.observe(event.map_err(failure)?, &mut service, &followers.routes)?;
                    }
                    message = data.socket.recv_from_source(None) => {
                        let (receipt, body) = message.map_err(failure)?;
                        data.receive(&mut service, &buffers, &followers.routes, receipt, body)?;
                    }
                    (publisher, received) = publications.receive() => {
                        let message = received.map_err(failure)?;
                        // PUB receive is lossy; it never holds a source on capacity.
                        if let Ok((message, retained)) = buffers.prepare_borrowed(&message)
                            && let Err(reason) = service.receive_publication(publisher, &message, retained)
                            && fatal(&reason)
                        {
                            return Err(failure(reason));
                        }
                    }
                    Some(lane) = control.paused.writable() => {
                        control.retry(lane, &mut service, &buffers, &followers.routes)?;
                    }
                    Some(lane) = data.paused.writable() => {
                        data.retry(lane, &mut service, &buffers, &followers.routes)?;
                    }
                    result = service.progress_with(|message| followers.send(outbound_socket(&self.peer, &self.data, &message), message), |probe| followers.wait(outbound_socket(&self.peer, &self.data, &probe), probe)) => {
                        if !self.shutdown.is_requested() {
                            result.map_err(failure)?;
                        }
                    },
                    _ = tick.tick() => {
                        for &peer in brokers.keys() {
                            service.retry(peer).map_err(failure)?;
                        }
                    }
                }
            }
        }
        .await;
        drop(publications);
        let closed = followers.close().await;
        result.and(closed.map_err(failure))
    }
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
    brokers: &BTreeMap<NodeId, Endpoint>,
    routes: &FollowerRoutes,
    buffers: &ReceiveBuffers,
) -> Result<(), StartupError> {
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

fn publish(
    reader: &omq_tokio::Socket,
    followers: Option<&omq_tokio::Socket>,
    service: &Service,
    message: omq_tokio::Message,
) -> Result<(), StartupError> {
    let frames = std::array::from_fn::<_, 3, _>(|i| {
        message.part_slice(i + 1).expect("validated publication")
    });
    let publication =
        ozzy_proto::decode_packet(&frames, service.envelope_limits()).map_err(failure)?;
    let follower = publication.envelope.opcode == ozzy_proto::Opcode::PreparePub;
    let socket = if follower {
        followers.ok_or_else(|| error("follower PUB missing"))?
    } else {
        reader
    };
    match socket.try_send(message) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => {
            if follower {
                ozzy_runtime::profiling::event(
                    ozzy_runtime::profiling::Event::ReplicaPublicationSendDrop,
                );
            }
            Ok(())
        }
        Err(TrySendError::Closed) => Err(error("publication socket closed")),
        Err(TrySendError::Error(error)) => Err(failure(error)),
    }
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

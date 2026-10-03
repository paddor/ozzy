//! One bounded SUB socket per broker, shared across every topic reader.

use super::{Arc, BTreeSet, BrokerLinkError, BrokerLinksConfig, Bytes, NodeId, Shared};
use crate::signal::CloseSignal;
use omq_tokio::{Socket, SocketType};
use std::time::Duration;

#[derive(Debug, Default)]
pub(super) struct Link {
    pub(super) closed: CloseSignal,
}

pub(super) struct Driver {
    socket: Socket,
    shared: Arc<Shared>,
    link: Arc<Link>,
    broker: NodeId,
    connected: bool,
    prefixes: BTreeSet<Bytes>,
    limits: ozzy_proto::EnvelopeLimits,
    observed: Option<u64>,
    desired: BTreeSet<Bytes>,
    updating: bool,
}

impl Driver {
    pub(super) fn new(
        context: &omq_tokio::Context,
        shared: Arc<Shared>,
        broker: NodeId,
        config: &BrokerLinksConfig,
    ) -> Result<(Arc<Link>, Self), BrokerLinkError> {
        let limits = config.reader.ok_or(BrokerLinkError::Configuration)?;
        let maximum = crate::transport::message_size_limit(config.parameters.receive.envelope)
            .and_then(|n| n.checked_add(16))
            .ok_or(BrokerLinkError::Configuration)?;
        let socket = context.socket(
            SocketType::Sub,
            crate::transport::socket_options()
                .recv_hwm(limits.queue_messages as u32)
                .max_message_size(maximum)
                .linger(Duration::ZERO),
        );
        let link = Arc::new(Link::default());
        Ok((
            link.clone(),
            Self {
                socket,
                shared,
                link,
                broker,
                connected: false,
                prefixes: BTreeSet::new(),
                limits: config.parameters.receive.envelope,
                observed: None,
                desired: BTreeSet::new(),
                updating: false,
            },
        ))
    }

    pub(super) async fn run(mut self) {
        let result = self.serve().await;
        let closed = self.socket.close().await.map_err(BrokerLinkError::from);
        if let Err(error) = result.and(closed) {
            let _ = self.shared.failure.set(error.to_string());
            self.shared.stop.close();
        }
        self.link.closed.close();
    }

    async fn serve(&mut self) -> Result<(), BrokerLinkError> {
        let mut ready = 0;
        loop {
            if self.shared.stop.is_closed() {
                return Ok(());
            }
            let seen = self.shared.readers.interests.generation();
            if self.observed != Some(seen) {
                self.desired = self.shared.readers.prefixes();
                self.observed = Some(seen);
                self.updating = true;
            }
            if !self.connected
                && let Some(endpoint) = self.shared.readers.endpoint(self.broker)
            {
                self.socket.connect(endpoint).await?;
                self.connected = true;
            }
            if self.updating && self.filters().await? {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                () = self.shared.stop.closed() => return Ok(()),
                () = self.shared.readers.interests.changed_after(seen) => {},
                message = self.socket.recv() => {
                    self.shared.readers.publication(self.broker, &message?, self.limits);
                    ready += 1;
                    if ready == 16 { ready = 0; tokio::task::yield_now().await; }
                }
            }
        }
    }

    async fn filters(&mut self) -> Result<bool, BrokerLinkError> {
        if let Some(prefix) = self.prefixes.difference(&self.desired).next().cloned() {
            self.socket.unsubscribe(prefix.clone()).await?;
            self.prefixes.remove(&prefix);
            return Ok(true);
        }
        if let Some(prefix) = self.desired.difference(&self.prefixes).next().cloned() {
            self.socket.subscribe(prefix.clone()).await?;
            self.prefixes.insert(prefix);
            return Ok(true);
        }
        self.updating = false;
        Ok(false)
    }
}

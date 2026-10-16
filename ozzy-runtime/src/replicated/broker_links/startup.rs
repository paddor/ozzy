//! Owner startup: bind the shared state, open both PEER sockets, connect
//! every broker, and spawn its drivers.

use super::{
    BrokerLinkError, BrokerLinks, BrokerLinksConfig, Inner, Peer, Shared, driver, publications,
    readers, routing,
};
use crate::{
    frontend::{LinkIds, LinkSessions},
    replicated::WriterRuntime,
    signal::{CloseSignal, StateSignal},
};
use arc_swap::ArcSwapOption;
use omq_tokio::{IdentitySocket, SocketType};
use ozzy_proto::{NodeId, handshake};
use std::{collections::BTreeMap, rc::Rc, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc};

/// Options of the control and data PEER sockets, sized for `slots` requests.
fn peer_options(
    config: &BrokerLinksConfig,
    slots: usize,
) -> Result<omq_tokio::Options, BrokerLinkError> {
    let message_bytes = crate::transport::message_size_limit(config.parameters.receive.envelope)
        .ok_or(BrokerLinkError::Configuration)?;
    // Control slots, APPEND requests, and a few handshake and watch frames.
    let hwm = slots
        .checked_add(config.append.map_or(0, |limits| limits.requests))
        .and_then(|count| count.checked_add(4))
        .and_then(|count| u32::try_from(count).ok())
        .ok_or(BrokerLinkError::Configuration)?;
    // OMQ charges received backing to its connection until it drops. Readers
    // keep frames under their own reservation, which must bind first.
    let retained = config
        .reader
        .map_or(Some(0), |limits| {
            limits.reservation_bytes(config.parameters.receive, config.brokers.len())
        })
        .and_then(|bytes| bytes.checked_add(message_bytes))
        .ok_or(BrokerLinkError::Configuration)?;
    Ok(crate::transport::socket_options()
        .identity(crate::transport::peer_identity(config.local))
        .router_mandatory(true)
        .send_hwm(hwm)
        .recv_hwm(hwm)
        .max_message_size(message_bytes)
        .peer_recv_max_bytes(retained.max(omq_tokio::options::DEFAULT_PEER_MAX_BYTES))
        .linger(Duration::from_millis(5)))
}

/// Owner-wide state every broker's drivers share while the links start.
pub(super) struct Startup {
    sdk: WriterRuntime,
    pub(super) config: BrokerLinksConfig,
    pub(super) shared: Arc<Shared>,
    pub(super) control: Arc<IdentitySocket>,
    data: Arc<IdentitySocket>,
    /// Taken before any connect, so the driver sees every handshake.
    monitor: omq_tokio::MonitorStream,
    readers: Rc<readers::Registry>,
}

/// Owners started for each configured broker, in configuration order.
#[derive(Default)]
pub(super) struct Brokers {
    peers: BTreeMap<NodeId, Arc<Peer>>,
    links: Vec<driver::LinkState>,
    publications: Vec<Arc<publications::Link>>,
    publication_drivers: Vec<publications::Driver>,
}

impl Startup {
    /// Bind the owner state and open both PEER sockets, sized for `slots`
    /// requests. Nothing connects yet.
    pub(super) fn open(
        sdk: WriterRuntime,
        config: BrokerLinksConfig,
        slots: usize,
        link_ids: LinkIds,
        request_ids: LinkIds,
    ) -> Result<(Self, LinkSessions), BrokerLinkError> {
        let owner = sdk.driver().clone();
        let shared = Arc::new(Shared::new(&config, owner, request_ids)?);
        let sessions = LinkSessions::with_ids(
            config.local,
            config.parameters,
            handshake::OWNER,
            config.brokers.len(),
            link_ids,
        )?;
        let options = peer_options(&config, slots)?;
        let control = Arc::new(
            sdk.context()
                .socket(SocketType::Peer, options.clone())
                .identity_routing()?,
        );
        let data = Arc::new(
            sdk.context()
                .socket(SocketType::Peer, options)
                .identity_routing()?,
        );
        let monitor = control.monitor();
        let readers = Rc::new(readers::Registry::new(&config, &shared.readers));
        let startup = Self {
            sdk,
            config,
            shared,
            control,
            data,
            monitor,
            readers,
        };
        Ok((startup, sessions))
    }

    /// Connect both sockets to each broker and build its link driver, plus
    /// its publication driver when this owner reads. Each broker gets an
    /// even share of the `slots` control requests.
    pub(super) async fn connect(&self, slots: usize) -> Result<Brokers, BrokerLinkError> {
        let config = &self.config;
        let mut brokers = Brokers::default();
        for (index, address) in config.brokers.iter().enumerate() {
            let count =
                slots / config.brokers.len() + usize::from(index < slots % config.brokers.len());
            self.control.connect(address.endpoint.clone()).await?;
            self.data.connect(address.data_endpoint.clone()).await?;
            let (sender, input) = mpsc::channel(count);
            let peer = Arc::new(Peer {
                data: self.data.clone(),
                sender,
                slots: Arc::new(Semaphore::new(count)),
                doorbell: StateSignal::default(),
                closed: CloseSignal::default(),
                link: ArcSwapOption::empty(),
            });
            brokers.links.push(driver::LinkState::new(
                self,
                address.node,
                peer.clone(),
                input,
            ));
            brokers.peers.insert(address.node, peer);
            if config.reader.is_some() {
                let (link, driver) = publications::Driver::new(
                    self.sdk.context(),
                    self.shared.clone(),
                    self.readers.clone(),
                    address.node,
                    config,
                )?;
                brokers.publications.push(link);
                brokers.publication_drivers.push(driver);
            }
        }
        Ok(brokers)
    }

    /// Spawn the link driver and every publication driver on the owner.
    pub(super) fn spawn(
        self,
        sessions: LinkSessions,
        brokers: Brokers,
    ) -> Result<BrokerLinks, BrokerLinkError> {
        let config = self.config;
        // One driver turn of registrations.
        let (registrations, received) = mpsc::channel(driver::TURN);
        // A place per reader slot, so a new inbox never finds it full.
        let (inboxes, installs) = mpsc::channel(readers::Admission::capacity(&config)?.0.max(1));
        let inputs = driver::Inputs {
            control: self.control,
            data: self.data,
            monitor: self.monitor,
            registrations: received,
            inboxes: installs,
        };
        let local = driver::Local {
            sessions,
            routing: routing::Registry::new(config.maximum_partitions, config.routing_bytes),
            readers: self.readers,
        };
        let driver = driver::Driver::new(inputs, brokers.links, local, self.shared.clone());
        let links = BrokerLinks(Arc::new(Inner {
            config,
            peers: brokers.peers,
            registrations,
            inboxes,
            publications: brokers.publications,
            shared: self.shared,
            runtime: self.sdk,
        }));
        tokio::task::spawn_local(driver.run());
        for driver in brokers.publication_drivers {
            tokio::task::spawn_local(driver.run());
        }
        Ok(links)
    }
}

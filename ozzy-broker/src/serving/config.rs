use super::{Binding, Registration, failure};
use crate::{CheckedConfig, StartupError, TransportLimits};
use omq_tokio::Endpoint;
use ozzy_proto::{NodeId, data::DataLimits, directory::EndpointScheme, handshake};
use ozzy_runtime::{
    dispatch::{Budget, Budgets},
    frontend::{
        self, Access, DataLaneLimits, DataSender, Kind, ReceiveBuffers, ReceiveStorage, Service,
    },
    replica_transport::QueueLimits,
};
use std::{collections::BTreeMap, time::Duration};

pub(super) const WRITERS: usize = 32;
/// APPENDs one writer may have in flight on one partition. The partition
/// actor owns these request slots; OMQ and the shard queue backpressure intake.
pub(super) const WRITER_WINDOW: usize = 3;
/// Coordinator plan proposals one partition may have in flight.
pub(super) const COORDINATOR_PLANS: usize = 2;
/// Proposal arenas of one partition's native services: per writer, one open
/// slot and its APPEND slots, client rejection and consumer control slots,
/// plus the coordinator's plan arenas.
pub(crate) const fn native_arenas(clients: usize) -> usize {
    WRITERS * (WRITER_WINDOW + 1) + 2 * clients + COORDINATOR_PLANS
}
type Handoffs = Vec<(u32, tokio::sync::oneshot::Sender<super::Binding>)>;

pub(super) struct Config {
    pub time: super::context::Time,
    pub omq: omq_tokio::Context,
    pub local: NodeId,
    /// SDK link owners admitted at once (`limits.max_clients`).
    pub clients: usize,
    pub limits: DataLimits,
    pub envelope: ozzy_proto::EnvelopeLimits,
    pub buffers: ReceiveBuffers,
    pub transport: TransportLimits,
    pub remote: crate::RemoteBrokers,
    pub peers: usize,
    pub budgets: BTreeMap<u32, Budgets>,
    pub catalog: Vec<ozzy_proto::directory::TopicPage>,
    pub maximum_partitions: usize,
    /// The admin owner's lane and links, when the broker serves an admin
    /// endpoint.
    pub admin: Option<crate::admin::Hooks>,
}

impl Config {
    /// Admission budgets of one local shard.
    pub(super) fn budget(&self, shard: u32) -> Result<Budgets, StartupError> {
        self.budgets
            .get(&shard)
            .copied()
            .ok_or_else(|| failure("unknown shard budget"))
    }

    /// Bound writer and follower queues separately from canonical actor arenas.
    /// Each queue reserves one further frame while its actor is busy.
    pub(super) fn data_lane(&self, shard: u32, kind: Kind) -> Result<DataLaneLimits, StartupError> {
        let maximum = self.buffers.maximum_retained_bytes();
        let minimum = maximum
            .checked_mul(4)
            .ok_or_else(|| failure("data frame budget overflow"))?;
        let budget = self
            .budget(shard)?
            .data
            .bytes
            .min(128 * 1024 * 1024)
            .max(minimum)
            / 2;
        // Lossy publication needs room for storage-completion bursts. The
        // weighted byte bound still includes the separately reserved pending frame.
        let maximum_slots = if kind == Kind::Broker { 128 } else { 16 };
        let available = self.budget(shard)?.data.queue_slots.min(maximum_slots);
        if available == 0 {
            return Err(failure("data shard has no queue capacity"));
        }
        Ok(DataLaneLimits {
            slots: available,
            maximum_retained_bytes: maximum,
            retained_budget_bytes: budget,
        })
    }

    pub(super) fn new(
        checked: &CheckedConfig,
        context: super::ServingContext,
    ) -> Result<Self, StartupError> {
        let super::ServingContext { omq, time } = context;
        let deployment = checked.deployment.deployment();
        let local = NodeId::from_bytes(*checked.identity.broker_id(&checked.plan.name)?.as_bytes());
        let clients = deployment.limits.max_clients;
        let limits = native_limits(checked)?;
        let body = deployment
            .topics
            .values()
            .map(|topic| topic.max_append_bytes as usize)
            .max()
            .ok_or_else(|| failure("no topics"))?;
        let envelope = ozzy_proto::EnvelopeLimits {
            max_metadata_bytes: 64 * 1024,
            max_payload_bytes: body,
        };
        require_follower_pub_endpoints(checked)?;
        let brokers = peer_endpoints(checked, local)?;
        let followers = follower_routes(checked, local, &brokers)?;
        // SDK links, broker links, and repair links from each remote shard,
        // each with room for a replaced connection that still drains.
        let senders = clients + brokers.len() + followers.incoming.len();
        let transport = transport_limits(body, 2 * senders)?;
        let storage = receive_storage(checked, body, transport.message_bytes)?;
        let buffers = ReceiveBuffers::new(envelope, storage).map_err(failure)?;
        let budgets = shard_budgets(checked);
        let catalog = catalog(checked)?;
        Ok(Self {
            time,
            omq,
            local,
            clients,
            limits,
            envelope,
            buffers,
            transport,
            peers: clients + brokers.len(),
            remote: crate::RemoteBrokers {
                control: brokers,
                followers,
            },
            budgets,
            catalog,
            maximum_partitions: checked.plan.partitions.len(),
            admin: None,
        })
    }

    pub(super) fn handoff(
        &self,
        service: &mut Service,
        handoffs: Handoffs,
    ) -> Result<(), StartupError> {
        if let Some(admin) = &self.admin {
            admin.publish_links(service.links());
        }
        for (id, handoff) in handoffs {
            let port = service
                .port_with_publications(
                    &self.omq,
                    id,
                    self.budget(id)?,
                    ozzy_runtime::dispatch::Budget {
                        queue_slots: 2,
                        retained_messages: 2,
                        bytes: (self.transport.message_bytes + 4096) * 2,
                    },
                )
                .map_err(failure)?;
            handoff
                .send(Binding {
                    links: service.links(),
                    port,
                })
                .map_err(|_| failure("shard handoff observer disappeared"))?;
        }
        Ok(())
    }

    pub(super) fn service(
        &self,
        shards: Vec<Registration>,
    ) -> Result<(Service, Handoffs), StartupError> {
        let routes = self.routing_table(&shards)?;
        let states = shards
            .iter()
            .flat_map(|shard| shard.routes.clone())
            .collect::<Vec<_>>();
        let (dispatcher, handoffs) = self.dispatcher(routes, shards)?;
        let access = self
            .remote
            .control
            .keys()
            .map(|&peer| Access {
                peer,
                kind: Kind::Broker,
            })
            .collect::<Vec<_>>();
        let mut service = Service::new_with_broker_limits(
            dispatcher,
            self.handshake_parameters()?,
            DataLimits {
                envelope: self.envelope,
                ..self.limits
            },
            &access,
            frontend::LinkIds::random(),
        )
        .map_err(failure)?
        .with_trusted_clients(self.clients)
        .map_err(failure)?;
        self.install_directory(&mut service, states)?;
        Ok((service, handoffs))
    }

    /// Route every registered partition to its shard.
    fn routing_table(
        &self,
        shards: &[Registration],
    ) -> Result<frontend::RoutingTable, StartupError> {
        let placements = shards
            .iter()
            .flat_map(|shard| {
                shard.routes.iter().map(move |route| frontend::Placement {
                    group: route.group,
                    partition: route.partition,
                    shard: shard.id,
                })
            })
            .collect::<Vec<_>>();
        let ids = shards.iter().map(|shard| shard.id).collect::<Vec<_>>();
        frontend::RoutingTable::new(&ids, &placements, self.maximum_partitions, self.envelope)
            .map_err(failure)
    }

    /// The dispatcher with each shard's command and data lanes installed,
    /// plus the handoff that later delivers each shard its binding.
    fn dispatcher(
        &self,
        routes: frontend::RoutingTable,
        shards: Vec<Registration>,
    ) -> Result<(frontend::Dispatcher, Handoffs), StartupError> {
        let mut handoffs = Vec::with_capacity(shards.len());
        let mut data_lanes = Vec::with_capacity(shards.len());
        let lanes = shards
            .into_iter()
            .map(|shard| {
                handoffs.push((shard.id, shard.handoff));
                data_lanes.push((shard.id, shard.data));
                data_lanes.push((shard.id, shard.replica));
                data_lanes.push((shard.id, shard.broker_control));
                (shard.id, shard.sender)
            })
            .collect();
        let mut dispatcher = frontend::Dispatcher::new(
            self.local,
            routes,
            lanes,
            frontend::DispatcherLimits {
                peers: self.peers,
                replies: self.reply_limits(),
            },
        )
        .map_err(failure)?;
        install_data_lanes(&mut dispatcher, data_lanes)?;
        Ok((dispatcher, handoffs))
    }

    /// Queue bounds of shard replies to the dispatcher, by class.
    fn reply_limits(&self) -> frontend::ReplyLimits {
        let control = QueueLimits {
            messages: 8,
            bytes: 1024 * 1024,
            message_bytes: 128 * 1024,
        };
        let data = QueueLimits {
            messages: 4,
            bytes: self.transport.message_bytes * 4,
            message_bytes: self.transport.message_bytes,
        };
        frontend::ReplyLimits { data, control }
    }

    /// Install the topic catalog and the partition watch registry.
    fn install_directory(
        &self,
        service: &mut Service,
        states: Vec<ozzy_proto::directory::RouteState>,
    ) -> Result<(), StartupError> {
        service
            .install_catalog(
                frontend::TopicCatalog::new(
                    self.catalog.clone(),
                    self.catalog.len(),
                    self.maximum_partitions,
                )
                .map_err(failure)?,
            )
            .map_err(failure)?;
        service
            .install_watches(
                frontend::WatchRegistry::new(
                    frontend::WatchLimits {
                        partitions: self.maximum_partitions,
                        peers: self.clients,
                        registrations: self.clients * 1024,
                        interests_per_registration: 256,
                        pending_per_registration: 256,
                    },
                    states,
                )
                .map_err(failure)?,
            )
            .map_err(failure)
    }

    fn handshake_parameters(&self) -> Result<handshake::Parameters, StartupError> {
        let mut parameters =
            handshake::Parameters::streaming(self.limits, handshake::OWNER | (1 << 3))
                .map_err(failure)?;
        parameters.capabilities |= handshake::OWNER_ROUTING
            | handshake::OWNER_READ
            | handshake::OWNER_PROGRESS
            | handshake::OWNER_GROUPS;
        // Readers and writers share this endpoint and negotiate independent
        // command families. SDK profiles require their own capabilities.
        parameters.required_capabilities = 0;
        Ok(parameters)
    }
}

/// Bounds of the broker transport around the largest topic append body.
fn transport_limits(body: usize, peers: usize) -> Result<TransportLimits, StartupError> {
    let message_bytes = body
        .checked_add(64 * 1024 + 4096)
        .ok_or_else(|| failure("transport size overflow"))?;
    Ok(TransportLimits {
        send_messages: 1024,
        receive_messages: 1024,
        peers,
        publication_messages: 64,
        message_bytes,
        close_linger: Duration::from_millis(100),
    })
}

/// Replicated brokers each need an explicit follower PUB endpoint.
fn require_follower_pub_endpoints(checked: &CheckedConfig) -> Result<(), StartupError> {
    if checked.identity.brokers.len() == 3
        && checked
            .deployment
            .deployment()
            .brokers
            .values()
            .any(|broker| broker.endpoints.follower_pub.is_none())
    {
        return Err(failure(
            "replicated brokers require explicit follower PUB endpoints",
        ));
    }
    Ok(())
}

/// Receive backing for the local PEER endpoint scheme.
fn receive_storage(
    checked: &CheckedConfig,
    body: usize,
    message_bytes: usize,
) -> Result<ReceiveStorage, StartupError> {
    let endpoints = &checked.local_broker()?.endpoints;
    match EndpointScheme::parse(&endpoints.peer) {
        Some((EndpointScheme::Inproc, _)) => Ok(ReceiveStorage::Inproc {
            payload_backing_bytes: body,
        }),
        Some((scheme, _)) if scheme.compressed() => compressed_storage(message_bytes),
        _ => Ok(ReceiveStorage::Stream { message_bytes }),
    }
}

/// Control endpoint of every other configured broker.
fn peer_endpoints(
    checked: &CheckedConfig,
    local: NodeId,
) -> Result<BTreeMap<NodeId, Endpoint>, StartupError> {
    let deployment = checked.deployment.deployment();
    checked
        .identity
        .brokers
        .iter()
        .filter(|(_, id)| NodeId::from_bytes(*id.as_bytes()) != local)
        .map(|(name, id)| {
            Ok((
                NodeId::from_bytes(*id.as_bytes()),
                deployment
                    .broker(name)?
                    .endpoints
                    .peer
                    .parse()
                    .map_err(failure)?,
            ))
        })
        .collect()
}

/// Data and control admission budgets of each local shard.
fn shard_budgets(checked: &CheckedConfig) -> BTreeMap<u32, Budgets> {
    checked
        .plan
        .shards
        .iter()
        .map(|shard| {
            let budget = Budgets {
                data: Budget {
                    queue_slots: shard.budget.append_slots,
                    retained_messages: shard.budget.append_slots,
                    bytes: shard.budget.resident_bytes as usize,
                },
                control: Budget {
                    queue_slots: shard.budget.control_slots,
                    retained_messages: shard.budget.control_slots,
                    bytes: shard.budget.control_bytes as usize,
                },
            };
            (shard.id, budget)
        })
        .collect()
}

fn follower_routes(
    checked: &CheckedConfig,
    local: NodeId,
    brokers: &BTreeMap<NodeId, Endpoint>,
) -> Result<crate::FollowerRoutes, StartupError> {
    let deployment = checked.deployment.deployment();
    let mut followers = crate::FollowerRoutes::default();
    for placement in &checked.plan.partitions {
        let partition = checked
            .identity
            .partition(&placement.topic, placement.partition)?;
        followers.local.insert(
            ozzy_proto::GroupId::from_bytes(*partition.group.as_bytes()),
            placement.shard,
        );
    }
    for (name, id) in &checked.identity.brokers {
        let broker = NodeId::from_bytes(*id.as_bytes());
        if broker != local {
            followers.repairs.insert(
                broker,
                deployment
                    .broker(name)?
                    .endpoints
                    .data_peer
                    .parse()
                    .map_err(failure)?,
            );
        }
        if broker == local {
            continue;
        }
        if let Some(endpoint) = &deployment.broker(name)?.endpoints.follower_pub {
            followers
                .publications
                .insert(broker, endpoint.parse().map_err(failure)?);
        }
        for placement in checked.deployment.partition_placements(name)? {
            let partition = checked
                .identity
                .partition(&placement.topic, placement.partition)?;
            followers.destinations.insert(
                (
                    broker,
                    ozzy_proto::GroupId::from_bytes(*partition.group.as_bytes()),
                ),
                placement.shard,
            );
        }
        for shard in &checked.plan.shards {
            let alias = crate::FollowerRoutes::identity(broker, local, shard.id);
            if alias == local
                || brokers.contains_key(&alias)
                || followers
                    .incoming
                    .insert(alias, (broker, shard.id))
                    .is_some()
            {
                return Err(failure("repair identity collision"));
            }
        }
    }
    Ok(followers)
}

fn install_data_lanes(
    dispatcher: &mut frontend::Dispatcher,
    lanes: Vec<(u32, DataSender)>,
) -> Result<(), StartupError> {
    for (shard, lane) in lanes {
        dispatcher.install_data_lane(shard, lane).map_err(failure)?;
    }
    Ok(())
}

fn catalog(checked: &CheckedConfig) -> Result<Vec<ozzy_proto::directory::TopicPage>, StartupError> {
    let mut pages = Vec::new();
    let source = checked.topic_catalog()?;
    for (name, topic) in &checked.identity.topics {
        let mut first = 0;
        while first < topic.partitions.len() as u32 {
            let page = source
                .page(
                    &ozzy_proto::directory::TopicRequest {
                        name: name.clone(),
                        first,
                        maximum: 256,
                    },
                    64 * 1024,
                )
                .map_err(failure)?;
            first += page.partitions.len() as u32;
            pages.push(page);
        }
    }
    Ok(pages)
}

fn native_limits(checked: &CheckedConfig) -> Result<DataLimits, StartupError> {
    let deployment = checked.deployment.deployment();
    let profiles = deployment
        .topics
        .values()
        .map(|topic| {
            let resolved = topic.deduplication.is_some();
            let bound = |records| {
                ozzy_journal::operation::append_metadata_bound(1, records, resolved)
                    .ok_or_else(|| failure("append metadata bound overflows"))
            };
            let header = bound(0)?;
            Ok((
                topic.max_append_bytes as usize,
                header,
                bound(1)? - header,
                topic
                    .deduplication
                    .map_or(2048, |policy| (policy.max_entries as usize).min(2048)),
            ))
        })
        .collect::<Result<Vec<_>, StartupError>>()?;
    let records = profiles
        .iter()
        .map(|&(body, header, row, count)| count.min(body.saturating_sub(header) / (row + 1)))
        .min()
        .ok_or_else(|| failure("no topics"))?
        .max(1);
    let payload = profiles
        .iter()
        .try_fold(usize::MAX, |minimum, &(body, header, row, _)| {
            body.checked_sub(header + row * records)
                .filter(|bytes| *bytes > 0)
                .map(|bytes| minimum.min(bytes))
                .ok_or_else(|| failure("native request bound too small"))
        })?;
    Ok(DataLimits {
        envelope: ozzy_proto::EnvelopeLimits {
            max_metadata_bytes: 64 * 1024,
            max_payload_bytes: payload,
        },
        max_records: records,
        max_parts: records,
        max_record_bytes: payload,
    })
}

#[cfg(any(feature = "lz4-transport", feature = "zstd-transport"))]
#[expect(clippy::unnecessary_wraps, reason = "matches the featureless refusal")]
fn compressed_storage(message_bytes: usize) -> Result<ReceiveStorage, StartupError> {
    Ok(ReceiveStorage::Compressed { message_bytes })
}

#[cfg(not(any(feature = "lz4-transport", feature = "zstd-transport")))]
fn compressed_storage(_: usize) -> Result<ReceiveStorage, StartupError> {
    Err(failure(
        "compressed endpoints need the lz4-transport or zstd-transport feature",
    ))
}

use super::{Binding, Registration, failure};
use crate::{CheckedConfig, StartupError, TransportLimits};
use omq_tokio::Endpoint;
use ozzy_proto::{NodeId, data::DataLimits, handshake};
use ozzy_runtime::{
    dispatch::{Budget, Budgets},
    frontend::{self, Access, Kind, ReceiveBuffers, ReceiveStorage, Service},
    replica_transport::QueueLimits,
};
use std::{collections::BTreeMap, time::Duration};

pub(super) const CLIENTS: usize = 32;
pub(super) const WRITERS: usize = 32;
type Handoffs = Vec<(u32, tokio::sync::oneshot::Sender<super::Binding>)>;

pub(super) struct Config {
    pub local: NodeId,
    pub limits: DataLimits,
    pub envelope: ozzy_proto::EnvelopeLimits,
    pub buffers: ReceiveBuffers,
    pub transport: TransportLimits,
    pub brokers: BTreeMap<NodeId, Endpoint>,
    pub peers: usize,
    pub grants: usize,
    pub budgets: BTreeMap<u32, Budgets>,
    pub catalog: Vec<ozzy_proto::directory::TopicPage>,
    pub maximum_partitions: usize,
}

impl Config {
    pub(super) fn new(checked: &CheckedConfig) -> Result<Self, StartupError> {
        let deployment = checked.deployment.deployment();
        let local = NodeId::from_bytes(*checked.identity.brokers[&checked.plan.name].as_bytes());
        let limits = native_limits(checked)?;
        let body = deployment
            .topics
            .values()
            .map(|topic| topic.max_append_bytes as usize)
            .max()
            .expect("nonempty topics");
        let envelope = ozzy_proto::EnvelopeLimits {
            max_metadata_bytes: 64 * 1024,
            max_payload_bytes: body,
        };
        let message_bytes = body
            .checked_add(64 * 1024 + 4096)
            .ok_or_else(|| failure("transport size overflow"))?;
        let transport = TransportLimits {
            send_messages: 64,
            receive_messages: 64,
            message_bytes,
            close_linger: Duration::from_millis(100),
        };
        let endpoints = &deployment.brokers[&checked.plan.name].endpoints;
        let inproc = endpoints.peer.starts_with("inproc://");
        let buffers = ReceiveBuffers::new(
            envelope,
            if inproc {
                ReceiveStorage::Inproc {
                    payload_backing_bytes: body,
                }
            } else {
                ReceiveStorage::Stream { message_bytes }
            },
        )
        .map_err(failure)?;
        let brokers = checked
            .identity
            .brokers
            .iter()
            .filter(|(_, id)| NodeId::from_bytes(*id.as_bytes()) != local)
            .map(|(name, id)| {
                Ok((
                    NodeId::from_bytes(*id.as_bytes()),
                    deployment.brokers[name]
                        .endpoints
                        .peer
                        .parse()
                        .map_err(failure)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, StartupError>>()?;
        let mut budgets = BTreeMap::new();
        for shard in &checked.plan.shards {
            budgets.insert(
                shard.id,
                Budgets {
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
                },
            );
        }
        let catalog = catalog(checked)?;
        Ok(Self {
            local,
            limits,
            envelope,
            buffers,
            transport,
            peers: CLIENTS + brokers.len(),
            brokers,
            grants: checked
                .plan
                .partitions
                .len()
                .saturating_mul(4)
                .clamp(4, 65_536),
            budgets,
            catalog,
            maximum_partitions: checked.plan.partitions.len(),
        })
    }

    pub(super) fn handoff(
        &self,
        service: &mut Service,
        handoffs: Handoffs,
    ) -> Result<(), StartupError> {
        for (id, handoff) in handoffs {
            let budgets = &self.budgets;
            let port = service
                .port_with_publications(
                    id,
                    budgets[&id],
                    ozzy_runtime::dispatch::Budget {
                        queue_slots: 1,
                        retained_messages: 1,
                        bytes: self.transport.message_bytes + 4096,
                    },
                )
                .map_err(failure)?;
            let requests = service.grant_requests(id, self.grants).map_err(failure)?;
            handoff
                .send(Binding {
                    links: service.links(),
                    port,
                    requests,
                })
                .map_err(|_| failure("shard handoff observer disappeared"))?;
        }
        Ok(())
    }

    pub(super) fn service(
        &self,
        shards: Vec<Registration>,
    ) -> Result<(Service, Handoffs), StartupError> {
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
        let states = shards
            .iter()
            .flat_map(|shard| shard.routes.clone())
            .collect::<Vec<_>>();
        let routes =
            frontend::RoutingTable::new(&ids, &placements, self.maximum_partitions, self.envelope)
                .map_err(failure)?;
        let mut handoffs = Vec::with_capacity(shards.len());
        let lanes = shards
            .into_iter()
            .map(|shard| {
                handoffs.push((shard.id, shard.handoff));
                (shard.id, shard.sender)
            })
            .collect();
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
        let dispatcher = frontend::Dispatcher::new(
            self.local,
            routes,
            lanes,
            frontend::DispatcherLimits {
                peers: self.peers,
                grants_per_class: self.grants,
                replies: frontend::ReplyLimits { data, control },
            },
        )
        .map_err(failure)?;
        let access = self
            .brokers
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
        .with_trusted_clients(CLIENTS)
        .map_err(failure)?;
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
                        peers: CLIENTS,
                        registrations: CLIENTS * 1024,
                        interests_per_registration: 256,
                        pending_per_registration: 256,
                    },
                    states,
                )
                .map_err(failure)?,
            )
            .map_err(failure)?;
        Ok((service, handoffs))
    }

    fn handshake_parameters(&self) -> Result<handshake::Parameters, StartupError> {
        let mut parameters = handshake::Parameters::streaming(
            self.limits,
            handshake::OWNER | (1 << 3),
            65_536,
            self.budgets
                .values()
                .map(|budget| budget.data.bytes as u64)
                .sum(),
        )
        .map_err(failure)?;
        parameters.capabilities |= handshake::OWNER_ROUTING | handshake::OWNER_READ;
        // Readers and writers share this endpoint and negotiate independent
        // command families. SDK profiles require their own capabilities.
        parameters.required_capabilities = 0;
        Ok(parameters)
    }
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
    let native_body = deployment
        .topics
        .values()
        .map(|topic| topic.max_append_bytes)
        .min()
        .ok_or_else(|| failure("no topics"))? as usize;
    let records = 2048.min(native_body.saturating_sub(89) / 25).max(1);
    let payload = native_body
        .checked_sub(89 + 24 * records)
        .filter(|bytes| *bytes > 0)
        .ok_or_else(|| failure("native request bound too small"))?;
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

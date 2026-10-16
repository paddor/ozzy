//! Admin console owner. One thread serves the admin endpoint: counters come
//! from the telemetry registry, partition status from each shard over an
//! inproc lane, links from the frontend's published observations. It never
//! takes part in admission or confirmation, and a slow client or shard costs
//! only its own reply.

use crate::{CheckedConfig, StartupError, signal::CloseSignal};
use omq_tokio::{Context, Endpoint, IdentitySocket, Message, SocketType};
use ozzy_proto::admin::{
    self, Broker, Connections, Counters, EventKind, GroupCursor, GroupStatus, PartitionLog,
    PartitionName, Partitions, Reply, Request,
};
use ozzy_proto::{GroupId, NodeId, directory::RouteState};
use ozzy_runtime::{
    frontend::{Kind, Links},
    profiling,
    replica_actor::PartitionStatus,
    replica_journal::PartitionFacts,
};
use std::{
    collections::BTreeSet,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};

/// How long a partition request waits for the shards.
const SHARD_ANSWER: Duration = Duration::from_secs(1);
/// Queued messages per admin connection or shard lane.
const QUEUE: u32 = 16;
/// The admin owner's identity on the shard lane.
const LANE_IDENTITY: &[u8] = b"admin";

/// The bound admin sockets, waiting for their thread.
#[derive(Debug)]
pub(crate) struct Admin {
    public: IdentitySocket,
    shards: IdentitySocket,
    hooks: Hooks,
    broker: Broker,
    /// Local shard IDs, which can be sparse.
    shard_ids: Vec<u32>,
    started: Instant,
}

impl Admin {
    /// Bind the configured admin endpoint and the shards' inproc lane.
    /// `None` when the broker has no admin endpoint. The endpoint's PEER
    /// announces the broker's peer identity, which clients address.
    pub(crate) fn bind(
        checked: &CheckedConfig,
        context: &Context,
        local: NodeId,
        max_clients: usize,
    ) -> Result<Option<Self>, StartupError> {
        let Some(endpoint) = &checked.local_broker()?.endpoints.admin else {
            return Ok(None);
        };
        // OMQ charges each frame's in-memory size, not just its bytes.
        let identity = ozzy_runtime::transport::peer_identity(local);
        let public = peer(context, &identity, 4096);
        public.bind(endpoint).map_err(failure)?;
        // Inproc names are per OMQ context, which brokers in a harness share.
        let shards = peer(context, LANE_IDENTITY, 1 << 24);
        let lane = shards
            .bind(format!(
                "inproc://ozzy-admin-{}",
                ozzy_proto::RequestId::new()
            ))
            .map_err(failure)?;
        Ok(Some(Self {
            hooks: Hooks {
                lane,
                links: Arc::default(),
            },
            public: public.into_async().identity_routing().map_err(failure)?,
            shards: shards.into_async().identity_routing().map_err(failure)?,
            broker: broker(checked, local, max_clients)?,
            shard_ids: checked.plan.shards.iter().map(|shard| shard.id).collect(),
            started: Instant::now(),
        }))
    }

    /// What the shards and the frontend need to reach this owner.
    pub(crate) fn hooks(&self) -> Hooks {
        self.hooks.clone()
    }

    /// Serve on a new thread until `stop` closes.
    pub(crate) fn spawn(self, stop: CloseSignal) -> std::io::Result<std::thread::JoinHandle<()>> {
        std::thread::Builder::new()
            .name("ozzy/admin".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build();
                match runtime {
                    Ok(runtime) => runtime.block_on(self.serve(stop)),
                    Err(error) => profiling::log(profiling::Level::Error, &error.to_string()),
                }
            })
    }

    async fn serve(mut self, stop: CloseSignal) {
        let mut next = 0;
        loop {
            let (peer, request) = tokio::select! {
                () = stop.closed() => break,
                received = self.public.recv_from() => match received {
                    Ok(received) => received,
                    Err(_) => break,
                },
            };
            // TODO(admin-auth): any client of the endpoint is an admin. Check
            // the peer's CURVE key here once admins authenticate.
            let request = request.iter().next().unwrap_or_default();
            next += 1;
            let reply = tokio::select! {
                () = stop.closed() => break,
                reply = self.answer(&request, next) => reply,
            };
            // A client that stopped reading loses its reply, not the broker.
            let _ = self.public.try_send_to(&peer, Message::from(reply));
        }
        let _ = self.public.into_inner().close().await;
        let _ = self.shards.into_inner().close().await;
    }

    async fn answer(&mut self, request: &[u8], sequence: u64) -> Vec<u8> {
        let (id, reply) = match admin::decode_request(request) {
            Ok((id, Request::Broker)) => {
                self.broker.uptime_ms = self.uptime_ms();
                (id, Reply::Broker(self.broker.clone()))
            }
            Ok((id, Request::Counters)) => (id, Reply::Counters(self.counters())),
            Ok((id, Request::Partitions)) => (id, self.partitions(sequence).await),
            Ok((id, Request::Connections)) => (id, Reply::Connections(self.connections())),
            Err(_) => (
                admin::request_id(request).unwrap_or(0),
                Reply::Error("unknown request".into()),
            ),
        };
        admin::encode_reply(id, &reply).unwrap_or_else(|error| {
            admin::encode_reply(id, &Reply::Error(error.to_string())).unwrap_or_default()
        })
    }

    fn uptime_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    fn counters(&self) -> Counters {
        let threads = profiling::threads();
        let mut events = Vec::new();
        let mut totals = Vec::new();
        for (name, kind, value) in threads.totals.rows() {
            let kind = match kind {
                profiling::Kind::Count => EventKind::Count,
                profiling::Kind::Peak => EventKind::Peak,
                profiling::Kind::Gauge => EventKind::Gauge,
            };
            events.push((name.to_owned(), kind));
            totals.push(value);
        }
        let values = |totals: &profiling::Totals| totals.rows().map(|(.., value)| value).collect();
        Counters {
            uptime_ms: self.uptime_ms(),
            events,
            totals,
            threads: threads
                .live
                .iter()
                .map(|(name, totals)| (name.clone(), values(totals)))
                .chain([("exited".to_owned(), values(&threads.exited))])
                .collect(),
        }
    }

    /// Links to the other brokers and the number of connected clients. Empty
    /// until the frontend has bound.
    fn connections(&self) -> Connections {
        let Some(links) = self.hooks.links.get() else {
            return Connections {
                brokers: Vec::new(),
                clients: 0,
            };
        };
        Connections {
            brokers: self
                .broker
                .brokers
                .iter()
                .filter(|(_, id)| *id != self.broker.id)
                .map(|(_, id)| (*id, links.get(*id).is_some()))
                .collect(),
            clients: u32::try_from(links.established(Kind::Client)).unwrap_or(u32::MAX),
        }
    }

    /// Ask every shard for its partitions. Shards that do not answer within
    /// [`SHARD_ANSWER`] are counted as missing; a late answer is dropped by
    /// the next request, which has a new sequence number.
    async fn partitions(&mut self, sequence: u64) -> Reply {
        let uptime_ms = self.uptime_ms();
        let mut waiting = BTreeSet::new();
        for &shard in &self.shard_ids {
            let query = Message::from(sequence.to_be_bytes().to_vec());
            if self
                .shards
                .try_send_to(shard_identity(shard), query)
                .is_ok()
            {
                waiting.insert(shard);
            }
        }
        let missing = self.shard_ids.len() - waiting.len();
        let mut rows = Vec::new();
        let deadline = tokio::time::sleep(SHARD_ANSWER);
        tokio::pin!(deadline);
        while !waiting.is_empty() {
            let (shard, answer) = tokio::select! {
                () = &mut deadline => break,
                received = self.shards.recv_from() => match received {
                    Ok(received) => received,
                    Err(_) => break,
                },
            };
            let shard = self
                .shard_ids
                .iter()
                .copied()
                .find(|&id| shard_identity(id) == shard.as_ref());
            let answer = answer
                .iter()
                .next()
                .map(|bytes| admin::decode_reply(&bytes));
            if let (Some(shard), Some(Ok((id, Reply::Partitions(answer))))) = (shard, answer)
                && id == sequence
                && waiting.remove(&shard)
            {
                rows.extend(answer.rows);
            }
        }
        Reply::Partitions(Partitions {
            uptime_ms,
            missing_shards: (missing + waiting.len()) as u32,
            rows,
        })
    }
}

/// What the serving owners give the admin owner: the shards' lane, and the
/// frontend's link observations once it binds.
#[derive(Clone, Debug)]
pub(crate) struct Hooks {
    lane: Endpoint,
    links: Arc<OnceLock<Links>>,
}

impl Hooks {
    /// Hand over the frontend's links. Only the first call counts.
    pub(crate) fn publish_links(&self, links: Links) {
        let _ = self.links.set(links);
    }
}

/// A shard's end of the admin lane: it answers partition status queries.
#[derive(Debug)]
pub(crate) struct ShardLane(IdentitySocket);

impl ShardLane {
    /// Connect shard `id` to the admin owner's lane.
    pub(crate) fn connect(context: &Context, hooks: &Hooks, id: u32) -> Result<Self, StartupError> {
        let socket = peer(context, &shard_identity(id), 1 << 10);
        socket.connect(hooks.lane.clone()).map_err(failure)?;
        Ok(Self(
            socket.into_async().identity_routing().map_err(failure)?,
        ))
    }

    /// The next query's sequence number; `None` for a malformed query.
    /// Fails once the admin owner has gone.
    pub(crate) fn query(
        &self,
    ) -> impl Future<Output = Result<Option<u64>, omq_tokio::Error>> + use<> {
        let socket = self.0.clone_shared();
        async move {
            let (_, query) = socket.recv_from(None).await?;
            Ok(query
                .iter()
                .next()
                .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_ref()).ok())
                .map(u64::from_be_bytes))
        }
    }

    /// Answer query `sequence`. A full lane drops the answer; the admin owner
    /// then reports this shard as missing.
    pub(crate) fn answer(&self, sequence: u64, rows: Vec<admin::PartitionStatus>) {
        let reply = Reply::Partitions(Partitions {
            uptime_ms: 0,
            missing_shards: 0,
            rows,
        });
        if let Ok(bytes) = admin::encode_reply(sequence, &reply) {
            let _ = self.0.try_send_to(LANE_IDENTITY, Message::from(bytes));
        }
    }
}

/// One partition's admin row from its actor status, route, and committed
/// facts.
pub(crate) fn partition_status(
    group: GroupId,
    status: &PartitionStatus,
    route: &RouteState,
    facts: Option<&PartitionFacts>,
) -> admin::PartitionStatus {
    let mut row = admin::PartitionStatus {
        group,
        state: admin::PartitionState::Electing,
        ready: route.leader.is_some(),
        disk_pending: false,
        view: route.view,
        leader: route.leader,
        accepted: 0,
        committed: 0,
        applied: 0,
        pending_operations: 0,
        pending_body_bytes: 0,
        validation_cycles: 0,
        reclaimed_bytes: 0,
        written: 0,
        durable: 0,
        leader_commit: 0,
        followers: Vec::new(),
        log: facts.map(log),
    };
    match status {
        PartitionStatus::Local(snapshot) => {
            row.state = admin::PartitionState::Local;
            row.accepted = snapshot.accepted.op.0;
            row.committed = snapshot.committed.op.0;
            row.applied = snapshot.applied.op.0;
            row.pending_operations = snapshot.pending_operations as u64;
            row.pending_body_bytes = snapshot.pending_body_bytes as u64;
            row.written = snapshot.journal.written.0;
            row.durable = snapshot.journal.durable.0;
            row.leader_commit = snapshot.committed.op.0;
        }
        PartitionStatus::Replicated(status) => {
            row.ready = status.application_ready;
            row.disk_pending = status.disk_pending;
            row.validation_cycles = status.maintenance.validation_cycles;
            row.reclaimed_bytes = status.maintenance.reclaimed_bytes;
            if let Some(normal) = status.normal {
                row.state = admin::PartitionState::Normal;
                row.accepted = normal.accepted.op.0;
                row.committed = normal.committed.op.0;
                row.applied = normal.applied.op.0;
                row.pending_operations = normal.pending_operations as u64;
                row.pending_body_bytes = normal.pending_body_bytes as u64;
                row.written = normal.journal.written.0;
                row.durable = normal.journal.durable.0;
                row.leader_commit = normal.known_commit.0;
                row.followers = normal
                    .backups
                    .iter()
                    .flatten()
                    .map(|(id, acknowledged)| (*id, acknowledged.0))
                    .collect();
            }
        }
    }
    row
}

/// The admin view of a partition's committed state.
fn log(facts: &PartitionFacts) -> PartitionLog {
    PartitionLog {
        first_offset: facts.retained_from.get(),
        next_offset: facts.next_offset.get(),
        producers: u32::try_from(facts.producers).unwrap_or(u32::MAX),
        groups: facts
            .groups
            .iter()
            .map(|group| GroupStatus {
                id: group.group,
                name: group.name.clone(),
                revision: group.revision,
                members: group
                    .members
                    .iter()
                    .map(|member| (member.id, member.node))
                    .collect(),
            })
            .collect(),
        cursors: facts
            .sources
            .iter()
            .map(|source| GroupCursor {
                group: source.group,
                member: source.assignment.and_then(|assignment| assignment.member),
                offset: source.progress.map(ozzy_proto::Offset::get),
            })
            .collect(),
    }
}

fn broker(
    checked: &CheckedConfig,
    local: NodeId,
    max_clients: usize,
) -> Result<Broker, StartupError> {
    let partitions = checked
        .plan
        .partitions
        .iter()
        .map(|placement| {
            let partition = checked
                .identity
                .partition(&placement.topic, placement.partition)?;
            Ok(PartitionName {
                group: GroupId::from_bytes(*partition.group.as_bytes()),
                topic: placement.topic.clone(),
                partition: placement.partition,
                shard: placement.shard,
            })
        })
        .collect::<Result<_, StartupError>>()?;
    Ok(Broker {
        name: checked.plan.name.clone(),
        id: local,
        pid: std::process::id(),
        uptime_ms: 0,
        max_clients: max_clients as u32,
        shards: checked.plan.shards.len() as u32,
        brokers: checked
            .identity
            .brokers
            .iter()
            .map(|(name, id)| (name.clone(), NodeId::from_bytes(*id.as_bytes())))
            .collect(),
        partitions,
    })
}

/// Routing identity of shard `id` on the admin lane. OMQ reserves a leading
/// zero byte.
fn shard_identity(id: u32) -> [u8; 5] {
    let [a, b, c, d] = id.to_be_bytes();
    [b's', a, b, c, d]
}

/// A PEER named `identity` that accepts messages up to `bytes` long.
fn peer(context: &Context, identity: &[u8], bytes: usize) -> omq_tokio::blocking::Socket {
    context.blocking_socket(
        SocketType::Peer,
        ozzy_runtime::transport::socket_options()
            .identity(identity.to_vec())
            .max_message_size(bytes)
            .router_mandatory(true)
            .send_hwm(QUEUE)
            .recv_hwm(QUEUE)
            .linger(Duration::ZERO),
    )
}

fn failure(error: impl std::fmt::Display) -> StartupError {
    StartupError::Frontend(format!("admin endpoint: {error}"))
}

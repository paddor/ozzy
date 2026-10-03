mod admission;
mod build;
mod scheduler;
use build::build;

use super::{
    Binding, Registration,
    config::{CLIENTS, Config, WRITERS},
    failure,
    outbound::Outbound,
};
use crate::{JournalConfig, JournalPlan, ShardContext, StartupError};
use ozzy_proto::{GroupId, LinkSessionId, NodeId, append::Policy};
use ozzy_replication::JournalGeneration;
use ozzy_runtime::{
    dispatch::{self, Class},
    frontend::{self, Destination, IntakeMessage, Kind, ShardIntake},
    memory::Quota,
    replica_actor::{
        ActorIds, NativeAccess, NativeIntake, NativeIntakeConfig, NativeReceive, PartitionActors,
        PartitionStatus, PendingProposal, ProposalOutcome, ProposalSubmitter, RoutePublisher,
    },
    replica_journal::ProposalBuffer,
};
use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot};

const TIMER_INTERVAL: Duration = Duration::from_millis(2);

struct Bootstrap {
    group: GroupId,
    submitter: ProposalSubmitter,
    buffer: Option<ProposalBuffer>,
    pending: Option<Pin<Box<PendingProposal>>>,
    done: bool,
}

struct State {
    actors: PartitionActors,
    natives: Vec<NativeIntake>,
    identities: Vec<(GroupId, ozzy_proto::PartitionIncarnation)>,
    bootstrap: Vec<Option<Bootstrap>>,
    recoveries: BTreeMap<GroupId, crate::PartitionJournal>,
    admission: admission::ShardAdmission,
    destinations: Vec<Destination>,
    sessions: BTreeMap<(GroupId, NodeId), LinkSessionId>,
    /// Frontend link generation that every partition's sessions reflect.
    synced_links: Option<u64>,
    /// Partitions not yet compared with `synced_links`.
    unsynced: usize,
    routes: frontend::RoutingTable,
    publisher: Option<RoutePublisher>,
    outbound: Outbound,
    group_cursor: usize,
    indices: BTreeMap<GroupId, usize>,
    config: Arc<Config>,
}

pub(super) async fn run(
    mut shard: ShardContext,
    journals: Arc<JournalPlan>,
    config: Arc<Config>,
    registrations: mpsc::Sender<Registration>,
) -> Result<(), StartupError> {
    let budgets = config.budgets[&shard.plan.id];
    let (sender, intake) = ShardIntake::new(
        shard.memory.data.clone(),
        &shard.memory.control,
        dispatch::Limits {
            capacity: budgets,
            clients: config.peers,
            grants: config.grants,
        },
        (shard.partitions.len() * 4).max(1),
        3,
    )
    .map_err(failure)?;
    let mut state = Box::pin(build(&shard, &journals, config, intake)).await?;
    let routes = state
        .identities
        .iter()
        .map(|&(group, partition)| {
            state
                .actors
                .route_state(group, partition)
                .expect("known partition")
        })
        .collect();
    let result = async {
        let (handoff, binding) = oneshot::channel();
        tokio::select! {
            () = shard.shutdown.requested() => return Ok(()),
            result = registrations.send(Registration { id: shard.plan.id, sender, routes, handoff }) => result.map_err(failure)?,
        }
        let mut binding = tokio::select! {
            () = shard.shutdown.requested() => return Ok(()),
            result = binding => result.map_err(failure)?,
        };
        for native in state.natives.drain(..) {
            state.actors.install_native(native, binding.links.clone()).map_err(failure)?;
        }
        for (index, &(group, partition)) in state.identities.iter().enumerate() {
            if state.bootstrap[index].is_none() {
                continue;
            }
            state.actors.install_readers(group, ozzy_runtime::replica_actor::SharedReaderConfig {
                partition,
                limits: state.config.limits,
                subscriptions: CLIENTS,
            }, binding.links.clone()).map_err(failure)?;
        }
        state.publisher =
            Some(RoutePublisher::new(&state.actors, &state.identities, 16).map_err(failure)?);
        shard.ready()?;
        serve(&shard, &mut state, &mut binding).await
    }.await;
    drop(state.natives);
    drop(state.bootstrap);
    drop(state.admission);
    drop(state.outbound);
    let drained = state.actors.shutdown().await.map_err(failure);
    result.and(drained)
}

async fn serve(
    shard: &ShardContext,
    state: &mut State,
    binding: &mut Binding,
) -> Result<(), StartupError> {
    let start = Instant::now();
    let mut tick = tokio::time::interval(TIMER_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let link_generation = binding.links.generation();
        let request_generation = binding.requests.generation();
        let memory_generation = state.admission.intake.generation();
        let port_generation = binding.port.generation();
        let input = state.admission.intake.ready();
        let memory = state.admission.intake.changed_after(memory_generation);
        let port = binding.port.changed_after(port_generation);
        let requests = binding.requests.changed_after(request_generation);
        let links = binding.links.clone();
        tokio::select! {
            biased;
            () = shard.shutdown.requested() => return Ok(()),
            result = poll_fn(|cx| state.poll(cx, binding, start.elapsed())) => {
                if !shard.shutdown.is_requested() {
                    result?;
                }
            },
            () = input => {},
            () = memory => {},
            () = port => {},
            () = requests => {},
            () = links.changed_after(link_generation) => {},
            _ = tick.tick() => {},
        }
        if binding.requests.is_closed() {
            if shard.shutdown.is_requested() {
                return Ok(());
            }
            return Err(failure("dispatcher ended"));
        }
    }
}

fn timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

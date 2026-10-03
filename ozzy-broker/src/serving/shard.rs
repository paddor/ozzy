mod build;
mod scheduler;
use build::build;

use super::{
    Binding, Registration,
    config::{CLIENTS, Config, WRITER_WINDOW, WRITERS},
    failure,
    outbound::Outbound,
};
use crate::{JournalConfig, JournalPlan, ShardContext, StartupError};
use ozzy_proto::{GroupId, LinkSessionId, NodeId, append::Policy};
use ozzy_replication::JournalGeneration;
use ozzy_runtime::{
    dispatch::Class,
    frontend::{self, DataInput, DataReceiver, Kind},
    memory::Owner,
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
    incoming: [DataReceiver; 4],
    pending: [Option<DataInput>; 4],
    data_memory: Owner,
    replica_memory: Owner,
    control_memory: Owner,
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
    /// Reservation slots whose deferred input its actor can take now.
    config: Arc<Config>,
}

pub(super) async fn run(
    mut shard: ShardContext,
    journals: Arc<JournalPlan>,
    config: Arc<Config>,
    registrations: mpsc::Sender<Registration>,
    preflight: Arc<tokio::sync::Barrier>,
) -> Result<(), StartupError> {
    journals
        .inspect_recovery_intents(shard.plan.id, shard.io.clone())
        .await?;
    tokio::select! {
        () = shard.shutdown.requested() => return Ok(()),
        _ = preflight.wait() => {},
    }
    let budgets = config.budgets[&shard.plan.id];
    let lane = |kind, class| {
        let (slots, maximum, budget) = match class {
            Class::Data => config.data_lane(shard.plan.id, kind)?,
            Class::Control => {
                let maximum = config.buffers.retained_bytes_for_payload(0);
                (
                    budgets.control.queue_slots,
                    maximum,
                    budgets.control.bytes.max(maximum * 4) / 2,
                )
            }
        };
        frontend::data_channel(
            &config.omq,
            shard.plan.id,
            kind,
            class,
            slots,
            maximum,
            budget,
        )
        .map_err(failure)
    };
    let (data_sender, data) = lane(Kind::Client, Class::Data)?;
    let (replica_sender, replica) = lane(Kind::Broker, Class::Data)?;
    let (sender, control) = lane(Kind::Client, Class::Control)?;
    let (broker_control, replica_control) = lane(Kind::Broker, Class::Control)?;
    let mut state = Box::pin(build(
        &shard,
        &journals,
        config,
        [data, replica, control, replica_control],
    ))
    .await?;
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
            result = registrations.send(Registration { id: shard.plan.id, sender, broker_control, data: data_sender, replica: replica_sender, routes, handoff }) => result.map_err(failure)?,
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
        let port_generation = binding.port.generation();
        let [data, replica, control, replica_control] =
            std::array::from_fn(|index| state.incoming[index].ready());
        tokio::pin!(data, replica, control, replica_control);
        let port = binding.port.changed_after(port_generation);
        let links = binding.links.clone();
        tokio::select! {
            biased;
            () = shard.shutdown.requested() => return Ok(()),
            result = poll_fn(|cx| {
                if let Poll::Ready(result) = state.poll(cx, binding, start.elapsed()) {
                    return Poll::Ready(result);
                }
                // Progress can fill a deferred slot. Check it at each poll,
                // after progress, rather than using select's captured guard.
                for (lane, input) in [
                    (3, replica_control.as_mut()),
                    (2, control.as_mut()),
                    (1, replica.as_mut()),
                    (0, data.as_mut()),
                ] {
                    if state.pending[lane].is_none()
                        && let Poll::Ready(input) = input.poll(cx)
                    {
                        state.pending[lane] = Some(input.map_err(failure)?);
                        return Poll::Ready(Ok(()));
                    }
                }
                Poll::Pending
            }) => {
                if !shard.shutdown.is_requested() {
                    result?;
                }
            },
            () = port => {},
            () = links.changed_after(link_generation) => {},
            _ = tick.tick() => {},
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

mod build;
mod scheduler;
use build::build;

use super::{
    Binding, Registration,
    config::{COORDINATOR_PLANS, Config, WRITER_WINDOW, WRITERS},
    failure,
    outbound::Outbound,
};
use crate::{JournalConfig, JournalPlan, ShardContext, StartupError};
use ozzy_proto::{GroupId, LinkSessionId, NodeId, append::Policy};
use ozzy_replication::JournalGeneration;
use ozzy_runtime::{
    dispatch::Class,
    frontend::{self, DataInput, DataLaneLimits, DataReceiver, DataSender, Kind},
    memory::Owner,
    replica_actor::{
        ActorIds, GroupCoordinator, GroupCoordinatorConfig, NativeAccess, NativeIntake,
        NativeIntakeConfig, NativeReceive, PartitionActors, PartitionStatus, PendingProposal,
        ProposalOutcome, ProposalSubmitter, RoutePublisher, TopicTopology,
    },
    replica_journal::ProposalBuffer,
};
use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

/// Every partition is visited at least this often, as a safety net for a
/// wakeup nobody registered. Timers wake their partition when due.
const SAFETY_SWEEP: Duration = Duration::from_millis(100);

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
    coordinators: Vec<GroupCoordinator>,
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
    /// Partition status queries from the admin owner, when one runs.
    admin: Option<crate::admin::ShardLane>,
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
        () = shard.shutdown.closed() => return Ok(()),
        _ = preflight.wait() => {},
    }
    let (senders, incoming) = open_lanes(shard.plan.id, &config)?;
    let mut state = Box::pin(build(&shard, &journals, config, incoming)).await?;
    let routes = state.route_states()?;
    let (serving, observed) = tokio::sync::watch::channel(false);
    let result = async {
        let (handoff, binding) = oneshot::channel();
        let registration = Registration {
            id: shard.plan.id,
            serving: observed,
            sender: senders.control,
            broker_control: senders.broker_control,
            data: senders.data,
            replica: senders.replica,
            routes,
            handoff,
        };
        tokio::select! {
            () = shard.shutdown.closed() => return Ok(()),
            result = registrations.send(registration) => result.map_err(failure)?,
        }
        let mut binding = tokio::select! {
            () = shard.shutdown.closed() => return Ok(()),
            result = binding => result.map_err(failure)?,
        };
        state.install_services(&binding)?;
        shard.ready()?;
        serve(&shard, &mut state, &mut binding, &serving).await
    }
    .await;
    drop(state.natives);
    drop(state.coordinators);
    drop(state.bootstrap);
    drop(state.outbound);
    let drained = state.actors.shutdown().await.map_err(failure);
    result.and(drained)
}

/// Senders of the shard's four inbound lanes, registered with the frontend.
struct LaneSenders {
    data: DataSender,
    replica: DataSender,
    control: DataSender,
    broker_control: DataSender,
}

/// Open the client and broker data and control lanes of shard `id`. The
/// receivers are ordered data, replica, control, replica control.
fn open_lanes(id: u32, config: &Config) -> Result<(LaneSenders, [DataReceiver; 4]), StartupError> {
    let budgets = config.budget(id)?;
    let lane = |kind, class| {
        let limits = match class {
            Class::Data => config.data_lane(id, kind)?,
            Class::Control => {
                let maximum = config.buffers.retained_bytes_for_payload(0);
                DataLaneLimits {
                    slots: budgets.control.queue_slots,
                    maximum_retained_bytes: maximum,
                    retained_budget_bytes: budgets.control.bytes.max(maximum * 4) / 2,
                }
            }
        };
        frontend::data_channel(&config.omq, id, kind, class, limits).map_err(failure)
    };
    let (data, data_receiver) = lane(Kind::Client, Class::Data)?;
    let (replica, replica_receiver) = lane(Kind::Broker, Class::Data)?;
    let (control, control_receiver) = lane(Kind::Client, Class::Control)?;
    let (broker_control, replica_control) = lane(Kind::Broker, Class::Control)?;
    Ok((
        LaneSenders {
            data,
            replica,
            control,
            broker_control,
        },
        [
            data_receiver,
            replica_receiver,
            control_receiver,
            replica_control,
        ],
    ))
}

impl State {
    /// Each partition's route state, in partition order, for the frontend.
    fn route_states(&self) -> Result<Vec<ozzy_proto::directory::RouteState>, StartupError> {
        self.identities
            .iter()
            .map(|&(group, partition)| {
                self.actors
                    .route_state(group, partition)
                    .ok_or_else(|| failure("partition without route state"))
            })
            .collect()
    }

    /// Install the native intakes, coordinators, readers, and route publisher
    /// on the frontend links of `binding`.
    fn install_services(&mut self, binding: &Binding) -> Result<(), StartupError> {
        for native in self.natives.drain(..) {
            self.actors
                .install_native(native, binding.links.clone())
                .map_err(failure)?;
        }
        for coordinator in self.coordinators.drain(..) {
            self.actors
                .install_coordinator(coordinator, binding.links.clone())
                .map_err(failure)?;
        }
        for (index, &(group, partition)) in self.identities.iter().enumerate() {
            if self.bootstrap[index].is_none() {
                continue;
            }
            self.actors
                .install_readers(
                    group,
                    ozzy_runtime::replica_actor::SharedReaderConfig {
                        partition,
                        limits: self.config.limits,
                        subscriptions: self.config.clients,
                    },
                    binding.links.clone(),
                )
                .map_err(failure)?;
        }
        self.publisher =
            Some(RoutePublisher::new(&self.actors, &self.identities, 16).map_err(failure)?);
        Ok(())
    }

    /// Answer admin query `sequence` with every partition's status.
    fn answer_admin(&self, sequence: u64) {
        let Some(admin) = &self.admin else {
            return;
        };
        let rows = self
            .identities
            .iter()
            .filter_map(|&(group, partition)| {
                let status = self.actors.status(group)?;
                let route = self.actors.route_state(group, partition)?;
                let facts = self.actors.facts(group, partition);
                Some(crate::admin::partition_status(
                    group,
                    &status,
                    &route,
                    facts.as_ref(),
                ))
            })
            .collect();
        admin.answer(sequence, rows);
    }
}

async fn serve(
    shard: &ShardContext,
    state: &mut State,
    binding: &mut Binding,
    serving: &tokio::sync::watch::Sender<bool>,
) -> Result<(), StartupError> {
    let start = state.config.time.start();
    let mut alarm = start.alarm();
    loop {
        let link_generation = binding.links.generation();
        let port_generation = binding.port.generation();
        let [data, replica, control, replica_control] =
            std::array::from_fn(|index| state.incoming[index].ready());
        let admin = state.admin.as_ref().map(crate::admin::ShardLane::query);
        tokio::pin!(data, replica, control, replica_control, admin);
        let port = binding.port.changed_after(port_generation);
        let links = binding.links.clone();
        if !*serving.borrow() && state.serving() {
            serving.send_replace(true);
        }
        tokio::select! {
            biased;
            () = shard.shutdown.closed() => return Ok(()),
            result = poll_fn(|cx| {
                let now = start.elapsed();
                if let Poll::Ready(result) = state.poll(cx, binding, now) {
                    return Poll::Ready(result);
                }
                // Wake for the earliest timer the turn left.
                if alarm.poll_at(cx, state.next_deadline(now)).is_ready() {
                    return Poll::Ready(Ok(()));
                }
                if let Some(query) = admin.as_mut().as_pin_mut()
                    && let Poll::Ready(query) = query.poll(cx)
                {
                    match query {
                        Ok(Some(sequence)) => state.answer_admin(sequence),
                        Ok(None) => {}
                        // The admin owner stopped; stop listening.
                        Err(_) => state.admin = None,
                    }
                    return Poll::Ready(Ok(()));
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
                if !shard.shutdown.is_closed() {
                    result?;
                }
            },
            () = port => {},
            () = links.changed_after(link_generation) => {},
        }
    }
}

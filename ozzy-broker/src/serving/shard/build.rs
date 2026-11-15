use super::{
    ActorIds, Arc, BTreeMap, Bootstrap, COORDINATOR_PLANS, Config, DataReceiver, GroupCoordinator,
    GroupCoordinatorConfig, GroupId, JournalConfig, JournalGeneration, JournalPlan, NativeAccess,
    NativeIntake, NativeIntakeConfig, Outbound, PartitionActors, Policy, ShardContext,
    StartupError, State, TopicTopology, WRITER_WINDOW, WRITERS, failure, frontend,
};
use ozzy_runtime::replica_actor::PartitionActor;
use std::time::Duration;

mod recovered;
pub(super) use recovered::{RecoveredServices, recovered_log, recovered_services};

pub(super) async fn build(
    shard: &ShardContext,
    journals: &JournalPlan,
    config: Arc<Config>,
    incoming: [DataReceiver; 4],
) -> Result<State, StartupError> {
    let topologies = topologies(&config)?;
    let mut opening = Opening::new(shard, &config, &topologies);
    let opened = Box::pin(opening.open_all(journals)).await;
    if let Err(error) = opened {
        opening.shut_down().await;
        return Err(error);
    }
    let Opening {
        actors,
        natives,
        logs,
        handles,
        coordinators,
        bootstrap,
        recoveries,
        identities,
        ..
    } = opening;
    let routes = routing(shard.plan.id, &identities, &config)?;
    let runtime = runtime_logs(shard, journals, &config)?;
    let fixed = identities.len();
    let actors = scheduled(actors, fixed + log_capacity(journals), shard, &config)?;
    let indices = identities
        .iter()
        .enumerate()
        .map(|(index, &(group, _))| (group, index))
        .collect();
    Ok(State {
        actors,
        natives,
        logs,
        handles,
        coordinators,
        identities,
        bootstrap,
        recoveries,
        incoming,
        pending: std::array::from_fn(|_| None),
        replica_memory: shard.memory.replica.clone(),
        data_memory: shard.memory.data.clone(),
        control_memory: shard.memory.control.clone(),
        sessions: BTreeMap::new(),
        synced_links: None,
        unsynced: 0,
        synced_clock: None,
        routes,
        publisher: None,
        outbound: outbound(&config),
        indices,
        group_cursor: 0,
        admin: admin_lane(&config, shard.plan.id)?,
        grants: config.reader_grants.shard(shard.plan.id),
        shard: shard.plan.id,
        io: shard.io.clone(),
        fixed,
        runtime,
        config,
    })
}

/// The shard's lane to the admin owner, when one runs.
fn admin_lane(
    config: &Config,
    shard: u32,
) -> Result<Option<crate::admin::ShardLane>, StartupError> {
    config
        .admin
        .as_ref()
        .map(|admin| crate::admin::ShardLane::connect(&config.omq, admin, shard))
        .transpose()
}

/// Runtime logs the shard must have room for beside its partitions.
fn log_capacity(journals: &JournalPlan) -> usize {
    journals.logs.as_ref().map_or(0, |logs| logs.class.max_logs)
}

/// The shard's runtime log state, when the deployment has a log class.
fn runtime_logs(
    shard: &ShardContext,
    journals: &JournalPlan,
    config: &Config,
) -> Result<Option<super::logs::RuntimeLogs>, StartupError> {
    let Some(setup) = &journals.logs else {
        return Ok(None);
    };
    let commands = config
        .take_log_commands(shard.plan.id)
        .ok_or_else(|| failure("shard without a log command lane"))?;
    super::logs::RuntimeLogs::new(
        setup.clone(),
        &shard.plan.device,
        commands,
        config,
        &shard.io,
    )
    .map(Some)
}

/// Opens the shard's partitions. Holds the services opened so far, in
/// partition order, and the service recipes kept for later rejoins.
struct Opening<'a> {
    shard: &'a ShardContext,
    config: &'a Config,
    topologies: &'a BTreeMap<String, Arc<TopicTopology>>,
    actors: Vec<PartitionActor>,
    natives: Vec<NativeIntake>,
    logs: Vec<super::LogInstall>,
    handles: Vec<(String, u32, ozzy_runtime::log::Log)>,
    coordinators: Vec<GroupCoordinator>,
    bootstrap: Vec<Option<Bootstrap>>,
    recoveries: BTreeMap<GroupId, crate::PartitionJournal>,
    identities: Vec<(GroupId, ozzy_proto::PartitionIncarnation)>,
}

impl<'a> Opening<'a> {
    fn new(
        shard: &'a ShardContext,
        config: &'a Config,
        topologies: &'a BTreeMap<String, Arc<TopicTopology>>,
    ) -> Self {
        Self {
            shard,
            config,
            topologies,
            actors: Vec::new(),
            natives: Vec::new(),
            logs: Vec::new(),
            handles: Vec::new(),
            coordinators: Vec::new(),
            bootstrap: Vec::new(),
            recoveries: BTreeMap::new(),
            identities: Vec::new(),
        }
    }

    /// Open every partition placed on this shard. A failed partition keeps
    /// its actor here so the caller can shut it down with the others.
    async fn open_all(&mut self, journals: &JournalPlan) -> Result<(), StartupError> {
        let shard = self.shard.plan.id;
        for plan in journals
            .partitions
            .iter()
            .filter(|partition| partition.placement.shard == shard)
        {
            let (policy, _, group) = profile(&plan.config);
            if policy != ozzy_proto::append::Policy::LocalDurable {
                // Normal copies can withdraw after falling below retained
                // history. Keep their service recipe for every later rejoin.
                self.recoveries.insert(group, plan.clone());
            }
            let port = self.open_log(plan, group)?;
            if let Some(&intent) = journals.recovery.get(&group) {
                self.open_recovering(plan, intent).await?;
                self.logs.push(super::LogInstall {
                    group,
                    port,
                    lane: None,
                });
            } else {
                self.open_serving(plan, group, port).await?;
            }
            self.identities.push((group, plan.incarnation));
        }
        Ok(())
    }

    /// A partition under nonvoting recovery serves nothing until it rejoins.
    async fn open_recovering(
        &mut self,
        plan: &crate::PartitionJournal,
        intent: crate::RecoveryIntent,
    ) -> Result<(), StartupError> {
        let mut actor = open_recovery(self.shard, plan, intent, self.config).await?;
        if let Err(error) = actor.bind_receive_owner(&self.shard.memory.replica) {
            self.actors.push(actor);
            return Err(failure(error));
        }
        self.bootstrap.push(None);
        self.actors.push(actor);
        Ok(())
    }

    /// The handle and port of `plan`'s log. The handle waits for the
    /// shard's registration.
    fn open_log(
        &mut self,
        plan: &crate::PartitionJournal,
        group: GroupId,
    ) -> Result<ozzy_runtime::log::LogPort, StartupError> {
        let (_, body_bytes, _) = profile(&plan.config);
        let limits = super::super::config::log_limits(body_bytes);
        let (log, port) = ozzy_runtime::log::log_channel(plan.incarnation, limits)
            .ok_or_else(|| failure(format!("invalid log limits for group {group}")))?;
        self.handles
            .push((plan.placement.topic.clone(), plan.placement.partition, log));
        Ok(port)
    }

    /// A serving partition with its native intake, log, coordinator, and
    /// bootstrap.
    async fn open_serving(
        &mut self,
        plan: &crate::PartitionJournal,
        group: GroupId,
        port: ozzy_runtime::log::LogPort,
    ) -> Result<(), StartupError> {
        let mut started = Box::pin(open_actor(self.shard, plan, self.config)).await?;
        let limits = port.limits();
        match initialize_started(&mut started, self.shard, self.config, plan, self.topologies)
            .and_then(|services| {
                let lane = ozzy_runtime::replica_actor::LogLane::new(&mut started.actor, limits)
                    .map_err(failure)?;
                Ok((services, lane))
            }) {
            Ok(((native, coordinator), lane)) => {
                self.natives.push(native);
                self.coordinators.push(coordinator);
                self.logs.push(super::LogInstall {
                    group,
                    port,
                    lane: Some(lane),
                });
            }
            Err(error) => {
                self.actors.push(started.actor);
                return Err(error);
            }
        }
        self.bootstrap.push(Some(Bootstrap {
            group,
            submitter: started.proposal,
            buffer: Some(started.buffer),
            pending: None,
            done: false,
        }));
        self.actors.push(started.actor);
        Ok(())
    }

    /// Release the services before their actors, then drain every actor.
    async fn shut_down(self) {
        drop(self.natives);
        drop(self.logs);
        drop(self.coordinators);
        drop(self.bootstrap);
        for actor in self.actors {
            let _ = actor.shutdown().await;
        }
    }
}

fn initialize_started(
    started: &mut crate::StartedPartition,
    shard: &ShardContext,
    config: &Config,
    plan: &crate::PartitionJournal,
    topologies: &BTreeMap<String, Arc<TopicTopology>>,
) -> Result<(NativeIntake, GroupCoordinator), StartupError> {
    let (policy, _, group) = profile(&plan.config);
    started
        .actor
        .bind_receive_owner(&shard.memory.replica)
        .map_err(failure)?;
    started.actor.enable_publication();
    started.prepare_partition()?;
    let native = native_config(config, plan, group, policy);
    let native = native_intake(
        &mut started.actor,
        native,
        &shard.memory.data,
        &shard.memory.control,
    )?;
    let coordinator = coordinator_service(
        &mut started.actor,
        config,
        plan,
        topologies,
        &shard.memory.control,
    )?;
    Ok((native, coordinator))
}

fn scheduled(
    actors: Vec<ozzy_runtime::replica_actor::PartitionActor>,
    partitions: usize,
    shard: &ShardContext,
    config: &Config,
) -> Result<PartitionActors, StartupError> {
    let actors = PartitionActors::new(actors, partitions.max(1), 16)
        .and_then(|actors| actors.with_safety_sweep(super::SAFETY_SWEEP))
        .map_err(failure)?;
    Ok(match &config.beats {
        Some(beats) => actors.with_beats(beats.clone(), shard.plan.id),
        None => actors,
    })
}

fn outbound(config: &Config) -> Outbound {
    Outbound::new(
        config.peers,
        config.transport.message_bytes,
        config.envelope,
    )
}

fn routing(
    shard: u32,
    identities: &[(GroupId, ozzy_proto::PartitionIncarnation)],
    config: &Config,
) -> Result<frontend::RoutingTable, StartupError> {
    let placements = identities
        .iter()
        .map(|&(group, partition)| frontend::Placement {
            group,
            partition,
            shard,
        })
        .collect::<Vec<_>>();
    frontend::RoutingTable::new(
        &[shard],
        &placements,
        identities.len().max(1),
        config.envelope,
    )
    .map(|table| table.with_runtime(config.runtime_routes.clone()))
    .map_err(failure)
}

fn profile(config: &JournalConfig) -> (Policy, usize, GroupId) {
    match config {
        JournalConfig::Local(config) => (
            Policy::LocalDurable,
            config.append_limits.max_body_bytes,
            config.identity.group_id,
        ),
        JournalConfig::Replicated(config) => (
            Policy::QuorumDurable,
            config.append_limits.max_body_bytes,
            config.identity.group_id,
        ),
    }
}

async fn open_actor(
    shard: &ShardContext,
    plan: &crate::PartitionJournal,
    config: &Config,
) -> Result<crate::StartedPartition, StartupError> {
    let opened = tokio::select! {
        () = shard.shutdown.closed() => return Err(failure("partition startup stopped")),
        opened = plan.clone().open(shard.io.clone(), JournalGeneration(uuid::Uuid::now_v7().as_u128())) => opened?,
    };
    opened.into_actor(
        &shard.memory.data,
        &BTreeMap::new(),
        ActorIds::random(config.receipt_handles.clone()),
        config.journal_clock(),
    )
}

async fn open_recovery(
    shard: &ShardContext,
    plan: &crate::PartitionJournal,
    intent: crate::RecoveryIntent,
    config: &Config,
) -> Result<ozzy_runtime::replica_actor::PartitionActor, StartupError> {
    let generations = ozzy_runtime::replica_journal::OwnedRecoveryGenerations {
        attempt: JournalGeneration(uuid::Uuid::now_v7().as_u128()),
        temporary: JournalGeneration(uuid::Uuid::now_v7().as_u128()),
    };
    let opened = tokio::select! {
        () = shard.shutdown.closed() => return Err(failure("partition recovery startup stopped")),
        opened = plan.clone().recover(shard.io.clone(), generations, intent) => opened?,
    };
    opened.into_actor(
        &shard.memory.data,
        &BTreeMap::new(),
        ActorIds::random(config.receipt_handles.clone()),
        config.journal_clock(),
    )
}

/// Immutable topic shapes shared by every coordinator of a topic.
fn topologies(config: &Config) -> Result<BTreeMap<String, Arc<TopicTopology>>, StartupError> {
    let mut topologies: BTreeMap<String, (u64, Vec<(u32, ozzy_proto::PartitionIncarnation)>)> =
        BTreeMap::new();
    for page in &config.catalog {
        let entry = topologies
            .entry(page.name.clone())
            .or_insert_with(|| (page.partitioner_seed, Vec::new()));
        if entry.0 != page.partitioner_seed {
            return Err(failure("topic pages disagree on the partitioner seed"));
        }
        entry.1.extend(
            page.partitions
                .iter()
                .map(|partition| (partition.number, partition.incarnation)),
        );
    }
    Ok(topologies
        .into_iter()
        .map(|(name, (seed, mut partitions))| {
            partitions.sort_by_key(|(number, _)| *number);
            (name, Arc::new(TopicTopology { seed, partitions }))
        })
        .collect())
}

fn coordinator_service(
    actor: &mut ozzy_runtime::replica_actor::PartitionActor,
    config: &Config,
    plan: &crate::PartitionJournal,
    topologies: &BTreeMap<String, Arc<TopicTopology>>,
    control: &ozzy_runtime::memory::Owner,
) -> Result<GroupCoordinator, StartupError> {
    let (_, body_bytes, group) = profile(&plan.config);
    let topology = topologies
        .get(&plan.placement.topic)
        .ok_or_else(|| failure("partition topic is missing from the catalog"))?
        .clone();
    let fixed = 341 + 122 * topology.partitions.len();
    let max_members = body_bytes
        .saturating_sub(fixed)
        .checked_div(48)
        .unwrap_or(0)
        .clamp(1, ozzy_proto::group::MAX_MEMBERS);
    let coordinator = GroupCoordinatorConfig {
        local: config.local,
        group,
        partition: plan.incarnation,
        number: plan.placement.partition,
        topology,
        limits: config.envelope,
        requests: config.clients,
        plans: COORDINATOR_PLANS,
        max_members,
        membership_timeout: Duration::from_secs(10),
        drain_deadline: Duration::from_secs(5),
    };
    let arenas = (0..COORDINATOR_PLANS)
        .map(|_| {
            let mut arena = actor
                .lease_proposal_buffer_with_limits(ozzy_replication::PipelineLimits {
                    max_operations: 1,
                    max_body_bytes: coordinator.plan_bytes(),
                })
                .map_err(failure)?;
            arena.bind_owner(control).map_err(failure)?;
            Ok(arena)
        })
        .collect::<Result<Vec<_>, StartupError>>()?;
    let submitter = actor
        .take_submitter()
        .ok_or_else(|| failure("missing coordinator proposal lane"))?;
    GroupCoordinator::new(coordinator, submitter, arenas).map_err(failure)
}

fn native_intake(
    actor: &mut ozzy_runtime::replica_actor::PartitionActor,
    native: NativeIntakeConfig,
    data_memory: &ozzy_runtime::memory::Owner,
    control: &ozzy_runtime::memory::Owner,
) -> Result<NativeIntake, StartupError> {
    let count = native
        .required_buffers()
        .ok_or_else(|| failure("native arena overflow"))?;
    // Each writer's slots start with its writer-open arena.
    let stride = native.requests_per_writer + 1;
    let buffers = (0..count)
        .map(|index| {
            let mut buffer = actor
                .lease_proposal_buffer_with_limits(
                    native
                        .buffer_limits(index)
                        .ok_or_else(|| failure("native arena slot outside its layout"))?,
                )
                .map_err(failure)?;
            if index >= WRITERS * stride || index.is_multiple_of(stride) {
                buffer.bind_owner(control).map_err(failure)?;
            } else {
                buffer.bind_owner(data_memory).map_err(failure)?;
            }
            Ok(buffer)
        })
        .collect::<Result<Vec<_>, StartupError>>()?;
    let submitter = actor
        .take_submitter()
        .ok_or_else(|| failure("missing native proposal lane"))?;
    NativeIntake::new(native, submitter, buffers).map_err(failure)
}

/// Consumer progress updates one partition proposal carries at most.
const PROGRESS_BATCH: usize = 16;

/// A batch must fit the actor's live window and, replicated, one packet.
fn progress_batch(plan: &crate::PartitionJournal) -> usize {
    let window = match &plan.config {
        JournalConfig::Local(config) => config.append_limits.max_operations,
        JournalConfig::Replicated(config) => config.append_limits.max_operations,
    };
    let packet = match &plan.actors {
        crate::ActorSettings::Replicated { actor, .. } => actor.transfer.max_operations,
        crate::ActorSettings::Local(_) => window,
    };
    PROGRESS_BATCH.min(window).min(packet)
}

fn native_config(
    config: &Config,
    plan: &crate::PartitionJournal,
    group: GroupId,
    policy: Policy,
) -> NativeIntakeConfig {
    NativeIntakeConfig {
        local: config.local,
        group,
        partition: plan.incarnation,
        policy,
        access: NativeAccess::TrustedClients {
            clients: config.clients,
            writers: WRITERS,
        },
        limits: config.limits,
        reserve_dedup_results: plan.placement.deduplication.is_some(),
        requests_per_writer: WRITER_WINDOW,
        progress_requests: config.clients,
        progress_batch: progress_batch(plan),
        turn_slots: 16,
    }
}

use super::parts::{
    log_channel, log_lane, prepare_serving, profile, recovering_actor, serving_actor,
};
use super::{
    Arc, BTreeMap, Bootstrap, COORDINATOR_PLANS, Config, DataReceiver, GroupCoordinator,
    GroupCoordinatorConfig, GroupId, JournalConfig, JournalPlan, Lanes, NativeAccess, NativeIntake,
    NativeIntakeConfig, Outbound, PartitionActors, Policy, Services, ShardContext, ShardMemory,
    StartupError, State, Synced, TopicTopology, WRITER_WINDOW, WRITERS, failure, frontend,
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
        services,
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
        services,
        identities,
        bootstrap,
        recoveries,
        lanes: Lanes {
            incoming,
            pending: std::array::from_fn(|_| None),
        },
        memory: ShardMemory {
            data: shard.memory.data.clone(),
            replica: shard.memory.replica.clone(),
            control: shard.memory.control.clone(),
        },
        synced: Synced::default(),
        routes,
        publisher: None,
        outbound: outbound(&config),
        indices,
        group_cursor: 0,
        admin: admin_lane(&config, shard.plan.id)?,
        grants: config.reader_grants.shard(shard.plan.id),
        shard: shard.plan.id,
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
    services: Services,
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
            services: Services::default(),
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
            let port = self.open_log(plan)?;
            if let Some(&intent) = journals.recovery.get(&group) {
                self.open_recovering(plan, intent).await?;
                self.services.logs.push(super::LogInstall {
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
    ) -> Result<ozzy_runtime::log::LogPort, StartupError> {
        let (log, port) = log_channel(plan)?;
        let placement = &plan.placement;
        self.services
            .handles
            .push((placement.topic.clone(), placement.partition, log));
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
        match initialize_started(&mut started, self.shard, self.config, plan, self.topologies)
            .and_then(|services| Ok((services, log_lane(&mut started.actor, plan)?)))
        {
            Ok(((native, coordinator), lane)) => {
                self.services.natives.push(native);
                self.services.coordinators.push(coordinator);
                self.services.logs.push(super::LogInstall {
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
        let bootstrap = Bootstrap::new(group, started.proposal, started.buffer);
        self.bootstrap.push(Some(bootstrap));
        self.actors.push(started.actor);
        Ok(())
    }

    /// Release the services before their actors, then drain every actor.
    async fn shut_down(self) {
        // The handles go last, once the actors drained.
        let Services {
            natives,
            logs,
            coordinators,
            handles: _handles,
        } = self.services;
        drop(natives);
        drop(logs);
        drop(coordinators);
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
    prepare_serving(started, &shard.memory)?;
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
    .map(|table| table.with_runtime(config.logs.routes.clone()))
    .map_err(failure)
}

async fn open_actor(
    shard: &ShardContext,
    plan: &crate::PartitionJournal,
    config: &Config,
) -> Result<crate::StartedPartition, StartupError> {
    let opened = tokio::select! {
        () = shard.shutdown.closed() => return Err(failure("partition startup stopped")),
        opened = plan.clone().open(shard.io.clone(), crate::journals::generation()) => opened?,
    };
    serving_actor(opened, &shard.memory, config)
}

async fn open_recovery(
    shard: &ShardContext,
    plan: &crate::PartitionJournal,
    intent: crate::RecoveryIntent,
    config: &Config,
) -> Result<ozzy_runtime::replica_actor::PartitionActor, StartupError> {
    let generations = crate::journals::recovery_generations();
    let opened = tokio::select! {
        () = shard.shutdown.closed() => return Err(failure("partition recovery startup stopped")),
        opened = plan.clone().recover(shard.io.clone(), generations, intent) => opened?,
    };
    recovering_actor(opened, &shard.memory, config)
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

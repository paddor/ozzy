use super::{
    ActorIds, Arc, BTreeMap, Bootstrap, CLIENTS, Config, DataReceiver, GroupId, JournalConfig,
    JournalGeneration, JournalPlan, NativeAccess, NativeIntake, NativeIntakeConfig, Outbound,
    PartitionActors, Policy, ShardContext, StartupError, State, WRITER_WINDOW, WRITERS, failure,
    frontend, timestamp,
};

pub(super) async fn build(
    shard: &ShardContext,
    journals: &JournalPlan,
    config: Arc<Config>,
    incoming: [DataReceiver; 4],
) -> Result<State, StartupError> {
    let mut actors = Vec::new();
    let mut natives = Vec::new();
    let mut bootstrap = Vec::new();
    let mut recoveries = BTreeMap::new();
    let mut identities = Vec::new();
    let initialized = Box::pin(async {
        for plan in journals
            .partitions
            .iter()
            .filter(|partition| partition.placement.shard == shard.plan.id)
        {
            let (policy, _, group) = profile(&plan.config);
            if let Some(&intent) = journals.recovery.get(&group) {
                let mut actor = open_recovery(shard, plan, intent).await?;
                if let Err(error) = actor.bind_receive_owner(&shard.memory.replica) {
                    actors.push(actor);
                    return Err(failure(error));
                }
                identities.push((group, plan.incarnation));
                bootstrap.push(None);
                recoveries.insert(group, plan.clone());
                actors.push(actor);
                continue;
            }
            let mut started = Box::pin(open_actor(shard, plan)).await?;
            let initialized = (|| {
                started
                    .actor
                    .bind_receive_owner(&shard.memory.replica)
                    .map_err(failure)?;
                started.actor.enable_publication();
                started.prepare_partition()?;
                let native = native_config(&config, group, plan.incarnation, policy);
                native_intake(
                    &mut started.actor,
                    native,
                    &shard.memory.data,
                    &shard.memory.control,
                )
            })();
            match initialized {
                Ok(native) => natives.push(native),
                Err(error) => {
                    actors.push(started.actor);
                    return Err(error);
                }
            }
            identities.push((group, plan.incarnation));
            bootstrap.push(Some(Bootstrap {
                group,
                submitter: started.proposal,
                buffer: Some(started.buffer),
                pending: None,
                done: false,
            }));
            actors.push(started.actor);
        }
        Ok(())
    })
    .await;
    if let Err(error) = initialized {
        drop(natives);
        drop(bootstrap);
        for actor in actors {
            let _ = actor.shutdown().await;
        }
        return Err(error);
    }
    let routes = routing(shard.plan.id, &identities, config.envelope)?;
    let actors = scheduled(actors, identities.len())?;
    let indices = identities
        .iter()
        .enumerate()
        .map(|(index, &(group, _))| (group, index))
        .collect();
    Ok(State {
        actors,
        natives,
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
        routes,
        publisher: None,
        outbound: outbound(&config),
        indices,
        group_cursor: 0,
        config,
    })
}

fn scheduled(
    actors: Vec<ozzy_runtime::replica_actor::PartitionActor>,
    partitions: usize,
) -> Result<PartitionActors, StartupError> {
    // Partition timers start at 10 ms. Visit idle partitions often enough
    // to observe them at most 2 ms late.
    PartitionActors::new(actors, partitions.max(1), 16)
        .and_then(|actors| actors.with_timer_interval(super::TIMER_INTERVAL))
        .map_err(failure)
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
    envelope: ozzy_proto::EnvelopeLimits,
) -> Result<frontend::RoutingTable, StartupError> {
    let placements = identities
        .iter()
        .map(|&(group, partition)| frontend::Placement {
            group,
            partition,
            shard,
        })
        .collect::<Vec<_>>();
    frontend::RoutingTable::new(&[shard], &placements, identities.len().max(1), envelope)
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
            config.configuration.configuration().append_policy(),
            config.append_limits.max_body_bytes,
            config.identity.group_id,
        ),
    }
}

async fn open_actor(
    shard: &ShardContext,
    plan: &crate::PartitionJournal,
) -> Result<crate::StartedPartition, StartupError> {
    let opened = tokio::select! {
        () = shard.shutdown.requested() => return Err(failure("partition startup stopped")),
        opened = plan.clone().open(shard.io.clone(), JournalGeneration(uuid::Uuid::now_v7().as_u128())) => opened?,
    };
    opened.into_actor(
        &shard.memory.data,
        &BTreeMap::new(),
        ActorIds::random(),
        timestamp,
    )
}

async fn open_recovery(
    shard: &ShardContext,
    plan: &crate::PartitionJournal,
    intent: crate::RecoveryIntent,
) -> Result<ozzy_runtime::replica_actor::PartitionActor, StartupError> {
    let generations = ozzy_runtime::replica_journal::OwnedRecoveryGenerations {
        attempt: JournalGeneration(uuid::Uuid::now_v7().as_u128()),
        temporary: JournalGeneration(uuid::Uuid::now_v7().as_u128()),
    };
    let opened = tokio::select! {
        () = shard.shutdown.requested() => return Err(failure("partition recovery startup stopped")),
        opened = plan.clone().recover(shard.io.clone(), generations, intent) => opened?,
    };
    opened.into_actor(
        &shard.memory.data,
        &BTreeMap::new(),
        ActorIds::random(),
        timestamp,
    )
}

pub(super) fn recovered_services(
    actor: &mut ozzy_runtime::replica_actor::PartitionActor,
    plan: &crate::PartitionJournal,
    config: &Config,
    data_memory: &ozzy_runtime::memory::Owner,
    control: &ozzy_runtime::memory::Owner,
) -> Result<(Bootstrap, NativeIntake), StartupError> {
    let (policy, _, group) = profile(&plan.config);
    let crate::ActorSettings::Replicated {
        actor: settings, ..
    } = &plan.actors
    else {
        return Err(failure("recovered services require replicated settings"));
    };
    let limits = settings.transfer;
    let mut buffer = actor
        .lease_proposal_buffer_with_limits(limits)
        .map_err(failure)?;
    crate::actors::prepare_partition(&mut buffer, plan.cluster, &plan.placement, plan.incarnation)?;
    let submitter = actor
        .take_submitter()
        .ok_or_else(|| failure("missing recovered bootstrap lane"))?;
    let native = native_intake(
        actor,
        native_config(config, group, plan.incarnation, policy),
        data_memory,
        control,
    )?;
    Ok((
        Bootstrap {
            group,
            submitter,
            buffer: Some(buffer),
            pending: None,
            done: false,
        },
        native,
    ))
}

fn native_intake(
    actor: &mut ozzy_runtime::replica_actor::PartitionActor,
    native: NativeIntakeConfig,
    data_memory: &ozzy_runtime::memory::Owner,
    control: &ozzy_runtime::memory::Owner,
) -> Result<NativeIntake, StartupError> {
    let count = native
        .access
        .required_buffers(native.requests_per_writer)
        .ok_or_else(|| failure("native arena overflow"))?;
    // Each writer's slots start with its writer-open arena.
    let stride = native.requests_per_writer + 1;
    let buffers = (0..count)
        .map(|index| {
            let mut buffer = actor
                .lease_proposal_buffer_with_limits(
                    native.buffer_limits(index).expect("fixed native slot"),
                )
                .map_err(failure)?;
            if index < WRITERS * stride && index.is_multiple_of(stride) {
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

fn native_config(
    config: &Config,
    group: GroupId,
    partition: ozzy_proto::PartitionIncarnation,
    policy: Policy,
) -> NativeIntakeConfig {
    NativeIntakeConfig {
        local: config.local,
        group,
        partition,
        policy,
        access: NativeAccess::TrustedClients {
            clients: CLIENTS,
            writers: WRITERS,
        },
        limits: config.limits,
        requests_per_writer: WRITER_WINDOW,
        turn_slots: 16,
    }
}

use super::{
    ActorIds, Arc, BTreeMap, Bootstrap, CLIENTS, Class, Config, Destination, GroupId,
    JournalConfig, JournalGeneration, JournalPlan, Kind, NativeAccess, NativeIntake,
    NativeIntakeConfig, Outbound, PartitionActors, Policy, Quota, ShardContext, ShardIntake,
    StartupError, State, WRITERS, admission::ShardAdmission, failure, frontend, timestamp,
};

#[expect(
    clippy::too_many_lines,
    reason = "keep async initialization and partial-owner drain together"
)]
pub(super) async fn build(
    shard: &ShardContext,
    journals: &JournalPlan,
    config: Arc<Config>,
    mut intake: ShardIntake,
) -> Result<State, StartupError> {
    let mut actors = Vec::new();
    let mut natives = Vec::new();
    let mut bootstrap = Vec::new();
    let mut recoveries = BTreeMap::new();
    let mut identities = Vec::new();
    let mut destinations = Vec::new();
    let initialized = Box::pin(async {
        for plan in journals
            .partitions
            .iter()
            .filter(|partition| partition.placement.shard == shard.plan.id)
        {
            let (policy, body, group) = profile(&plan.config);
            let [data, control, replica, replica_control] =
                intake_destinations(&mut intake, group, body)?;
            if let Some(&intent) = journals.recovery.get(&group) {
                let mut actor = open_recovery(shard, plan, intent).await?;
                if let Err(error) = actor.bind_receive_capacity(replica.capacity()) {
                    actors.push(actor);
                    return Err(failure(error));
                }
                identities.push((group, plan.incarnation));
                bootstrap.push(None);
                recoveries.insert(group, plan.clone());
                actors.push(actor);
                destinations.extend([data, control, replica, replica_control]);
                continue;
            }
            let mut started = Box::pin(open_actor(shard, plan)).await?;
            let initialized = (|| {
                started
                    .actor
                    .bind_receive_capacity(replica.capacity())
                    .map_err(failure)?;
                started.prepare_partition()?;
                let native = native_config(&config, group, plan.incarnation, policy);
                native_intake(&mut started.actor, native, &data, &control)
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
            destinations.extend([data, control, replica, replica_control]);
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
    let admission = ShardAdmission::new(
        intake,
        identities
            .iter()
            .enumerate()
            .map(|(index, &(group, partition))| {
                (
                    frontend::Placement {
                        group,
                        partition,
                        shard: shard.plan.id,
                    },
                    destinations[index * 4 + 2].clone(),
                )
            })
            .collect(),
        config.budgets[&shard.plan.id],
        config.buffers,
    );
    Ok(State {
        actors,
        natives,
        identities,
        bootstrap,
        recoveries,
        admission,
        destinations,
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
    data: &Destination,
    control: &Destination,
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
        data,
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

fn intake_destinations(
    intake: &mut ShardIntake,
    group: GroupId,
    body: usize,
) -> Result<[Destination; 4], StartupError> {
    let data = Quota {
        bytes: body
            .checked_mul(2)
            .ok_or_else(|| failure("intake byte overflow"))?,
        buffers: 2,
    };
    Ok([
        intake
            .destination(group, Kind::Client, Class::Data, data)
            .map_err(failure)?,
        intake
            .destination(
                group,
                Kind::Client,
                Class::Control,
                Quota {
                    bytes: 130,
                    buffers: 2,
                },
            )
            .map_err(failure)?,
        intake
            .destination(group, Kind::Broker, Class::Data, data)
            .map_err(failure)?,
        intake
            .destination(group, Kind::Broker, Class::Control, Quota::default())
            .map_err(failure)?,
    ])
}

fn native_intake(
    actor: &mut ozzy_runtime::replica_actor::PartitionActor,
    native: NativeIntakeConfig,
    data: &Destination,
    control: &Destination,
) -> Result<NativeIntake, StartupError> {
    let count = native
        .access
        .required_buffers(1)
        .ok_or_else(|| failure("native arena overflow"))?;
    let buffers = (0..count)
        .map(|index| {
            let mut buffer = actor
                .lease_proposal_buffer_with_limits(
                    native.buffer_limits(index).expect("fixed native slot"),
                )
                .map_err(failure)?;
            buffer
                .bind_capacity(if index < WRITERS * 2 && index.is_multiple_of(2) {
                    control.capacity()
                } else {
                    data.capacity()
                })
                .map_err(failure)?;
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
        requests_per_writer: 1,
        turn_slots: 16,
    }
}

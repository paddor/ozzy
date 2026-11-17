//! The services a partition gets back when it rejoins after recovery.

use super::{
    Bootstrap, Config, GroupCoordinator, GroupId, NativeIntake, StartupError, coordinator_service,
    failure, native_config, native_intake, profile, topologies,
};

pub(in super::super) fn recovered_services(
    actor: &mut ozzy_runtime::replica_actor::PartitionActor,
    plan: &crate::PartitionJournal,
    config: &Config,
    data_memory: &ozzy_runtime::memory::Owner,
    control: &ozzy_runtime::memory::Owner,
) -> Result<RecoveredServices, StartupError> {
    let (policy, body_bytes, group) = profile(&plan.config);
    let crate::ActorSettings::Replicated {
        actor: settings, ..
    } = &plan.actors
    else {
        return Err(failure("recovered services require replicated settings"));
    };
    let bootstrap = recovered_bootstrap(actor, plan, settings.transfer, group)?;
    let native = native_intake(
        actor,
        native_config(config, plan, group, policy),
        data_memory,
        control,
    )?;
    let coordinator = coordinator_service(actor, config, plan, &topologies(config)?, control)?;
    let lane = ozzy_runtime::replica_actor::LogLane::new(
        actor,
        super::super::super::config::log_limits(body_bytes),
    )
    .map_err(failure)?;
    Ok(RecoveredServices {
        bootstrap,
        native,
        coordinator,
        lane,
    })
}

/// The bootstrap of a partition that rejoined after recovery.
fn recovered_bootstrap(
    actor: &mut ozzy_runtime::replica_actor::PartitionActor,
    plan: &crate::PartitionJournal,
    limits: ozzy_replication::PipelineLimits,
    group: GroupId,
) -> Result<Bootstrap, StartupError> {
    let mut buffer = actor
        .lease_proposal_buffer_with_limits(limits)
        .map_err(failure)?;
    crate::actors::prepare_partition(&mut buffer, plan.cluster, &plan.placement, plan.incarnation)?;
    let submitter = actor
        .take_submitter()
        .ok_or_else(|| failure("missing recovered bootstrap lane"))?;
    Ok(Bootstrap {
        group,
        submitter,
        buffer: Some(buffer),
        pending: None,
        done: false,
    })
}

/// What a runtime log gets back when it rejoins after recovery: its
/// bootstrap and the lane of its log service.
pub(in super::super) fn recovered_log(
    actor: &mut ozzy_runtime::replica_actor::PartitionActor,
    plan: &crate::PartitionJournal,
) -> Result<(Bootstrap, ozzy_runtime::replica_actor::LogLane), StartupError> {
    let (_, body_bytes, group) = profile(&plan.config);
    let crate::ActorSettings::Replicated {
        actor: settings, ..
    } = &plan.actors
    else {
        return Err(failure("recovered services require replicated settings"));
    };
    let bootstrap = recovered_bootstrap(actor, plan, settings.transfer, group)?;
    let lane = ozzy_runtime::replica_actor::LogLane::new(
        actor,
        super::super::super::config::log_limits(body_bytes),
    )
    .map_err(failure)?;
    Ok((bootstrap, lane))
}

/// The services a partition gets back when it rejoins after recovery.
pub(in super::super) struct RecoveredServices {
    pub(in super::super) bootstrap: Bootstrap,
    pub(in super::super) native: NativeIntake,
    pub(in super::super) coordinator: GroupCoordinator,
    pub(in super::super) lane: ozzy_runtime::replica_actor::LogLane,
}

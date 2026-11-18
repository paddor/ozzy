//! The services a partition gets back when it rejoins after recovery.

use super::{
    Bootstrap, Config, GroupCoordinator, NativeIntake, ShardMemory, StartupError,
    coordinator_service, failure, log_lane, native_config, native_intake, profile, topologies,
};
use ozzy_runtime::replica_actor::{LogLane, PartitionActor};

pub(in super::super) fn recovered_services(
    actor: &mut PartitionActor,
    plan: &crate::PartitionJournal,
    config: &Config,
    memory: &ShardMemory,
) -> Result<RecoveredServices, StartupError> {
    let (policy, _, group) = profile(&plan.config);
    // The services take their proposal lanes in this order.
    let bootstrap = recovered_bootstrap(actor, plan)?;
    let native = native_intake(
        actor,
        native_config(config, plan, group, policy),
        &memory.data,
        &memory.control,
    )?;
    let control = &memory.control;
    let coordinator = coordinator_service(actor, config, plan, &topologies(config)?, control)?;
    Ok(RecoveredServices {
        bootstrap,
        native,
        coordinator,
        lane: log_lane(actor, plan)?,
    })
}

/// The bootstrap of a partition that rejoined after recovery.
fn recovered_bootstrap(
    actor: &mut PartitionActor,
    plan: &crate::PartitionJournal,
) -> Result<Bootstrap, StartupError> {
    let (_, _, group) = profile(&plan.config);
    let crate::ActorSettings::Replicated {
        actor: settings, ..
    } = &plan.actors
    else {
        return Err(failure("recovered services require replicated settings"));
    };
    let mut buffer = actor
        .lease_proposal_buffer_with_limits(settings.transfer)
        .map_err(failure)?;
    crate::actors::prepare_partition(&mut buffer, plan.cluster, &plan.placement, plan.incarnation)?;
    let submitter = actor
        .take_submitter()
        .ok_or_else(|| failure("missing recovered bootstrap lane"))?;
    Ok(Bootstrap::new(group, submitter, buffer))
}

/// What a runtime log gets back when it rejoins after recovery: its
/// bootstrap and the lane of its log service.
pub(in super::super) fn recovered_log(
    actor: &mut PartitionActor,
    plan: &crate::PartitionJournal,
) -> Result<(Bootstrap, LogLane), StartupError> {
    Ok((recovered_bootstrap(actor, plan)?, log_lane(actor, plan)?))
}

/// The services a partition gets back when it rejoins after recovery.
pub(in super::super) struct RecoveredServices {
    pub(in super::super) bootstrap: Bootstrap,
    pub(in super::super) native: NativeIntake,
    pub(in super::super) coordinator: GroupCoordinator,
    pub(in super::super) lane: LogLane,
}

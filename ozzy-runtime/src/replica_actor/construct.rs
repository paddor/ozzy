//! Building a replica actor from storage-validated startup: journal limit
//! checks, leased arenas, the protocol driver and peer bindings.

use super::history::Lookup;
use super::{
    ActorConfig, ActorError, ActorIds, ActorStatus, Configuration, Duration, JournalGeneration,
    LogSource, NodeId, NormalReplica, PeerBinding, PipelineLimits, Prefix, ReplicaActor,
    ReplicaDriver, ReplicaOutbox, Scope, VecDeque, ingress, maintenance, normal, startup, wire,
};
use crate::replica_journal::{AppendBuffer, JournalStartup, ReplicaJournal};
use std::cell::Cell;
use std::rc::Rc;

impl ReplicaActor {
    /// Consume storage-validated startup and reserve actor buffers before serving.
    /// The journal must have two available append-buffer leases at these bounds:
    /// one for journal/transfer work and one for contiguous backup receive staging.
    /// At least two command slots must be available for write/sync overlap.
    /// Receipt aliases come from a private source. A broker hosting several
    /// groups shares one `ReceiptHandles` through `new_with_ids`.
    pub fn new(
        journal: ReplicaJournal,
        startup: JournalStartup,
        config: ActorConfig,
    ) -> Result<Self, ActorError> {
        Self::new_with_ids(
            journal,
            startup,
            config,
            ActorIds::random(super::ReceiptHandles::default()),
        )
    }

    /// Construct with an explicit protocol ID source. The simulator must give
    /// each actor incarnation its own namespace, including after restart.
    pub fn new_with_ids(
        journal: ReplicaJournal,
        startup: JournalStartup,
        mut config: ActorConfig,
        ids: ActorIds,
    ) -> Result<Self, ActorError> {
        config.retention_interval = journal.retention_interval(config.retention_interval);
        Self::construct(journal, startup, &config, ids)
    }

    fn construct(
        journal: ReplicaJournal,
        startup: JournalStartup,
        config: &ActorConfig,
        mut ids: ActorIds,
    ) -> Result<Self, ActorError> {
        let configuration = startup.configuration();
        let local = startup.local();
        let (journal_scope, source) = recovered_source(&startup);
        let activated = startup
            .recovered()
            .is_none()
            .then_some((journal_scope, startup.generation()));
        let wire_limits = config.wire_limits()?;
        let buffer = lease_checked_buffer(&journal, startup.generation(), config)?;
        let (ingress, submitters) = ingress::Ingress::new(
            config.proposal_lanes,
            config.proposal_capacity,
            buffer.owner_generation(),
            config.transfer,
        );
        let (driver, work) = start_protocol(&journal, startup, activated, config, &mut ids)?;
        Ok(Self {
            outbox: ReplicaOutbox::new(configuration, local, config.control, config.data)?,
            bindings: peer_bindings(configuration, local, config, Some(work.receive_epoch()))?,
            startup_probe: startup_probe(config, source, &driver, &mut ids)?,
            maintenance: maintenance::Turns::new(config.retention_interval),
            recovery_required: false,
            donors: None,
            more_work: false,
            configuration,
            local,
            config: *config,
            status: Rc::new(Cell::new(ActorStatus {
                normal: driver.normal().map(NormalReplica::snapshot),
                ..ActorStatus::initial(driver.scope())
            })),
            wanted_pin: Some((driver.scope(), source)),
            driver,
            ids,
            journal,
            wire_limits,
            metadata: vec![0; wire_limits.envelope.max_metadata_bytes],
            payload: vec![0; config.transfer.max_body_bytes],
            history_allocator: buffer.allocator(),
            buffer: Some(buffer),
            history_receive_allocator: None,
            spare: None,
            pending: None,
            pending_replay: None,
            pending_sync: None,
            pending_persistence: VecDeque::with_capacity(config.pipeline.max_operations),
            promise: None,
            pinned: None,
            start: None,
            lookup: Lookup::default(),
            transfer: None,
            staged: None,
            journal_scope,
            activated,
            ack_at: Duration::ZERO,
            acked: None,
            ingress,
            submitters,
            work,
        })
    }
}

/// The recovered journal scope, or the configured one, and the local source
/// it accepted through.
fn recovered_source(startup: &JournalStartup) -> (Scope, LogSource) {
    let recovered = startup.recovered();
    let source = LogSource {
        voter: startup.local(),
        generation: startup.generation(),
        accepted: recovered.map_or(Prefix::GENESIS, |state| state.log.accepted),
    };
    let scope = recovered.map_or(startup.configuration().scope(), |state| state.scope);
    (scope, source)
}

/// Lease the actor's append arena once the journal can hold its pipeline.
fn lease_checked_buffer(
    journal: &ReplicaJournal,
    generation: JournalGeneration,
    config: &ActorConfig,
) -> Result<AppendBuffer, ActorError> {
    check_journal_limits(journal, config)?;
    checked_arena(journal.lease_append_buffer()?, generation, config.pipeline)
}

/// A leased arena of this worker generation that holds `required`.
pub(super) fn checked_arena(
    buffer: AppendBuffer,
    generation: JournalGeneration,
    required: PipelineLimits,
) -> Result<AppendBuffer, ActorError> {
    if buffer.owner_generation() != generation {
        return Err(ActorError::StartupMismatch);
    }
    if required.max_operations > buffer.limits().max_operations
        || required.max_body_bytes > buffer.limits().max_body_bytes
    {
        return Err(ActorError::Limits);
    }
    Ok(buffer)
}

/// Start the protocol driver and its normal work. A backup stages received
/// PREPAREs in a second leased arena.
fn start_protocol(
    journal: &ReplicaJournal,
    startup: JournalStartup,
    activated: Option<(Scope, JournalGeneration)>,
    config: &ActorConfig,
    ids: &mut ActorIds,
) -> Result<(ReplicaDriver, normal::Work), ActorError> {
    let configuration = startup.configuration();
    let local = startup.local();
    let driver = startup.into_driver(Duration::ZERO, config.timing, config.pipeline)?;
    let receive_buffer = journal.lease_append_buffer()?;
    let peers = normal::remote_transmitters(configuration, local, driver.scope(), config, ids)?;
    let flow = normal::Flow::new(peers, activated, config)?;
    let work = normal::Work::new(config, ids.channel(driver.scope())?, receive_buffer, flow)?;
    Ok((driver, work))
}

/// A restarted retaining replica first asks peers whether it fell behind. An
/// empty one asks too: a copy can stop before it stores any operation.
fn startup_probe(
    config: &ActorConfig,
    source: LogSource,
    driver: &ReplicaDriver,
    ids: &mut ActorIds,
) -> Result<Option<startup::Probe>, ActorError> {
    if config.retention_interval.is_none() || driver.normal().is_some() {
        return Ok(None);
    }
    let request = wire::RecoveryRequest {
        scope: driver.scope(),
        request_id: ids.request()?,
        nonce: ids.request()?,
        nonvoting: false,
    };
    Ok(Some(startup::Probe::new(request, source.accepted)))
}

/// Reject an actor whose pipeline the journal cannot hold or command.
fn check_journal_limits(journal: &ReplicaJournal, config: &ActorConfig) -> Result<(), ActorError> {
    if journal.command_capacity() < 2 {
        return Err(ActorError::Limits);
    }
    let backlog = journal.backlog();
    // A backup can hold its whole retained window accepted but unapplied.
    if config.pipeline.max_operations > backlog.max_operations
        || config.pipeline.max_body_bytes > backlog.max_body_bytes
        || config.pipeline.backup().max_operations > journal.unapplied_operations()
    {
        return Err(ActorError::Limits);
    }
    Ok(())
}

/// Sessions bound to each configured peer voter, by voter slot.
pub(super) fn peer_bindings(
    configuration: Configuration,
    local: NodeId,
    config: &ActorConfig,
    receive_epoch: Option<ozzy_replication::flow::ReceiveEpoch>,
) -> Result<[Option<PeerBinding>; 3], ActorError> {
    let mut bindings = [None; 3];
    for (index, &peer) in configuration.voters().iter().enumerate() {
        if peer != local && config.sessions[index].as_bytes() != &[0; 16] {
            let binding = PeerBinding::new(configuration, peer, config.sessions[index])?;
            bindings[index] = Some(match receive_epoch {
                Some(epoch) => binding.with_receive_epoch(epoch),
                None => binding,
            });
        }
    }
    Ok(bindings)
}

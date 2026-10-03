//! Shard-owned admission lifecycle. Actors supply authority and capacity;
//! this owner backs and retires promises without transport or disk access.

use super::{Binding, StartupError, failure};
use ozzy_proto::{GroupId, LinkSessionId, NodeId, PartitionIncarnation};
use ozzy_replication::{
    PipelineLimits,
    flow::{Channel, Report},
    wire::FetchOps,
};
use ozzy_runtime::{
    dispatch::{self, Budgets, Class, Client},
    frontend::{self, Destination, GrantRequest, IntakeError, Kind, ReceiveBuffers, ShardIntake},
};
use std::{collections::BTreeMap, task::Context};

mod actors;
pub(super) use actors::{ActorAdapter, client_ready};

const TURN: usize = 16;

pub(super) use ozzy_replication::wire::ReceiveFence as ReceivePurpose;

#[derive(Clone, Copy, Debug)]
pub(super) enum ReceiveDemand {
    Normal {
        peer: NodeId,
        report: Report,
        available: PipelineLimits,
        minimum_body_bytes: usize,
    },
    History(FetchOps),
}

impl ReceiveDemand {
    fn peer(self) -> NodeId {
        match self {
            Self::Normal { peer, .. } => peer,
            Self::History(history) => history.source.voter,
        }
    }

    fn purpose(self) -> ReceivePurpose {
        match self {
            Self::Normal { report, .. } => ReceivePurpose::Normal(report.channel),
            Self::History(history) => ReceivePurpose::History(history),
        }
    }
}

pub(super) trait AdmissionActors {
    fn receive_demand(&self, group: GroupId) -> Option<ReceiveDemand>;
    fn receive_window(&self, group: GroupId) -> Option<(Report, PipelineLimits)>;
    fn receive_is_current(&self, group: GroupId, purpose: ReceivePurpose) -> bool;
    fn client_ready(&self, group: GroupId, partition: PartitionIncarnation) -> bool;
    fn has_work(&self, request: GrantRequest) -> Result<bool, StartupError>;
    fn grant_receive(
        &mut self,
        channel: Channel,
        operations: u64,
        bytes: u64,
    ) -> Result<(), StartupError>;
    fn revoke_receive(&mut self, channel: Channel) -> Result<(), IntakeError>;
}

pub(super) struct ShardAdmission {
    pub(super) intake: ShardIntake,
    partitions: Vec<(frontend::Placement, Destination)>,
    indices: BTreeMap<GroupId, usize>,
    channels: BTreeMap<GroupId, (GrantRequest, ReceivePurpose)>,
    clients: BTreeMap<NodeId, (LinkSessionId, Client)>,
    settled: Vec<(usize, GrantRequest)>,
    requested: Vec<(GrantRequest, usize)>,
    limits: Budgets,
    buffers: ReceiveBuffers,
    pub(super) cursor: usize,
    group_cursor: usize,
}

impl ShardAdmission {
    pub(super) fn new(
        intake: ShardIntake,
        partitions: Vec<(frontend::Placement, Destination)>,
        limits: Budgets,
        buffers: ReceiveBuffers,
    ) -> Self {
        let indices = partitions
            .iter()
            .enumerate()
            .map(|(index, (placement, _))| (placement.group, index))
            .collect();
        Self {
            intake,
            partitions,
            indices,
            channels: BTreeMap::new(),
            clients: BTreeMap::new(),
            settled: Vec::with_capacity(TURN),
            requested: Vec::with_capacity(TURN),
            limits,
            buffers,
            cursor: 0,
            group_cursor: 0,
        }
    }

    pub(super) fn begin_turn(
        &mut self,
        cx: &mut Context<'_>,
        binding: &mut Binding,
        actors: &mut impl AdmissionActors,
    ) -> Result<(), StartupError> {
        self.reconcile_channels(actors)?;
        self.install_grants(cx, binding, actors)?;
        self.replenish_receive(binding, actors)?;
        self.intake
            .reconcile(&binding.links, self.cursor, TURN, |request| {
                fence(actors, &mut self.channels, request)
            })
            .map_err(failure)?;
        self.intake
            .replenish_controls(self.cursor, TURN)
            .map_err(failure)?;
        Ok(())
    }

    pub(super) fn finish_turn(
        &mut self,
        binding: &mut Binding,
        actors: &mut impl AdmissionActors,
    ) -> Result<(), StartupError> {
        self.settle_destinations(binding, actors)?;
        self.admit_requests(binding, actors)?;
        self.cursor = (self.cursor + TURN) % self.intake.reservation_slots();
        self.group_cursor = (self.group_cursor + TURN) % self.partitions.len().max(1);
        Ok(())
    }

    fn group_indices(&self) -> impl Iterator<Item = usize> + use<> {
        let count = self.partitions.len();
        let first = self.group_cursor;
        (0..count.min(TURN)).map(move |offset| (first + offset) % count)
    }

    fn admit(
        &mut self,
        binding: &mut Binding,
        actors: &mut impl AdmissionActors,
        request: GrantRequest,
        retained_bytes: usize,
    ) -> Result<(), StartupError> {
        if binding
            .links
            .get(request.binding.peer)
            .map(|link| link.binding)
            != Some(request.binding)
        {
            binding.requests.dismiss(request);
            return Ok(());
        }
        if request.binding.kind == Kind::Client
            && !actors.client_ready(
                request.route.placement.group,
                request.route.placement.partition,
            )
        {
            return Ok(());
        }
        let receive_fence =
            if request.binding.kind == Kind::Broker && request.route.class == Class::Data {
                let group = request.route.placement.group;
                let Some(demand) = actors.receive_demand(group) else {
                    binding.requests.dismiss(request);
                    return Ok(());
                };
                if demand.peer() != request.binding.peer {
                    binding.requests.dismiss(request);
                    return Ok(());
                }
                if let ReceiveDemand::Normal { available, .. } = demand
                    && (available.max_operations == 0 || available.max_body_bytes == 0)
                    && !self.channels.contains_key(&group)
                {
                    return Ok(());
                }
                Some(demand.purpose())
            } else {
                None
            };

        let size = self.receive_size(request, receive_fence, actors, retained_bytes);
        self.clients.retain(|peer, (_, client)| {
            binding.links.get(*peer).is_some()
                || client.usage(Class::Data) != dispatch::Budget::default()
                || client.usage(Class::Control) != dispatch::Budget::default()
        });
        if !self.clients.contains_key(&request.binding.peer) {
            let client = self
                .intake
                .client(request.binding.session, self.limits)
                .map_err(failure)?;
            self.clients
                .insert(request.binding.peer, (request.binding.session, client));
        }
        let (session, client) = self
            .clients
            .get_mut(&request.binding.peer)
            .expect("admitted client");
        if *session != request.binding.session {
            client
                .replace_session(request.binding.session)
                .map_err(failure)?;
            *session = request.binding.session;
        }
        let admission = self.intake.install(
            &mut binding.port,
            &binding.links,
            request,
            receive_fence,
            client,
            size,
        );
        match admission {
            // The scope has a reservation. Pending and unused credit needs
            // no demand, and spent credit is asked for again when it settles.
            Err(IntakeError::Session) | Ok(false) => binding.requests.dismiss(request),
            Err(IntakeError::Admission(dispatch::Error::Full)) => {
                ozzy_runtime::profiling::event(ozzy_runtime::profiling::Event::IntakeGrantFull);
                self.intake
                    .revoke_idle(request.route.class, self.cursor, 16, |request| {
                        fence(actors, &mut self.channels, request)
                    })
                    .map_err(failure)?;
            }
            Ok(true) | Err(IntakeError::Port(frontend::PortError::Admission(_))) => {}
            Err(IntakeError::Memory(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                ozzy_runtime::profiling::event(ozzy_runtime::profiling::Event::IntakeCanonicalFull);
                self.intake
                    .revoke_idle(request.route.class, self.cursor, 16, |request| {
                        fence(actors, &mut self.channels, request)
                    })
                    .map_err(failure)?;
            }
            Err(error) => return Err(failure(error)),
        }
        Ok(())
    }

    fn receive_size(
        &self,
        request: GrantRequest,
        receive_fence: Option<ReceivePurpose>,
        actors: &impl AdmissionActors,
        retained_bytes: usize,
    ) -> frontend::ReceiveSize {
        let retained_bytes =
            if request.binding.kind == Kind::Client && request.route.class == Class::Data {
                self.buffers.class_retained_bytes(retained_bytes)
            } else {
                retained_bytes
            };
        frontend::ReceiveSize {
            retained_bytes,
            canonical_body_bytes: actors
                .receive_demand(request.route.placement.group)
                .and_then(|demand| match demand {
                    ReceiveDemand::Normal {
                        minimum_body_bytes, ..
                    } if receive_fence.is_some() => Some(minimum_body_bytes),
                    _ => None,
                }),
        }
    }

    fn install_grants(
        &mut self,
        cx: &mut Context<'_>,
        binding: &mut Binding,
        actors: &mut impl AdmissionActors,
    ) -> Result<(), StartupError> {
        let mut installed = Vec::with_capacity(16);
        self.intake
            .poll_installations(cx, &binding.links, self.cursor, 16, |request, purpose| {
                installed.push((request, purpose));
            })
            .map_err(failure)?;
        for (request, purpose) in installed {
            if request.binding.kind == Kind::Broker && request.route.class == Class::Data {
                let group = request.route.placement.group;
                match actors.receive_demand(group).filter(|demand| {
                    demand.peer() == request.binding.peer && Some(demand.purpose()) == purpose
                }) {
                    Some(ReceiveDemand::History(history)) => {
                        self.channels
                            .insert(group, (request, ReceivePurpose::History(history)));
                    }
                    Some(ReceiveDemand::Normal {
                        report, available, ..
                    }) if available.max_operations > 0 && available.max_body_bytes > 0 => {
                        let plan_bytes = self.partitions[self.indices[&group]]
                            .1
                            .capacity()
                            .remaining()
                            .bytes
                            / 2;
                        actors.grant_receive(
                            report.channel,
                            available.max_operations.min(64) as u64,
                            available.max_body_bytes.min(plan_bytes) as u64,
                        )?;
                        self.channels
                            .insert(group, (request, ReceivePurpose::Normal(report.channel)));
                    }
                    _ => {
                        self.intake
                            .revoke(request, |request| {
                                fence(actors, &mut self.channels, request)
                            })
                            .map_err(failure)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn reconcile_channels(
        &mut self,
        actors: &mut impl AdmissionActors,
    ) -> Result<(), StartupError> {
        for index in self.group_indices() {
            let group = self.partitions[index].0.group;
            let Some(&(request, purpose)) = self.channels.get(&group) else {
                continue;
            };
            let current = actors.receive_is_current(group, purpose);
            if !current {
                self.intake
                    .revoke(request, |request| {
                        fence(actors, &mut self.channels, request)
                    })
                    .map_err(failure)?;
            }
        }
        Ok(())
    }

    fn replenish_receive(
        &mut self,
        binding: &Binding,
        actors: &mut impl AdmissionActors,
    ) -> Result<(), StartupError> {
        for (request, destination) in self.intake.idle_grants(&binding.links, self.cursor, 16) {
            if request.binding.kind != Kind::Broker {
                continue;
            }
            let group = destination.group();
            let Some(&(owner, ReceivePurpose::Normal(channel))) = self.channels.get(&group) else {
                continue;
            };
            let Some((report, available)) = actors.receive_window(group) else {
                continue;
            };
            if owner != request || report.channel != channel {
                continue;
            }
            if let Some((operations, bytes)) = receive_grant(
                report,
                available,
                destination.capacity().remaining().bytes / 2,
                match actors.receive_demand(group) {
                    Some(ReceiveDemand::Normal {
                        minimum_body_bytes, ..
                    }) => minimum_body_bytes,
                    _ => 0,
                },
            ) {
                actors.grant_receive(channel, operations, bytes)?;
            }
        }
        Ok(())
    }

    fn settle_destinations(
        &mut self,
        binding: &mut Binding,
        actors: &mut impl AdmissionActors,
    ) -> Result<(), StartupError> {
        let mut settled = std::mem::take(&mut self.settled);
        for (slot, _, request) in self.intake.spent() {
            if !actors.has_work(request)? {
                settled.push((slot, request));
            }
        }
        for (slot, request) in settled.drain(..) {
            let current = binding
                .links
                .get(request.binding.peer)
                .map(|link| link.binding)
                == Some(request.binding);
            let can_replenish = match request.binding.kind {
                Kind::Client => {
                    request.route.class == Class::Data
                        && actors.client_ready(
                            request.route.placement.group,
                            request.route.placement.partition,
                        )
                }
                Kind::Broker => self
                    .channels
                    .get(&request.route.placement.group)
                    .is_some_and(|&(owner, purpose)| {
                        owner == request
                            && matches!(purpose, ReceivePurpose::Normal(_))
                            && actors.receive_is_current(request.route.placement.group, purpose)
                    }),
            };
            if can_replenish && current {
                match self.intake.replenish_data_slot(slot, &binding.links) {
                    Ok(true) => continue,
                    Err(IntakeError::Admission(dispatch::Error::Full)) => {
                        self.reclaim_idle_receive(actors)?;
                        continue;
                    }
                    Ok(false)
                    | Err(
                        IntakeError::Session | IntakeError::Admission(dispatch::Error::Revoked),
                    ) => {}
                    Err(IntakeError::Memory(error))
                        if error.kind() == std::io::ErrorKind::WouldBlock =>
                    {
                        self.reclaim_idle_receive(actors)?;
                        continue;
                    }
                    Err(error) => return Err(failure(error)),
                }
            }
            let scope = self
                .intake
                .settle_slot(slot, |request| fence(actors, &mut self.channels, request))
                .map_err(failure)?;
            if let Some((request, bytes)) = scope
                && request.binding.kind == Kind::Client
                && request.route.class == Class::Data
            {
                self.admit(binding, actors, request, bytes)?;
            }
        }
        self.settled = settled;
        Ok(())
    }

    fn reclaim_idle_receive(
        &mut self,
        actors: &mut impl AdmissionActors,
    ) -> Result<(), StartupError> {
        self.intake
            .revoke_idle(Class::Data, self.cursor, TURN, |request| {
                fence(actors, &mut self.channels, request)
            })
            .map(|_| ())
            .map_err(failure)
    }

    fn admit_requests(
        &mut self,
        binding: &mut Binding,
        actors: &mut impl AdmissionActors,
    ) -> Result<(), StartupError> {
        // A credit-compliant leader sends only a control probe while its data
        // window is empty. Back the hinted follower window before advertising;
        // waiting for a refused data packet here would deadlock both brokers.
        let mut demanded = Vec::with_capacity(16);
        for index in self.group_indices() {
            let placement = self.partitions[index].0;
            let group = placement.group;
            if let Some(demand) = actors.receive_demand(group)
                && let Some(link) = binding.links.get(demand.peer())
            {
                let request = GrantRequest {
                    binding: link.binding,
                    route: frontend::Routed {
                        placement,
                        class: Class::Data,
                        writer: None,
                    },
                };
                if matches!(demand, ReceiveDemand::Normal { .. })
                    || self.channels.get(&group) != Some(&(request, demand.purpose()))
                {
                    demanded.push((
                        request,
                        match demand {
                            ReceiveDemand::Normal {
                                minimum_body_bytes, ..
                            } => self.buffers.retained_bytes_for_payload(minimum_body_bytes),
                            ReceiveDemand::History(_) => self.buffers.maximum_retained_bytes(),
                        },
                    ));
                }
            }
        }
        for (request, bytes) in demanded {
            self.admit(binding, actors, request, bytes)?;
        }
        let mut requested = std::mem::take(&mut self.requested);
        binding.requests.pending_into(&mut requested, 16);
        for (request, bytes) in requested.drain(..) {
            self.admit(binding, actors, request, bytes)?;
        }
        self.requested = requested;
        Ok(())
    }
}

fn receive_grant(
    report: ozzy_replication::flow::Report,
    available: ozzy_replication::PipelineLimits,
    backed_bytes: usize,
    minimum_body_bytes: usize,
) -> Option<(u64, u64)> {
    let unused_operations = report.operation_limit - (report.received.op.0 - report.base.op.0);
    let unused_bytes = report.byte_limit - report.received_bytes;
    // Refill low windows, rather than publishing each operation's returned
    // count. Byte credit is independent and may need renewal sooner for a
    // large payload. Both additions remain backed by actual free capacity.
    let target = unused_operations
        .saturating_add(available.max_operations as u64)
        .min(64);
    let operations = if unused_operations <= target / 2 {
        target
            .saturating_sub(unused_operations)
            .min(available.max_operations as u64)
    } else {
        0
    };
    let bytes =
        if unused_bytes <= backed_bytes as u64 / 2 || unused_bytes < minimum_body_bytes as u64 {
            (backed_bytes as u64)
                .saturating_sub(unused_bytes)
                .min(available.max_body_bytes as u64)
        } else {
            0
        };
    (operations != 0 || bytes != 0).then_some((operations, bytes))
}

#[cfg(test)]
mod tests;

fn fence(
    actors: &mut impl AdmissionActors,
    channels: &mut BTreeMap<GroupId, (GrantRequest, ReceivePurpose)>,
    request: GrantRequest,
) -> Result<(), IntakeError> {
    if request.binding.kind == Kind::Broker
        && request.route.class == Class::Data
        && let Some(&(owner, purpose)) = channels.get(&request.route.placement.group)
        && owner == request
    {
        if let ReceivePurpose::Normal(channel) = purpose
            && actors.receive_is_current(request.route.placement.group, purpose)
        {
            actors.revoke_receive(channel)?;
        }
        channels.remove(&request.route.placement.group);
    }
    Ok(())
}

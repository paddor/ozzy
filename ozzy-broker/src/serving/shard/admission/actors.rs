//! Production adapter: observes partition authority without owning another copy.

use super::super::Bootstrap;
use super::{AdmissionActors, ReceiveDemand, ReceivePurpose, StartupError, failure};
use ozzy_proto::{GroupId, NodeId, PartitionIncarnation};
use ozzy_replication::flow::Channel;
use ozzy_runtime::{
    dispatch::Class,
    frontend::{GrantRequest, IntakeError, Kind},
    replica_actor::PartitionActors,
};
use std::collections::BTreeMap;

pub(in crate::serving::shard) struct ActorAdapter<'a> {
    pub actors: &'a mut PartitionActors,
    pub bootstrap: &'a [Option<Bootstrap>],
    pub indices: &'a BTreeMap<GroupId, usize>,
    pub local: NodeId,
}

pub(in crate::serving::shard) fn client_ready(
    actors: &PartitionActors,
    bootstrap: &[Option<Bootstrap>],
    indices: &BTreeMap<GroupId, usize>,
    local: NodeId,
    group: GroupId,
    partition: PartitionIncarnation,
) -> bool {
    indices.get(&group).is_some_and(|&index| {
        bootstrap[index]
            .as_ref()
            .is_some_and(|bootstrap| bootstrap.done)
    }) || actors
        .route_state(group, partition)
        .is_some_and(|route| route.leader.is_some_and(|leader| leader != local))
}

impl AdmissionActors for ActorAdapter<'_> {
    fn receive_demand(&self, group: GroupId) -> Option<ReceiveDemand> {
        if let Some(history) = self.actors.ops_receive_demand(group) {
            return Some(ReceiveDemand::History(history));
        }
        self.actors.receive_target(group)?;
        let (peer, report) = self.actors.receive_credit(group)?;
        let available = self.actors.receive_capacity(group)?;
        Some(ReceiveDemand::Normal {
            peer,
            report,
            available,
            minimum_body_bytes: self.actors.receive_body_bytes(group)?,
        })
    }

    fn receive_window(
        &self,
        group: GroupId,
    ) -> Option<(
        ozzy_replication::flow::Report,
        ozzy_replication::PipelineLimits,
    )> {
        let (_, report) = self.actors.receive_credit(group)?;
        Some((report, self.actors.receive_capacity(group)?))
    }

    fn receive_is_current(&self, group: GroupId, purpose: ReceivePurpose) -> bool {
        match purpose {
            ReceivePurpose::Normal(channel) => self.actors.receive_channel(group) == Some(channel),
            ReceivePurpose::History(history) => {
                self.actors.ops_receive_demand(group) == Some(history)
            }
        }
    }

    fn client_ready(&self, group: GroupId, partition: PartitionIncarnation) -> bool {
        client_ready(
            self.actors,
            self.bootstrap,
            self.indices,
            self.local,
            group,
            partition,
        )
    }

    fn has_work(&self, request: GrantRequest) -> Result<bool, StartupError> {
        let group = request.route.placement.group;
        match (
            request.binding.kind,
            request.route.class,
            request.route.writer,
        ) {
            (Kind::Client, Class::Data, Some(writer)) => {
                self.actors
                    .native_writer_has_work(group, request.binding.peer, writer)
            }
            (Kind::Client, _, _) => self.actors.native_has_work(group),
            (Kind::Broker, Class::Data, _) => self.actors.receive_has_work(group),
            (Kind::Broker, Class::Control, _) => Ok(false),
        }
        .map_err(failure)
    }

    fn grant_receive(
        &mut self,
        channel: Channel,
        operations: u64,
        bytes: u64,
    ) -> Result<(), StartupError> {
        self.actors
            .grant_receive(channel, operations, bytes)
            .map_err(failure)
    }

    fn revoke_receive(&mut self, channel: Channel) -> Result<(), IntakeError> {
        self.actors
            .revoke_receive(channel)
            .map(|_| ())
            .map_err(|_| IntakeError::Invariant)
    }
}

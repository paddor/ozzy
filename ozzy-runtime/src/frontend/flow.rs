//! Bounded compact-channel routing hints. Actors retain all partition authority.

use std::collections::BTreeMap;

use super::{Kind, ReceiveError};
use omq_tokio::Message;
use ozzy_proto::{GroupId, NodeId};
use ozzy_replication::{flow::Channel, wire::FlowState};

/// One latest alias per configured group, discarded with its peer session.
#[derive(Debug, Default)]
pub(super) struct Channels(BTreeMap<GroupId, (u32, Channel)>);

impl Channels {
    pub(super) fn bind(&mut self, state: FlowState) -> bool {
        let group = state.report.channel.scope.group_id;
        if self
            .0
            .iter()
            .any(|(old_group, (handle, _))| *handle == state.handle && *old_group != group)
        {
            return false;
        }
        if self.0.get(&group).is_some_and(|(handle, channel)| {
            *handle > state.handle || (*handle == state.handle && *channel != state.report.channel)
        }) {
            return false;
        }
        self.0.insert(group, (state.handle, state.report.channel));
        true
    }

    pub(super) fn channel(&self, handle: u32) -> Option<Channel> {
        self.0
            .values()
            .find(|(bound, _)| *bound == handle)
            .map(|(_, channel)| *channel)
    }
}

impl super::Service {
    pub(super) fn receive_compact(
        &mut self,
        message: &omq_tokio::Message,
        retained_bytes: usize,
    ) -> Result<Option<super::Routed>, super::ReceiveError> {
        let peer = message
            .part_slice(0)
            .and_then(|p| <[u8; 16]>::try_from(p).ok())
            .map(NodeId::from_bytes)
            .ok_or(ReceiveError::Peer)?;
        let link = self.links().get(peer).ok_or(ReceiveError::Peer)?;
        if link.binding.kind != Kind::Broker {
            return Err(ReceiveError::Peer);
        }
        let compact =
            ozzy_replication::wire::CompactState::decode(message.part_slice(1).unwrap_or_default())
                .map_err(|_| ReceiveError::Peer)?;
        let channel = self
            .dispatcher
            .peers
            .get(&peer)
            .and_then(|p| p.incoming_channels.channel(compact.handle))
            .ok_or(ReceiveError::Peer)?;
        let routed = Message::multipart_payloads([
            omq_tokio::message::Payload::from_slice(peer.as_bytes()),
            omq_tokio::message::Payload::from_slice(channel.scope.group_id.as_bytes()),
            omq_tokio::message::Payload::from_slice(message.part_slice(1).unwrap()),
        ]);
        self.dispatcher
            .dispatch_data(peer, routed, retained_bytes)
            .map(Some)
            .map_err(|rejected| ReceiveError::Dispatch(rejected.reason))
    }

    pub(super) fn bind_incoming_channel(
        &mut self,
        peer: ozzy_proto::NodeId,
        packet: ozzy_proto::Packet<'_>,
        envelope: ozzy_proto::EnvelopeLimits,
    ) -> Result<(), super::ReceiveError> {
        let link = self.links().get(peer).ok_or(ReceiveError::Peer)?;
        if link.binding.kind != Kind::Broker
            || packet.envelope.session != Some(link.binding.session)
            || packet.envelope.sender != peer
        {
            return Err(ReceiveError::Peer);
        }
        let state = ozzy_replication::wire::flow_state_route(packet, envelope)
            .map_err(|_| ReceiveError::Peer)?;
        if !self
            .dispatcher
            .routes
            .partitions
            .contains_key(&state.report.channel.scope.group_id)
            || !self
                .dispatcher
                .peers
                .get_mut(&peer)
                .ok_or(ReceiveError::Peer)?
                .incoming_channels
                .bind(state)
        {
            return Err(ReceiveError::Peer);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ozzy_replication::{
        Digest, Prefix, Scope,
        flow::{ReceiveEpoch, Report},
    };
    fn state(handle: u32, group: u8, epoch: u128) -> FlowState {
        FlowState {
            handle,
            request_id: None,
            repair_limit: None,
            report: Report {
                channel: Channel {
                    scope: Scope {
                        group_id: GroupId::from_bytes([group; 16]),
                        configuration_epoch: 1,
                        configuration_digest: Digest::from_bytes([9; 32]),
                        view: 0,
                    },
                    epoch: ReceiveEpoch::new(epoch).unwrap(),
                },
                revision: 1,
                base: Prefix::GENESIS,
                received: Prefix::GENESIS,
                received_bytes: 0,
            },
        }
    }
    #[test]
    fn aliases_replace_one_group_without_reusing_or_accepting_retired_handles() {
        let mut channels = Channels::default();
        let first = state(1, 2, 1);
        assert!(channels.bind(first));
        assert!(!channels.bind(state(1, 3, 1)), "cross-group collision");
        assert!(
            !channels.bind(state(1, 2, 2)),
            "same alias, different epoch"
        );
        assert!(channels.bind(state(2, 2, 2)));
        assert_eq!(channels.channel(1), None);
        assert!(
            !channels.bind(first),
            "delayed opening cannot reinstall an old alias"
        );
        assert_eq!(channels.0.len(), 1);
        assert_eq!(channels.channel(2).unwrap().epoch.get(), 2);
        // A replaced peer owns a fresh map. No alias crosses its link session.
        assert_eq!(Channels::default().channel(2), None);
    }
}

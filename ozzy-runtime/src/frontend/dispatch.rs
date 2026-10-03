//! Nonblocking dispatch state. Network handshake and physical socket ownership
//! sit outside this state machine, so deterministic schedules can drive it too.

use std::collections::BTreeMap;
use std::future::Future;

use omq_tokio::Message;
use ozzy_proto::NodeId;

use super::ReplyLimits;
use super::{
    Binding, DataInput, DataSendError, DataSender, Kind, Routed, RoutingError, RoutingTable,
};
use crate::dispatch::Class;
use crate::replica_transport::Queue;

/// Fixed connection and routing-index bounds, independent of frame byte limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatcherLimits {
    /// Live independently established peer bindings.
    pub peers: usize,
    /// Independent outgoing queues per peer. Retained backing remains charged
    /// to its payload owner, including after socket submission.
    pub replies: ReplyLimits,
}

#[derive(Debug)]
pub(super) struct Peer {
    pub(super) binding: Binding,
    pub(super) incoming_channels: super::flow::Channels,
    pub(super) outgoing_channels: super::flow::Channels,
    pub(super) replies: [Queue; 2],
    pub(super) next_reply_class: usize,
}

/// One dispatcher owns link routing and source pressure. It holds no pending
/// ingress messages and never waits for a destination. Application shards alone
/// issue or replenish reservations. This is not a session negotiation service.
#[derive(Debug)]
pub struct Dispatcher {
    pub(super) local: NodeId,
    pub(super) routes: RoutingTable,
    data_lanes: BTreeMap<(u32, Kind, Class), DataSender>,
    pub(super) peers: BTreeMap<NodeId, Peer>,
    pub(super) limits: DispatcherLimits,
    pub(super) replied: Option<NodeId>,
}

impl Dispatcher {
    /// Every configured application shard supplies exactly one sender. No shard
    /// has transport duties. More partitions do not create producer lanes.
    pub fn new(
        local: NodeId,
        routes: RoutingTable,
        lanes: Vec<(u32, DataSender)>,
        limits: DispatcherLimits,
    ) -> Result<Self, SetupError> {
        if local.as_bytes() == &[0; 16] || limits.peers == 0 {
            return Err(SetupError::Limits);
        }
        limits.replies.validate()?;
        let shards = lanes
            .iter()
            .map(|(shard, _)| *shard)
            .collect::<std::collections::BTreeSet<_>>();
        if shards != routes.shards {
            return Err(SetupError::Destination);
        }
        let mut dispatcher = Self {
            local,
            routes,
            data_lanes: BTreeMap::new(),
            peers: BTreeMap::new(),
            limits,
            replied: None,
        };
        for (shard, lane) in lanes {
            dispatcher.install_data_lane(shard, lane)?;
        }
        Ok(dispatcher)
    }

    /// Attach an independent bounded writer or follower queue to this shard.
    /// Control keeps its separately reserved intake.
    pub fn install_data_lane(&mut self, shard: u32, lane: DataSender) -> Result<(), SetupError> {
        if lane.shard() != shard
            || !self.routes.shards.contains(&shard)
            || self
                .data_lanes
                .contains_key(&(shard, lane.kind(), lane.class()))
        {
            return Err(SetupError::Destination);
        }
        self.data_lanes
            .insert((shard, lane.kind(), lane.class()), lane);
        Ok(())
    }

    /// Route a client APPEND. A full shard returns the same
    /// frame to the caller for OMQ source-lane backpressure.
    pub fn dispatch_data(
        &mut self,
        peer: NodeId,
        message: Message,
        retained_bytes: usize,
    ) -> Result<Routed, Rejected> {
        let Some(binding) = self.peers.get(&peer).map(|peer| peer.binding) else {
            return Err(Rejected {
                reason: Rejection::Peer,
                message,
            });
        };
        let route = match self.routes.route(&message, binding) {
            Ok(route) => route,
            Err(error) => {
                return Err(Rejected {
                    reason: Rejection::Routing(error),
                    message,
                });
            }
        };
        let Some(lane) =
            self.data_lanes
                .get_mut(&(route.placement.shard, binding.kind, route.class))
        else {
            return Err(Rejected {
                reason: Rejection::Data(DataPressure::Closed),
                message,
            });
        };
        let input = DataInput {
            binding,
            route,
            message,
        };
        let generation = lane.space_generation();
        match lane.try_send(input, retained_bytes) {
            Ok(()) => Ok(route),
            Err(error) => {
                let (reason, input) = match error {
                    DataSendError::Full(input) => (
                        DataPressure::Full {
                            shard: route.placement.shard,
                            kind: binding.kind,
                            class: route.class,
                            generation,
                        },
                        input,
                    ),
                    DataSendError::Closed(input) => (DataPressure::Closed, input),
                    DataSendError::Oversized(input) => (DataPressure::Charge, input),
                    DataSendError::Invalid(input) => (DataPressure::Invalid, input),
                };
                Err(Rejected {
                    reason: Rejection::Data(reason),
                    message: input.message,
                })
            }
        }
    }

    /// Wait for a shard dequeue after the generation captured before a failed
    /// producer enqueue. The returned future owns its signal, not the dispatcher.
    pub fn data_space_changed_after(
        &self,
        shard: u32,
        kind: Kind,
        class: Class,
        generation: u64,
    ) -> Option<impl Future<Output = ()> + use<>> {
        self.data_lanes
            .get(&(shard, kind, class))
            .map(|lane| lane.space_changed_after(generation))
    }

    /// Install independently established link state. Queued work keeps its old
    /// session stamp for destination validation. Returns whether binding changed.
    pub fn bind(&mut self, binding: Binding) -> Result<bool, SetupError> {
        if binding.peer == self.local
            || binding.peer.as_bytes() == &[0; 16]
            || binding.session.as_bytes() == &[0; 16]
        {
            return Err(SetupError::Binding);
        }
        if let Some(peer) = self.peers.get(&binding.peer) {
            if peer.binding == binding {
                return Ok(false);
            }
            if peer.binding.session == binding.session {
                return Err(SetupError::Binding);
            }
        } else if self.peers.len() >= self.limits.peers {
            return Err(SetupError::Full);
        }
        self.peers.insert(
            binding.peer,
            Peer {
                binding,
                incoming_channels: super::flow::Channels::default(),
                outgoing_channels: super::flow::Channels::default(),
                next_reply_class: 0,
                replies: [
                    Queue::new(self.limits.replies.control).map_err(|_| SetupError::Allocation)?,
                    Queue::new(self.limits.replies.data).map_err(|_| SetupError::Allocation)?,
                ],
            },
        );
        Ok(true)
    }

    /// Forget a closed link and reclaim its unused reservations. Destination
    /// actors must also fence the old session before accepting further work.
    pub fn disconnect(&mut self, peer: NodeId) {
        self.peers.remove(&peer);
    }
}

/// Setup failed before installing or replacing dispatch state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum SetupError {
    /// Reserving bounded outbound slots failed before replacing link state.
    #[error("dispatcher queue allocation failed")]
    Allocation,
    /// Invalid metadata limits.
    #[error("invalid dispatcher limits")]
    Limits,
    /// Missing, duplicate, foreign, or inconsistent destination lane.
    #[error("invalid dispatcher destination")]
    Destination,
    /// Unknown, obsolete, or inconsistent peer/session/role binding.
    #[error("invalid dispatcher binding")]
    Binding,
    /// Bounded peer routing table is full.
    #[error("dispatcher metadata capacity exhausted")]
    Full,
}

/// Unqueued traffic remains owned by the caller for bounded rejection or close.
#[derive(Debug)]
pub struct Rejected {
    /// Routing or admission failure. This never proves an earlier retry failed.
    pub reason: Rejection,
    /// Original frames. The dispatcher retained no overflow copy.
    pub message: Message,
}

/// A client APPEND did not enter its bounded shard queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DataPressure {
    /// The shard must dequeue work before this producer source resumes.
    Full {
        /// Destination application shard.
        shard: u32,
        /// Independent writer or follower queue.
        kind: Kind,
        /// Independent control or data reservation.
        class: Class,
        /// Queue-space generation captured before the failed enqueue.
        generation: u64,
    },
    /// The shard no longer receives producer APPENDs.
    Closed,
    /// The frame exceeded its conservative received-backing allowance.
    Charge,
    /// The message was not a client APPEND for the selected shard.
    Invalid,
}

/// A frame never reached a destination actor during this attempt.
#[derive(Debug, thiserror::Error)]
pub enum Rejection {
    /// No current independently established binding for this connection.
    #[error("unknown dispatcher peer")]
    Peer,
    /// Supplied charge does not cover even the visible message footprint.
    #[error("insufficient dispatcher backing charge")]
    Charge,
    /// OMQ inproc destination is full, closed, or rejected the frame.
    #[error("producer shard queue unavailable: {0:?}")]
    Data(DataPressure),
    /// Invalid routing fields or stale session.
    #[error(transparent)]
    Routing(#[from] RoutingError),
}

#[cfg(test)]
pub(super) mod tests;

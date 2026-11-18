//! The three-voter group and timing the driver tests share.

use std::time::Duration;

use ozzy_proto::{GroupId, NodeId};
use ozzy_replication::driver::{ReplicaDriver, Timing};
use ozzy_replication::{Configuration, Digest, JournalGeneration, NormalReplica, PipelineLimits};

pub(crate) const LIMITS: PipelineLimits = PipelineLimits {
    max_operations: 8,
    max_body_bytes: 1024,
};

pub(crate) fn node(index: u8) -> NodeId {
    NodeId::from_bytes([index + 1; 16])
}

pub(crate) fn configuration() -> Configuration {
    Configuration::new(
        GroupId::from_bytes([7; 16]),
        1,
        Digest::from_bytes([8; 32]),
        [node(0), node(1), node(2)],
    )
    .unwrap()
}

pub(crate) fn normal(index: u8) -> NormalReplica {
    NormalReplica::bootstrap(
        configuration(),
        node(index),
        JournalGeneration(u128::from(index) + 1),
        LIMITS,
    )
    .unwrap()
}

pub(crate) fn timing() -> Timing {
    Timing {
        heartbeat: Duration::from_millis(10),
        primary_timeout: Duration::from_millis(100),
        retransmit: Duration::from_millis(10),
        election_timeout: Duration::from_millis(200),
        max_election_timeout: Duration::from_secs(2),
    }
}

pub(crate) fn driver(normal: NormalReplica) -> ReplicaDriver {
    ReplicaDriver::from_normal(normal, Duration::ZERO, timing()).unwrap()
}

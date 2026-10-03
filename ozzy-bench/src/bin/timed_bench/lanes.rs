//! Fixed writer identity for timed native workloads.

use ozzy_proto::ProducerId;

pub(super) fn producer(lane: usize) -> ProducerId {
    ProducerId::from_bytes([2 + lane as u8 * 3; 16])
}

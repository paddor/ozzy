//! Beats between this broker and its peers, which let idle replicated
//! groups rest.

use super::config::Config;
use crate::CheckedConfig;
use ozzy_proto::NodeId;
use ozzy_runtime::frontend::{self, Service};
use std::{collections::BTreeMap, time::Duration};

/// One beat per replica heartbeat. A peer silent for the replica primary
/// timeout proves nothing, so a resting group fails over as fast as one
/// that sends its own heartbeats.
const BEAT_TIMING: frontend::BeatTiming = frontend::BeatTiming {
    interval: Duration::from_millis(100),
    timeout: Duration::from_secs(1),
};

/// Beats with `brokers` on `timer`, or none for a single broker.
pub(super) fn beats(
    checked: &CheckedConfig,
    brokers: &BTreeMap<NodeId, omq_tokio::Endpoint>,
    seed: u64,
    timer: &super::context::Timer,
) -> Option<frontend::Beats> {
    let timer = timer.clone();
    (!brokers.is_empty()).then(|| {
        frontend::Beats::new(
            checked.plan.shards.iter().map(|shard| shard.id),
            brokers.keys().copied(),
            BEAT_TIMING,
            seed,
            move || timer.elapsed(),
        )
    })
}

impl Config {
    /// Send and receive the beats that let idle groups rest.
    pub(super) fn install_beats(&self, service: &mut Service) {
        if let Some(beats) = &self.beats {
            service.install_beats(beats.clone());
        }
    }
}

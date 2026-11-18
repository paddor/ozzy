//! What the broker handle and the shards share of runtime logs.

use super::Config;
use crate::logs::{COMMANDS, Command, LogTable};
use crate::replacement::Replacement;
use ozzy_runtime::frontend::RuntimeRoutes;
use std::{
    collections::BTreeMap,
    sync::{Mutex, PoisonError},
};
use tokio::sync::mpsc;

/// One shard's log command lane.
struct LogLane {
    sender: mpsc::Sender<Command>,
    receiver: Mutex<Option<mpsc::Receiver<Command>>>,
}

impl LogLane {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(COMMANDS);
        Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
        }
    }
}

/// Runtime log state of one broker, shared by its handle and its shards.
pub(in super::super) struct LogConfig {
    /// Broker routes of replicated runtime logs.
    pub(in super::super) routes: RuntimeRoutes,
    /// Every runtime log of this broker and its state.
    pub(in super::super) table: LogTable,
    /// Each shard's log command lane: the broker handle's sender, and the
    /// receiver until its shard takes it.
    lanes: BTreeMap<u32, LogLane>,
    /// Whether this broker replaces one that lost its stores. It then
    /// makes every replicated log as a nonvoting copy.
    pub(in super::super) replacement: Replacement,
}

impl LogConfig {
    pub(super) fn new(checked: &crate::CheckedConfig, replacement: Replacement) -> Self {
        let shards = checked.plan.shards.iter();
        Self {
            routes: RuntimeRoutes::default(),
            table: LogTable::default(),
            lanes: shards.map(|shard| (shard.id, LogLane::new())).collect(),
            replacement,
        }
    }
}

impl Config {
    /// The broker handle's way to each shard's runtime logs.
    pub(in super::super) fn log_handles(
        &self,
        setup: Option<std::sync::Arc<crate::logs::LogSetup>>,
    ) -> super::super::logs::LogHandles {
        let lanes = self.logs.lanes.iter();
        super::super::logs::LogHandles {
            setup,
            lanes: lanes
                .map(|(shard, lane)| (*shard, lane.sender.clone()))
                .collect(),
            table: self.logs.table.clone(),
        }
    }

    /// The receiving end of a shard's log command lane, once.
    pub(in super::super) fn take_log_commands(
        &self,
        shard: u32,
    ) -> Option<mpsc::Receiver<Command>> {
        self.logs
            .lanes
            .get(&shard)?
            .receiver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

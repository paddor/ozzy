//! Each shard's lane for runtime log commands from the broker handle.

use super::Config;
use crate::logs::{COMMANDS, Command};
use std::sync::{Mutex, PoisonError};
use tokio::sync::mpsc;

/// One shard's log command lane.
pub(in super::super) struct LogLane {
    pub(in super::super) sender: mpsc::Sender<Command>,
    receiver: Mutex<Option<mpsc::Receiver<Command>>>,
}

impl LogLane {
    pub(super) fn new() -> Self {
        let (sender, receiver) = mpsc::channel(COMMANDS);
        Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
        }
    }
}

impl Config {
    /// The broker handle's way to each shard's runtime logs.
    pub(in super::super) fn log_handles(
        &self,
        setup: Option<std::sync::Arc<crate::logs::LogSetup>>,
    ) -> super::super::logs::LogHandles {
        super::super::logs::LogHandles {
            setup,
            lanes: self
                .log_lanes
                .iter()
                .map(|(shard, lane)| (*shard, lane.sender.clone()))
                .collect(),
            table: self.log_table.clone(),
        }
    }

    /// The receiving end of a shard's log command lane, once.
    pub(in super::super) fn take_log_commands(
        &self,
        shard: u32,
    ) -> Option<mpsc::Receiver<Command>> {
        self.log_lanes
            .get(&shard)?
            .receiver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

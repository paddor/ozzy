//! The process's recent diagnostics, for consoles. Logging is rare, so one
//! lock costs little; the newest entries replace the oldest.

use crate::Level;
use std::{
    collections::VecDeque,
    sync::{Mutex, PoisonError},
    time::Instant,
};

/// Entries kept across all threads.
const ENTRIES: usize = 64;

static RECENT: Mutex<VecDeque<LogEntry>> = Mutex::new(VecDeque::new());

/// One recent diagnostic.
#[derive(Clone, Debug)]
pub struct LogEntry {
    /// When it was logged.
    pub at: Instant,
    /// Its severity.
    pub level: Level,
    /// The logging thread's name, or its ID when unnamed.
    pub thread: String,
    /// At most 256 UTF-8 bytes.
    pub message: String,
}

pub(crate) fn keep(level: Level, message: &str) {
    let thread = std::thread::current();
    let entry = LogEntry {
        at: Instant::now(),
        level,
        thread: thread
            .name()
            .map_or_else(|| format!("{:?}", thread.id()), str::to_owned),
        message: message.to_owned(),
    };
    let mut recent = RECENT.lock().unwrap_or_else(PoisonError::into_inner);
    if recent.len() == ENTRIES {
        recent.pop_front();
    }
    recent.push_back(entry);
}

/// The latest 64 diagnostics of every thread, oldest first, whether or not
/// telemetry publishes them.
pub fn recent_logs() -> Vec<LogEntry> {
    let recent = RECENT.lock().unwrap_or_else(PoisonError::into_inner);
    recent.iter().cloned().collect()
}

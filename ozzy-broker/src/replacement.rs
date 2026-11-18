//! A broker that replaces one whose stores were lost.
//!
//! Such a broker may have voted in any replicated log of its deployment,
//! and nothing on its new stores says in which. So it holds none as a
//! voter: every replicated partition without a store, and every runtime
//! log it is told to create, starts as a nonvoting copy and votes only
//! once it holds the log of the other two. A marker on the first device
//! keeps this up across restarts until the embedder ends it.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{
    CheckedConfig, JournalConfig, JournalPlan, RecoveryIntent, StartupError, failure, files,
    io_error,
};

const MARKER: &str = "OZZY_REPLACING";

/// Whether this broker replaces a lost one, and the marker that says so.
/// Clones share one flag: the broker handle ends what its shards read.
#[derive(Clone, Debug, Default)]
pub(crate) struct Replacement {
    on: Arc<AtomicBool>,
    marker: Option<PathBuf>,
}

fn marker(checked: &CheckedConfig) -> Result<Option<PathBuf>, StartupError> {
    Ok(checked.first_device_root()?.map(|root| root.join(MARKER)))
}

impl Replacement {
    /// A replacement without a marker, as a harness on its own storage
    /// declares one.
    pub(crate) fn declared() -> Self {
        Self {
            on: Arc::new(AtomicBool::new(true)),
            marker: None,
        }
    }

    /// Read the marker of this broker's first device.
    pub(crate) fn load(checked: &CheckedConfig) -> Result<Self, StartupError> {
        let marker = marker(checked)?;
        let on = match &marker {
            Some(marker) => files::exists(marker)?,
            None => false,
        };
        Ok(Self {
            on: Arc::new(AtomicBool::new(on)),
            marker,
        })
    }

    /// Put the marker on the first device, durably. Its root exists.
    pub(crate) fn begin(checked: &CheckedConfig) -> Result<(), StartupError> {
        let Some(marker) = marker(checked)? else {
            return Err(failure("a replacement needs a device"));
        };
        if files::exists(&marker)? {
            return Ok(());
        }
        files::publish(&marker, "replacing\n", false)
    }

    pub(crate) fn on(&self) -> bool {
        // Relaxed: the flag orders nothing else. A creation that misses
        // the end only makes one more nonvoting copy.
        self.on.load(Ordering::Relaxed)
    }

    /// Remove the marker, durably, then stop making nonvoting copies.
    pub(crate) fn end(&self) -> Result<(), StartupError> {
        if let Some(marker) = &self.marker {
            files::remove_if_present(marker)?;
            files::sync_directory(crate::parent_directory(marker)?)?;
        }
        // Relaxed, as the load: only the marker must be gone first, and
        // this thread removed it.
        self.on.store(false, Ordering::Relaxed);
        Ok(())
    }
}

impl JournalPlan {
    /// Start every replicated partition without a store as a nonvoting
    /// replacement, and resume every recovery a stop interrupted. Stores
    /// that finished theirs open as usual. An explicit selection stays.
    pub(crate) fn replace_lost(mut self) -> Result<Self, StartupError> {
        for plan in &self.partitions {
            let JournalConfig::Replicated(config) = &plan.config else {
                continue;
            };
            let group = config.identity.group_id;
            if self.recovery.contains_key(&group) {
                continue;
            }
            let intent = if !files::exists(&config.root)? {
                RecoveryIntent::Replace
            } else if ozzy_journal_segment::recovery_unfinished(&config.root)
                .map_err(|error| io_error(&config.root, error))?
            {
                RecoveryIntent::Resume
            } else {
                continue;
            };
            self.recovery.insert(group, intent);
        }
        Ok(self)
    }
}

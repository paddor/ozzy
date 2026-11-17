//! A broker that replaces one whose stores were lost.
//!
//! Such a broker may have voted in any replicated log of its deployment,
//! and nothing on its new stores says in which. So it holds none as a
//! voter: every replicated partition without a store, and every runtime
//! log it is told to create, starts as a nonvoting copy and votes only
//! once it holds the log of the other two. A marker on the first device
//! keeps this up across restarts until the embedder ends it.

use std::{
    fs, io,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{
    CheckedConfig, JournalConfig, JournalPlan, RecoveryIntent, StartupError, io_error, provisioning,
};

const MARKER: &str = "OZZY_REPLACING";

/// Whether this broker replaces a lost one, and the marker that says so.
#[derive(Clone, Debug, Default)]
pub(crate) struct Replacement {
    on: Arc<AtomicBool>,
    marker: Option<PathBuf>,
}

fn marker(checked: &CheckedConfig) -> Result<Option<PathBuf>, StartupError> {
    let devices = &checked.local_broker()?.devices;
    Ok(devices
        .values()
        .next()
        .map(|device| device.root.join(MARKER)))
}

fn exists(path: &std::path::Path) -> Result<bool, StartupError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error(path, error)),
    }
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
            Some(marker) => exists(marker)?,
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
            return Err(StartupError::Runtime("a replacement needs a device".into()));
        };
        if exists(&marker)? {
            return Ok(());
        }
        provisioning::publish_identity(&marker, "replacing\n")
    }

    /// Tell the runtime logs of `journals` what this says.
    #[must_use]
    pub(crate) fn armed(self, journals: &JournalPlan) -> Self {
        if let (true, Some(logs)) = (self.on(), &journals.logs) {
            logs.replacement().store(true, Ordering::Relaxed);
        }
        self
    }

    pub(crate) fn on(&self) -> bool {
        // Read at start and by creations, which may miss the end by one
        // more nonvoting copy.
        self.on.load(Ordering::Relaxed)
    }

    /// Remove the marker, durably, then stop making nonvoting copies.
    pub(crate) fn end(&self) -> Result<(), StartupError> {
        if let Some(marker) = &self.marker {
            match fs::remove_file(marker) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => {
                    return Err(io_error(marker, error));
                }
                _ => {}
            }
            let parent = crate::parent_directory(marker)?;
            fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| io_error(parent, error))?;
        }
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
            let intent = if !exists(&config.root)? {
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

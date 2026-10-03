//! Explicit nonvoting startup. Selection is checked before any worker or write.

use super::{JournalConfig, JournalPlan, PartitionJournal};
use crate::{ActorSettings, StartupError};
use ozzy_proto::{LinkSessionId, NodeId};
use ozzy_runtime::{
    memory::Owner,
    replica_actor::{ActorIds, PartitionActor, RecoveryActor, RecoveryTiming, ScheduledRecovery},
    replica_journal::{
        OwnedRecoveringJournal, OwnedRecoveryGenerations, OwnedRecoveryOpen, RecoveryStartup,
        ShardRecoveringJournal,
    },
};
use std::collections::BTreeMap;

/// Operator-selected startup intent. Every variant remains nonvoting until full
/// recovery publication and a subsequent election. No automatic fallback exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryIntent {
    /// Create an absent replacement store using its existing identity binding.
    Replace,
    /// Preserve an existing store while removing its voting eligibility durably.
    Quarantine,
    /// Continue an exact nonvoting marker, allowing bounded sealed-file repair.
    Resume,
    /// Continue an exact marker using full transfer instead of selective repair.
    ResumeFull,
}

/// Exact configured partition to start in recovery. The broker never derives a
/// path, identity, or membership from the operator's topic text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverySelection {
    pub topic: String,
    pub partition: u32,
    pub intent: RecoveryIntent,
}

impl JournalPlan {
    /// Validate the whole selection without files, threads, or mutations. Reject
    /// duplicate/unknown partitions and explicit single-broker stores.
    pub fn select_recovery(
        mut self,
        selections: &[RecoverySelection],
    ) -> Result<Self, StartupError> {
        for selection in selections {
            let plan = self
                .partitions
                .iter()
                .find(|plan| {
                    plan.placement.topic == selection.topic
                        && plan.placement.partition == selection.partition
                })
                .ok_or_else(|| failure(selection, "unknown partition"))?;
            let JournalConfig::Replicated(config) = &plan.config else {
                return Err(failure(
                    selection,
                    "single-broker partitions cannot recover",
                ));
            };
            if self
                .recovery
                .insert(config.identity.group_id, selection.intent)
                .is_some()
            {
                return Err(failure(selection, "duplicate recovery selection"));
            }
        }
        Ok(self)
    }

    /// Inspect all selected stores before any worker can mutate an earlier one.
    /// Backend startup repeats these checks under its own directory ownership.
    pub(crate) fn check_recovery_intents(&self) -> Result<(), StartupError> {
        for plan in &self.partitions {
            let JournalConfig::Replicated(config) = &plan.config else {
                continue;
            };
            let Some(&intent) = self.recovery.get(&config.identity.group_id) else {
                continue;
            };
            let metadata = std::fs::symlink_metadata(&config.root);
            if intent == RecoveryIntent::Replace {
                match metadata {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(crate::io_error(&config.root, error)),
                    Ok(_) => {
                        return Err(crate::io_error(
                            &config.root,
                            std::io::ErrorKind::AlreadyExists.into(),
                        ));
                    }
                }
                let parent = config.root.parent().expect("configured partition parent");
                let parent_metadata = std::fs::symlink_metadata(parent)
                    .map_err(|error| crate::io_error(parent, error))?;
                if !parent_metadata.is_dir() || parent_metadata.is_symlink() {
                    return Err(crate::io_error(
                        parent,
                        std::io::ErrorKind::InvalidInput.into(),
                    ));
                }
                continue;
            }
            let metadata = metadata.map_err(|error| crate::io_error(&config.root, error))?;
            if !metadata.is_dir() || metadata.is_symlink() {
                return Err(crate::io_error(
                    &config.root,
                    std::io::ErrorKind::InvalidInput.into(),
                ));
            }
            let configuration = config.configuration.encode();
            let inspected = if intent == RecoveryIntent::Quarantine {
                ozzy_journal_segment::GroupDirectory::open_for_repair(
                    &config.root,
                    config.identity,
                    config.limits.metadata,
                    &configuration,
                )
            } else {
                ozzy_journal_segment::GroupDirectory::inspect_recovering(
                    &config.root,
                    config.identity,
                    config.limits.metadata,
                    &configuration,
                )
            };
            inspected.map_err(|source| StartupError::Journal {
                path: config.root.clone(),
                source: source.into(),
            })?;
        }
        Ok(())
    }
}

pub(crate) struct OpenedRecovery {
    owner: OwnedRecoveringJournal,
    startup: RecoveryStartup,
    settings: ActorSettings,
    path: std::path::PathBuf,
}

impl PartitionJournal {
    pub(crate) async fn recover(
        self,
        io: ozzy_io::Local,
        generations: OwnedRecoveryGenerations,
        intent: RecoveryIntent,
    ) -> Result<OpenedRecovery, StartupError> {
        let JournalConfig::Replicated(config) = self.config else {
            return Err(StartupError::Runtime(
                "recovery requires three brokers".into(),
            ));
        };
        let mode = match intent {
            RecoveryIntent::Replace => OwnedRecoveryOpen::FormatNew {
                segment_capacity: config.limits.io.max_segment_bytes,
            },
            RecoveryIntent::Quarantine => OwnedRecoveryOpen::Quarantine,
            RecoveryIntent::Resume => OwnedRecoveryOpen::Resume,
            RecoveryIntent::ResumeFull => OwnedRecoveryOpen::ResumeFull,
        };
        let (owner, startup) = OwnedRecoveringJournal::start(config, io, generations, mode)
            .await
            .map_err(|source| StartupError::Journal {
                path: self.placement.directory.clone(),
                source,
            })?;
        Ok(OpenedRecovery {
            owner,
            startup,
            settings: self.actors,
            path: self.placement.directory,
        })
    }
}

impl OpenedRecovery {
    pub(crate) fn into_actor(
        mut self,
        memory: &Owner,
        sessions: &BTreeMap<NodeId, LinkSessionId>,
        ids: ActorIds,
        timestamp: impl Fn() -> u64 + 'static,
    ) -> Result<PartitionActor, StartupError> {
        let error = |source| StartupError::Journal {
            path: self.path.clone(),
            source,
        };
        self.owner.bind_append_memory(memory).map_err(error)?;
        let ActorSettings::Replicated { journal, mut actor } = self.settings else {
            return Err(StartupError::Runtime("recovery actor mode differs".into()));
        };
        for (slot, peer) in self.startup.configuration().voters().iter().enumerate() {
            if *peer != self.startup.local()
                && let Some(session) = sessions.get(peer)
            {
                if session.as_bytes() == &[0; 16] {
                    return Err(StartupError::Runtime(
                        "zero established broker session".into(),
                    ));
                }
                actor.sessions[slot] = *session;
            }
        }
        let journal =
            ShardRecoveringJournal::from_owned(self.owner, journal, timestamp).map_err(error)?;
        let actor = RecoveryActor::new_with_ids(
            journal,
            self.startup,
            *actor,
            RecoveryTiming::default(),
            ids,
        )
        .map_err(|source| StartupError::Runtime(source.to_string()))?;
        Ok(PartitionActor::Recovering(Box::new(
            ScheduledRecovery::new(actor),
        )))
    }
}

fn failure(selection: &RecoverySelection, reason: &str) -> StartupError {
    StartupError::Runtime(format!(
        "recovery {}/{}: {reason}",
        selection.topic, selection.partition
    ))
}

//! Exact-prefix data synchronization and recovery-evidence publication.
//!
//! `O_DSYNC` groups publish `DURABLE` on the device writer pool while later
//! writes continue and the journal owner keeps installing them. Buffered
//! groups synchronize inline.

use ozzy_replication::SyncTicket;

use super::JournalGeneration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Key {
    pub(super) owner: JournalGeneration,
    pub(super) sequence: u64,
    pub(super) ticket: SyncTicket,
}

/// Completed journal work awaiting exact-ticket consumption by its actor.
/// Dropping this notification cannot undo completed writes or publication.
#[derive(Debug)]
#[must_use = "finish this completion on its owning journal, or shut it down"]
pub struct ReadyJournalSync(pub(super) Key);

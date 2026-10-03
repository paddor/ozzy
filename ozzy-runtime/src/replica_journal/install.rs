//! Bounded, worker-owned selected-history installation and application activation.

use ozzy_journal_segment::BodyEncoding;
use ozzy_replication::driver::ActivationTicket;
use ozzy_replication::{InstallTicket, Prefix, PreparedOperation};

use super::commands::Action;
use super::{AppendBuffer, JournalCompletion, Rejected, ReplicaJournal, SubmitError};

/// Physical staging policy. Source, chunk, and manifest bounds come from the owning partition configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstallationConfig {
    /// Capacity of each new segment, at most the configured history-reader bound.
    pub segment_capacity: u64,
    /// Local physical representation; canonical identity is unchanged.
    pub body_encoding: BodyEncoding,
    /// Total capacity of newly allocated segments in this attempt, excluding old pins.
    pub max_staged_bytes: u64,
    /// Maximum exclusive-create probes per segment around unselected orphan files.
    pub max_orphan_probes: usize,
}

/// A staged chunk, not a durable installation, commit, or application completion.
#[derive(Debug)]
pub struct InstalledChunk {
    pub(super) ticket: InstallTicket,
    pub(super) end: Prefix,
    pub(super) buffer: AppendBuffer,
}

impl InstalledChunk {
    /// Exact installation to which these bytes belong.
    pub const fn ticket(&self) -> InstallTicket {
        self.ticket
    }
    /// Complete staged prefix after this chunk.
    pub const fn end(&self) -> Prefix {
        self.end
    }
    /// Worker-verified metadata strictly after the effective committed floor.
    /// Feed this bounded slice to `ReplicaDriver::validate_install_suffix` when
    /// nonempty. Application semantics are checked by `finish_installation`.
    pub fn prepared(&self) -> &[PreparedOperation] {
        let first = self
            .buffer
            .prepared
            .partition_point(|op| op.prefix().op <= self.ticket.committed().op);
        &self.buffer.prepared[first..]
    }
    /// Reclaim the same leased arena for the next chunk or later normal appends.
    pub fn into_buffer(self) -> AppendBuffer {
        self.buffer
    }
}

/// Durable selected publication plus semantic history validation and commit-floor recovery.
/// No new-view quorum or fresh-append authority is supplied by this evidence alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InstalledJournal {
    pub(super) ticket: InstallTicket,
    pub(super) applied: Prefix,
}

impl InstalledJournal {
    /// Exact core-issued action completed by the worker.
    pub const fn ticket(self) -> InstallTicket {
        self.ticket
    }
    /// Rebuilt committed application boundary, excluding the private accepted-only tail.
    pub const fn applied(self) -> Prefix {
        self.applied
    }
}

impl ReplicaJournal {
    /// Start selected-generation staging after the exact durable view promise.
    /// Old history remains selected until finish. Keep any advertised source pinned;
    /// a selected local source must already have been captured. Disk work, including
    /// protected-prefix copying, stays on the worker. No normal writes during staging.
    pub fn begin_installation(
        &mut self,
        ticket: InstallTicket,
        config: InstallationConfig,
    ) -> Result<JournalCompletion<InstallTicket>, SubmitError> {
        self.submit(
            ticket,
            |ticket, done| Action::BeginInstall {
                ticket,
                config,
                done,
            },
            |action| match action {
                Action::BeginInstall { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Stage the next contiguous canonical chunk strictly after `protected_committed`.
    /// The transport actor must first validate request/source correlation. Worker
    /// validates body syntax, original views, hashes, bounds, and selected lineage.
    /// A dropped completion does not cancel staging. Any action error fences the
    /// worker; do not retry ambiguous writes into the same installation.
    #[expect(
        clippy::result_large_err,
        reason = "return arena ownership on queue backpressure"
    )]
    pub fn install_chunk(
        &mut self,
        ticket: InstallTicket,
        buffer: AppendBuffer,
    ) -> Result<JournalCompletion<InstalledChunk>, Rejected<AppendBuffer>> {
        self.submit(
            buffer,
            |buffer, done| Action::InstallChunk {
                ticket,
                buffer,
                done,
            },
            |action| match action {
                Action::InstallChunk { buffer, .. } => buffer,
                _ => unreachable!("submission preserves command kind"),
            },
        )
    }

    /// Abort healthy unpublished staging after all earlier chunk commands settle.
    /// Do not submit after `finish_installation` or after any uncertain I/O error.
    /// The old selected journal and promise remain intact. Cleanup removes only
    /// this attempt's unselected files on the worker. Complete driver abandonment
    /// only after success; failure fences the worker and requires reopen.
    pub fn abort_installation(
        &mut self,
        ticket: InstallTicket,
    ) -> Result<JournalCompletion<InstallTicket>, SubmitError> {
        self.submit(
            ticket,
            |ticket, done| Action::AbortInstall { ticket, done },
            |action| match action {
                Action::AbortInstall { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Sync all staged bytes, atomically publish the exact selected tail, and replay
    /// its application semantics privately. Only success permits core installation
    /// completion. Historical plans are discarded as replay advances, not retained
    /// in the live pipeline. Failed publication/replay requires reopen/recovery.
    pub fn finish_installation(
        &mut self,
        ticket: InstallTicket,
    ) -> Result<JournalCompletion<InstalledJournal>, SubmitError> {
        self.submit(
            ticket,
            |ticket, done| Action::FinishInstall { ticket, done },
            |action| match action {
                Action::FinishInstall { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }

    /// Publish the whole-tail quorum commit and activate its privately rebuilt image.
    /// Recheck the returned ticket with `ReplicaDriver::complete_activation` before
    /// exposing readiness. A newer view may arrive while this admitted I/O settles.
    pub fn activate_installed(
        &mut self,
        ticket: ActivationTicket,
    ) -> Result<JournalCompletion<ActivationTicket>, SubmitError> {
        self.submit(
            ticket,
            |ticket, done| Action::Activate { ticket, done },
            |action| match action {
                Action::Activate { ticket, .. } => ticket,
                _ => unreachable!("submission preserves command kind"),
            },
        )
        .map_err(|rejected| rejected.reason)
    }
}

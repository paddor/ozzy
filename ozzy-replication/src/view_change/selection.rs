//! History selection from a quorum of reports, verified against the chosen
//! source's pinned history.

use super::{LogSource, SelectedView, ViewChange, ViewChangeError};
use crate::{Digest, OpNumber, Prefix, VoterSet};

impl ViewChange {
    /// Select the highest last-normal-view, then longest accepted lineage.
    ///
    /// `lookup` must be a nonblocking lookup into already verified, pinned
    /// canonical history anchored by the supplied source's exact tail digest.
    /// An isolated matching hash or a chunk from another generation is not such
    /// evidence. It returns the source's digest at the prefix's op; the prefix
    /// is the expected value, a fetch hint and never evidence. Missing bytes
    /// return `None`; fetch/verify them outside the core.
    /// At most six protected-prefix lookups are needed for three voters.
    ///
    /// Selection includes this primary's report and another distinct voter. It
    /// protects every reported commit and same-normal-view accepted prefix.
    /// Higher-normal-view histories may discard only older uncertain suffixes.
    pub fn select(
        &mut self,
        mut lookup: impl FnMut(LogSource, Prefix) -> Option<Digest>,
    ) -> Result<SelectedView, ViewChangeError> {
        self.require_promised()?;
        self.require_primary()?;
        if let Some(selected) = self.selected {
            return Ok(selected);
        }
        let reports = self.reports;
        let Some(mut report) = reports[self.local] else {
            return Err(ViewChangeError::ReportQuorumMissing);
        };
        let mut chosen = self.configuration.voter(self.local);
        let voters: VoterSet = reports
            .iter()
            .filter_map(|(voter, candidate)| candidate.map(|_| voter))
            .collect();
        if voters.len() < 2 {
            return Err(ViewChangeError::ReportQuorumMissing);
        }
        for (voter, candidate) in reports.iter() {
            let Some(candidate) = candidate else {
                continue;
            };
            let rank = (candidate.log.last_normal_view, candidate.log.accepted.op);
            let best = (report.log.last_normal_view, report.log.accepted.op);
            if rank == best && candidate.log.accepted != report.log.accepted {
                return Err(self.conflict());
            }
            if rank > best {
                report = *candidate;
                chosen = self.configuration.voter(voter);
            }
        }
        let source = LogSource {
            voter: chosen,
            generation: report.generation,
            accepted: report.log.accepted,
        };
        let mut committed = Prefix::GENESIS;
        for candidate in reports.values().flatten() {
            if candidate.log.committed.op > committed.op {
                committed = candidate.log.committed;
            }
            self.verify_prefix(source, candidate.log.committed, &mut lookup)?;
            if candidate.log.last_normal_view == report.log.last_normal_view {
                self.verify_prefix(source, candidate.log.accepted, &mut lookup)?;
            }
        }
        let selected = SelectedView {
            scope: self.scope(),
            source,
            committed,
            voters,
        };
        self.selected = Some(selected);
        Ok(selected)
    }

    pub(crate) fn verify_prefix(
        &mut self,
        source: LogSource,
        prefix: Prefix,
        lookup: &mut impl FnMut(LogSource, Prefix) -> Option<Digest>,
    ) -> Result<(), ViewChangeError> {
        if prefix.op > source.accepted.op {
            return Err(self.conflict());
        }
        let digest = if prefix.op == OpNumber(0) {
            Digest::ZERO
        } else if prefix.op == source.accepted.op {
            source.accepted.digest
        } else {
            lookup(source, prefix).ok_or(ViewChangeError::HistoryMissing)?
        };
        if digest != prefix.digest {
            return Err(self.conflict());
        }
        Ok(())
    }
}

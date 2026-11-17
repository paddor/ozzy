//! A selection that replaces nothing: it ends at the copy's own durable end,
//! digest included, so the copy already holds every selected operation.
//! The segments stay as they are and only the manifest and the writer
//! generation change.

use super::{Journal, SuffixRequest};
use crate::SuffixReplacementError as Error;
use crate::directory::asynchronous::active_segment;
use crate::metadata::position_before;
use crate::suffix::streaming;

impl Journal {
    /// Whether `request` selects exactly this journal's log, so `keep_suffix`
    /// can install it without a new segment. The tail after the retained
    /// prefix is then the one the caller would stage. An invalid request
    /// fails as it would at `begin_suffix_replacement`.
    pub fn keeps_suffix(&self, request: &SuffixRequest) -> Result<bool, Error> {
        streaming::validate_stream_request(&self.suffix_source(), request)?;
        let replacement = request.replacement;
        let end = position_before(self.writer.durable_position().next_chain())?;
        Ok(request.accepted == end
            && active_segment(&self.manifest.segments)?.capacity == replacement.segment_capacity)
    }

    /// Install a selection `keeps_suffix` accepted: publish its hard state
    /// over the unchanged segments, then fence the old writer generation.
    /// This consumes ownership even on uncertain errors.
    pub async fn keep_suffix(mut self, request: SuffixRequest) -> Result<Self, Error> {
        if !self.keeps_suffix(&request)? {
            return Err(Error::IncompleteSuffix);
        }
        self.validate_authority_files().await?;
        let mut next = self.next_manifest()?;
        next.promised_view = request.replacement.promised_view;
        next.last_normal_view = request.replacement.last_normal_view;
        next.accepted = request.accepted;
        next.committed = request.replacement.committed;
        self.install_selected(next).await?;
        self.writer.rebind(request.replacement.writer_generation)?;
        Ok(self)
    }
}

//! Split a received suffix so its first operations validate alone.

use ozzy_journal::operation::CanonicalOperation;

use super::{AppendBuffer, JournalError, RequestKind};

impl AppendBuffer {
    /// Move the first `count` canonical operations into the empty `head`,
    /// keeping their wire-verified digests and provenance. Both validate
    /// again. False changes neither when memory for the copy is short.
    pub(crate) fn move_front(
        &mut self,
        count: usize,
        head: &mut Self,
    ) -> Result<bool, JournalError> {
        if self.request_kind != RequestKind::Canonical
            || count == 0
            || count >= self.entries.len()
            || !head.is_empty()
        {
            return Err(JournalError::AppendMismatch);
        }
        let end = self.entries[count].body.start;
        for reserved in [
            head.reserve_incoming(end),
            self.bodies.reserve(0).map(|()| true),
        ] {
            match reserved {
                Ok(true) => {}
                Ok(false) => return Ok(false),
                Err(JournalError::Io(error)) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            }
        }
        for entry in &self.entries[..count] {
            let operation = CanonicalOperation {
                body: &self.bodies[entry.body.clone()],
                ..entry.envelope
            };
            head.push_entry(operation, entry.digest, entry.primary_payload_validated)?;
        }
        self.bodies.mutable()?.drain(..end);
        self.entries.drain(..count);
        for entry in &mut self.entries {
            entry.body = entry.body.start - end..entry.body.end - end;
        }
        self.validated = false;
        self.prepared.clear();
        self.body_digests.clear();
        Ok(true)
    }
}

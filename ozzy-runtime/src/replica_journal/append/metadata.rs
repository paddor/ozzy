//! Hash turns are bounded across the entire arena, independent of operation size.

use super::{AppendBuffer, CanonicalOperation, Digest, JournalError, PreparedOperation};
use ozzy_journal::operation::{canonical_body_digest, canonical_body_hasher};
use ozzy_replication::driver::ValidationTicket;

const HASH_TURN_BYTES: usize = 64 * 1024;

impl AppendBuffer {
    /// Hash at most one byte allowance before yielding this shard. Verified
    /// immutable wire bodies reuse their digest. Private proposal bytes are
    /// hashed again on every attempt, including after offset reassignment.
    /// Cancellation installs no metadata or authority; another attempt restarts
    /// with the unchanged body bytes and bounded scratch already in this lease.
    pub(in crate::replica_journal) async fn prepare_metadata_async(
        &mut self,
        ticket: ValidationTicket,
        assign: bool,
    ) -> Result<(), JournalError> {
        self.prepared.clear();
        self.body_digests.clear();
        let mut remaining = HASH_TURN_BYTES;
        for entry in &self.entries {
            let digest = match entry.digest {
                Some(digest) => digest,
                None => hash(&self.bodies[entry.body.clone()], &mut remaining).await,
            };
            self.body_digests.push(digest);
        }
        self.prepare_headers(ticket, assign)
    }

    fn prepare_headers(
        &mut self,
        ticket: ValidationTicket,
        assign: bool,
    ) -> Result<(), JournalError> {
        let scope = ticket.scope();
        let mut predecessor = ticket.accepted();
        for (entry, &digest) in self.entries.iter_mut().zip(&self.body_digests) {
            let op_number = predecessor
                .op
                .0
                .checked_add(1)
                .ok_or(JournalError::AppendMismatch)?;
            if assign {
                entry.envelope.group_id = scope.group_id;
                entry.envelope.configuration_epoch = scope.configuration_epoch;
                entry.envelope.original_view = scope.view;
                entry.envelope.op_number = op_number;
                entry.envelope.previous_digest = predecessor.digest;
            }
            let operation = CanonicalOperation {
                body: &self.bodies[entry.body.clone()],
                ..entry.envelope
            };
            if operation.group_id != scope.group_id
                || operation.configuration_epoch != scope.configuration_epoch
                || operation.original_view != scope.view
                || operation.op_number != op_number
                || operation.previous_digest != predecessor.digest
            {
                return Err(JournalError::AppendMismatch);
            }
            let metadata = PreparedOperation::from_verified(&operation, digest);
            predecessor = metadata.prefix();
            self.prepared.push(metadata);
        }
        Ok(())
    }
}

async fn hash(mut body: &[u8], remaining: &mut usize) -> Digest {
    if body.len() <= *remaining {
        *remaining -= body.len();
        return canonical_body_digest(body);
    }
    let mut hasher = canonical_body_hasher();
    while !body.is_empty() {
        if *remaining == 0 {
            crate::replica_journal::shard::yield_turn().await;
            *remaining = HASH_TURN_BYTES;
        }
        let count = body.len().min(*remaining);
        hasher.update(&body[..count]);
        body = &body[count..];
        *remaining -= count;
    }
    hasher.finish()
}

#[cfg(test)]
mod tests;

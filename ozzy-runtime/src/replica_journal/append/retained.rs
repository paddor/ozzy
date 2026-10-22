//! Immutable history retained independently of a reusable caller arena.

use std::ops::Range;

use ozzy_journal::operation::{CanonicalOperation, Digest, ValidatedWireAppend};
use ozzy_replication::PreparedOperation;

use super::Descriptor;

/// Immutable bounded history retained independently of a reusable caller arena.
#[derive(Debug)]
pub(in crate::replica_journal) struct RetainedAppend {
    pub(super) entries: Vec<(Descriptor, Digest)>,
    pub(super) bodies: bytes::Bytes,
    pub(super) retained_bytes: usize,
    pub(super) validated: bool,
    pub(in crate::replica_journal) prepared: Vec<PreparedOperation>,
}

impl RetainedAppend {
    pub(in crate::replica_journal) fn operations(
        &self,
    ) -> impl ExactSizeIterator<Item = CanonicalOperation<'_>> {
        self.entries.iter().map(|(entry, _)| CanonicalOperation {
            body: &self.bodies[entry.body.clone()],
            ..entry.envelope
        })
    }

    pub(in crate::replica_journal) fn verified_operations(
        &self,
    ) -> impl ExactSizeIterator<Item = (CanonicalOperation<'_>, Digest)> {
        self.operations()
            .zip(self.entries.iter().map(|(_, digest)| *digest))
    }

    pub(in crate::replica_journal) fn payload(&self, part: &[u8]) -> bytes::Bytes {
        self.bodies.slice_ref(part)
    }
    pub(in crate::replica_journal) fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    /// Producer proofs in operation order; `None` for operations without one.
    pub(in crate::replica_journal) fn proofs(
        &self,
    ) -> impl ExactSizeIterator<Item = Option<ValidatedWireAppend>> {
        self.entries.iter().map(|(entry, _)| entry.proof)
    }
    /// Whether every operation in `range` has a validated payload, and whether
    /// all of their payloads are producer-prepared.
    pub(in crate::replica_journal) fn validated_payloads_prepared(
        &self,
        range: Range<usize>,
    ) -> Option<bool> {
        let mut prepared = true;
        for (entry, _) in self.entries.get(range)? {
            prepared &= match entry.proof {
                Some(proof) => proof.payload_is_prepared(),
                None if entry.primary_payload_validated => false,
                None => return None,
            };
        }
        Some(prepared)
    }
    /// Whether every immutable body passed the journal owner's limits.
    pub(in crate::replica_journal) const fn validated(&self) -> bool {
        self.validated
    }
}

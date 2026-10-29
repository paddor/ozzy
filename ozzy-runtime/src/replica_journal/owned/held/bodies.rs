//! Bodies of held `Schedule` and `Carry` operations kept in memory, so a
//! release step reads no storage for them.

use std::collections::BTreeMap;

use bytes::Bytes;
use ozzy_journal::operation::{OperationKind, ReleaseKey};

use super::Held;

/// Body bytes one delay partition keeps. Operations past this budget are
/// read from storage when they are due.
const HELD_BODY_BYTES: usize = 4 << 20;

impl Held {
    /// Kind and body of a held operation kept in memory.
    pub(in crate::replica_journal::owned) fn body(
        &self,
        number: u64,
    ) -> Option<(OperationKind, Bytes)> {
        self.bodies.get(number)
    }

    /// Drop every kept body and keep no more.
    #[cfg(test)]
    pub(in crate::replica_journal::owned) fn keep_no_bodies(&mut self) {
        self.bodies.budget = 0;
        self.bodies.retain(|_, _| false);
    }
}

#[derive(Debug)]
struct Body {
    kind: OperationKind,
    bytes: Bytes,
    /// Release key of its last record.
    last: ReleaseKey,
}

/// Held operation bodies by number, within `HELD_BODY_BYTES`.
#[derive(Debug)]
pub(super) struct Bodies {
    by_operation: BTreeMap<u64, Body>,
    bytes: usize,
    budget: usize,
}

impl Default for Bodies {
    fn default() -> Self {
        Self {
            by_operation: BTreeMap::new(),
            bytes: 0,
            budget: HELD_BODY_BYTES,
        }
    }
}

impl Bodies {
    /// Keep a copy of `body` while it fits.
    pub(super) fn keep(&mut self, number: u64, kind: OperationKind, body: &[u8], last: ReleaseKey) {
        if body.len() > self.budget - self.bytes || self.by_operation.contains_key(&number) {
            return;
        }
        self.bytes += body.len();
        let bytes = Bytes::copy_from_slice(body);
        self.by_operation.insert(number, Body { kind, bytes, last });
    }

    pub(super) fn get(&self, number: u64) -> Option<(OperationKind, Bytes)> {
        self.by_operation
            .get(&number)
            .map(|body| (body.kind, body.bytes.clone()))
    }

    /// Drop the bodies whose records are all released through `through`.
    pub(super) fn release(&mut self, through: ReleaseKey) {
        self.retain(|_, body| body.last > through);
    }

    /// Drop the bodies before `operation`, whose records a carry moved.
    pub(super) fn carried(&mut self, operation: u64) {
        self.retain(|number, _| number >= operation);
    }

    fn retain(&mut self, mut keep: impl FnMut(u64, &Body) -> bool) {
        let bytes = &mut self.bytes;
        self.by_operation.retain(|number, body| {
            let kept = keep(*number, body);
            if !kept {
                *bytes -= body.bytes.len();
            }
            kept
        });
    }
}

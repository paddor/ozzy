//! Generation-scoped accounting for complete journal writes and syncs.
//!
//! This does not perform I/O or decide commit. The adapter reports a write only
//! after a complete sealed group, and a sync only after the required barrier.
//! Tickets identify local actions, not hashes, quorum evidence, or disk proofs.

/// One active journal writer incarnation, supplied by the owning runtime.
///
/// Never reuse a value across journals, reopen, or selected-lineage replacement.
/// It is independent of the persisted segment/manifest generation and VSR view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalGeneration(pub u128);

/// Contiguous group-operation prefix. Zero denotes the empty journal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct OpNumber(pub u64);

/// One admitted, indivisible local write group. Fields cannot be forged externally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteTicket {
    generation: JournalGeneration,
    first: OpNumber,
    through: OpNumber,
}

impl WriteTicket {
    /// Writer incarnation that admitted this group.
    pub const fn generation(self) -> JournalGeneration {
        self.generation
    }
    /// First operation in this group, inclusive.
    pub const fn first(self) -> OpNumber {
        self.first
    }
    /// Last operation in this group, inclusive.
    pub const fn through(self) -> OpNumber {
        self.through
    }
}

/// Prefix captured when a synchronization request is issued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncTicket {
    generation: JournalGeneration,
    through: OpNumber,
}

impl SyncTicket {
    /// Writer incarnation that issued this synchronization.
    pub const fn generation(self) -> JournalGeneration {
        self.generation
    }
    /// Complete written prefix this synchronization may acknowledge.
    pub const fn through(self) -> OpNumber {
        self.through
    }
}

/// Read-only local progress. None of these positions implies quorum commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalSnapshot {
    /// Current writer incarnation.
    pub generation: JournalGeneration,
    /// Last admitted operation.
    pub accepted: OpNumber,
    /// Last operation in a completely written group.
    pub written: OpNumber,
    /// Last operation covered by successful sync evidence.
    pub durable: OpNumber,
    /// Uncertain I/O outcome blocks subsequent actions until recovery.
    pub faulted: bool,
}

/// Single-writer local completion state. No allocation, runtime, or I/O.
#[derive(Debug)]
pub struct JournalProgress {
    state: JournalSnapshot,
}

impl JournalProgress {
    /// Start from an independently verified and synchronized recovered prefix.
    ///
    /// The caller must complete storage recovery before supplying this prefix.
    /// Bytes visible only in a surviving OS page cache are not stable evidence.
    /// Reopening requires a fresh generation, including after an I/O error.
    pub const fn recover(generation: JournalGeneration, through: OpNumber) -> Self {
        Self {
            state: JournalSnapshot {
                generation,
                accepted: through,
                written: through,
                durable: through,
                faulted: false,
            },
        }
    }

    /// Inspect progress without obtaining mutation authority.
    pub const fn snapshot(&self) -> JournalSnapshot {
        self.state
    }

    /// Reserve a contiguous group after external payload/capacity validation.
    /// Errors leave all state unchanged. Zero and counter wrap are rejected.
    pub fn admit(&mut self, operations: u64) -> Result<WriteTicket, ProgressError> {
        self.require_active(self.state.generation)?;
        if operations == 0 {
            return Err(ProgressError::EmptyGroup);
        }
        let through = self
            .state
            .accepted
            .0
            .checked_add(operations)
            .ok_or(ProgressError::OpExhausted)?;
        let ticket = WriteTicket {
            generation: self.state.generation,
            first: OpNumber(self.state.accepted.0 + 1),
            through: OpNumber(through),
        };
        self.state.accepted = ticket.through;
        Ok(ticket)
    }

    /// Report a complete sealed write, in journal order. Duplicates are harmless.
    /// Partial writes must not call this method. They remain the adapter's work.
    pub fn complete_write(&mut self, ticket: WriteTicket) -> Result<(), ProgressError> {
        self.require_active(ticket.generation)?;
        if ticket.through > self.state.accepted {
            return Err(ProgressError::BeyondAccepted);
        }
        if ticket.through <= self.state.written {
            return Ok(());
        }
        if Some(ticket.first.0) != self.state.written.0.checked_add(1) {
            return Err(ProgressError::WriteGap);
        }
        self.state.written = ticket.through;
        Ok(())
    }

    /// Capture the complete written prefix before submitting a sync action.
    pub fn begin_sync(&self) -> Result<SyncTicket, ProgressError> {
        self.require_active(self.state.generation)?;
        Ok(SyncTicket {
            generation: self.state.generation,
            through: self.state.written,
        })
    }

    /// Report successful sync for the captured prefix, not newer written data.
    /// Duplicate/older completions cannot lower the durable prefix.
    pub fn complete_sync(&mut self, ticket: SyncTicket) -> Result<(), ProgressError> {
        self.require_active(ticket.generation)?;
        if ticket.through > self.state.written {
            return Err(ProgressError::BeyondWritten);
        }
        self.state.durable = self.state.durable.max(ticket.through);
        Ok(())
    }

    /// Report one complete write whose backend operation also synchronized the
    /// exact group before returning. This supports transactional local adapters
    /// that do not expose separate write and sync callbacks.
    pub fn complete_durable_write(&mut self, ticket: WriteTicket) -> Result<(), ProgressError> {
        self.complete_write(ticket)?;
        self.state.durable = self.state.durable.max(ticket.through);
        Ok(())
    }

    /// An uncertain write/sync faults this generation. Past evidence is retained.
    /// A stale error from a replaced generation cannot fault its replacement.
    pub fn fail(&mut self, generation: JournalGeneration) -> Result<(), ProgressError> {
        if generation != self.state.generation {
            return Err(ProgressError::StaleGeneration);
        }
        self.state.faulted = true;
        Ok(())
    }

    fn require_active(&self, generation: JournalGeneration) -> Result<(), ProgressError> {
        if generation != self.state.generation {
            return Err(ProgressError::StaleGeneration);
        }
        if self.state.faulted {
            return Err(ProgressError::Faulted);
        }
        Ok(())
    }
}

/// Invalid or stale journal transition. No error advances any prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProgressError {
    /// The writer has an uncertain I/O outcome and needs recovery.
    #[error("journal generation is faulted")]
    Faulted,
    /// Completion belongs to another writer incarnation.
    #[error("stale journal generation")]
    StaleGeneration,
    /// A write group must contain at least one operation.
    #[error("empty journal group")]
    EmptyGroup,
    /// Operation numbering cannot wrap.
    #[error("journal operation number exhausted")]
    OpExhausted,
    /// Completion claims operations that were never admitted.
    #[error("write completion exceeds admitted prefix")]
    BeyondAccepted,
    /// An earlier complete write has not been reported.
    #[error("write completion leaves a journal gap")]
    WriteGap,
    /// Sync claims data not known to have been completely written.
    #[error("sync completion exceeds written prefix")]
    BeyondWritten,
}

#[cfg(kani)]
mod proofs {
    use super::*;

    #[kani::proof]
    fn transitions_preserve_prefix_order_or_leave_state_unchanged() {
        let before = JournalSnapshot {
            generation: JournalGeneration(kani::any()),
            accepted: OpNumber(kani::any()),
            written: OpNumber(kani::any()),
            durable: OpNumber(kani::any()),
            faulted: kani::any(),
        };
        // Inductive precondition: every public constructor/transition must
        // establish this ordering. Event fields themselves are unrestricted.
        kani::assume(before.durable <= before.written && before.written <= before.accepted);
        let mut progress = JournalProgress { state: before };
        let generation = JournalGeneration(kani::any());
        let through = OpNumber(kani::any());
        let result = match kani::any::<u8>() {
            0 => progress.admit(kani::any()).map(|_| ()),
            1 => progress.complete_write(WriteTicket {
                generation,
                first: OpNumber(kani::any()),
                through,
            }),
            2 => progress.complete_sync(SyncTicket {
                generation,
                through,
            }),
            3 => progress.complete_durable_write(WriteTicket {
                generation,
                first: OpNumber(kani::any()),
                through,
            }),
            _ => progress.fail(generation),
        };
        let after = progress.snapshot();
        assert!(after.durable <= after.written && after.written <= after.accepted);
        assert!(after.durable >= before.durable);
        assert!(after.written >= before.written);
        assert!(after.accepted >= before.accepted);
        if result.is_err() {
            assert_eq!(after, before);
        }
    }

    #[kani::proof]
    fn admission_is_atomic_and_cannot_wrap() {
        let start: u64 = kani::any();
        let count: u64 = kani::any();
        let mut progress = JournalProgress::recover(JournalGeneration(1), OpNumber(start));
        let before = progress.snapshot();
        match progress.admit(count) {
            Ok(ticket) => {
                assert!(count > 0);
                assert!(ticket.first.0 > start);
                assert_eq!(Some(ticket.through.0), start.checked_add(count));
                assert_eq!(progress.snapshot().written, before.written);
                assert_eq!(progress.snapshot().durable, before.durable);
            }
            Err(_) => {
                assert!(count == 0 || start.checked_add(count).is_none());
                assert_eq!(progress.snapshot(), before);
            }
        }
    }

    #[kani::proof]
    fn sync_cannot_acknowledge_later_writes() {
        let start: u64 = kani::any();
        let first_count = u64::from(kani::any::<u8>()) + 1;
        let second_count = u64::from(kani::any::<u8>()) + 1;
        let mut progress = JournalProgress::recover(JournalGeneration(1), OpNumber(start));
        let Ok(first) = progress.admit(first_count) else {
            return;
        };
        progress.complete_write(first).unwrap();
        let sync = progress.begin_sync().unwrap();
        let Ok(second) = progress.admit(second_count) else {
            return;
        };
        progress.complete_write(second).unwrap();
        progress.complete_sync(sync).unwrap();
        assert_eq!(progress.snapshot().durable, first.through);
        assert!(progress.snapshot().durable < progress.snapshot().written);
    }
}

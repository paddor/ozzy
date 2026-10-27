//! Concrete actor-owned storage execution. All normal policies share one owner.

use super::{ShardJournal, SubmitError, commands::Gate, receiving::driver::local, shard};
use std::task::{Context, Poll, Waker};

/// The waker of the owner's last poll by its partition. Work started between
/// polls takes its first step under it, so a file operation it suspends on
/// still wakes that partition.
#[derive(Debug)]
pub(super) struct PollWaker(Waker);

impl PollWaker {
    pub(super) fn new() -> Self {
        Self(Waker::noop().clone())
    }

    /// Keep the waker of this poll; an equivalent one is not cloned again.
    pub(super) fn observe(&mut self, cx: &Context<'_>) {
        self.0.clone_from(cx.waker());
    }

    pub(super) fn waker(&self) -> Waker {
        self.0.clone()
    }
}

#[derive(Debug)]
pub(super) enum Execution {
    Normal(ShardJournal),
    Recovery(local::Execution),
}

impl Execution {
    pub(super) fn control_capacity(&self, accepted: ozzy_replication::OpNumber) -> usize {
        match self {
            Self::Normal(owner) => owner.control_capacity(accepted),
            Self::Recovery(_) => 0,
        }
    }
    pub(super) fn poll_finished(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        match self {
            Self::Normal(owner) => owner.poll_finished(cx),
            Self::Recovery(owner) => owner.poll_finished(cx),
        }
    }
    pub(super) fn is_closed(&self) -> bool {
        match self {
            Self::Normal(owner) => owner.is_closed(),
            Self::Recovery(owner) => owner.is_closed(),
        }
    }
    pub(super) fn available(&self) -> usize {
        match self {
            Self::Normal(owner) => owner.available(),
            Self::Recovery(owner) => owner.available(),
        }
    }
    pub(super) fn settled(&self) -> bool {
        match self {
            Self::Normal(owner) => owner.settled(),
            Self::Recovery(_) => false,
        }
    }
    pub(super) fn retention_busy(&self) -> bool {
        match self {
            Self::Normal(owner) => owner.retention_busy(),
            Self::Recovery(_) => false,
        }
    }
    pub(super) fn cleaning(&self) -> bool {
        match self {
            Self::Normal(owner) => owner.cleaning(),
            Self::Recovery(_) => false,
        }
    }
    pub(super) fn release_wait(&self) -> Option<u64> {
        match self {
            Self::Normal(owner) => owner.release_wait(),
            Self::Recovery(_) => None,
        }
    }
    pub(super) fn validation_ready(&self, buffer: &super::AppendBuffer) -> bool {
        match self {
            Self::Normal(owner) => owner.validation_ready(buffer),
            Self::Recovery(_) => false,
        }
    }
    pub(super) fn close(&mut self) {
        match self {
            Self::Normal(owner) => owner.close(),
            Self::Recovery(owner) => owner.close(),
        }
    }
    /// Take the normal owner for one command its gate admits now.
    pub(super) fn admit(&mut self, gate: Gate<'_>) -> Result<shard::Admitted<'_>, SubmitError> {
        match self {
            Self::Normal(owner) => owner.admit(gate).ok_or(SubmitError::Full),
            // A nonvoting recovery owner runs only transfer work.
            Self::Recovery(_) => Err(SubmitError::Stopped),
        }
    }
    /// Take the nonvoting recovery owner for one transfer command.
    pub(super) fn admit_receive(&mut self) -> Result<local::Admitted<'_>, SubmitError> {
        match self {
            Self::Recovery(owner) => owner.admit().ok_or(SubmitError::Full),
            Self::Normal(_) => Err(SubmitError::Stopped),
        }
    }
    pub(super) fn recovery(&mut self) -> Option<&mut local::Execution> {
        match self {
            Self::Recovery(owner) => Some(owner),
            Self::Normal(_) => None,
        }
    }
}

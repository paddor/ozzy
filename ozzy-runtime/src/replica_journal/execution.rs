//! Concrete actor-owned storage execution. All normal policies share one owner.

use super::{ShardJournal, receiving::driver::local};
use std::task::{Context, Poll};

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
    #[expect(
        clippy::result_large_err,
        reason = "refusal preserves ownership of the original action"
    )]
    pub(super) fn submit(
        &mut self,
        command: super::commands::Command,
    ) -> Result<(), (super::SubmitError, super::commands::Command)> {
        match self {
            Self::Normal(owner) => owner
                .submit(command)
                .map_err(|command| (super::SubmitError::Full, command)),
            Self::Recovery(owner) => owner
                .submit(command)
                .map_err(|command| (super::SubmitError::Full, command)),
        }
    }
    pub(super) fn recovery(&mut self) -> &mut local::Execution {
        match self {
            Self::Recovery(owner) => owner,
            Self::Normal(_) => unreachable!("recovery owner never changes execution in place"),
        }
    }
}

use super::{
    ActorError, AdmittedAppend, Live, LocalActor, Pending, PendingSync, ProposalOutcome,
    ProposalValidation, ValidationTicket,
};
use crate::replica_actor::io::SyncEvent;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

enum Completed {
    Propose(ProposalValidation),
    Admit(AdmittedAppend),
    Apply(ValidationTicket),
}

impl Pending {
    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Completed, crate::replica_journal::JournalError>> {
        match self {
            Self::Propose(work) => Pin::new(work).poll(cx).map_ok(Completed::Propose),
            Self::Admit(work) => Pin::new(work).poll(cx).map_ok(Completed::Admit),
            Self::Apply(work) => Pin::new(work).poll(cx).map_ok(Completed::Apply),
        }
    }
}

impl LocalActor {
    pub(super) fn progress(&mut self, cx: &mut Context<'_>) -> Result<bool, ActorError> {
        if let Poll::Ready(error) = self.journal.poll_stopped(cx) {
            return Err(error.into());
        }
        let mut changed = false;
        if let Some(work) = self.persistence.front_mut()
            && let Poll::Ready(result) = Pin::new(&mut work.0).poll(cx)
        {
            let (_, started_at) = self.persistence.pop_front().expect("completed write");
            crate::profiling::finish(crate::profiling::Stage::LocalWrite, started_at);
            self.driver.complete_write(result?)?;
            changed = true;
        }
        let sync = self
            .sync
            .as_mut()
            .map_or(Poll::Pending, |sync| std::pin::pin!(sync.wait()).poll(cx));
        if let Poll::Ready(result) = sync {
            self.sync = match result? {
                SyncEvent::Ready(ready) => {
                    crate::profiling::finish(
                        crate::profiling::Stage::LocalSyncPublish,
                        self.sync_started_at.take(),
                    );
                    let install = self
                        .journal
                        .finish_pipelined_sync(ready)
                        .map_err(|rejected| rejected.reason)?;
                    self.sync_install_started_at = crate::profiling::start();
                    Some(PendingSync::Install(install))
                }
                SyncEvent::Installed(ticket) => {
                    crate::profiling::finish(
                        crate::profiling::Stage::LocalSyncInstall,
                        self.sync_install_started_at.take(),
                    );
                    self.driver.complete_sync(ticket)?;
                    None
                }
            };
            changed = true;
        }
        if let Some(pending) = &mut self.pending
            && let Poll::Ready(result) = pending.poll(cx)
        {
            self.pending = None;
            self.complete(result?)?;
            changed = true;
        }
        if let Some(front) = self.live.front()
            && front.buffer.is_some()
            && front.through.op <= self.driver.snapshot().applied.op
        {
            let live = self.live.pop_front().expect("confirmed request");
            live.reply.finish(
                live.buffer.expect("admitted buffer").into(),
                ProposalOutcome::Committed {
                    scope: self.driver.begin_validation()?.scope(),
                    through: live.through,
                },
            );
            changed = true;
        }
        changed |= self.schedule()?;
        if !self.closing && self.pending.is_none() && self.waiting.is_none() {
            self.ingress.resume();
            changed |= std::pin::pin!(self.ingress.ready()).poll(cx).is_ready();
        }
        Ok(changed)
    }

    fn complete(&mut self, done: Completed) -> Result<(), ActorError> {
        match done {
            Completed::Apply(ticket) => {
                crate::profiling::finish(
                    crate::profiling::Stage::LocalApply,
                    self.apply_started_at.take(),
                );
                self.driver.apply_through(ticket.committed())?;
            }
            Completed::Admit(admitted) => {
                crate::profiling::finish(
                    crate::profiling::Stage::LocalAdmit,
                    self.admit_started_at.take(),
                );
                let (ticket, buffer, persistence) = admitted.into_parts();
                let live = self.live.back_mut().ok_or(ActorError::History)?;
                if live.through.op != ticket.through() || live.buffer.is_some() {
                    return Err(ActorError::History);
                }
                live.buffer = Some(buffer);
                live.reply.admitted();
                self.persistence
                    .push_back((persistence, crate::profiling::start()));
            }
            Completed::Propose(proposal) => {
                crate::profiling::finish(
                    crate::profiling::Stage::LocalPropose,
                    self.propose_started_at.take(),
                );
                self.complete_proposal(proposal)?;
            }
        }
        Ok(())
    }

    fn complete_proposal(&mut self, proposal: ProposalValidation) -> Result<(), ActorError> {
        let reply = self.validating.take().ok_or(ActorError::History)?;
        match proposal {
            ProposalValidation::Rejected { reason, buffer } => {
                reply.finish(buffer, ProposalOutcome::Invalid(reason));
            }
            ProposalValidation::Resolved {
                through, buffer, ..
            } => self.live.push_back(Live {
                through,
                reply,
                buffer: Some(buffer.0),
            }),
            ProposalValidation::Ready(validated) if self.closing => {
                reply.finish(validated.into_buffer().into(), ProposalOutcome::NotAdmitted);
            }
            ProposalValidation::Ready(validated) => {
                let ticket = self
                    .driver
                    .prepare_validated(validated.validation(), validated.prepared())?;
                self.live.push_back(Live {
                    through: self.driver.snapshot().accepted,
                    reply,
                    buffer: None,
                });
                self.pending = Some(Pending::Admit(
                    self.journal
                        .admit_append(ticket, validated)
                        .map_err(|rejected| rejected.reason)?,
                ));
                self.admit_started_at = crate::profiling::start();
            }
        }
        Ok(())
    }
}

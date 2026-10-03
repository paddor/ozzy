use super::{
    ActorError, AdmittedAppend, Live, LocalActor, Pending, PendingSync, ProposalOutcome,
    ProposalValidation, ValidatedAppend, WriteTicket,
};
use crate::replica_actor::io::SyncEvent;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

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
        let sync = if self.journal.available_command_slots() == 0 {
            Poll::Pending
        } else {
            self.sync
                .as_mut()
                .map_or(Poll::Pending, |sync| std::pin::pin!(sync.wait()).poll(cx))
        };
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
        if self.journal.available_command_slots() != 0 {
            changed |= self.poll_pending(cx)?;
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

    fn poll_pending(&mut self, cx: &mut Context<'_>) -> Result<bool, ActorError> {
        let Some(mut pending) = self.pending.take() else {
            return Ok(false);
        };
        let changed = match &mut pending {
            Pending::Ready(_) => false,
            Pending::Propose(work) => match Pin::new(work).poll(cx) {
                Poll::Pending => false,
                Poll::Ready(result) => {
                    crate::profiling::finish(
                        crate::profiling::Stage::LocalPropose,
                        self.propose_started_at.take(),
                    );
                    self.complete_proposal(result?)?;
                    true
                }
            },
            Pending::Admit(work) => match Pin::new(work).poll(cx) {
                Poll::Pending => false,
                Poll::Ready(result) => {
                    self.complete_admission(result?)?;
                    true
                }
            },
            Pending::Apply(work) => match Pin::new(work).poll(cx) {
                Poll::Pending => false,
                Poll::Ready(result) => {
                    crate::profiling::finish(
                        crate::profiling::Stage::LocalApply,
                        self.apply_started_at.take(),
                    );
                    self.driver.apply_through(result?.committed())?;
                    true
                }
            },
        };
        if !changed {
            self.pending = Some(pending);
        }
        Ok(changed)
    }

    fn complete_admission(&mut self, admitted: AdmittedAppend) -> Result<(), ActorError> {
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
                self.admit_ready(ticket, validated)?;
            }
        }
        Ok(())
    }

    pub(super) fn admit_ready(
        &mut self,
        ticket: WriteTicket,
        validated: ValidatedAppend,
    ) -> Result<bool, ActorError> {
        match self.journal.admit_append(ticket, validated) {
            Ok(completion) => {
                self.pending = Some(Pending::Admit(completion));
                self.admit_started_at = crate::profiling::start();
                Ok(true)
            }
            Err(rejected) if rejected.reason == crate::replica_journal::SubmitError::Full => {
                self.pending = Some(Pending::Ready(Box::new((ticket, rejected.value))));
                Ok(false)
            }
            Err(rejected) => Err(rejected.reason.into()),
        }
    }
}

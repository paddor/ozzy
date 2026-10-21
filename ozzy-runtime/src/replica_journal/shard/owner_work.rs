//! Owner work a shard starts by itself between commands: writes, rolls,
//! retirement, cleanup, packs, sealed indexes and identity refreshes.

use super::{Completed, Install, Job, ShardJournal, State};
use crate::replica_journal::{OwnedWriteStep, commands::logged_fault};
use futures::FutureExt;

impl ShardJournal {
    /// Start or install one piece of owner work. `true` when one step ran.
    pub(super) fn step_owner(&mut self) -> bool {
        if self.install_completed()
            || self.start_retirement_job()
            || self.start_cleanup_job()
            || self.start_pack_job()
            || self.start_index_job()
        {
            return true;
        }
        self.idle = self.failed;
        // Retirement and packs install with one manifest publication between
        // writes, like a roll: no new write starts until in-flight writes
        // drain. Their file work already ran as a job.
        let retiring = !self.failed
            && self
                .state
                .as_ref()
                .is_some_and(|state| state.owner.retirement_ready());
        let packing = !self.failed
            && self
                .state
                .as_ref()
                .is_some_and(|state| state.owner.pack_ready());
        self.start_retirement_install(retiring)
            || self.start_pack_install(packing)
            || (!retiring && !packing && self.start_write())
            || self.start_identity_refresh()
    }

    fn install_completed(&mut self) -> bool {
        let Some(state) = self.state.as_mut() else {
            return false;
        };
        let Some(done) = self.completed.pop_front() else {
            return false;
        };
        self.failed |= match done {
            Completed::Write(done) => logged_fault(&state.owner.complete_write(done)),
            Completed::Job(install) => install(state),
        };
        self.failed |= state.owner.is_faulted();
        true
    }

    fn start_retirement_job(&mut self) -> bool {
        let Some(work) = self
            .state
            .as_mut()
            .and_then(|state| state.owner.take_retirement_work())
        else {
            return false;
        };
        self.jobs.push(
            async move {
                let done = Box::pin(work.run()).await;
                Box::new(move |state: &mut State| {
                    state.owner.complete_retirement(done);
                    false
                }) as Install
            }
            .boxed_local(),
        );
        true
    }

    fn start_cleanup_job(&mut self) -> bool {
        let Some(work) = self
            .state
            .as_mut()
            .and_then(|state| state.owner.take_cleanup_work())
        else {
            return false;
        };
        self.jobs.push(
            async move {
                let done = Box::pin(work.run()).await;
                Box::new(move |state: &mut State| {
                    state.owner.complete_cleanup(done);
                    false
                }) as Install
            }
            .boxed_local(),
        );
        true
    }

    fn start_pack_job(&mut self) -> bool {
        if self.closing {
            return false;
        }
        let Some(work) = self
            .state
            .as_mut()
            .and_then(|state| state.owner.take_pack_work())
        else {
            return false;
        };
        self.jobs.push(
            async move {
                let done = Box::pin(work.run()).await;
                Box::new(move |state: &mut State| {
                    state.owner.complete_pack(done);
                    false
                }) as Install
            }
            .boxed_local(),
        );
        true
    }

    fn start_index_job(&mut self) -> bool {
        if self.closing {
            return false;
        }
        let Some(job) = self
            .state
            .as_mut()
            .and_then(|state| state.owner.take_index_work())
        else {
            return false;
        };
        self.jobs.push(
            async move {
                let built = Box::pin(job.run()).await;
                Box::new(move |state: &mut State| {
                    state.owner.complete_index_work(&built);
                    false
                }) as Install
            }
            .boxed_local(),
        );
        true
    }

    fn start_retirement_install(&mut self, retiring: bool) -> bool {
        if !retiring
            || !self.writes.is_empty()
            || self
                .state
                .as_ref()
                .is_none_or(|state| state.sync_busy || state.rolling)
        {
            return false;
        }
        self.hold_owner(|mut state| async move {
            let failed = logged_fault(&state.owner.finish_retirement().await);
            (state, None, failed)
        })
    }

    fn start_pack_install(&mut self, packing: bool) -> bool {
        if !packing
            || !self.writes.is_empty()
            || self
                .state
                .as_ref()
                .is_none_or(|state| state.sync_busy || state.rolling)
        {
            return false;
        }
        self.hold_owner(|mut state| async move {
            let failed = logged_fault(&state.owner.finish_pack().await);
            (state, None, failed)
        })
    }

    fn start_write(&mut self) -> bool {
        if self.failed || self.writes.len() >= self.config.write_depth {
            return false;
        }
        let Some(state) = self.state.as_mut() else {
            return false;
        };
        match state.owner.prepare_write() {
            Ok(OwnedWriteStep::Idle) => {
                self.idle = true;
                false
            }
            Ok(OwnedWriteStep::Write(work)) => {
                self.writes.push_back(
                    async move {
                        let started = crate::profiling::start();
                        let done = work.write().await;
                        crate::profiling::finish(
                            crate::profiling::Stage::JournalPhysicalWrite,
                            started,
                        );
                        done
                    }
                    .boxed_local(),
                );
                true
            }
            Ok(OwnedWriteStep::RollRequired) if self.writes.is_empty() && !state.sync_busy => {
                match state.owner.begin_roll(self.config.roll_probes) {
                    Ok(work) => {
                        state.rolling = true;
                        self.jobs.push(
                            async move {
                                let done = work.publish().await;
                                Box::new(move |state: &mut State| {
                                    state.rolling = false;
                                    state.owner.complete_roll(done).is_err()
                                }) as Install
                            }
                            .boxed_local(),
                        );
                    }
                    Err(_) => self.failed = true,
                }
                true
            }
            Ok(OwnedWriteStep::Waiting | OwnedWriteStep::RollRequired) => false,
            Err(_) => {
                self.failed = true;
                true
            }
        }
    }

    fn start_identity_refresh(&mut self) -> bool {
        if self.failed
            || !self.writes.is_empty()
            || self
                .state
                .as_ref()
                .is_none_or(|state| state.sync_busy || !state.owner.identity_refresh_due())
        {
            return false;
        }
        self.hold_owner(|mut state| async move {
            let failed = state.owner.refresh_settled_identities().await.is_err();
            (state, None, failed)
        })
    }

    /// Run owner work that holds the whole journal until it returns. `true`
    /// when it started, which needs an owner no other work holds.
    fn hold_owner<F>(&mut self, work: impl FnOnce(Box<State>) -> F) -> bool
    where
        F: std::future::Future<Output = (Box<State>, Option<Job>, bool)> + 'static,
    {
        let Some(state) = self.state.take() else {
            return false;
        };
        self.active = Some(work(state).boxed_local());
        true
    }
}

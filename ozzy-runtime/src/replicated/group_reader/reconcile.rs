//! Applying membership views: activate, revoke, and drain owned sources.

use super::{
    BrokerLinkError, Draining, GroupReader, GroupReaderError, ProcessingIdentity, ProgressError,
    Source, Window, driver, membership, sources,
};
use crate::signal::CloseSignal;
use ozzy_proto::group::Owned;

impl GroupReader {
    /// Apply the latest membership view: release revoked sources, activate
    /// confirmed ones, and report what this instance still owns.
    pub(super) fn reconcile(&mut self) -> Result<(), GroupReaderError> {
        if !self.view.has_changed().unwrap_or(true) {
            return self.reconcile_progress();
        }
        let view = self.view.borrow_and_update().clone();
        if let Some(failure) = view.failure {
            return Err(failure.into());
        }
        self.rejoin(view.generation);
        self.revoke_untargeted(&view)?;
        if let Some(group) = view.group {
            if self.group.is_some_and(|current| current != group) {
                return Err(GroupReaderError::Fenced);
            }
            self.group = Some(group);
        }
        for activation in &view.activations {
            if self
                .sources
                .get(&activation.number)
                .is_some_and(|source| source.plan_revision == activation.plan_revision)
                || !view.targets(activation.partition, activation.plan_revision)
            {
                continue;
            }
            self.activate(activation)?;
        }
        self.reconcile_progress()
    }

    /// Revoke sources the view no longer targets or whose window stopped.
    fn revoke_untargeted(&mut self, view: &membership::View) -> Result<(), GroupReaderError> {
        let revoked: Vec<u32> = self
            .sources
            .values()
            .filter(|source| {
                !view.targets(source.partition, source.plan_revision)
                    || source.window.stopped().is_some()
            })
            .map(|source| source.number)
            .collect();
        for number in revoked {
            let source = &self.sources[&number];
            if let Some(error) = source.window.stopped()
                && !matches!(error, ProgressError::Fenced | ProgressError::Closed)
            {
                return Err(error.into());
            }
            self.revoke(number);
        }
        Ok(())
    }

    /// Start reading one confirmed target from its activation cursor, with
    /// its own progress window and driver.
    fn activate(&mut self, activation: &membership::Activation) -> Result<(), GroupReaderError> {
        if activation.cursor < activation.retained_from {
            return Err(GroupReaderError::HistoryExpired {
                partition: activation.number,
                cursor: activation.cursor,
                retained_from: activation.retained_from,
            });
        }
        // An older copy of this source can no longer confirm anything.
        for draining in &mut self.draining {
            if draining.source.partition == activation.partition {
                draining.source.window.close(ProgressError::Fenced);
            }
        }
        let identity = ProcessingIdentity {
            group: self
                .group
                .ok_or(GroupReaderError::Broker(BrokerLinkError::Response))?,
            partition: activation.partition,
            member: self.config.member,
            assignment_epoch: activation.epoch,
            policy: self.reader.policy(),
        };
        let (window, mut publisher) = Window::new(
            identity,
            activation.cursor,
            self.config.window,
            &self.budget,
        )?;
        let stop = CloseSignal::default();
        publisher.notify_on(self.activity.clone());
        drop(driver::start(
            self.reader.routes().clone(),
            activation.number,
            publisher,
            self.config.retry,
            Some(stop.clone()),
        )?);
        self.reader.attach(activation.number, activation.cursor)?;
        self.observations_dirty = true;
        self.sources.insert(
            activation.number,
            Source {
                number: activation.number,
                partition: activation.partition,
                plan_revision: activation.plan_revision,
                epoch: activation.epoch,
                window,
                stop,
            },
        );
        Ok(())
    }

    fn reconcile_progress(&mut self) -> Result<(), GroupReaderError> {
        while let Some((number, error)) = self
            .sources
            .values()
            .find_map(|source| source.window.stopped().map(|error| (source.number, error)))
        {
            if !matches!(error, ProgressError::Fenced | ProgressError::Closed) {
                return Err(error.into());
            }
            self.revoke(number);
        }
        for source in self.sources.values_mut() {
            source.window.retire(self.config.window);
        }
        self.drain();
        self.publish_sources();
        Ok(())
    }

    fn rejoin(&mut self, generation: u64) {
        if self.generation != generation {
            self.generation = generation;
            self.group = None;
            self.observations_dirty = true;
            for mut source in std::mem::take(&mut self.sources).into_values() {
                self.reader.detach(source.number);
                source.window.close(ProgressError::Fenced);
            }
            for mut draining in std::mem::take(&mut self.draining) {
                draining.source.window.close(ProgressError::Fenced);
            }
        }
    }

    /// Stop delivery now; confirmations continue until drained or the deadline.
    pub(super) fn revoke(&mut self, number: u32) {
        let Some(source) = self.sources.remove(&number) else {
            return;
        };
        self.reader.detach(number);
        self.observations_dirty = true;
        self.draining.push(Draining { source });
    }

    pub(super) fn drain(&mut self) {
        let window = self.config.window;
        let before = self.draining.len();
        self.draining.retain_mut(|draining| {
            draining.source.window.retire(window);
            if draining.source.window.drained() {
                draining.source.window.close(ProgressError::Fenced);
                return false;
            }
            true
        });
        self.observations_dirty |= before != self.draining.len();
    }

    pub(super) fn publish_sources(&mut self) {
        if !std::mem::take(&mut self.observations_dirty) {
            return;
        }
        let observations = self
            .sources
            .values()
            .map(|source| (source, false))
            .chain(
                self.draining
                    .iter()
                    .map(|draining| (&draining.source, true)),
            )
            .map(|(source, draining)| sources::Observation {
                owned: Owned {
                    partition: source.partition,
                    plan_revision: source.plan_revision,
                    epoch: source.epoch,
                },
                progress: source.window.observer(),
                stop: source.stop.clone(),
                generation: self.generation,
                draining,
            })
            .collect();
        self.observations.send_replace(observations);
    }
}

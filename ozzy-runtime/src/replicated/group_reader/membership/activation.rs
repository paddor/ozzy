//! Activation relay: assign requests for committed targets, and their retries.

use super::{
    Activation, Assigning, BrokerLinkError, Failure, Membership, ProgressError, Target, UNSELECTED,
    assign, find, group,
};
use futures::stream::FuturesUnordered;
use std::time::Duration;

/// When a refused activation request retries, and its current backoff.
pub(super) struct ActivationRetry {
    due: Duration,
    backoff: Duration,
}

impl Membership {
    /// When the earliest retry of a refused activation is due.
    pub(super) fn activation_due(&self) -> Option<Duration> {
        self.activation_retries
            .iter()
            .filter(|(key, _)| !self.inflight.contains(key))
            .filter(|((partition, revision), _)| {
                find(&self.targets, *partition, *revision).is_some_and(|target| target.activatable)
            })
            .map(|(_, retry)| retry.due)
            .min()
    }

    pub(super) fn activate(&mut self, assigning: &mut FuturesUnordered<Assigning>, now: Duration) {
        let Some(group) = self.group else {
            return;
        };
        for target in &self.targets {
            let key = (target.partition, target.plan_revision);
            if !target.activatable
                || self.inflight.contains(&key)
                || self.skipped.contains(&key)
                || self
                    .activation_retries
                    .get(&key)
                    .is_some_and(|retry| now < retry.due)
                || self
                    .activations
                    .get(&target.partition)
                    .is_some_and(|activation| activation.plan_revision == target.plan_revision)
            {
                continue;
            }
            self.inflight.insert(key);
            let request = group::AssignRequest {
                authority: UNSELECTED,
                partition: target.partition,
                group,
                member: Some(self.settings.member),
                plan_revision: target.plan_revision,
                initial: self.settings.initial,
                policy: self.routes.metadata().policy(),
            };
            let routes = self.routes.clone();
            let timeout = self.settings.retry.response_timeout;
            let target = *target;
            assigning.push(Box::pin(async move {
                (
                    target,
                    assign(routes, target.number, request, timeout).await,
                )
            }));
        }
    }

    pub(super) fn assigned(
        &mut self,
        target: Target,
        result: Result<group::AssignReply, BrokerLinkError>,
        now: Duration,
    ) -> Result<(), Failure> {
        let key = (target.partition, target.plan_revision);
        self.inflight.remove(&key);
        if !self.targets.iter().any(|current| {
            current.partition == target.partition && current.plan_revision == target.plan_revision
        }) {
            return Ok(());
        }
        match result {
            Ok(reply) => {
                self.activation_retries.remove(&key);
                if reply.partition != target.partition
                    || Some(reply.group) != self.group
                    || reply.member != Some(self.settings.member)
                    || reply.plan_revision != target.plan_revision
                    || reply.policy != self.routes.metadata().policy()
                    || reply.epoch == 0
                    || reply.cursor > reply.end
                    || reply.retained_from > reply.end
                {
                    return Err(Failure::Response);
                }
                self.activations.insert(
                    target.partition,
                    Activation {
                        number: target.number,
                        partition: target.partition,
                        plan_revision: target.plan_revision,
                        epoch: reply.epoch,
                        cursor: reply.cursor,
                        retained_from: reply.retained_from,
                    },
                );
            }
            // A newer plan already reached the source; the next reply moves on.
            Err(BrokerLinkError::Progress(ProgressError::Fenced)) => {
                self.activation_retries.remove(&key);
                self.skipped.insert(key);
            }
            Err(BrokerLinkError::InitialPosition(position)) => {
                return Err(Failure::InitialPosition {
                    partition: target.number,
                    position,
                });
            }
            Err(error) => {
                self.retry(target.number, error)?;
                let retry = self
                    .activation_retries
                    .entry(key)
                    .or_insert(ActivationRetry {
                        due: now,
                        backoff: self.settings.retry.initial_backoff,
                    });
                retry.due = now.saturating_add(retry.backoff);
                retry.backoff = self.settings.retry.next_backoff(retry.backoff);
            }
        }
        Ok(())
    }
}

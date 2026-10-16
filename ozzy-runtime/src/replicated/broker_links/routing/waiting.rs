//! Shared owner waits. Capture generations before checking route state.

use super::{BrokerLinkError, TopicRoutes};
use ozzy_proto::{directory::RouteState, nack::AuthorityHint};
use std::{future::Future, pin::pin, time::Duration};

impl TopicRoutes {
    pub(in crate::replicated) async fn leader(
        &self,
        number: u32,
        refresh: Duration,
    ) -> Result<RouteState, BrokerLinkError> {
        let clock = self.links.clock();
        loop {
            let generation = self.generation();
            if let Some(route) = self.route(number)?.filter(|route| route.leader.is_some()) {
                return Ok(route);
            }
            tokio::select! {
                result = self.changed_after(generation) => result?,
                () = clock.until(clock.now().saturating_add(refresh)) => self.refresh(number)?,
            }
        }
    }

    /// Cancel only this observer when its selected source changes. Its accepted
    /// broker work and other logical owners on these physical links continue.
    pub(in crate::replicated) async fn while_current<F: Future>(
        &self,
        number: u32,
        selected: &RouteState,
        work: F,
    ) -> Result<F::Output, BrokerLinkError> {
        let mut generation = self.generation();
        let mut running = pin!(work);
        loop {
            if self
                .route(number)?
                .is_none_or(|route| route.view != selected.view || route.leader != selected.leader)
            {
                return Err(BrokerLinkError::Session);
            }
            tokio::select! {
                result = running.as_mut() => return Ok(result),
                result = self.changed_after(generation) => {
                    result?;
                    generation = self.generation();
                }
            }
        }
    }

    /// Unrelated link or route notifications cannot shorten retry pacing.
    pub(in crate::replicated) async fn wait_retry(
        &self,
        number: u32,
        selected: &RouteState,
        backoff: Duration,
    ) -> Result<bool, BrokerLinkError> {
        let clock = self.links.clock();
        let deadline = clock.now().saturating_add(backoff);
        loop {
            let generation = self.generation();
            if self.route(number)?.is_some_and(|route| {
                route.leader.is_some()
                    && (route.view != selected.view || route.leader != selected.leader)
            }) {
                return Ok(true);
            }
            tokio::select! {
                result = self.changed_after(generation) => result?,
                () = clock.until(deadline) => return Ok(false),
            }
        }
    }

    pub(in crate::replicated) fn validate_hint(
        hint: Option<AuthorityHint>,
        selected: &RouteState,
    ) -> Result<(), BrokerLinkError> {
        if hint.is_some_and(|hint| {
            hint.authority.group_id != selected.group
                || hint.authority.config_epoch != selected.config_epoch
                || !selected.members.contains(&hint.primary)
        }) {
            return Err(BrokerLinkError::Response);
        }
        Ok(())
    }
}

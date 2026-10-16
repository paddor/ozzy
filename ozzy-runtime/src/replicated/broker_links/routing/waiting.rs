//! Shared owner waits. Capture generations before checking route state.

use super::{BrokerLinkError, TopicRoutes};
use ozzy_proto::{NodeId, directory::RouteState, nack::AuthorityHint};
use std::{future::Future, pin::pin, time::Duration};

/// A route observation that names its current leader.
#[derive(Clone, Debug)]
pub(in crate::replicated) struct LeaderRoute {
    pub(in crate::replicated) leader: NodeId,
    pub(in crate::replicated) route: RouteState,
}

impl TopicRoutes {
    pub(in crate::replicated) async fn leader(
        &self,
        number: u32,
        refresh: Duration,
    ) -> Result<LeaderRoute, BrokerLinkError> {
        let clock = self.links.clock();
        loop {
            let generation = self.generation();
            let selected = self.route(number)?.and_then(|route| {
                Some(LeaderRoute {
                    leader: route.leader?,
                    route,
                })
            });
            if let Some(selected) = selected {
                return Ok(selected);
            }
            tokio::select! {
                result = self.changed_after(generation) => result?,
                () = clock.until(clock.now().saturating_add(refresh)) => self.refresh(number)?,
            }
        }
    }

    /// Call the partition's current leader once, with its authority and a
    /// deadline `timeout` from now, while it stays the selected leader.
    /// Returns the route the call used.
    pub(in crate::replicated) async fn call_leader<T, Fut>(
        &self,
        number: u32,
        timeout: Duration,
        call: impl FnOnce(ozzy_proto::append::Authority, ozzy_proto::NodeId, Duration) -> Fut,
    ) -> Result<(RouteState, Result<T, BrokerLinkError>), BrokerLinkError>
    where
        Fut: Future<Output = Result<T, BrokerLinkError>>,
    {
        let LeaderRoute { leader, route } = self.leader(number, timeout).await?;
        let deadline = self.links.clock().now().saturating_add(timeout);
        let result = self
            .while_current(number, &route, call(route.authority(), leader, deadline))
            .await;
        Ok((route, result.and_then(|result| result)))
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

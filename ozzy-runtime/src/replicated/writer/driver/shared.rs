//! Partition routing on shared physical broker links. No per-partition sockets.

use super::{Error, Failure, RetryPolicy, Session, SessionBinding};
use crate::replicated::{
    TopicRoutes,
    broker_links::{LeaderRoute, append},
};
use std::sync::atomic::Ordering;

mod opening;
use opening::Opening;

/// The partition a shared writer appends to, and how its attempts retry.
pub(in crate::replicated::writer) struct Target {
    pub(in crate::replicated::writer) routes: TopicRoutes,
    pub(in crate::replicated::writer) number: u32,
    pub(in crate::replicated::writer) retry: RetryPolicy,
}

pub(in crate::replicated::writer) async fn run_shared(
    mut shared: super::super::state::Driver,
    mut connection: append::Connection,
    target: Target,
    operation: Option<ozzy_proto::OperationId>,
) {
    let stop = shared.stop.clone();
    let links = target.routes.links().clone();
    let work = target.write(&mut shared, &mut connection, Opening::new(operation));
    tokio::select! {
        () = stop.closed() => {},
        () = links.closed() => shared.fail(Error::SharedLink(crate::replicated::BrokerLinkError::Closed).into()),
        result = work => {
            if let Err(error) = result { shared.fail(error.into()); }
        }
    }
}

impl Target {
    /// Wait for the first record, then append through each selected leader
    /// until the writer finishes or fails.
    async fn write(
        &self,
        shared: &mut super::super::state::Driver,
        connection: &mut append::Connection,
        mut opening: Opening,
    ) -> Result<(), Error> {
        let (routes, number, retry) = (&self.routes, self.number, self.retry);
        // Opening an idle partition writer creates no watch or network work.
        while shared.next.load(Ordering::Acquire) & !super::super::state::SEALED
            == shared.config.next_sequence
        {
            shared.work.ready().await;
            shared.work.drain(|| ());
        }
        routes.interest(number)?;
        let mut backoff = retry.initial_backoff;
        loop {
            let selected = routes.leader(number, retry.response_timeout).await?;
            let index = routes
                .metadata()
                .brokers()
                .iter()
                .position(|broker| broker.node == selected.leader)
                .ok_or(Error::Response)?;
            let settings = &shared.shared;
            shared
                .batches
                .select_route(index, &settings.config, &settings.work);
            let attempt = self
                .attempt(shared, connection, &selected, &mut opening)
                .await;
            // This removes only this writer's IDs and reply queue. HELLO,
            // physical sessions, watches, and other writers remain current.
            connection.invalidate_session();
            let later = match attempt {
                Ok(()) => return Ok(()),
                Err(Failure::Fatal(error)) => return Err(error),
                Err(Failure::Later) => true,
                Err(Failure::Retry(hint)) => {
                    TopicRoutes::validate_hint(hint, &selected.route)
                        .map_err(|_| Error::Response)?;
                    false
                }
            };
            routes.refresh(number)?;
            // Only another view or leader ends the backoff early. The
            // refresh answer and other writers on these links also change
            // the generation, and a broker that refused keeps refusing.
            // A broker that asked to try later is asked again at the
            // initial interval, because only it knows when it is ready.
            if later {
                backoff = retry.initial_backoff;
            }
            let moved = routes.wait_retry(number, &selected.route, backoff).await?;
            backoff = if moved || later {
                retry.initial_backoff
            } else {
                retry.next_backoff(backoff)
            };
        }
    }

    async fn attempt(
        &self,
        shared: &mut super::super::state::Driver,
        connection: &mut append::Connection,
        selected: &LeaderRoute,
        opening: &mut Opening,
    ) -> Result<(), Failure> {
        let (routes, retry) = (&self.routes, self.retry);
        let clock = routes.links().clock();
        let remote = selected.leader;
        let work = async {
            tokio::select! {
            result = connection.refresh_session(remote) => result.map_err(|error| Failure::Fatal(error.into()))?,
            () = clock.until(clock.now().saturating_add(retry.handshake_timeout)) => return Err(Failure::Retry(None)),
            }
            let authority = selected.route.authority();
            opening.confirm(routes, selected, &shared.config).await?;
            Session::new(shared, remote, connection.clock())
                .run(
                    shared,
                    SessionBinding {
                        connection,
                        remote,
                        local: routes.links().local(),
                        authority,
                        retry,
                    },
                )
                .await
        };
        match routes
            .while_current(self.number, &selected.route, work)
            .await
        {
            Ok(result) => result,
            Err(crate::replicated::BrokerLinkError::Session) => Err(Failure::Retry(None)),
            Err(error) => Err(Failure::Fatal(error.into())),
        }
    }
}

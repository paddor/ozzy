//! Partition routing on shared physical broker links. No per-partition sockets.

use super::{Authority, Error, Failure, RetryPolicy, Session, SessionBinding};
use crate::replicated::{TopicRoutes, broker_links::append};
use std::sync::atomic::Ordering;

mod opening;
use opening::Opening;

pub(in crate::replicated::writer) async fn run_shared(
    mut shared: super::super::state::Driver,
    mut connection: append::Connection,
    routes: TopicRoutes,
    number: u32,
    retry: RetryPolicy,
    operation: Option<ozzy_proto::OperationId>,
) {
    let stop = shared.stop.clone();
    let links = routes.links().clone();
    let mut opening = Opening::new(operation);
    let work = async {
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
            let route = routes.leader(number, retry.response_timeout).await?;
            let remote = route.leader.expect("known route leader");
            let index = routes
                .metadata()
                .brokers()
                .iter()
                .position(|broker| broker.node == remote)
                .ok_or(Error::Response)?;
            let settings = &shared.shared;
            shared
                .batches
                .select_route(index, &settings.config, &settings.work);
            let attempt = attempt(
                &mut shared,
                &mut connection,
                &routes,
                number,
                &route,
                retry,
                &mut opening,
            )
            .await;
            // This removes only this writer's IDs and reply queue. HELLO,
            // physical sessions, watches, and other writers remain current.
            connection.invalidate_session();
            let later = match attempt {
                Ok(()) => return Ok::<(), Error>(()),
                Err(Failure::Fatal(error)) => return Err(error),
                Err(Failure::Later) => true,
                Err(Failure::Retry(hint)) => {
                    TopicRoutes::validate_hint(hint, &route).map_err(|_| Error::Response)?;
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
            let moved = routes.wait_retry(number, &route, backoff).await?;
            backoff = if moved || later {
                retry.initial_backoff
            } else {
                backoff.saturating_mul(2).min(retry.max_backoff)
            };
        }
    };
    tokio::select! {
        () = stop.closed() => {},
        () = links.closed() => shared.fail(Error::SharedLink(crate::replicated::BrokerLinkError::Closed).into()),
        result = work => {
            if let Err(error) = result { shared.fail(error.into()); }
        }
    }
}

async fn attempt(
    shared: &mut super::super::state::Driver,
    connection: &mut append::Connection,
    routes: &TopicRoutes,
    number: u32,
    route: &ozzy_proto::directory::RouteState,
    retry: RetryPolicy,
    opening: &mut Opening,
) -> Result<(), Failure> {
    let clock = routes.links().clock();
    let remote = route.leader.expect("selected leader");
    let work = async {
        tokio::select! {
        result = connection.refresh_session(remote) => result.map_err(|error| Failure::Fatal(error.into()))?,
        () = clock.until(clock.now().saturating_add(retry.handshake_timeout)) => return Err(Failure::Retry(None)),
        }
        let authority = Authority {
            group_id: route.group,
            config_epoch: route.config_epoch,
            view: route.view,
        };
        opening.confirm(routes, route, &shared.config).await?;
        Session::new(shared, remote, connection.clock())
            .run(
                shared,
                SessionBinding {
                    connection,
                    remote,
                    local: routes.links().local(),
                    authority: Some(authority),
                    retry,
                },
            )
            .await
    };
    match routes.while_current(number, route, work).await {
        Ok(result) => result,
        Err(crate::replicated::BrokerLinkError::Session) => Err(Failure::Retry(None)),
        Err(error) => Err(Failure::Fatal(error.into())),
    }
}

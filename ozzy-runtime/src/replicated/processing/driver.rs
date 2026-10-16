//! One source owner submits cumulative intent through existing SDK broker links.

use super::{ProcessingIdentity, ProgressError, progress::Publisher};
use crate::replicated::{
    BrokerLinkError, RetryPolicy, TopicRoutes, broker_links::LeaderRoute, retry::classify,
};
use crate::signal::CloseSignal;
use ozzy_proto::{Offset, directory::RouteState, nack::RetryClass, progress};
use std::time::Duration;
use tokio::task::JoinHandle;

/// Spawn on the existing SDK owner, never the application executor. Dropping
/// the caller's window requests closure; an admitted broker action may finish.
pub(crate) fn start(
    routes: TopicRoutes,
    number: u32,
    publisher: Publisher,
    retry: RetryPolicy,
    stop: Option<CloseSignal>,
) -> Result<JoinHandle<()>, ProgressError> {
    routes
        .links()
        .require_progress()
        .map_err(|error| terminal(&error))?;
    let identity = publisher.observer().identity();
    if retry.validate().is_err()
        || routes.metadata().policy() != identity.policy
        || routes
            .metadata()
            .partition(number)
            .is_none_or(|partition| partition.incarnation != identity.partition)
    {
        return Err(ProgressError::Configuration);
    }
    let owner = routes.links().runtime().driver().clone();
    Ok(owner.spawn(run(routes, number, publisher, retry, stop)))
}

async fn run(
    routes: TopicRoutes,
    number: u32,
    mut publisher: Publisher,
    retry: RetryPolicy,
    stop: Option<CloseSignal>,
) {
    let observer = publisher.observer();
    let links = routes.links().clone();
    let work = drive(&routes, number, &mut publisher, retry);
    let result = tokio::select! {
        biased;
        error = observer.closing() => Err(error),
        () = async {
            match stop {
                Some(stop) => stop.closed().await,
                None => std::future::pending().await,
            }
        } => Err(ProgressError::Fenced),
        () = links.closed() => Err(ProgressError::Closed),
        result = work => result,
    };
    if let Err(error) = result {
        publisher.fail(error);
    }
}

async fn drive(
    routes: &TopicRoutes,
    number: u32,
    publisher: &mut Publisher,
    retry: RetryPolicy,
) -> Result<(), ProgressError> {
    let clock = routes.links().clock();
    let identity = publisher.observer().identity();
    routes.interest(number).map_err(|error| terminal(&error))?;
    let mut backoff = retry.initial_backoff;
    loop {
        publisher.ready().await?;
        crate::profiling::event(crate::profiling::Event::ProcessingReady);
        let selected = match routes.leader(number, retry.response_timeout).await {
            Ok(selected) => selected,
            Err(error) => {
                crate::profiling::event(crate::profiling::Event::ProcessingRetry);
                classify(&error, None)
                    .map_err(|error| terminal(&error))?
                    .ok_or_else(|| terminal(&error))?;
                routes.refresh(number).map_err(|error| terminal(&error))?;
                clock.until(clock.now().saturating_add(backoff)).await;
                backoff = retry.next_backoff(backoff);
                continue;
            }
        };
        let route = &selected.route;
        let cursor = publisher.ready().await?;
        let request = request(&identity, route, cursor);
        let deadline = clock.now().saturating_add(retry.response_timeout);
        let result = commit(routes, number, &selected, request, deadline).await;
        let later = match result {
            Ok(confirmed) => {
                crate::profiling::event(crate::profiling::Event::ProcessingConfirmed);
                publisher.confirm(confirmed.cursor.get(), cursor)?;
                backoff = retry.initial_backoff;
                // The next ready() rereads intent advanced during this request.
                // Confirming one captured prefix cannot consume a newer one.
                continue;
            }
            Err(error) => {
                crate::profiling::event(crate::profiling::Event::ProcessingRetry);
                let class = classify(&error, Some(route))
                    .map_err(|error| terminal(&error))?
                    .ok_or_else(|| terminal(&error))?;
                class == RetryClass::AfterBackoff
            }
        };
        routes.refresh(number).map_err(|error| terminal(&error))?;
        if later {
            backoff = retry.initial_backoff;
        }
        let moved = routes
            .wait_retry(number, route, backoff)
            .await
            .map_err(|error| terminal(&error))?;
        backoff = if moved || later {
            retry.initial_backoff
        } else {
            retry.next_backoff(backoff)
        };
    }
}

/// Commit `identity`'s window through `cursor` under `route`'s authority.
fn request(identity: &ProcessingIdentity, route: &RouteState, cursor: u64) -> progress::Request {
    progress::Request {
        authority: route.authority(),
        assignment: progress::Assignment {
            group: identity.group,
            partition: identity.partition,
            member: identity.member,
            epoch: identity.assignment_epoch,
        },
        policy: identity.policy,
        cursor: Some(Offset::new(cursor)),
    }
}

/// Send `request` to the selected leader by `deadline`, while its route
/// stays current.
async fn commit(
    routes: &TopicRoutes,
    number: u32,
    selected: &LeaderRoute,
    request: progress::Request,
    deadline: Duration,
) -> Result<progress::Confirmed, BrokerLinkError> {
    crate::profiling::event(crate::profiling::Event::ProcessingRequest);
    let started = crate::profiling::start();
    let result = routes
        .while_current(
            number,
            &selected.route,
            routes
                .links()
                .group_progress_until(selected.leader, request, deadline),
        )
        .await
        .and_then(|result| result);
    crate::profiling::finish(crate::profiling::Stage::ProcessingRoundtrip, started);
    result
}

fn terminal(error: &BrokerLinkError) -> ProgressError {
    match error {
        BrokerLinkError::Progress(error) => *error,
        BrokerLinkError::Configuration => ProgressError::Configuration,
        BrokerLinkError::Closed | BrokerLinkError::Failed(_) => ProgressError::Closed,
        _ => ProgressError::Response,
    }
}

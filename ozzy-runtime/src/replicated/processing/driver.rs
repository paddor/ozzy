//! One source owner submits cumulative intent through existing SDK broker links.

use super::{ProgressError, progress::Publisher};
use crate::replicated::{BrokerLinkError, RetryPolicy, TopicRoutes, retry::classify};
use crate::signal::CloseSignal;
use ozzy_proto::{Offset, append::Authority, nack::RetryClass, progress};
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
        let route = match routes.leader(number, retry.response_timeout).await {
            Ok(route) => route,
            Err(error) => {
                crate::profiling::event(crate::profiling::Event::ProcessingRetry);
                classify(&error, None)
                    .map_err(|error| terminal(&error))?
                    .ok_or_else(|| terminal(&error))?;
                routes.refresh(number).map_err(|error| terminal(&error))?;
                clock.until(clock.now().saturating_add(backoff)).await;
                backoff = backoff.saturating_mul(2).min(retry.max_backoff);
                continue;
            }
        };
        let cursor = publisher.ready().await?;
        let request = progress::Request {
            authority: Authority {
                group_id: route.group,
                config_epoch: route.config_epoch,
                view: route.view,
            },
            assignment: progress::Assignment {
                group: identity.group,
                partition: identity.partition,
                member: identity.member,
                epoch: identity.assignment_epoch,
            },
            policy: identity.policy,
            cursor: Some(Offset::new(cursor)),
        };
        let deadline = clock.now().saturating_add(retry.response_timeout);
        crate::profiling::event(crate::profiling::Event::ProcessingRequest);
        let started = crate::profiling::start();
        let result = routes
            .while_current(
                number,
                &route,
                routes.links().group_progress_until(
                    route.leader.expect("selected leader"),
                    request,
                    deadline,
                ),
            )
            .await
            .and_then(|result| result);
        crate::profiling::finish(crate::profiling::Stage::ProcessingRoundtrip, started);
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
                let class = classify(&error, Some(&route))
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
            .wait_retry(number, &route, backoff)
            .await
            .map_err(|error| terminal(&error))?;
        backoff = if moved || later {
            retry.initial_backoff
        } else {
            backoff.saturating_mul(2).min(retry.max_backoff)
        };
    }
}

fn terminal(error: &BrokerLinkError) -> ProgressError {
    match error {
        BrokerLinkError::Progress(error) => *error,
        BrokerLinkError::Configuration => ProgressError::Configuration,
        BrokerLinkError::Closed | BrokerLinkError::Failed(_) => ProgressError::Closed,
        _ => ProgressError::Response,
    }
}

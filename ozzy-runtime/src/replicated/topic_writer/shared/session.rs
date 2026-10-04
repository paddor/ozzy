//! Attaching resolves every partition before caller admission starts.

use super::{RetryPolicy, TopicRoutes, TopicWriterError};
use crate::replicated::BrokerLinkError;
use ozzy_proto::{OperationId, ProducerId, append::Authority, nack::RetryClass, producer};

pub(super) async fn resolve(
    routes: &TopicRoutes,
    number: u32,
    producer: ProducerId,
    takeover: bool,
    retry: RetryPolicy,
) -> Result<producer::Opened, TopicWriterError> {
    let partition = routes
        .metadata()
        .partition(number)
        .ok_or(TopicWriterError::Configuration)?;
    let mut open = producer::Open {
        authority: Authority {
            group_id: partition.group,
            config_epoch: partition.config_epoch,
            view: 0,
        },
        partition: partition.incarnation,
        producer,
        mode: producer::Mode::Resume,
        expected_epoch: None,
        operation: next_operation(routes)?,
    };
    let current = request(routes, number, open, retry).await?;
    if !takeover {
        return Ok(current);
    }
    open.mode = producer::Mode::Fence;
    open.expected_epoch = Some(current.epoch);
    open.operation = next_operation(routes)?;
    request(routes, number, open, retry).await
}

fn next_operation(routes: &TopicRoutes) -> Result<OperationId, TopicWriterError> {
    Ok(OperationId::from_bytes(
        *routes.links().next_request()?.as_bytes(),
    ))
}

async fn request(
    routes: &TopicRoutes,
    number: u32,
    mut open: producer::Open,
    retry: RetryPolicy,
) -> Result<producer::Opened, TopicWriterError> {
    routes.interest(number)?;
    let clock = routes.links().clock();
    let mut backoff = retry.initial_backoff;
    loop {
        let generation = routes.generation();
        let route = routes.route(number)?;
        let Some(route) = route.filter(|route| route.leader.is_some()) else {
            tokio::select! {
                changed = routes.changed_after(generation) => changed?,
                () = clock.until(clock.now().saturating_add(retry.response_timeout)) => routes.refresh(number)?,
            }
            continue;
        };
        open.authority = Authority {
            group_id: route.group,
            config_epoch: route.config_epoch,
            view: route.view,
        };
        let deadline = clock.now().saturating_add(retry.response_timeout);
        let response = tokio::select! {
            result = routes.links().open_producer(
                route.leader.expect("known leader"),
                open,
                routes.metadata().policy(),
            ) => result,
            () = clock.until(deadline) => Err(BrokerLinkError::Timeout),
        };
        match response {
            Ok(opened) => return Ok(opened),
            Err(
                BrokerLinkError::Timeout | BrokerLinkError::Session | BrokerLinkError::Transport(_),
            ) => {}
            Err(BrokerLinkError::Rejected { retry, .. }) if retry != RetryClass::Permanent => {}
            Err(error) => return Err(error.into()),
        }
        routes.refresh(number)?;
        clock.until(clock.now().saturating_add(backoff)).await;
        backoff = backoff.saturating_mul(2).min(retry.max_backoff);
    }
}

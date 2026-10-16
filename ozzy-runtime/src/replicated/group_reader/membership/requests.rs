//! Requests to a partition's current coordinator.

use super::{
    Authority, BrokerLinkError, Duration, Future, JoinReply, JoinRequest, NodeId, TopicRoutes,
    classify, group,
};

/// Send one request to the partition's current leader. An error that does
/// not allow a retry ends the caller.
async fn leader_call<T, Fut>(
    routes: &TopicRoutes,
    number: u32,
    timeout: Duration,
    call: impl FnOnce(Authority, NodeId, Duration) -> Fut,
) -> Result<T, BrokerLinkError>
where
    Fut: Future<Output = Result<T, BrokerLinkError>>,
{
    let (route, result) = routes.call_leader(number, timeout, call).await?;
    if let Err(error) = &result {
        classify(error, Some(&route))?;
    }
    result
}

pub(super) async fn join(
    routes: TopicRoutes,
    number: u32,
    mut request: JoinRequest,
    timeout: Duration,
) -> Result<JoinReply, BrokerLinkError> {
    let links = routes.links().clone();
    leader_call(&routes, number, timeout, |authority, leader, deadline| {
        request.authority = authority;
        async move { links.group_join_until(leader, request, deadline).await }
    })
    .await
}

pub(super) async fn assign(
    routes: TopicRoutes,
    number: u32,
    mut request: group::AssignRequest,
    timeout: Duration,
) -> Result<group::AssignReply, BrokerLinkError> {
    routes.interest(number)?;
    let links = routes.links().clone();
    leader_call(&routes, number, timeout, |authority, leader, deadline| {
        request.authority = authority;
        async move { links.group_assign_until(leader, request, deadline).await }
    })
    .await
}

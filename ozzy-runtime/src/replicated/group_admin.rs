//! Consumer-group inspection and deletion over shared broker links.
//!
//! Deletion first revokes the incarnation on every source at the maximum plan
//! revision, which fences its members' activations and progress, then retires
//! the coordinator image so the name can start a new incarnation. Retrying a
//! failed deletion repeats both steps; each is idempotent.

use super::{
    BrokerLinkError, BrokerLinks, RetryPolicy, TopicRoutes, broker_links::LeaderRoute,
    retry::classify,
};
use ozzy_proto::{
    ConsumerGroupId, NodeId,
    append::Authority,
    group::{self, Start},
    nack::RetryClass,
};
use std::{future::Future, time::Duration};

/// Attempts per broker request across authority changes and backoff refusals.
const ATTEMPTS: u32 = 16;

/// Committed coordinator image of one consumer group. Sources confirm
/// activations separately; targets describe intent only.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct GroupInspection {
    /// Group incarnation. A deleted and recreated name has a new one.
    pub group: ConsumerGroupId,
    /// Committed coordinator revision.
    pub revision: u64,
    /// First position of sources without a cursor.
    pub start: Start,
    /// Processing instances in member ID order.
    pub members: Vec<group::InspectedMember>,
    /// Intended ownership in numeric partition order.
    pub targets: Vec<group::InspectedTarget>,
}

impl BrokerLinks {
    /// Read the committed coordinator image of group `name` on `topic`, or
    /// `None` when no group has that name.
    pub async fn inspect_group(
        &self,
        topic: &str,
        name: &str,
    ) -> Result<Option<GroupInspection>, BrokerLinkError> {
        let routes = self.group_routes(topic, name).await?;
        inspect(&routes, name).await
    }

    /// Delete group `name` on `topic` and return the retired incarnation, or
    /// `None` when no group has that name. Its members' activations and
    /// progress are fenced on every source before the name is freed; a later
    /// reader with that name creates a new incarnation without old cursors.
    /// Readers of the retired incarnation stop with a deletion error.
    pub async fn delete_group(
        &self,
        topic: &str,
        name: &str,
    ) -> Result<Option<ConsumerGroupId>, BrokerLinkError> {
        let routes = self.group_routes(topic, name).await?;
        let Some(inspection) = inspect(&routes, name).await? else {
            return Ok(None);
        };
        let group = inspection.group;
        let metadata = routes.metadata().clone();
        for number in 0..metadata.partition_count() as u32 {
            let partition = metadata
                .partition(number)
                .ok_or(BrokerLinkError::Response)?
                .incarnation;
            let reply = with_leader(&routes, number, |authority, leader, deadline| {
                routes.links().group_assign_until(
                    leader,
                    group::AssignRequest {
                        authority,
                        partition,
                        group,
                        member: None,
                        plan_revision: u64::MAX,
                        initial: Start::Earliest,
                        policy: metadata.policy(),
                    },
                    deadline,
                )
            })
            .await?;
            if reply.member.is_some() || reply.plan_revision != u64::MAX {
                return Err(BrokerLinkError::Response);
            }
        }
        let coordinator = metadata.coordinator(name).clone();
        // A concurrent deletion may already have retired the incarnation.
        with_leader(
            &routes,
            coordinator.number,
            |authority, leader, deadline| {
                routes.links().group_delete_until(
                    leader,
                    group::DeleteRequest {
                        authority,
                        coordinator: coordinator.incarnation,
                        group,
                        name: name.to_owned(),
                    },
                    deadline,
                )
            },
        )
        .await?;
        Ok(Some(group))
    }

    async fn group_routes(&self, topic: &str, name: &str) -> Result<TopicRoutes, BrokerLinkError> {
        if name.is_empty() || name.len() > group::MAX_NAME_BYTES {
            return Err(BrokerLinkError::Configuration);
        }
        self.require_groups()?;
        let metadata = self.topic(topic).await?;
        if metadata.partition_count() == 0 || metadata.partition_count() > group::MAX_PARTITIONS {
            return Err(BrokerLinkError::Configuration);
        }
        self.routes(metadata).await
    }
}

async fn inspect(
    routes: &TopicRoutes,
    name: &str,
) -> Result<Option<GroupInspection>, BrokerLinkError> {
    let coordinator = routes.metadata().coordinator(name).clone();
    let reply = with_leader(routes, coordinator.number, |authority, leader, deadline| {
        routes.links().group_inspect_until(
            leader,
            group::InspectRequest {
                authority,
                coordinator: coordinator.incarnation,
                name: name.to_owned(),
            },
            deadline,
        )
    })
    .await?;
    Ok(reply.group.map(|group| GroupInspection {
        group,
        revision: reply.revision,
        start: reply.initial,
        members: reply.members,
        targets: reply.targets,
    }))
}

/// One request to a partition leader, retried across authority changes and
/// backoff refusals. Permanent rejections return at once.
async fn with_leader<T, F, Fut>(
    routes: &TopicRoutes,
    number: u32,
    mut call: F,
) -> Result<T, BrokerLinkError>
where
    F: FnMut(Authority, NodeId, Duration) -> Fut,
    Fut: Future<Output = Result<T, BrokerLinkError>>,
{
    let retry = RetryPolicy::default();
    let clock = routes.links().clock();
    routes.interest(number)?;
    let mut backoff = retry.initial_backoff;
    for attempt in 1..=ATTEMPTS {
        let LeaderRoute { leader, route } = tokio::select! {
            selected = routes.leader(number, retry.response_timeout) => selected?,
            () = clock.until(clock.now().saturating_add(retry.response_timeout)) => continue,
        };
        let deadline = clock.now().saturating_add(retry.response_timeout);
        let error = match routes
            .while_current(number, &route, call(route.authority(), leader, deadline))
            .await
        {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) | Err(error) => error,
        };
        let Some(class) = classify(&error, Some(&route))? else {
            return Err(error);
        };
        if attempt == ATTEMPTS {
            return Err(error);
        }
        if class != RetryClass::AfterBackoff {
            routes.refresh(number)?;
        }
        clock.until(clock.now().saturating_add(backoff)).await;
        backoff = retry.next_backoff(backoff);
    }
    Err(BrokerLinkError::Timeout)
}

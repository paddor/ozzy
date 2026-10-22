//! Broker routes from the checked configuration: peers, followers, and repairs.

use super::super::failure;
use crate::{CheckedConfig, StartupError};
use omq_tokio::Endpoint;
use ozzy_proto::NodeId;
use std::collections::BTreeMap;

/// Control endpoint of every other configured broker.
pub(super) fn peer_endpoints(
    checked: &CheckedConfig,
    local: NodeId,
) -> Result<BTreeMap<NodeId, Endpoint>, StartupError> {
    let deployment = checked.deployment.deployment();
    checked
        .identity
        .brokers
        .iter()
        .filter(|(_, id)| NodeId::from_bytes(*id.as_bytes()) != local)
        .map(|(name, id)| {
            Ok((
                NodeId::from_bytes(*id.as_bytes()),
                deployment
                    .broker(name)?
                    .endpoints
                    .peer
                    .parse()
                    .map_err(failure)?,
            ))
        })
        .collect()
}

pub(super) fn follower_routes(
    checked: &CheckedConfig,
    local: NodeId,
    brokers: &BTreeMap<NodeId, Endpoint>,
) -> Result<crate::FollowerRoutes, StartupError> {
    let deployment = checked.deployment.deployment();
    let mut followers = crate::FollowerRoutes::default();
    for placement in &checked.plan.partitions {
        let partition = checked
            .identity
            .partition(&placement.topic, placement.partition)?;
        followers.local.insert(
            ozzy_proto::GroupId::from_bytes(*partition.group.as_bytes()),
            placement.shard,
        );
    }
    for (name, id) in &checked.identity.brokers {
        let broker = NodeId::from_bytes(*id.as_bytes());
        if broker != local {
            followers.repairs.insert(
                broker,
                deployment
                    .broker(name)?
                    .endpoints
                    .data_peer
                    .parse()
                    .map_err(failure)?,
            );
        }
        if broker == local {
            continue;
        }
        if let Some(endpoint) = &deployment.broker(name)?.endpoints.follower_pub {
            followers
                .publications
                .insert(broker, endpoint.parse().map_err(failure)?);
        }
        for placement in checked.deployment.partition_placements(name)? {
            let partition = checked
                .identity
                .partition(&placement.topic, placement.partition)?;
            followers.destinations.insert(
                (
                    broker,
                    ozzy_proto::GroupId::from_bytes(*partition.group.as_bytes()),
                ),
                placement.shard,
            );
        }
        for shard in &checked.plan.shards {
            let alias = crate::FollowerRoutes::identity(broker, local, shard.id);
            if alias == local
                || brokers.contains_key(&alias)
                || followers
                    .incoming
                    .insert(alias, (broker, shard.id))
                    .is_some()
            {
                return Err(failure("repair identity collision"));
            }
        }
    }
    Ok(followers)
}

/// Replicated brokers each need an explicit follower PUB endpoint.
pub(super) fn require_follower_pub_endpoints(checked: &CheckedConfig) -> Result<(), StartupError> {
    if checked.identity.brokers.len() == 3
        && checked
            .deployment
            .deployment()
            .brokers
            .values()
            .any(|broker| broker.endpoints.follower_pub.is_none())
    {
        return Err(failure(
            "replicated brokers require explicit follower PUB endpoints",
        ));
    }
    Ok(())
}

//! TCP layer of the shared real-broker memory harness, for wheels and benchmarks.
#![allow(clippy::unwrap_used)]
use ozzy_config::DeploymentMode;
use ozzy_sim::broker::Cluster;
use std::net::TcpListener;

fn endpoint(transport: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    format!("{transport}://{}", listener.local_addr().unwrap())
}

pub(super) async fn cluster(
    mode: DeploymentMode,
    transport: &str,
    bytes: usize,
    layout: Option<(u64, u64)>,
) -> Cluster {
    let mut limits = ozzy_sim::broker::IMAGE_LIMITS;
    limits.total_bytes = bytes;
    if let Some((segment, _)) = layout {
        limits.file_bytes = limits.file_bytes.max(segment as usize);
    }
    Cluster::configured_sized(mode, None, limits, |deployment| {
        let topic = deployment.topics.get_mut("orders").unwrap();
        topic.max_append_bytes = 256 * 1024;
        if let Some((segment, bytes)) = layout {
            topic.retention.max_bytes = Some(bytes);
            topic.segment_bytes = segment;
        }
        for broker in deployment.brokers.values_mut() {
            broker.endpoints.peer = endpoint(transport);
            broker.endpoints.data_peer = endpoint(transport);
            broker.endpoints.reader_pub = endpoint(transport);
            if broker.endpoints.follower_pub.is_some() {
                broker.endpoints.follower_pub = Some(endpoint(transport));
            }
        }
    })
    .await
}

pub(super) fn addresses(cluster: &Cluster) -> Vec<(String, String, String)> {
    cluster
        .configs
        .iter()
        .map(|(checked, local)| {
            let endpoints = &checked.deployment.deployment().brokers[&checked.plan.name].endpoints;
            (
                local.broker.to_string(),
                endpoints.peer.clone(),
                endpoints.data_peer.clone(),
            )
        })
        .collect()
}

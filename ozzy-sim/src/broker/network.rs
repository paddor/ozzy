//! TCP setup shared by installed language packages and their benchmarks.
#![allow(clippy::unwrap_used)]
use super::Cluster;
use ozzy_config::DeploymentMode;
use std::net::TcpListener;

/// Start the real broker with bounded memory storage and ephemeral TCP endpoints.
/// Optional layout supplies segment bytes and retention bytes per partition.
pub async fn cluster(
    mode: DeploymentMode,
    transport: &str,
    bytes: usize,
    layout: Option<(u64, u64)>,
) -> Cluster {
    let mut limits = super::IMAGE_LIMITS;
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
        // Keep all probes bound until the complete deployment has unique ports.
        let mut listeners = Vec::new();
        let mut endpoint = || {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let endpoint = format!("{transport}://{}", listener.local_addr().unwrap());
            listeners.push(listener);
            endpoint
        };
        for broker in deployment.brokers.values_mut() {
            broker.endpoints.peer = endpoint();
            broker.endpoints.data_peer = endpoint();
            broker.endpoints.reader_pub = endpoint();
            if broker.endpoints.follower_pub.is_some() {
                broker.endpoints.follower_pub = Some(endpoint());
            }
        }
    })
    .await
}

/// Persistent IDs and control/data endpoints for clients of this cluster.
pub fn addresses(cluster: &Cluster) -> Vec<(String, String, String)> {
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

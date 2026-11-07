use crate::Result;
use omq_tokio::{Context, Message, MonitorEvent, Options, SocketType};
use ozzy_proto::admin::{self, Reply, Request};
use serde_json::{Value, json};

pub async fn snapshot(endpoint: &str) -> Result<Value> {
    let context = Context::new();
    let socket = context.socket(SocketType::Peer, Options::default());
    let mut monitor = socket.monitor();
    socket.connect(endpoint).await?;
    let broker = loop {
        if let MonitorEvent::HandshakeSucceeded { peer, .. } = monitor.recv().await? {
            break peer
                .peer_identity
                .ok_or("admin handshake has no identity")?;
        }
    };
    let socket = socket.identity_routing()?;
    let mut replies = Vec::new();
    for (id, request) in [Request::Broker, Request::Partitions, Request::Connections]
        .iter()
        .enumerate()
    {
        let id = id as u64 + 1;
        socket
            .send_to(&broker, Message::from(admin::encode_request(id, request)?))
            .await?;
        let (from, message) = socket.recv_from().await?;
        if from.as_ref() != broker.as_ref() {
            return Err("admin replied from another identity".into());
        }
        let bytes = message.iter().next().ok_or("empty admin reply")?;
        let (answered, reply) = admin::decode_reply(&bytes)?;
        if answered != id {
            return Err("admin reply correlation mismatch".into());
        }
        replies.push(reply);
    }
    socket.into_inner().close().await?;
    let [
        Reply::Broker(broker),
        Reply::Partitions(partitions),
        Reply::Connections(connections),
    ] = replies.as_slice()
    else {
        return Err("unexpected admin reply".into());
    };
    let rows: Vec<_> = partitions
        .rows
        .iter()
        .map(|row| {
            let name = broker.partitions.iter().find(|p| p.group == row.group);
            json!({"group": row.group.to_string(), "topic": name.map(|p| &p.topic),
                "partition": name.map(|p| p.partition), "ready": row.ready,
                "view": row.view, "leader": row.leader.map(|id| id.to_string()),
                "accepted": row.accepted, "committed": row.committed,
                "applied": row.applied, "durable": row.durable,
                "disk_pending": row.disk_pending, "state": format!("{:?}", row.state)})
        })
        .collect();
    Ok(json!({"id": broker.id.to_string(), "pid": broker.pid,
        "missing_shards": partitions.missing_shards, "partitions": rows,
        "clock_paused": connections.clock_paused,
        "clock_offsets": connections.clock_offsets.iter()
            .map(|(id, offset)| (id.to_string(), offset)).collect::<Vec<_>>() }))
}

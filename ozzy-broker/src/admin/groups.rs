//! Consumer group deletion. Brokers never send group requests to each other,
//! so the admin owner deletes through its own client link to every broker, as
//! the SDK does: it fences the group on each partition's leader, then frees
//! the name on its coordinator's leader.

use super::failure;
use crate::{CheckedConfig, StartupError};
use omq_tokio::{Context, Identity, IdentitySocket, Message};
use ozzy_proto::{
    ConsumerGroupId, EnvelopeLimits, NodeId,
    admin::{self, Reply},
    data::DataLimits,
    handshake,
};
use ozzy_runtime::{
    profiling,
    replicated::{BrokerLinkError, BrokerLinks, BrokerLinksConfig, TopicRoutes, WriterRuntime},
};
use std::time::Duration;
use tokio::task::JoinHandle;
use uuid::Uuid;

/// How long one deletion may take. Deleting again finishes a cut one.
const DEADLINE: Duration = Duration::from_secs(60);
/// Group inspection and plans are control metadata; no record reaches this
/// link.
const LIMITS: DataLimits = DataLimits {
    envelope: EnvelopeLimits {
        max_metadata_bytes: 64 * 1024,
        max_payload_bytes: 4096,
    },
    max_records: 1,
    max_parts: 1,
    max_record_bytes: 4096,
};

/// Runs one group deletion at a time beside the admin requests.
#[derive(Debug)]
pub(super) struct Deleter {
    context: Context,
    /// The admin endpoint, to reply when a deletion ends.
    replies: IdentitySocket,
    /// The link of every deletion, under a fresh client ID each time.
    config: BrokerLinksConfig,
    /// Started by the first deletion; its SDK thread then idles.
    runtime: Option<WriterRuntime>,
    running: Option<JoinHandle<()>>,
}

impl Deleter {
    pub(super) fn new(
        checked: &CheckedConfig,
        context: &Context,
        clock: ozzy_runtime::replicated::SdkClock,
        replies: IdentitySocket,
    ) -> Result<Self, StartupError> {
        let mut parameters =
            handshake::Parameters::group(LIMITS, handshake::CONSUMER).map_err(failure)?;
        parameters.capabilities |= handshake::OWNER_ROUTING;
        let partitions = checked.plan.partitions.len();
        let mut config = BrokerLinksConfig {
            local: NodeId::from_bytes([0; 16]),
            brokers: checked.broker_addresses()?,
            parameters,
            requests: 4,
            control_bytes: 0,
            routing_bytes: TopicRoutes::reservation_bytes(partitions)
                .ok_or_else(|| failure("group deletion routing overflow"))?,
            append: None,
            reader: None,
            maximum_partitions: partitions,
            request_timeout: Duration::from_secs(5),
            retry_interval: Duration::from_millis(100),
            clock,
        };
        config.control_bytes = config
            .control_reservation_bytes()
            .ok_or_else(|| failure("group deletion control overflow"))?;
        Ok(Self {
            context: context.clone(),
            replies,
            config,
            runtime: None,
            running: None,
        })
    }

    /// Start deleting group `name` on `topic`. The reply to request `id` goes
    /// to `peer` when the deletion ends. Refused while another deletion runs.
    pub(super) fn start(
        &mut self,
        peer: Identity,
        id: u64,
        topic: String,
        name: String,
    ) -> Result<(), Reply> {
        if self
            .running
            .as_ref()
            .is_some_and(|task| !task.is_finished())
        {
            return Err(Reply::Error("another group deletion is running".into()));
        }
        let socket = self.replies.clone();
        let runtime = if let Some(runtime) = &self.runtime {
            runtime.clone()
        } else {
            let runtime = WriterRuntime::with_context(self.context.clone())
                .map_err(|error| Reply::Error(format!("SDK runtime: {error}")))?;
            self.runtime.insert(runtime).clone()
        };
        let mut config = self.config.clone();
        config.local = NodeId::from_bytes(*Uuid::now_v7().as_bytes());
        self.running = Some(tokio::spawn(async move {
            let deleted = tokio::time::timeout(DEADLINE, delete(&runtime, config, &topic, &name));
            let reply = match deleted.await {
                Ok(Ok(group)) => {
                    if let Some(group) = group {
                        // TODO(admin-auth): name the admin once admins
                        // authenticate.
                        profiling::log(
                            profiling::Level::Info,
                            &format!("admin deleted consumer group {topic}/{name} ({group})"),
                        );
                    }
                    Reply::GroupDeleted(group)
                }
                Ok(Err(error)) => Reply::Error(format!("delete {topic}/{name}: {error}")),
                Err(_) => Reply::Error(format!(
                    "delete {topic}/{name}: no result in {} s; delete again to finish",
                    DEADLINE.as_secs()
                )),
            };
            if let Ok(bytes) = admin::encode_reply(id, &reply) {
                let _ = socket.try_send_to(&peer, Message::from(bytes));
            }
        }));
        Ok(())
    }

    /// Stop a running deletion. Its group may stay fenced with the name
    /// taken; deleting again finishes it.
    pub(super) fn stop(&mut self) {
        if let Some(task) = self.running.take() {
            task.abort();
        }
    }
}

async fn delete(
    runtime: &WriterRuntime,
    config: BrokerLinksConfig,
    topic: &str,
    name: &str,
) -> Result<Option<ConsumerGroupId>, BrokerLinkError> {
    let links = BrokerLinks::connect(runtime, config).await?;
    let deleted = links.delete_group(topic, name).await;
    let closed = links.shutdown().await;
    let deleted = deleted?;
    closed?;
    Ok(deleted)
}

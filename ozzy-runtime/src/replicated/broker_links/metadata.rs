//! Complete, independently validated catalogs from bounded broker attempts.

use futures::{StreamExt, stream::FuturesUnordered};
use ozzy_proto::directory;

use super::{BrokerLinkError, BrokerLinks, Duration, NodeId, TopicMetadata, driver};

impl BrokerLinks {
    /// Fetch complete numeric partition metadata from any reachable broker.
    /// Attempts share the request deadline and existing control reservations.
    pub async fn topic(&self, name: &str) -> Result<TopicMetadata, BrokerLinkError> {
        if name.is_empty() || name.len() > 128 {
            return Err(BrokerLinkError::Configuration);
        }
        let deadline = self
            .0
            .config
            .clock
            .now()
            .saturating_add(self.0.config.request_timeout);
        let mut attempts = self
            .0
            .peers
            .keys()
            .map(|&broker| self.topic_from(broker, name, deadline))
            .collect::<FuturesUnordered<_>>();
        let mut failure = BrokerLinkError::Timeout;
        while let Some(result) = attempts.next().await {
            match result {
                Ok(metadata) => return Ok(metadata),
                Err(error @ (BrokerLinkError::Timeout | BrokerLinkError::Session)) => {
                    failure = error;
                }
                Err(error) => return Err(error),
            }
        }
        Err(failure)
    }

    async fn topic_from(
        &self,
        broker: NodeId,
        name: &str,
        deadline: Duration,
    ) -> Result<TopicMetadata, BrokerLinkError> {
        let mut pages = Vec::new();
        let mut first = 0;
        loop {
            if self.0.config.clock.now() >= deadline {
                return Err(BrokerLinkError::Timeout);
            }
            let response = self
                .request_until(
                    broker,
                    driver::Body::Topic(directory::TopicRequest {
                        name: name.to_owned(),
                        first,
                        maximum: directory::Limits::default().partitions as u16,
                    }),
                    deadline,
                )
                .await;
            let message = match response {
                Ok(reply) => reply.message()?,
                Err(BrokerLinkError::Session) => {
                    // Fetch the catalog afresh after fencing without extending
                    // the deadline or combining pages from canceled requests.
                    pages.clear();
                    first = 0;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let packet =
                driver::packet(&message, broker, self.0.config.parameters.receive.envelope)?;
            let page = directory::decode_topic_page(
                packet,
                self.0.config.parameters.receive.envelope,
                directory::Limits::default(),
            )?;
            if page.name != name
                || page.first != first
                || page.total as usize > self.0.config.maximum_partitions
            {
                return Err(BrokerLinkError::Response);
            }
            first = first
                .checked_add(page.partitions.len() as u32)
                .ok_or(BrokerLinkError::Response)?;
            let total = page.total;
            pages.push(page);
            if first == total {
                break;
            }
        }
        Ok(TopicMetadata::from_pages(
            pages,
            self.0.config.maximum_partitions,
        )?)
    }
}

//! Explicit production connection metadata supplied by benchmark control.

pub(super) mod broker;
mod clients;
mod coordinator;
mod deployment;
mod results;
mod settings;
pub(super) use clients::Clients;
pub(super) use coordinator::coordinate;
pub(super) use deployment::Prepared;
pub(super) use results::topology;
pub(in crate::bench) use settings::Options;
pub(super) use settings::Settings;

use super::{Config, Result, error};
use ozzy_proto::{NodeId, handshake};
use ozzy_runtime::replicated::{
    AppendLinkLimits, BrokerAddress, BrokerLinks, BrokerLinksConfig, ReaderLinkLimits, SdkClock,
    SharedTopicWriterConfig, TopicRoutes, WriterRuntime,
};
use serde_json::Value;
use std::time::Duration;

const DIRECTORY_METADATA_BYTES: usize = 64 * 1024;

pub(super) fn writer_settings(config: &Config) -> SharedTopicWriterConfig {
    SharedTopicWriterConfig {
        limits: config.writer_limits(),
        compress_payloads: config.args.payload_compression
            == crate::bench::PayloadCompression::Adaptive,
        batch_target_bytes: super::config::sdk_batch_target(&config.args)
            .min(config.writer_limits().envelope.max_payload_bytes),
        max_producers: 1,
        inflight_appends: config.args.writer_inflight_appends,
    }
}

pub(super) struct Setup {
    pub(super) topic: String,
    pub(super) partitions: usize,
    brokers: Vec<BrokerAddress>,
}

impl Setup {
    pub(super) fn parse(config: &Config, value: &Value) -> Result<Self> {
        let brokers = value["brokers"]
            .as_array()
            .ok_or_else(|| error("missing native brokers"))?
            .iter()
            .map(|broker| {
                Ok(BrokerAddress {
                    node: NodeId::from_bytes(
                        *uuid::Uuid::parse_str(text(broker, "node")?)?.as_bytes(),
                    ),
                    endpoint: text(broker, "endpoint")?.parse()?,
                    data_endpoint: text(broker, "data_endpoint")?.parse()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if brokers.len() != config.args.system.brokers() {
            return Err(error(
                "native broker membership or confirmation policy mismatch",
            ));
        }
        Ok(Self {
            topic: text(value, "topic")?.into(),
            partitions: bound(value, "partitions")?,
            brokers,
        })
    }

    pub(super) async fn writer_links(
        &self,
        runtime: &WriterRuntime,
        config: &Config,
        value: &Value,
    ) -> Result<BrokerLinks> {
        let append = AppendLinkLimits {
            writers: bound(value, "writers")?,
            requests: bound(value, "requests")?,
            records: bound(value, "records")?,
            bytes: bound(value, "bytes")?,
        };
        let links = self.writer_config(config, append)?;
        Ok(BrokerLinks::connect(runtime, links).await?)
    }

    fn writer_config(
        &self,
        config: &Config,
        append: AppendLinkLimits,
    ) -> Result<BrokerLinksConfig> {
        let mut limits = config.writer_limits();
        // Topic directory metadata is independent of the configured APPEND bound.
        limits.envelope.max_metadata_bytes = limits
            .envelope
            .max_metadata_bytes
            .max(DIRECTORY_METADATA_BYTES);
        let mut parameters = handshake::Parameters::streaming(limits, handshake::PRODUCER)?;
        parameters.capabilities |= handshake::OWNER_ROUTING;
        let mut links = self.links_config(parameters)?;
        links.append = Some(append);
        links.validate()?;
        Ok(links)
    }

    pub(super) async fn reader_links(
        &self,
        runtime: &WriterRuntime,
        config: &Config,
        value: &Value,
    ) -> Result<BrokerLinks> {
        let reader = ReaderLinkLimits {
            subscriptions: bound(value, "subscriptions")?,
            bytes: bound(value, "bytes")?,
            queue_messages: bound(value, "queue_messages")?,
        };
        let links = self.reader_config(config, reader)?;
        Ok(BrokerLinks::connect(runtime, links).await?)
    }

    fn reader_config(
        &self,
        config: &Config,
        reader: ReaderLinkLimits,
    ) -> Result<BrokerLinksConfig> {
        let limits = Self::reader_limits(config);
        let mut parameters = handshake::Parameters::reader(limits, handshake::CONSUMER)?;
        parameters.capabilities |= handshake::OWNER_ROUTING;
        let mut links = self.links_config(parameters)?;
        links.reader = Some(reader);
        links.validate()?;
        Ok(links)
    }

    fn reader_limits(config: &Config) -> ozzy_proto::data::DataLimits {
        let mut limits = config.reader_limits();
        limits.max_record_bytes = config.args.record_bytes;
        limits.envelope.max_metadata_bytes = limits
            .envelope
            .max_metadata_bytes
            .max(DIRECTORY_METADATA_BYTES);
        limits
    }

    fn links_config(&self, parameters: handshake::Parameters) -> Result<BrokerLinksConfig> {
        let mut config = BrokerLinksConfig {
            local: NodeId::from_bytes(*uuid::Uuid::now_v7().as_bytes()),
            brokers: self.brokers.clone(),
            parameters,
            requests: 12,
            control_bytes: 0,
            routing_bytes: TopicRoutes::reservation_bytes(self.partitions)
                .ok_or_else(|| error("native SDK routing reservation overflow"))?,
            append: None,
            reader: None,
            maximum_partitions: self.partitions,
            request_timeout: Duration::from_secs(30),
            retry_interval: Duration::from_millis(10),
            clock: SdkClock::default(),
        };
        config.control_bytes = config
            .control_reservation_bytes()
            .ok_or_else(|| error("native SDK control reservation overflow"))?;
        Ok(config)
    }
}

fn text<'a>(value: &'a Value, name: &str) -> Result<&'a str> {
    value[name]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| error(format!("missing native {name}")))
}

fn bound(value: &Value, name: &str) -> Result<usize> {
    value[name]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|&value| value != 0)
        .ok_or_else(|| error(format!("missing or invalid native {name} bound")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench::Args;
    use clap::Parser;
    use serde_json::json;

    #[tokio::test(flavor = "current_thread")]
    async fn default_production_writer_profile_reserves_its_control_window() {
        let config = Config::production(Args::parse_from([
            "bench",
            "--processes",
            "--network-ingress",
            "--streaming",
            "--duration",
            "1",
        ]))
        .unwrap();
        let value = json!({"topic": "benchmark", "partitions": 16, "brokers":
            (0..3).map(|index| json!({
                "node": uuid::Uuid::from_bytes([index + 1; 16]).to_string(),
                "endpoint": format!("tcp://127.0.0.1:{}", 100 + u16::from(index)),
                "data_endpoint": format!("tcp://127.0.0.1:{}", 200 + u16::from(index)),
            })).collect::<Vec<_>>()
        });
        let setup = Setup::parse(&config, &value).unwrap();
        let runtime = WriterRuntime::new().unwrap();
        // Link construction is local and never waits for a remote HELLO.
        // The closed addresses keep this a configuration-only reproducer.
        let links = setup
            .writer_links(
                &runtime,
                &config,
                &json!({
                    "writers": 64, "requests": 12, "records": 12288, "bytes": 1024 * 1024 * 1024,
                }),
            )
            .await
            .unwrap();
        links.shutdown().await.unwrap();
    }
}

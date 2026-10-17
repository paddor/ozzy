//! Shared configuration for language adapters. Log state remains in `ozzy`.
#![forbid(unsafe_code)]
#![warn(missing_docs)]
use ozzy::{
    AppendLinkLimits, BrokerAddress, BrokerLinksConfig, DataLimits, EnvelopeLimits, NodeId,
    ReaderLinkLimits, SdkClock, SharedTopicWriterConfig, handshake,
};
use std::time::Duration;

/// Checked reservations shared by the Java and Python facades.
#[derive(Debug, Clone)]
pub struct Config {
    /// Shared session and transport reservations.
    pub links: BrokerLinksConfig,
    /// Per-producer batching and admission configuration.
    pub writer: SharedTopicWriterConfig,
}
impl Config {
    /// Validate finite bounds and reserve all configured client roles.
    ///
    /// `bounds`: packet bytes, record bytes, outbound records/parts, inbound
    /// records/parts. `capacity`: partitions, producers, consumers, reader queue.
    pub fn new(
        brokers: Vec<BrokerAddress>,
        bounds: [usize; 6],
        capacity: [usize; 4],
        timeout: Duration,
        compress: bool,
    ) -> Result<Self, String> {
        let [packet, record, records, parts, reader_records, reader_parts] = bounds;
        let limits = DataLimits {
            envelope: EnvelopeLimits {
                max_metadata_bytes: 65536,
                max_payload_bytes: packet,
            },
            max_record_bytes: record,
            max_records: records,
            max_parts: parts,
        };
        let mut writer = SharedTopicWriterConfig::new(limits);
        writer.compress_payloads = compress;
        let receive = DataLimits {
            max_records: reader_records,
            max_parts: reader_parts,
            ..limits
        };
        let [partitions, producers, consumers, reader_queue_messages] = capacity;
        let mut reader = ReaderLinkLimits {
            subscriptions: partitions.checked_mul(consumers).ok_or_else(invalid)?,
            bytes: 0,
            queue_messages: reader_queue_messages,
        };
        reader.bytes = reader
            .reservation_bytes(receive, brokers.len())
            .ok_or_else(invalid)?;
        let append = append_limits(&writer, partitions, producers)?;
        let mut parameters =
            handshake::Parameters::streaming(receive, handshake::PRODUCER | handshake::CONSUMER)
                .map_err(|e| e.to_string())?;
        parameters.capabilities |= handshake::OWNER_ROUTING | handshake::OWNER_READ;
        let mut links = BrokerLinksConfig {
            local: NodeId::new(),
            brokers,
            parameters,
            requests: 64,
            control_bytes: 0,
            routing_bytes: 4 * 1024 * 1024,
            append: Some(append),
            reader: Some(reader),
            maximum_partitions: partitions,
            request_timeout: timeout,
            retry_interval: timeout.min(Duration::from_millis(50)),
            clock: SdkClock::default(),
        };
        links.control_bytes = links.control_reservation_bytes().ok_or_else(invalid)?;
        links.validate().map_err(|e| e.to_string())?;
        Ok(Self { links, writer })
    }
}
fn invalid() -> String {
    "invalid or overflowing client limits".into()
}
fn append_limits(
    writer: &SharedTopicWriterConfig,
    partitions: usize,
    producers: usize,
) -> Result<AppendLinkLimits, String> {
    let reservation = writer.link_reservation(65536).ok_or_else(invalid)?;
    let writers = partitions.checked_mul(producers).ok_or_else(invalid)?;
    let requests = writers.checked_add(1).ok_or_else(invalid)?;
    Ok(AppendLinkLimits {
        writers,
        requests,
        records: requests
            .checked_mul(writer.limits.max_records)
            .ok_or_else(invalid)?,
        bytes: writers
            .checked_mul(reservation.idle_bytes)
            .and_then(|idle| {
                requests
                    .checked_mul(reservation.request_bytes)?
                    .checked_add(idle)
            })
            .ok_or_else(invalid)?,
    })
}

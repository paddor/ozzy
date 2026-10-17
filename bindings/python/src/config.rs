use crate::sdk_error;
use ozzy::{
    AppendLinkLimits, BrokerAddress, BrokerLinksConfig, DataLimits, EnvelopeLimits, NodeId,
    ReaderLinkLimits, SdkClock, SharedTopicWriterConfig, handshake,
};
use pyo3::{exceptions::PyValueError, prelude::*};
use std::time::Duration;

type Address = (Vec<u8>, String, String);

#[pyclass(module = "ozzy._native", frozen, skip_from_py_object)]
#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) links: BrokerLinksConfig,
    pub(crate) writer: SharedTopicWriterConfig,
}

#[pymethods]
impl Config {
    #[new]
    fn new(
        brokers: Vec<Address>,
        bounds: (usize, usize, usize, usize, usize, usize),
        capacity: (usize, usize, usize, usize),
        timeout: f64,
        compression: &str,
    ) -> PyResult<Self> {
        let brokers = brokers
            .into_iter()
            .map(address)
            .collect::<PyResult<Vec<_>>>()?;
        let (packet, record, records, parts, reader_records, reader_parts) = bounds;
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
        writer.compress_payloads = match compression {
            "raw" => false,
            "lz4" => true,
            _ => return Err(PyValueError::new_err("compression must be 'raw' or 'lz4'")),
        };
        let receive = DataLimits {
            max_records: reader_records,
            max_parts: reader_parts,
            ..limits
        };
        let (partitions, producers, consumers, reader_queue_messages) = capacity;
        let mut reader = ReaderLinkLimits {
            subscriptions: partitions.checked_mul(consumers).ok_or_else(invalid)?,
            bytes: 0,
            queue_messages: reader_queue_messages,
        };
        reader.bytes = reader
            .reservation_bytes(receive, brokers.len())
            .ok_or_else(invalid)?;
        let append = append_limits(&writer, partitions, producers)?;
        let timeout = Duration::try_from_secs_f64(timeout).map_err(|_| invalid())?;
        let mut parameters =
            handshake::Parameters::streaming(receive, handshake::PRODUCER | handshake::CONSUMER)
                .map_err(sdk_error)?;
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
        links
            .validate()
            .map_err(|error| PyValueError::new_err(error.to_string()))?;
        Ok(Self { links, writer })
    }
}

fn invalid() -> PyErr {
    PyValueError::new_err("invalid or overflowing client limits")
}

fn append_limits(
    writer: &SharedTopicWriterConfig,
    partitions: usize,
    producers: usize,
) -> PyResult<AppendLinkLimits> {
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

fn address((id, peer, data): Address) -> PyResult<BrokerAddress> {
    let id: [u8; 16] = id
        .try_into()
        .map_err(|_| PyValueError::new_err("broker ID must be 16 bytes"))?;
    Ok(BrokerAddress {
        node: NodeId::from_bytes(id),
        endpoint: peer.parse().map_err(sdk_error)?,
        data_endpoint: data.parse().map_err(sdk_error)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_selects_rust_payload_packing() {
        for (codec, expected, queue) in [
            ("raw", false, 1),
            ("lz4", true, 8),
            ("lz4", true, 32),
            ("raw", false, 64),
        ] {
            let config = Config::new(
                vec![(vec![1; 16], "inproc://peer".into(), "inproc://data".into())],
                (32768, 4096, 8, 32, 2048, 2048),
                (4, 1, 1, queue),
                5.0,
                codec,
            );
            assert!(config.is_ok());
            if let Ok(config) = config {
                assert_eq!(config.writer.compress_payloads, expected);
                assert_eq!(config.writer.limits.max_records, 8);
                assert_eq!(config.writer.limits.max_parts, 32);
                assert_eq!(config.links.parameters.receive.max_records, 2048);
                assert_eq!(config.links.parameters.receive.max_parts, 2048);
                assert_eq!(
                    config.links.reader.map(|limits| limits.queue_messages),
                    Some(queue)
                );
            }
        }
    }
}

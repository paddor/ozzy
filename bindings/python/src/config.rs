use crate::sdk_error;
use ozzy::{BrokerAddress, NodeId};
use pyo3::{exceptions::PyValueError, prelude::*};
use std::{ops::Deref, time::Duration};

type Address = (Vec<u8>, String, String);

#[pyclass(module = "ozzy._native", frozen, skip_from_py_object)]
#[derive(Debug, Clone)]
pub(crate) struct Config(ozzy_bindings::Config);

impl Deref for Config {
    type Target = ozzy_bindings::Config;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
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
        let timeout = Duration::try_from_secs_f64(timeout)
            .map_err(|_| PyValueError::new_err("invalid request timeout"))?;
        let compress = match compression {
            "raw" => false,
            "lz4" => true,
            _ => return Err(PyValueError::new_err("compression must be 'raw' or 'lz4'")),
        };
        ozzy_bindings::Config::new(brokers, bounds.into(), capacity.into(), timeout, compress)
            .map(Self)
            .map_err(PyValueError::new_err)
    }
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

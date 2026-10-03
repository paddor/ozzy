//! Worker-owned storage and endpoint reservations before broker formatting.

use super::{BrokerWorker, DeploymentArtifact, check_storage_parent};
use crate::{BenchResult, bench_error};
use ozzy_config::Endpoints;
use std::{net::IpAddr, net::TcpListener, path::Path, path::PathBuf};

/// Run resources allocated on the broker host. Keep reservations through
/// initialization; release them immediately before serving fixed addresses.
#[derive(Debug)]
pub struct PreparedBroker {
    directory: tempfile::TempDir,
    broker: String,
    endpoints: Endpoints,
    reservations: Vec<TcpListener>,
}

impl PreparedBroker {
    pub fn new(parent: &Path, index: usize, bind: IpAddr, brokers: usize) -> BenchResult<Self> {
        if ![1, 3].contains(&brokers)
            || index >= brokers
            || bind.is_unspecified()
            || bind.is_multicast()
        {
            return Err(bench_error("invalid production broker preparation"));
        }
        std::fs::create_dir_all(parent)?;
        check_storage_parent(parent)?;
        let directory = tempfile::Builder::new()
            .prefix(&format!("native-broker-{index}-"))
            .tempdir_in(std::fs::canonicalize(parent)?)?;
        let mut reservations = Vec::new();
        let mut endpoint = || -> BenchResult<String> {
            let listener = TcpListener::bind(std::net::SocketAddr::new(bind, 0))?;
            let endpoint = format!("tcp://{}", listener.local_addr()?);
            reservations.push(listener);
            Ok(endpoint)
        };
        let endpoints = Endpoints {
            peer: endpoint()?,
            reader_pub: endpoint()?,
            follower_pub: (brokers == 3).then(&mut endpoint).transpose()?,
        };
        Ok(Self {
            directory,
            broker: format!("broker-{index}"),
            endpoints,
            reservations,
        })
    }

    pub fn directory(&self) -> &Path {
        self.directory.path()
    }

    pub fn root(&self) -> PathBuf {
        self.directory().join(&self.broker)
    }

    pub fn endpoints(&self) -> &Endpoints {
        &self.endpoints
    }

    /// Refuse a deployment that does not use these exact worker-owned resources.
    /// Installing shared documents does not format storage or generate identity.
    pub fn install(&self, source: &str, identity: &str) -> BenchResult<BrokerWorker> {
        let deployment = ozzy_config::Deployment::parse(source)?.validate()?;
        let broker = deployment
            .deployment()
            .brokers
            .get(&self.broker)
            .ok_or_else(|| bench_error("prepared broker absent from deployment"))?;
        if broker.endpoints != self.endpoints
            || broker.devices.len() != 1
            || broker
                .devices
                .values()
                .any(|device| device.root != self.root())
        {
            return Err(bench_error(
                "deployment differs from prepared broker resources",
            ));
        }
        deployment.broker_plan(&self.broker, &ozzy_broker::host_resources()?)?;
        Ok(BrokerWorker {
            deployment: DeploymentArtifact::install(self.directory(), source, identity)?,
            broker: self.broker.clone(),
            identity: self.directory().join(format!("{}.identity", self.broker)),
        })
    }

    pub fn release_endpoints(&mut self) {
        self.reservations.clear();
    }
}

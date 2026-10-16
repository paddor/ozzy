//! Worker-owned storage and endpoint reservations before broker formatting.

use super::{BrokerWorker, DeploymentArtifact, check_storage_parent};
use crate::{BenchResult, bench_error};
use ozzy_config::Endpoints;
use ozzy_proto::directory::EndpointScheme;
use std::{net::IpAddr, net::TcpListener, path::Path, path::PathBuf};

/// Broker endpoint transport between processes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum Transport {
    /// Plain `tcp://`.
    #[default]
    Tcp,
    /// `lz4+tcp://`: OMQ compresses each connection on its I/O thread.
    Lz4,
    /// `zstd+tcp://`: as `lz4`, with zstd.
    Zstd,
}

impl Transport {
    /// The endpoint scheme this transport binds.
    #[must_use]
    pub const fn scheme(self) -> EndpointScheme {
        match self {
            Self::Tcp => EndpointScheme::Tcp,
            Self::Lz4 => EndpointScheme::Lz4Tcp,
            Self::Zstd => EndpointScheme::ZstdTcp,
        }
    }

    /// The `--transport` value selecting this transport.
    #[must_use]
    pub const fn flag(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Lz4 => "lz4",
            Self::Zstd => "zstd",
        }
    }
}

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
    /// Reserve disk-backed storage and fixed TCP endpoints for one of one or three brokers.
    pub fn new(parent: &Path, index: usize, bind: IpAddr, brokers: usize) -> BenchResult<Self> {
        Self::with_transport(parent, index, bind, brokers, Transport::Tcp)
    }

    /// As `new`, naming the reserved endpoints with `transport`'s scheme.
    pub fn with_transport(
        parent: &Path,
        index: usize,
        bind: IpAddr,
        brokers: usize,
        transport: Transport,
    ) -> BenchResult<Self> {
        if ![1, 3].contains(&brokers)
            || index >= brokers
            || bind.is_unspecified()
            || bind.is_multicast()
        {
            return Err(bench_error("invalid production broker preparation"));
        }
        let directory = Self::directory_in(parent, index)?;
        let mut reservations = Vec::new();
        let mut endpoint = || -> BenchResult<String> {
            let listener = TcpListener::bind(std::net::SocketAddr::new(bind, 0))?;
            let endpoint = format!(
                "{}://{}",
                transport.scheme().as_str(),
                listener.local_addr()?
            );
            reservations.push(listener);
            Ok(endpoint)
        };
        let endpoints = Endpoints {
            peer: endpoint()?,
            data_peer: endpoint()?,
            reader_pub: endpoint()?,
            follower_pub: (brokers == 3).then(&mut endpoint).transpose()?,
            admin: None,
        };
        Ok(Self {
            directory,
            broker: format!("broker-{index}"),
            endpoints,
            reservations,
        })
    }

    /// Memory-backed broker for one process: inproc endpoints and no TCP
    /// reservations. The directory holds deployment files; the device root
    /// names the broker's memory image, not files on disk.
    pub fn inproc(parent: &Path, index: usize, brokers: usize) -> BenchResult<Self> {
        if ![1, 3].contains(&brokers) || index >= brokers {
            return Err(bench_error("invalid inproc broker preparation"));
        }
        let directory = Self::directory_in(parent, index)?;
        let namespace = uuid::Uuid::now_v7();
        let endpoint = |plane: &str| format!("inproc://ozzy-bench-{namespace}-{plane}");
        let endpoints = Endpoints {
            peer: endpoint("peer"),
            data_peer: endpoint("data"),
            reader_pub: endpoint("readers"),
            follower_pub: (brokers == 3).then(|| endpoint("followers")),
            admin: None,
        };
        Ok(Self {
            directory,
            broker: format!("broker-{index}"),
            endpoints,
            reservations: Vec::new(),
        })
    }

    fn directory_in(parent: &Path, index: usize) -> BenchResult<tempfile::TempDir> {
        std::fs::create_dir_all(parent)?;
        check_storage_parent(parent)?;
        Ok(tempfile::Builder::new()
            .prefix(&format!("native-broker-{index}-"))
            .tempdir_in(std::fs::canonicalize(parent)?)?)
    }

    /// Worker-owned temporary directory retained through the benchmark deployment.
    pub fn directory(&self) -> &Path {
        self.directory.path()
    }

    /// Device storage root selected for this named broker.
    pub fn root(&self) -> PathBuf {
        self.directory().join(&self.broker)
    }

    /// Fixed endpoint reservations to install into the shared deployment document.
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

    /// Release bound TCP reservations immediately before production sockets bind.
    pub fn release_endpoints(&mut self) {
        self.reservations.clear();
    }
}

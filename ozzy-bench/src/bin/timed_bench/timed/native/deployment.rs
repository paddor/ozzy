//! Assemble one deployment from resources prepared on each broker host.

use super::{Config, Result, error, settings};
use crate::bench::timed::{launch::Process, receive_all, send_all};
use ozzy_bench::native::DeploymentArtifact;
use ozzy_config::DeploymentIdentity;
use serde_json::{Value, json};
use std::path::Path;

pub(in crate::bench::timed) struct Prepared {
    pub(super) artifact: DeploymentArtifact,
    pub(super) configuration: Value,
    pub(super) brokers: Vec<settings::Broker>,
    pub(super) identity: DeploymentIdentity,
    partitions: usize,
}

impl Prepared {
    pub(in crate::bench::timed) async fn new(
        config: &Config,
        workers: &mut [Process],
        directory: &Path,
    ) -> Result<Self> {
        if workers.len() != config.args.system.brokers() {
            return Err(error("missing production broker placements"));
        }
        send_all(workers, &json!({"command": "prepare"}))?;
        let resources = receive_all(workers, "prepared").await?;
        let brokers = specifications(config, workers, &resources)?;
        Self::from_brokers(config, brokers, directory)
    }

    fn from_brokers(
        config: &Config,
        brokers: Vec<settings::Broker>,
        directory: &Path,
    ) -> Result<Self> {
        let settings = config
            .native
            .as_ref()
            .ok_or_else(|| error("missing production deployment settings"))?;
        let source = settings.document(&brokers)?;
        let configuration = serde_json::to_value(source.parse::<toml::Value>()?)?;
        let artifact = DeploymentArtifact::initialize(directory, &source)?;
        let identity = DeploymentIdentity::decode(&std::fs::read_to_string(&artifact.identity)?)?;
        Ok(Self {
            artifact,
            configuration,
            brokers,
            identity,
            partitions: settings.partitions,
        })
    }

    pub(super) fn initialize(&self) -> Result<Value> {
        Ok(json!({"command": "initialize",
            "configuration": std::fs::read_to_string(&self.artifact.configuration)?,
            "identity": std::fs::read_to_string(&self.artifact.identity)?,
        }))
    }

    pub(in crate::bench::timed) fn connect(&self, profile: &Value) -> Result<Value> {
        let mut native = profile.clone();
        if !native.is_object() {
            return Err(error("invalid production SDK reservation profile"));
        }
        native["topic"] = json!("benchmark");
        native["partitions"] = json!(self.partitions);
        native["brokers"] = json!(
            self.brokers
                .iter()
                .map(|broker| json!({"node": self.identity.brokers[&broker.name],
                "endpoint": broker.endpoints.peer}))
                .collect::<Vec<_>>()
        );
        Ok(json!({"command": "connect", "native": native}))
    }
}

fn specifications(
    config: &Config,
    workers: &[Process],
    resources: &[Value],
) -> Result<Vec<settings::Broker>> {
    let digest = ozzy_bench::provenance::digest(&std::env::current_exe()?)?;
    workers
        .iter()
        .zip(resources)
        .enumerate()
        .map(|(index, (worker, row))| {
            let placement = worker
                .placement
                .as_ref()
                .ok_or_else(|| error("missing broker placement"))?;
            let name = format!("broker-{index}");
            let parent = placement
                .storage_dir
                .as_deref()
                .unwrap_or(&config.args.storage_dir);
            if row["index"] != index
                || row["broker"] != name
                || (!worker.remote && row["pid"] != worker.id())
                || row["pid"].as_u64().is_none_or(|pid| pid == 0)
                || row["executable_xxh3_128"] != digest
                || row["storage"]["requested_parent"] != json!(parent)
            {
                return Err(error(
                    "prepared broker identity, build, or storage mismatch",
                ));
            }
            placement.verify_execution(&row["execution"])?;
            let directory = Path::new(super::text(row, "directory")?);
            let root = Path::new(super::text(row, "root")?);
            let endpoints: ozzy_config::Endpoints =
                serde_json::from_value(row["endpoints"].clone())?;
            if !directory.is_absolute()
                || root != directory.join(&name)
                || endpoints.follower_pub.is_some() != (workers.len() == 3)
            {
                return Err(error("invalid prepared broker resources"));
            }
            for endpoint in [&endpoints.peer, &endpoints.reader_pub]
                .into_iter()
                .chain(endpoints.follower_pub.iter())
            {
                let address = endpoint
                    .strip_prefix("tcp://")
                    .ok_or_else(|| error("prepared endpoint must use TCP"))?
                    .parse::<std::net::SocketAddr>()?;
                if address.ip() != placement.bind || address.port() == 0 {
                    return Err(error("prepared endpoint differs from broker placement"));
                }
            }
            Ok(settings::Broker {
                name,
                root: root.into(),
                endpoints,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;

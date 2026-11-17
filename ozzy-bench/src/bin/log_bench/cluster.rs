//! One or three brokers in this process on real files, provisioned from
//! code, and the log they serve.

use super::{Kind, Mode, Options, Result, Transport};
use ozzy_broker::log::{Log, LogRole};
use ozzy_broker::{Broker, LogSpec, host_resources, provision};
use ozzy_config::{
    Broker as BrokerConfig, Cluster as ClusterConfig, Deployment, DeploymentMode, Device,
    Endpoints, LogClass, ResourceLimits, Topic, Topology, ValidatedDeployment,
};
use ozzy_proto::{GroupId, NodeId, PartitionIncarnation};
use std::collections::BTreeMap;
use std::time::Duration;

const TOPIC: &str = "bench";
const LOG: GroupId = GroupId::from_bytes([0xB7; 16]);

fn endpoints(options: &Options, index: usize, replicated: bool) -> Endpoints {
    let name = |slot: u16, kind: &str| match options.transport {
        Transport::Inproc => format!("inproc://log-bench-{index}-{kind}"),
        Transport::Tcp => {
            let port = options.port + index as u16 * 4 + slot;
            format!("tcp://127.0.0.1:{port}")
        }
    };
    Endpoints {
        peer: name(0, "peer"),
        data_peer: name(1, "data"),
        reader_pub: name(2, "readers"),
        follower_pub: replicated.then(|| name(3, "followers")),
        admin: None,
    }
}

fn deployment(options: &Options) -> Result<ValidatedDeployment> {
    let (mode, count) = match options.mode {
        Mode::Single => (DeploymentMode::Single, 1),
        Mode::Three => (DeploymentMode::Three, 3),
    };
    let brokers = (0..count)
        .map(|index| {
            let root = options.root.join(format!("broker-{index}/data"));
            let mut device = Device::new(root, "local");
            device.workers.aio_depth = options.aio_depth;
            let broker = BrokerConfig {
                id: None,
                endpoints: endpoints(options, index, count == 3),
                devices: BTreeMap::from([("data".to_owned(), device)]),
                topology: Topology::default(),
            };
            (format!("broker-{index}"), broker)
        })
        .collect();
    let runtime = options.kind == Kind::Runtime;
    let topic = Topic {
        partitions: 1,
        segment_bytes: options.segment_bytes,
        max_append_bytes: options.append_bytes(),
        ..Topic::default()
    };
    let config = Deployment {
        cluster: ClusterConfig {
            id: Some(uuid::Uuid::from_u128(0x10C_BE4C)),
            mode,
            max_clock_offset_millis: 1000,
        },
        limits: ResourceLimits::default(),
        brokers,
        topics: BTreeMap::from([(TOPIC.to_owned(), topic)]),
        logs: runtime.then(|| LogClass {
            segment_bytes: options.segment_bytes,
            max_append_bytes: options.append_bytes(),
            max_logs: 1,
        }),
    };
    Ok(config.validate()?)
}

/// The running brokers.
#[derive(Debug)]
pub(crate) struct Cluster {
    brokers: Vec<Broker>,
    kind: Kind,
}

pub(crate) async fn start(options: &Options) -> Result<Cluster> {
    if options.root.exists() {
        std::fs::remove_dir_all(&options.root)?;
    }
    let deployment = deployment(options)?;
    let host = host_resources()?;
    let shared = (options.transport == Transport::Inproc).then(omq_tokio::Context::new);
    let mut brokers = Vec::new();
    let mut members = Vec::new();
    for index in 0..deployment.deployment().brokers.len() {
        let name = format!("broker-{index}");
        let identity = options.root.join(format!("{name}/local.identity"));
        let (checked, local) = provision(deployment.clone(), &name, &identity, &host).await?;
        members.push(NodeId::from_bytes(
            *checked.identity.brokers[&name].as_bytes(),
        ));
        brokers.push(match shared.clone() {
            Some(context) => Broker::start_trusted_with_context(checked, local, context).await?,
            None => Broker::start_trusted(checked, local).await?,
        });
    }
    if options.kind == Kind::Runtime {
        let spec = LogSpec {
            id: LOG,
            incarnation: PartitionIncarnation::from_bytes([0xB8; 16]),
            members,
        };
        for broker in &brokers {
            broker.create_log(spec.clone()).await?;
        }
    }
    Ok(Cluster {
        brokers,
        kind: options.kind,
    })
}

impl Cluster {
    fn log(&self, broker: &Broker) -> Option<Log> {
        match self.kind {
            Kind::Topic => broker.log(TOPIC, 0),
            Kind::Runtime => broker.runtime_log(LOG),
        }
    }

    /// The log's handle on the broker that leads it, once every copy has a
    /// role in the view.
    pub(crate) async fn leader(&self, limit: Duration) -> Result<Log> {
        let settled = async {
            loop {
                let mut logs: Vec<Log> = self.brokers.iter().filter_map(|b| self.log(b)).collect();
                let role = |log: &Log| log.status().borrow().role;
                let joined = logs
                    .iter()
                    .all(|log| matches!(role(log), LogRole::Leader | LogRole::Follower));
                let leader = logs.iter().position(|log| role(log) == LogRole::Leader);
                if let (true, Some(leader)) = (joined && logs.len() == self.brokers.len(), leader) {
                    return logs.swap_remove(leader);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        Ok(tokio::time::timeout(limit, settled).await?)
    }

    pub(crate) async fn stop(self) -> Result<()> {
        for broker in self.brokers {
            broker.shutdown().await?;
        }
        Ok(())
    }
}

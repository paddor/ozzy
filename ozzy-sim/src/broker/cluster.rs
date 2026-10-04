//! Production cluster lifecycle and storage faults shared with the simulator.
use super::{Control, deployment_with_resources, provision, start_image_selected, start_images};
use crate::client::Client;
use ozzy_broker::{Broker, CheckedConfig, RecoveryIntent, RecoverySelection};
use ozzy_config::{BrokerIdentity, Confirmation, DeploymentMode};
use ozzy_io::simulation::Image;
use ozzy_runtime::replicated::{TopicReader, WriterRuntime};
use std::{path::PathBuf, sync::Arc, time::Duration};
use uuid::Uuid;

/// Sustained real-broker cluster shared by integration tests and overnight runs.
#[derive(Debug)]
pub struct Cluster {
    /// OMQ context used by brokers and SDKs.
    pub runtime: WriterRuntime,
    /// Validated placement and persistent broker identities.
    pub configs: Vec<(CheckedConfig, BrokerIdentity)>,
    /// Live production broker instances.
    pub brokers: Vec<Broker>,
    /// Bounded memory-storage fault controls.
    pub controls: Vec<Arc<Control>>,
    /// Tasks yielding memory images after physical drain.
    pub images: Vec<tokio::task::JoinHandle<Image>>,
    policy: Confirmation,
    /// Number of injected torn-write faults.
    pub faults: usize,
}

impl Cluster {
    /// Start a bounded four-partition cluster with rolling retention.
    pub async fn new(policy: Confirmation) -> Self {
        let runtime = WriterRuntime::new().unwrap();
        let root = PathBuf::from(format!("/ozzy-churn-{}", Uuid::now_v7()));
        let resources = ozzy_config::HostResources {
            cpus: std::collections::BTreeMap::from([(0, Some(0))]),
            memory_nodes: [0].into(),
            linux_aio: true,
        };
        let deployment = deployment_with_resources(
            &root,
            if policy == Confirmation::LocalDurable {
                DeploymentMode::Single
            } else {
                DeploymentMode::Three
            },
            policy,
            4,
            &resources,
            |config| {
                config.topics.get_mut("orders").unwrap().retention = ozzy_config::TopicRetention {
                    max_age_secs: Some(120),
                    max_bytes: Some(2 * 1024 * 1024),
                };
            },
        );
        let configs = deployment.clone();
        let (brokers, controls, images) = start_images(&runtime, deployment, true).await;
        Self {
            runtime,
            configs,
            brokers,
            controls,
            images,
            policy,
            faults: 0,
        }
    }

    /// Restore one stopped broker from its explicitly supplied memory image.
    pub async fn restore(&mut self, index: usize, image: Image) {
        let (checked, local) = self.configs[index].clone();
        let selections: Vec<_> = if self.policy == Confirmation::LocalDurable {
            vec![]
        } else {
            checked
                .plan
                .partitions
                .iter()
                .map(|partition| RecoverySelection {
                    topic: partition.topic.clone(),
                    partition: partition.partition,
                    intent: if image
                        .bytes(&partition.directory.join("CONFIGURATION"), false)
                        .unwrap()
                        .starts_with(b"OZYRECOV")
                    {
                        RecoveryIntent::ResumeFull
                    } else {
                        RecoveryIntent::Quarantine
                    },
                })
                .collect()
        };
        let (broker, control, task) =
            start_image_selected(&self.runtime, checked, local, true, image, &selections).await;
        self.brokers.insert(index, broker);
        self.controls.insert(index, control);
        self.images.insert(index, task);
    }

    /// Stop and restore one broker without discarding its dirty image.
    pub async fn restart(&mut self, index: usize) {
        self.brokers.remove(index).shutdown().await.unwrap();
        self.controls.remove(index);
        let image = self.images.remove(index).await.unwrap();
        self.restore(index, image).await;
    }

    /// Replace one store with a newly formatted, nonvoting recovery target.
    pub async fn restart_fresh(&mut self, index: usize) {
        self.brokers.remove(index).shutdown().await.unwrap();
        self.controls.remove(index);
        self.images.remove(index).await.unwrap();
        let (checked, local) = &self.configs[index];
        let image = Box::pin(provision(checked, local, true)).await;
        self.restore(index, image).await;
    }

    /// Wait until every recovering partition has published its configuration.
    pub async fn wait_recovered(&self, index: usize) {
        if self.policy == Confirmation::LocalDurable {
            return;
        }
        live_many(
            &self.brokers,
            "publish recovered partition configurations",
            async {
                loop {
                    let image = self.controls[index].image().await;
                    if self.configs[index]
                        .0
                        .plan
                        .partitions
                        .iter()
                        .all(|partition| {
                            image
                                .bytes(&partition.directory.join("CONFIGURATION"), false)
                                .is_ok_and(|bytes| {
                                    ozzy_replication::ConfigurationRecord::decode(bytes).is_ok()
                                })
                        })
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            },
        )
        .await;
    }

    async fn torn_wave(&mut self, client: &mut Client, wave: usize) -> usize {
        let index = (wave / 6) % self.brokers.len();
        let failed = self.brokers.remove(index);
        self.controls[index].short_next_record_write(1 + wave % 127);
        let pending = live_many(
            &self.brokers,
            "admit during torn write",
            client.queue_varied(wave),
        )
        .await;
        let count = pending.len();
        assert!(
            live_many(&self.brokers, "observe torn-write fencing", failed.closed())
                .await
                .is_err()
        );
        assert!(failed.shutdown().await.is_err());
        self.controls.remove(index);
        let image = self.images.remove(index).await.unwrap();
        live_many(
            &self.brokers,
            "confirm with torn writer fenced",
            client.confirm(pending),
        )
        .await;
        self.restore(index, image).await;
        // Do not remove another copy while this store still needs both original
        // donors. The next fault's confirmation will require a recovered voter.
        self.wait_recovered(index).await;
        self.faults += 1;
        count
    }

    /// Run varied records, fault one copy, and verify every live consumer record.
    pub async fn verify_wave(
        &mut self,
        client: &mut Client,
        reader: &mut TopicReader,
        wave: usize,
    ) -> usize {
        let positions = client.positions();
        let count = if wave % 6 == 2 && self.policy != Confirmation::LocalDurable {
            self.torn_wave(client, wave).await
        } else {
            if wave % 6 == 2 {
                let control = self.controls[0].clone();
                control.hold_record_writes(true);
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    control.hold_record_writes(false);
                });
            }
            let pending = live_many(
                &self.brokers,
                "admit memory churn",
                client.queue_varied(wave),
            )
            .await;
            let count = pending.len();
            live_many(
                &self.brokers,
                "confirm memory churn",
                client.confirm(pending),
            )
            .await;
            count
        };
        if wave % 6 == 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        live_many(
            &self.brokers,
            "verify live memory churn",
            client.read(reader, positions),
        )
        .await;
        count
    }

    /// Join brokers and their bounded physical storage workers.
    pub async fn shutdown(self) {
        for broker in self.brokers {
            broker.shutdown().await.unwrap();
        }
        for image in self.images {
            image.await.unwrap();
        }
    }
}

async fn live_many<T>(
    brokers: &[Broker],
    stage: &str,
    operation: impl std::future::Future<Output = T>,
) -> T {
    tokio::select! {
        result = operation => result,
        (result, broker, _) = futures::future::select_all(brokers.iter().map(|broker| Box::pin(broker.closed()))) => {
            panic!("broker {broker} exited during {stage}: {result:?}")
        },
        () = tokio::time::sleep(Duration::from_secs(5)) => panic!("no progress during {stage}"),
    }
}

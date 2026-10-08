//! Production cluster lifecycle and storage faults shared with the simulator.
use super::{Control, deployment_with_resources, provision, start_image_selected, start_images};
use crate::client::Client;
use ozzy_broker::{Broker, CheckedConfig, RecoveryIntent, RecoverySelection, ServingContext};
use ozzy_config::{BrokerIdentity, Confirmation, DeploymentMode};
use ozzy_io::simulation::Image;
use ozzy_runtime::replicated::{SdkClock, TopicReader, WriterRuntime};
use std::{path::PathBuf, sync::Arc, time::Duration};
use uuid::Uuid;
mod quorum;

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
    context: ServingContext,
    retired_physical_events: u64,
    stopped: std::collections::BTreeMap<usize, Arc<Control>>,
    /// Number of injected torn-write faults.
    pub faults: usize,
}

impl Cluster {
    /// Start a bounded four-partition cluster with rolling retention.
    pub async fn new(policy: Confirmation) -> Self {
        Self::configured(policy, None, |_| {}).await
    }

    /// Configure the shared deployment and optionally step broker time manually.
    pub async fn configured(
        policy: Confirmation,
        clock: Option<SdkClock>,
        configure: impl FnOnce(&mut ozzy_config::Deployment),
    ) -> Self {
        let runtime = WriterRuntime::new().unwrap();
        let context = clock.map_or_else(
            || ServingContext::from(runtime.context().clone()),
            |clock| ServingContext::simulated(runtime.context().clone(), clock, 1_000_000),
        );
        let root = PathBuf::from(format!("/ozzy-churn-{}", Uuid::now_v7()));
        let resources = ozzy_config::HostResources {
            cpus: std::collections::BTreeMap::from([(0, Some(0))]),
            memory_nodes: [0].into(),
            linux_aio: true,
        };
        let deployment = deployment_with_resources(
            &root,
            match policy {
                Confirmation::LocalDurable => DeploymentMode::Single,
                Confirmation::DiskQuorum | Confirmation::ReplicatedPersisting => {
                    DeploymentMode::Three
                }
            },
            policy,
            4,
            &resources,
            |config| {
                config.topics.get_mut("orders").unwrap().retention = ozzy_config::TopicRetention {
                    max_age_secs: Some(120),
                    max_bytes: Some(2 * 1024 * 1024),
                };
                configure(config);
            },
        );
        let configs = deployment.clone();
        let (brokers, controls, images) = start_images(context.clone(), deployment, true).await;
        Self {
            runtime,
            configs,
            brokers,
            controls,
            images,
            policy,
            context,
            retired_physical_events: 0,
            stopped: std::collections::BTreeMap::new(),
            faults: 0,
        }
    }

    /// Restore one stopped broker from its explicitly supplied memory image.
    pub async fn restore(&mut self, index: usize, image: Image) {
        let (checked, _) = &self.configs[index];
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
        self.install(index, image, &selections).await;
    }

    /// Restore an orderly stopped image through ordinary production startup.
    /// Exact journal/configuration eligibility checks still apply.
    pub async fn restore_clean(&mut self, index: usize, image: Image) {
        self.install(index, image, &[]).await;
    }

    async fn install(&mut self, index: usize, image: Image, selections: &[RecoverySelection]) {
        let (checked, local) = self.configs[index].clone();
        let (broker, control, task) = start_image_selected(
            self.context.clone(),
            checked,
            local,
            true,
            image,
            selections,
        )
        .await;
        self.brokers.insert(index, broker);
        self.controls.insert(index, control);
        self.images.insert(index, task);
        self.stopped.remove(&index);
    }

    /// Stop and restore one broker without discarding its dirty image.
    pub async fn restart(&mut self, index: usize) {
        let image = self.stop(index).await;
        self.restore(index, image).await;
    }

    /// Cut one live memory device, discard unobserved physical work, and restore
    /// its explicit crash image through the production recovery selection.
    pub async fn crash(&mut self, index: usize, power_loss: bool) {
        self.controls[index].crash(power_loss);
        // Device loss can first be observed during journal shutdown. It must
        // not prevent socket and worker cleanup or reactivate an old source.
        let _ = self.brokers.remove(index).shutdown().await;
        let control = self.controls.remove(index);
        let image = self.images.remove(index).await.unwrap();
        self.retired_physical_events += control.physical_events();
        self.stopped.insert(index, control);
        self.restore(index, image).await;
    }

    /// Stop one owner and retain its image. Remove higher indexes first when
    /// stopping several brokers, and restore them in increasing index order.
    pub async fn stop(&mut self, index: usize) -> Image {
        self.brokers.remove(index).shutdown().await.unwrap();
        let control = self.controls.remove(index);
        let image = self.images.remove(index).await.unwrap();
        self.retired_physical_events += control.physical_events();
        self.stopped.insert(index, control);
        image
    }

    /// Replace one store with a newly formatted, nonvoting recovery target.
    pub async fn restart_fresh(&mut self, index: usize) {
        self.stop(index).await;
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
        let control = self.controls.remove(index);
        let image = self.images.remove(index).await.unwrap();
        self.retired_physical_events += control.physical_events();
        self.stopped.insert(index, control);
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

    /// Physical events across current and previously stopped device owners.
    pub fn physical_events(&self) -> u64 {
        self.retired_physical_events
            + self
                .controls
                .iter()
                .map(|control| control.physical_events())
                .sum::<u64>()
    }

    /// Current and stopped physical owners paired with their stable config index.
    /// Stopped owners retain their final image if an interrupted fault loses progress.
    pub fn storage_controls(&self) -> Vec<(usize, &Arc<Control>)> {
        let mut live = self.controls.iter();
        (0..self.configs.len())
            .filter_map(|index| {
                self.stopped
                    .get(&index)
                    .or_else(|| live.next())
                    .map(|control| (index, control))
            })
            .collect()
    }
}

async fn live_many<T>(
    brokers: &[Broker],
    stage: &str,
    operation: impl std::future::Future<Output = T>,
) -> T {
    // The enclosing verified-progress watchdog bounds the complete boundary.
    // A separate stage deadline would reject bounded storage pauses and normal
    // election backoff before that watchdog expires.
    tokio::select! {
        result = operation => result,
        (result, broker, _) = futures::future::select_all(brokers.iter().map(|broker| Box::pin(broker.closed()))) => {
            panic!("broker {broker} exited during {stage}: {result:?}")
        },
    }
}

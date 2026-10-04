use super::*;

mod fresh;

pub(super) struct MemoryCluster {
    pub(super) runtime: WriterRuntime,
    pub(super) configs: Vec<(CheckedConfig, BrokerIdentity)>,
    pub(super) brokers: Vec<Broker>,
    controls: Vec<Arc<Control>>,
    images: Vec<tokio::task::JoinHandle<Image>>,
    policy: Confirmation,
    pub(super) faults: usize,
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_recovery_reclaims_old_segments_and_preserves_selected_payloads() {
    for policy in [Confirmation::DiskQuorum, Confirmation::ReplicatedPersisting] {
        let mut cluster = MemoryCluster::new(policy).await;
        let mut client =
            SdkClient::open_with_runtime(&cluster.configs[0].0, &cluster.runtime).await;
        for wave in 0..4 {
            let pending = live_many(
                &cluster.brokers,
                "seed repeated recovery",
                client.queue_varied(wave),
            )
            .await;
            live_many(
                &cluster.brokers,
                "confirm repeated recovery",
                client.confirm(pending),
            )
            .await;
            let image = cluster.controls[2].image().await;
            let old = cluster.configs[2]
                .0
                .plan
                .partitions
                .iter()
                .flat_map(|partition| {
                    super::super::retention::selected(&image, &partition.directory)
                        .segments
                        .into_iter()
                        .map(move |segment| {
                            let name = if segment.file_generation == 0 {
                                format!("{}.log", segment.segment_id)
                            } else {
                                format!("{}.{}.log", segment.segment_id, segment.file_generation)
                            };
                            partition.directory.join("segments").join(name)
                        })
                })
                .collect::<Vec<_>>();
            assert!(!old.is_empty());
            drop(image);
            cluster.restart(2).await;
            cluster.wait_recovered(2).await;
            live_many(
                &cluster.brokers,
                "reclaim unselected recovery segments",
                async {
                    loop {
                        let image = cluster.controls[2].image().await;
                        if old.iter().all(|path| !image.exists(path, false)) {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                },
            )
            .await;
            live_many(
                &cluster.brokers,
                "verify selected recovery payloads",
                client.replay(),
            )
            .await;
        }
        live_many(
            &cluster.brokers,
            "close repeated recovery SDK",
            client.close(),
        )
        .await;
        cluster.shutdown().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn repeated_multipartition_attachments_reuse_sdk_capacity_with_a_live_reader() {
    let cluster = MemoryCluster::new(Confirmation::ReplicatedPersisting).await;
    let mut client = SdkClient::open_with_runtime(&cluster.configs[0].0, &cluster.runtime).await;
    let mut reader = live_many(
        &cluster.brokers,
        "open attachment reader",
        client.reader(false),
    )
    .await;
    for wave in 0..40 {
        client = live_many(
            &cluster.brokers,
            "repeat multipartition attachment",
            client.reopen_producer(wave % 2 == 0),
        )
        .await;
        let positions = client.positions();
        let pending = live_many(
            &cluster.brokers,
            "admit attachment records",
            client.queue_varied(wave),
        )
        .await;
        live_many(
            &cluster.brokers,
            "confirm attachment records",
            client.confirm(pending),
        )
        .await;
        live_many(
            &cluster.brokers,
            "verify attachment reader",
            client.read(&mut reader, positions),
        )
        .await;
    }
    live_many(
        &cluster.brokers,
        "replay attachment records",
        client.replay(),
    )
    .await;
    live_many(&cluster.brokers, "close attachment reader", reader.close())
        .await
        .unwrap();
    live_many(&cluster.brokers, "close attachment SDK", client.close()).await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "current_thread")]
async fn restart_during_unfinished_recovery_resumes_the_nonvoting_store() {
    let mut cluster = MemoryCluster::new(Confirmation::ReplicatedPersisting).await;
    let mut client = SdkClient::open_with_runtime(&cluster.configs[0].0, &cluster.runtime).await;
    let pending = live_many(
        &cluster.brokers,
        "seed interrupted recovery",
        client.queue_varied(0),
    )
    .await;
    live_many(&cluster.brokers, "confirm seed", client.confirm(pending)).await;
    for control in &cluster.controls[..2] {
        control.hold_record_writes(true);
    }
    cluster.restart(2).await;
    let image = cluster.controls[2].image().await;
    assert!(
        cluster.configs[2]
            .0
            .plan
            .partitions
            .iter()
            .all(|partition| {
                image
                    .bytes(&partition.directory.join("CONFIGURATION"), false)
                    .unwrap()
                    .starts_with(b"OZYRECOV")
            }),
        "both donor barriers must keep all target partitions nonvoting"
    );
    cluster.restart(2).await;
    for control in &cluster.controls[..2] {
        control.hold_record_writes(false);
    }
    let pending = live_many(
        &cluster.brokers,
        "admit after interrupted recovery",
        client.queue_varied(1),
    )
    .await;
    live_many(
        &cluster.brokers,
        "confirm after interrupted recovery",
        client.confirm(pending),
    )
    .await;
    live_many(
        &cluster.brokers,
        "verify interrupted recovery payloads",
        client.replay(),
    )
    .await;
    live_many(
        &cluster.brokers,
        "close interrupted recovery SDK",
        client.close(),
    )
    .await;
    cluster.shutdown().await;
}

impl MemoryCluster {
    pub(super) async fn new(policy: Confirmation) -> Self {
        let runtime = WriterRuntime::new().unwrap();
        let root = PathBuf::from(format!("/ozzy-churn-{}", Uuid::now_v7()));
        let deployment = deployment_with(
            &root,
            if policy == Confirmation::LocalDurable {
                DeploymentMode::Single
            } else {
                DeploymentMode::Three
            },
            policy,
            4,
            |config| {
                config.topics.get_mut("orders").unwrap().retention = ozzy_config::TopicRetention {
                    max_age_secs: Some(120),
                    max_bytes: Some(2 * 1024 * 1024),
                };
            },
        );
        let configs = super::super::retention::copy_deployment(&deployment);
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

    async fn restore(&mut self, index: usize, image: Image) {
        let (checked, local) =
            super::super::retention::copy_deployment(&self.configs[index..=index]).remove(0);
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

    pub(super) async fn restart(&mut self, index: usize) {
        self.brokers.remove(index).shutdown().await.unwrap();
        self.controls.remove(index);
        let image = self.images.remove(index).await.unwrap();
        self.restore(index, image).await;
    }

    async fn restart_fresh(&mut self, index: usize) {
        self.brokers.remove(index).shutdown().await.unwrap();
        self.controls.remove(index);
        self.images.remove(index).await.unwrap();
        let (checked, local) = &self.configs[index];
        let image = Box::pin(provision(checked, local, true)).await;
        self.restore(index, image).await;
    }

    pub(super) async fn wait_recovered(&self, index: usize) {
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

    async fn torn_wave(&mut self, client: &mut SdkClient, wave: usize) -> usize {
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

    pub(super) async fn verify_wave(
        &mut self,
        client: &mut SdkClient,
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

    pub(super) async fn shutdown(self) {
        for broker in self.brokers {
            broker.shutdown().await.unwrap();
        }
        for image in self.images {
            image.await.unwrap();
        }
    }
}

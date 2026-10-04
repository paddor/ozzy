use super::*;
use ozzy_runtime::replicated::{ReaderStart, SdkClock, TopicReader, TopicReaderConfig};

#[tokio::test(flavor = "current_thread")]
async fn inproc_memory_manual_broker_time_drives_timestamp_seeks_and_age_retention() {
    tokio::time::timeout(Duration::from_secs(60), async {
        for policy in [
            Confirmation::LocalDurable,
            Confirmation::DiskQuorum,
            Confirmation::ReplicatedPersisting,
        ] {
            let runtime = WriterRuntime::new().unwrap();
            let clock = SdkClock::manual();
            let context = ozzy_broker::ServingContext::simulated(
                runtime.context().clone(),
                clock.clone(),
                1_000_000,
            );
            let root = PathBuf::from(format!("/ozzy-clock-{}", uuid::Uuid::now_v7()));
            let resources = HostResources {
                cpus: BTreeMap::from([(0, Some(0))]),
                memory_nodes: [0].into(),
                linux_aio: true,
            };
            let mode = match policy {
                Confirmation::LocalDurable => DeploymentMode::Single,
                Confirmation::DiskQuorum | Confirmation::ReplicatedPersisting => {
                    DeploymentMode::Three
                }
            };
            let configs =
                broker::deployment_with_resources(&root, mode, policy, 2, &resources, |config| {
                    config.topics.get_mut("orders").unwrap().retention =
                        ozzy_config::TopicRetention {
                            max_age_secs: Some(1),
                            max_bytes: None,
                        };
                });
            let checked = configs[0].0.clone();
            let (brokers, _, images) =
                progress(&clock, broker::start_images(context, configs, true)).await;
            let mut client = progress(&clock, Client::open_with_runtime(&checked, &runtime)).await;
            let pending = progress(&clock, client.queue(0)).await;
            progress(&clock, client.confirm(pending)).await;
            let mut reader = progress(&clock, client.reader(false)).await;
            progress(&clock, client.read(&mut reader, vec![0; 2])).await;
            reader.close().await.unwrap();
            let positions = client.positions();
            verify_future_seek(&clock, &client, &positions).await;
            client.discard_verified();
            let until = clock.now() + Duration::from_secs(5);
            progress(&clock, async {
                while clock.now() < until {
                    tokio::task::yield_now().await;
                }
            })
            .await;
            let pending = progress(&clock, client.queue(1)).await;
            progress(&clock, client.confirm(pending)).await;
            let floor = progress(&clock, client.retained_floor()).await;
            assert_eq!(
                floor.get(),
                positions[0] as u64,
                "age retirement ignored broker time"
            );
            progress(&clock, client.replay()).await;
            client.close().await;
            for broker in brokers {
                broker.shutdown().await.unwrap();
            }
            for image in images {
                image.await.unwrap();
            }
        }
    })
    .await
    .expect("manual broker time did not preserve bounded progress");
}

pub(super) async fn progress<T>(
    clock: &SdkClock,
    operation: impl std::future::Future<Output = T>,
) -> T {
    let mut operation = std::pin::pin!(operation);
    let mut tick = tokio::time::interval(Duration::from_millis(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            result = &mut operation => return result,
            _ = tick.tick() => clock.advance(clock.now() + Duration::from_millis(10)).unwrap(),
        }
    }
}

async fn verify_future_seek(clock: &SdkClock, client: &Client, positions: &[usize]) {
    // Fixed simulated epoch: real Unix timestamps would incorrectly
    // match this future seek and deliver a record instead of parking.
    let mut future = progress(
        clock,
        TopicReader::open(
            client.links.clone(),
            "orders",
            TopicReaderConfig {
                start: ReaderStart::Timestamp(1_010_000),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    progress(clock, async {
        loop {
            {
                let mut next = std::pin::pin!(future.next());
                assert!(futures::poll!(next.as_mut()).is_pending());
            }
            let checkpoint = future.checkpoint();
            if checkpoint.positions.len() == positions.len()
                && checkpoint.positions.iter().all(|(partition, offset)| {
                    offset.get() == positions[*partition as usize] as u64
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    future.close().await.unwrap();
}

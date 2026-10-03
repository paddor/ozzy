use super::plan;
use crate::{ApplicationShards, ShardIo};
use ozzy_io::{
    Class, Limits, Operation, Quota,
    simulation::{Config, Controller, Effect, Image, ImageLimits},
};
use std::{
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[tokio::test]
async fn a_held_file_job_leaves_other_partitions_and_timer_observations_runnable() {
    let plan = plan(4);
    let (mut device, clients) = Controller::new(
        Config {
            limits: Limits {
                shards: 2,
                data: Quota {
                    operations: 8,
                    bytes: 65536,
                },
                progress: Quota {
                    operations: 4,
                    bytes: 65536,
                },
            },
            handles: 16,
            image: ImageLimits {
                nodes: 32,
                directory_entries: 64,
                file_bytes: 4096,
                total_bytes: 65536,
            },
            trace_events: 128,
        },
        Image::default(),
    )
    .unwrap();
    let lanes = plan
        .shards
        .iter()
        .zip(clients)
        .map(|(shard, client)| ShardIo {
            shard: shard.id,
            controller: plan.controllers[0].name.clone(),
            client,
        })
        .collect();
    let completed = Arc::new(AtomicUsize::new(0));
    let times = Arc::new(AtomicUsize::new(0));
    let ticks = Arc::new(tokio::sync::Semaphore::new(0));
    let shards = ApplicationShards::start(&plan, lanes, {
        let completed = completed.clone();
        let times = times.clone();
        let ticks = ticks.clone();
        move |mut context| {
            let completed = completed.clone();
            let times = times.clone();
            let ticks = ticks.clone();
            async move {
                let mut tasks = Vec::new();
                for partition in &context.partitions {
                    let path = format!("/partition-{}", partition.partition).into();
                    let io = context.io.clone();
                    let completed = completed.clone();
                    tasks.push(tokio::task::spawn_local(async move {
                        io.execute(Class::Data, Operation::CreateDirectory { path })
                            .await
                            .unwrap();
                        completed.fetch_add(1, Ordering::Release);
                    }));
                }
                // The test explicitly chooses when each shard observes time.
                tasks.push(tokio::task::spawn_local(async move {
                    ticks.acquire().await.unwrap().forget();
                    times.fetch_add(1, Ordering::Release);
                }));
                context.ready()?;
                context.shutdown.requested().await;
                for task in tasks {
                    task.await.unwrap();
                }
                Ok(())
            }
        }
    })
    .await
    .unwrap();
    ticks.add_permits(2);
    let held = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut held = None;
        loop {
            for (job, _) in device.jobs() {
                if matches!(device.operation(job), Some(Operation::CreateDirectory { path }) if path == Path::new("/partition-0")) {
                    held = Some(job);
                } else {
                    device.execute(job, Effect::Normal).unwrap();
                    device.deliver(job).unwrap();
                }
            }
            if completed.load(Ordering::Acquire) == 3 && times.load(Ordering::Acquire) == 2
                && let Some(job) = held { break job; }
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    assert!(!device.image().exists(Path::new("/partition-0"), false));
    // Partition 2 shares the held partition's application thread and I/O lane.
    assert!(device.image().exists(Path::new("/partition-2"), false));
    device.execute(held, Effect::Normal).unwrap();
    device.deliver(held).unwrap();
    shards.shutdown().await.unwrap();
    assert_eq!(completed.load(Ordering::Acquire), 4);
    device.begin_shutdown().unwrap().wait().await;
}

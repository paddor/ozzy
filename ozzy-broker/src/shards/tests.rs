use super::ApplicationShards;
use crate::DevicePools;
use ozzy_config::{Affinity, BrokerPlan, Deployment, HostResources, IoBackend, QueueBudget, Shard};
use std::{
    collections::HashSet,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

mod storage;

#[tokio::test]
async fn startup_rejects_missing_duplicate_and_swapped_device_assignments() {
    for invalid in 0..4 {
        let mut plan = plan(2);
        let (devices, mut lanes) = DevicePools::start(&plan).unwrap();
        match invalid {
            0 => plan.controllers.clear(),
            1 => plan.controllers.push(plan.controllers[0].clone()),
            2 => {
                lanes.pop();
            }
            _ => {
                let (left, right) = lanes.split_at_mut(1);
                std::mem::swap(&mut left[0].client, &mut right[0].client);
            }
        }
        assert!(super::validate_lanes(&plan, lanes).is_err());
        devices.shutdown().await;
    }
}

fn plan(partitions: u32) -> BrokerPlan {
    let mut config = Deployment::parse(include_str!(
        "../../../ozzy-config/tests/fixtures/single.toml"
    ))
    .unwrap();
    config.topics.get_mut("orders").unwrap().partitions = partitions;
    let broker = config.brokers.get_mut("laptop").unwrap();
    broker.devices.get_mut("ssd").unwrap().workers.backend = IoBackend::Pool;
    broker.topology.shards = [0, 7]
        .into_iter()
        .map(|id| Shard {
            id,
            device: "ssd".into(),
            affinity: Affinity::default(),
            budget: QueueBudget::default(),
        })
        .collect();
    config
        .validate()
        .unwrap()
        .broker_plan(
            "laptop",
            &HostResources {
                cpus: [(0, Some(0))].into(),
                memory_nodes: [0].into(),
                linux_aio: true,
            },
        )
        .unwrap()
}

#[tokio::test]
async fn non_send_partition_tasks_share_symmetric_fixed_shard_threads() {
    for partitions in [1, 64] {
        let plan = plan(partitions);
        let (devices, lanes) = DevicePools::start(&plan).unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let finished = Arc::new(AtomicUsize::new(0));
        let shards = ApplicationShards::start(&plan, lanes, {
            let seen = seen.clone();
            let finished = finished.clone();
            move |mut context| {
                let id = std::thread::current().id();
                seen.lock().unwrap().push((context.plan.id, id));
                let finished = finished.clone();
                // Neither this state nor these futures can leave their owner.
                let local = Rc::new(std::cell::Cell::new(0));
                async move {
                    let mut tasks = Vec::new();
                    for _ in &context.partitions {
                        let shutdown = context.shutdown.clone();
                        let local = local.clone();
                        let memory = context.memory.data.clone();
                        tasks.push(tokio::task::spawn_local(async move {
                            assert_eq!(std::thread::current().id(), id);
                            let payload = memory.lease(32).await.unwrap().freeze();
                            shutdown.requested().await;
                            drop(payload);
                            local.set(local.get() + 1);
                        }));
                    }
                    context.ready()?;
                    context.shutdown.requested().await;
                    for task in tasks {
                        task.await.unwrap();
                    }
                    context.memory.data.trim_cache();
                    assert_eq!(context.memory.data.allocated_bytes(), 0);
                    assert_eq!(local.get(), context.partitions.len());
                    finished.fetch_add(local.get(), Ordering::Release);
                    Ok(())
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(shards.thread_count(), 2);
        let seen = seen.lock().unwrap().clone();
        assert_eq!(
            seen.iter().map(|(_, id)| *id).collect::<HashSet<_>>().len(),
            2
        );
        assert!(
            seen.iter()
                .all(|(_, id)| *id != std::thread::current().id())
        );
        shards.shutdown().await.unwrap();
        assert_eq!(finished.load(Ordering::Acquire), partitions as usize);
        devices.shutdown().await;
    }
}

#[tokio::test]
async fn startup_reports_the_shard_error_before_readiness() {
    let plan = plan(1);
    let (devices, lanes) = DevicePools::start(&plan).unwrap();
    let error = ApplicationShards::start(&plan, lanes, |_context| async {
        Err(crate::StartupError::Runtime("startup sentinel".into()))
    })
    .await
    .unwrap_err();
    assert!(error.to_string().contains("startup sentinel"), "{error}");
    devices.shutdown().await;
}

#[tokio::test]
async fn failed_placement_wakes_other_shards_still_initializing() {
    let mut plan = plan(16);
    let (devices, lanes) = DevicePools::start(&plan).unwrap();
    plan.shards[1].affinity.cpu = Some(u32::MAX);
    let drained = Arc::new(AtomicUsize::new(0));
    let startup = ApplicationShards::start(&plan, lanes, {
        let drained = drained.clone();
        move |context| {
            let drained = drained.clone();
            async move {
                // This shard intentionally never publishes readiness.
                context.shutdown.requested().await;
                drained.fetch_add(1, Ordering::Release);
                Ok(())
            }
        }
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(5), startup)
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(drained.load(Ordering::Acquire), 1);
    devices.shutdown().await;
}

#[tokio::test]
async fn cancellation_of_shutdown_observation_does_not_drop_worker_drain() {
    let plan = plan(2);
    let (devices, lanes) = DevicePools::start(&plan).unwrap();
    let drain = Arc::new(tokio::sync::Semaphore::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let shards = ApplicationShards::start(&plan, lanes, {
        let drain = drain.clone();
        let completed = completed.clone();
        move |mut context| {
            let drain = drain.clone();
            let completed = completed.clone();
            async move {
                context.ready()?;
                context.shutdown.requested().await;
                drain.acquire().await.unwrap().forget();
                completed.fetch_add(1, Ordering::Release);
                Ok(())
            }
        }
    })
    .await
    .unwrap();
    let mut shutdown = Box::pin(shards.shutdown());
    assert!(futures::poll!(&mut shutdown).is_pending());
    drop(shutdown);
    assert_eq!(completed.load(Ordering::Acquire), 0);
    drain.add_permits(2);
    shards.closed().await.unwrap();
    assert_eq!(completed.load(Ordering::Acquire), 2);
    devices.shutdown().await;
}

#[tokio::test]
async fn one_runtime_panic_stops_and_drains_the_other_shards() {
    let plan = plan(2);
    let (devices, lanes) = DevicePools::start(&plan).unwrap();
    let trigger = Arc::new(tokio::sync::Semaphore::new(0));
    let drained = Arc::new(AtomicUsize::new(0));
    let shards = ApplicationShards::start(&plan, lanes, {
        let trigger = trigger.clone();
        let drained = drained.clone();
        move |mut context| {
            let trigger = trigger.clone();
            let drained = drained.clone();
            async move {
                context.ready()?;
                if context.plan.id == 0 {
                    trigger.acquire().await.unwrap().forget();
                    panic!("injected application failure");
                }
                context.shutdown.requested().await;
                drained.fetch_add(1, Ordering::Release);
                Ok(())
            }
        }
    })
    .await
    .unwrap();
    trigger.add_permits(1);
    assert!(shards.closed().await.is_err());
    assert_eq!(drained.load(Ordering::Acquire), 1);
    devices.shutdown().await;
}

#[tokio::test]
async fn dropping_startup_requests_independent_thread_shutdown() {
    let plan = plan(2);
    let (devices, lanes) = DevicePools::start(&plan).unwrap();
    let finished = Arc::new(tokio::sync::Semaphore::new(0));
    let mut startup = Box::pin(ApplicationShards::start(&plan, lanes, {
        let finished = finished.clone();
        move |context| {
            let finished = finished.clone();
            async move {
                context.shutdown.requested().await;
                finished.add_permits(1);
                Ok(())
            }
        }
    }));
    assert!(futures::poll!(&mut startup).is_pending());
    drop(startup);
    tokio::time::timeout(std::time::Duration::from_secs(5), finished.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    devices.shutdown().await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn affinity_is_installed_before_factory_and_local_state_allocation() {
    use rustix::thread::{CpuSet, sched_getaffinity};
    let inherited = sched_getaffinity(None).unwrap();
    let cpu = (0..CpuSet::MAX_CPU)
        .find(|&cpu| inherited.is_set(cpu))
        .unwrap();
    let mut plan = plan(2);
    let (devices, lanes) = DevicePools::start(&plan).unwrap();
    for shard in &mut plan.shards {
        shard.affinity.cpu = Some(cpu as u32);
    }
    let shards = ApplicationShards::start(&plan, lanes, move |mut context| {
        let actual = sched_getaffinity(None).unwrap();
        assert_eq!(
            (0..CpuSet::MAX_CPU)
                .filter(|&cpu| actual.is_set(cpu))
                .collect::<Vec<_>>(),
            [cpu]
        );
        let mut local = context.memory.data.try_lease(4096).unwrap();
        local.fill(42);
        async move {
            context.ready()?;
            context.shutdown.requested().await;
            assert_eq!(local[0], 42);
            Ok(())
        }
    })
    .await
    .unwrap();
    assert_eq!(sched_getaffinity(None).unwrap(), inherited);
    shards.shutdown().await.unwrap();
    devices.shutdown().await;
}

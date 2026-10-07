use super::{DevicePools, initializer};
use ozzy_config::{Affinity, BrokerPlan, Deployment, HostResources, IoBackend, QueueBudget, Shard};
use ozzy_io::{Backend, Class, OpenMode, Operation, Outcome, WriteBuffer};
use ozzy_io_pool::Worker;
use std::sync::{Arc, Mutex};

fn plan(partitions: u32, backend: IoBackend) -> BrokerPlan {
    let mut config = Deployment::parse(include_str!(
        "../../../ozzy-config/tests/fixtures/single.toml"
    ))
    .unwrap();
    let topic = config.topics.get_mut("orders").unwrap();
    topic.partitions = partitions;
    topic.max_append_bytes = 1024;
    let broker = config.brokers.get_mut("laptop").unwrap();
    let device = broker.devices.get_mut("ssd").unwrap();
    device.workers.backend = backend;
    device.workers.write_threads = 2;
    device.workers.max_inflight = 2;
    device.workers.queued_jobs = 12;
    device.workers.queued_bytes = 12 * 4096;
    device.workers.progress_jobs = 3;
    device.workers.progress_bytes = 3 * 4096;
    device.workers.open_handles = 24;
    let mut second = device.clone();
    second.root = "/second/data".into();
    broker.devices.insert("second".into(), second);
    broker.topology.shards = [3, 90, 202]
        .into_iter()
        .map(|id| Shard {
            id,
            device: if id == 90 { "second" } else { "ssd" }.into(),
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
#[expect(
    clippy::too_many_lines,
    reason = "checks the complete shared pool lifecycle"
)]
async fn multiple_devices_and_partitions_share_one_fixed_pool_and_budget() {
    for partitions in [1, 64] {
        let plan = plan(partitions, IoBackend::Pool);
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (pools, mut lanes) = DevicePools::start_with(&plan, |_| {
            let seen = seen.clone();
            Arc::new(move |role| {
                seen.lock().unwrap().push(role);
                Ok(())
            })
        })
        .unwrap();
        let mut roles = seen.lock().unwrap().clone();
        roles.sort();
        assert_eq!(&roles[..2], &[Worker::Data(0), Worker::Data(1)]);
        assert_eq!(&roles[2..], &[Worker::Progress; 2]);
        assert_eq!(pools.pools.len(), 1);
        assert_eq!(
            lanes.iter().map(|lane| lane.shard).collect::<Vec<_>>(),
            [3, 90, 202]
        );
        let limits = lanes[0].client.admission().limits();
        assert_eq!(limits.shards, 3);
        assert_eq!(limits.data.operations, 12);
        for (index, lane) in lanes.iter().enumerate() {
            assert_eq!(lane.client.shard(), index);
            assert_eq!(lane.client.admission().limits(), limits);
        }
        let quota = limits.share(0, Class::Data);
        let directory = tempfile::tempdir().unwrap();
        let reservations: Vec<_> = (0..quota.operations)
            .map(|index| {
                lanes[0]
                    .client
                    .submit(
                        Class::Data,
                        Operation::Open {
                            path: directory.path().join(format!("held-{index}")),
                            mode: OpenMode::CreateNew,
                            direct: false,
                            data_sync: false,
                        },
                    )
                    .unwrap()
            })
            .collect();
        assert_eq!(
            lanes[1].client.admission().used(0, Class::Data).operations,
            quota.operations
        );
        assert!(matches!(
            lanes[0].client.submit(
                Class::Data,
                Operation::Open {
                    path: directory.path().join("full"),
                    mode: OpenMode::CreateNew,
                    direct: false,
                    data_sync: false,
                },
            ),
            Err(rejected) if rejected.error.kind() == std::io::ErrorKind::WouldBlock
        ));
        let other = lanes[1]
            .client
            .submit(
                Class::Data,
                Operation::Open {
                    path: directory.path().join("other-shard"),
                    mode: OpenMode::CreateNew,
                    direct: false,
                    data_sync: false,
                },
            )
            .unwrap();
        let progress = lanes[0]
            .client
            .submit(
                Class::Progress,
                Operation::Open {
                    path: directory.path().join("progress"),
                    mode: OpenMode::CreateNew,
                    direct: false,
                    data_sync: false,
                },
            )
            .unwrap();
        for completion in reservations {
            drop(completion.await.unwrap());
        }
        drop(other.await.unwrap());
        drop(progress.await.unwrap());
        for lane in &mut lanes {
            let path = directory.path().join(lane.shard.to_string());
            let opened = lane
                .client
                .submit(
                    Class::Data,
                    Operation::Open {
                        path: path.clone(),
                        mode: OpenMode::CreateNew,
                        direct: false,
                        data_sync: false,
                    },
                )
                .unwrap()
                .await
                .unwrap();
            let Outcome::Opened(handle) = &*opened else {
                panic!("handle")
            };
            let handle = handle.clone();
            drop(opened);
            lane.client
                .submit(
                    Class::Data,
                    Operation::Write {
                        handle,
                        offset: 0,
                        data: WriteBuffer::from_vec(b"shared device".to_vec()),
                    },
                )
                .unwrap()
                .await
                .unwrap();
            assert_eq!(std::fs::read(path).unwrap(), b"shared device");
        }
        pools.shutdown().await;
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn aio_pool_places_all_roles_before_execution() {
    use rustix::thread::{CpuSet, sched_getaffinity};
    let inherited = sched_getaffinity(None).unwrap();
    let cpus: Vec<_> = (0..CpuSet::MAX_CPU)
        .filter(|&cpu| inherited.is_set(cpu))
        .take(4)
        .map(|cpu| cpu as u32)
        .collect();
    assert_ne!(cpus.len(), 0);
    let mut plan = plan(16, IoBackend::Aio);
    let workers = &mut plan.controllers[0].workers;
    workers.cpus = vec![cpus[0], *cpus.get(1).unwrap_or(&cpus[0])];
    workers.progress_cpu = Some(*cpus.get(2).unwrap_or(&cpus[0]));
    workers.aio_cpu = Some(*cpus.get(3).unwrap_or(&cpus[0]));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (pools, _) = DevicePools::start_with(&plan, |controller| {
        let place = initializer(controller);
        let seen = seen.clone();
        Arc::new(move |role| {
            place(role)?;
            let mask = sched_getaffinity(None)?;
            let pinned: Vec<_> = (0..CpuSet::MAX_CPU)
                .filter(|&cpu| mask.is_set(cpu))
                .collect();
            seen.lock().unwrap().push((role, pinned));
            Ok(())
        })
    })
    .unwrap();
    let mut actual = seen.lock().unwrap().clone();
    actual.sort();
    let workers = &plan.controllers[0].workers;
    assert_eq!(
        actual,
        vec![
            (Worker::Data(0), vec![workers.cpus[0] as usize]),
            (Worker::Data(1), vec![workers.cpus[1] as usize]),
            (
                Worker::Progress,
                vec![workers.progress_cpu.unwrap() as usize]
            ),
            (
                Worker::Progress,
                vec![workers.progress_cpu.unwrap() as usize]
            ),
            (Worker::Direct, vec![workers.aio_cpu.unwrap() as usize]),
        ]
    );
    assert_eq!(sched_getaffinity(None).unwrap(), inherited);
    pools.shutdown().await;
}

#[test]
fn failed_placement_is_not_a_ready_broker() {
    let mut plan = plan(16, IoBackend::Pool);
    plan.controllers[0].workers.progress_cpu = Some(u32::MAX);
    assert!(DevicePools::start(&plan).is_err());
}

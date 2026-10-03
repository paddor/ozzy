//! One persisted TCP deployment. Every broker process uses its own volume/shards.

use super::{Client, fixture};
use crate::command;
use ozzy_broker::{CheckedConfig, check_config, host_resources, load_deployment};
use ozzy_config::Confirmation;
use std::{fmt::Write, fs, future::Future, net::TcpListener, path::PathBuf, process::Command};

pub(super) struct Cluster {
    directory: tempfile::TempDir,
    config: PathBuf,
    shared: PathBuf,
    local: [PathBuf; 3],
    roots: [PathBuf; 3],
}

impl Cluster {
    pub(super) fn new(policy: Confirmation) -> Self {
        let policy = match policy {
            Confirmation::DiskQuorum => "disk-quorum",
            Confirmation::ReplicatedPersisting => "replicated-persisting",
            Confirmation::LocalDurable => panic!("cluster fixture requires three brokers"),
        };
        let directory = tempfile::tempdir().unwrap();
        let config = directory.path().join("deployment.toml");
        // Reserve all endpoints together, then persist explicit ports for every restart.
        let sockets: [TcpListener; 12] =
            std::array::from_fn(|_| TcpListener::bind("127.0.0.1:0").unwrap());
        let roots = std::array::from_fn(|index| directory.path().join(format!("broker-{index}")));
        let mut source = format!(
            "[cluster]\nmode = \"three\"\n\
             [topics.orders]\nconfirmation = \"{policy}\"\npartitions = 4\n\
             segment_bytes = 1048576\nmax_append_bytes = 65536\n"
        );
        for index in 0..3 {
            let peer = sockets[index * 4].local_addr().unwrap().port();
            let data = sockets[index * 4 + 3].local_addr().unwrap().port();
            let readers = sockets[index * 4 + 1].local_addr().unwrap().port();
            let followers = sockets[index * 4 + 2].local_addr().unwrap().port();
            let root = roots[index].display();
            write!(
                source,
                "[brokers.broker-{index}.endpoints]\n\
                 peer = \"tcp://127.0.0.1:{peer}\"\n\
                 data_peer = \"tcp://127.0.0.1:{data}\"\n\
                 reader_pub = \"tcp://127.0.0.1:{readers}\"\n\
                 follower_pub = \"tcp://127.0.0.1:{followers}\"\n\
                 [brokers.broker-{index}.devices.ssd]\nroot = \"{root}\"\n\
                 controller = \"ssd\"\n\
                 [brokers.broker-{index}.devices.ssd.workers]\n\
                 backend = \"pool\"\nwrite_threads = 1\n"
            )
            .unwrap();
            for shard in 0..=index {
                write!(
                    source,
                    "[[brokers.broker-{index}.topology.shards]]\n\
                     id = {}\ndevice = \"ssd\"\n",
                    shard * 7
                )
                .unwrap();
            }
        }
        fs::write(&config, source).unwrap();
        Self {
            shared: directory.path().join("shared.identity"),
            local: std::array::from_fn(|index| {
                directory.path().join(format!("broker-{index}.identity"))
            }),
            directory,
            config,
            roots,
        }
    }

    fn command(&self, index: usize, operation: &str) -> Command {
        let mut command = command(&self.config);
        command
            .arg(operation)
            .arg("--broker")
            .arg(format!("broker-{index}"))
            .arg("--identity")
            .arg(&self.shared)
            .arg("--local-identity")
            .arg(&self.local[index]);
        command
    }

    pub(super) fn provision(&self) {
        fixture::success(
            command(&self.config)
                .arg("init")
                .arg("--identity")
                .arg(&self.shared),
        );
        for (index, root) in self.roots.iter().enumerate() {
            fixture::success(&mut self.command(index, "init-broker"));
            fs::create_dir(root).unwrap();
            fixture::success(&mut self.command(index, "init-volumes"));
            fixture::success(&mut self.command(index, "format"));
        }
    }

    pub(super) fn checked(&self, index: usize) -> CheckedConfig {
        check_config(
            load_deployment(&self.config).unwrap(),
            &self.shared,
            &format!("broker-{index}"),
            &host_resources().unwrap(),
        )
        .unwrap()
    }

    pub(super) fn partition(&self, index: usize, number: u32) -> PathBuf {
        self.checked(index)
            .plan
            .partitions
            .into_iter()
            .find(|plan| plan.topic == "orders" && plan.partition == number)
            .unwrap()
            .directory
    }

    pub(super) fn serve(&self, index: usize) -> Command {
        let mut command = self.command(index, "serve");
        command.arg("--trusted-transport");
        command
    }

    pub(super) fn recover(&self, index: usize, selections: &[&str]) -> Command {
        let mut command = self.command(index, "recover");
        command.arg("--trusted-transport").args(selections);
        command
    }

    fn start_one(&self, index: usize, round: usize, selections: &[&str]) -> fixture::Running {
        let command = if selections.is_empty() {
            self.serve(index)
        } else {
            self.recover(index, selections)
        };
        fixture::Running::spawn(
            command,
            self.directory
                .path()
                .join(format!("broker-{index}-round-{round}.log")),
            None,
        )
    }

    pub(super) fn start(&self) -> Processes {
        Processes {
            brokers: std::array::from_fn(|index| Some(self.start_one(index, 0, &[]))),
        }
    }
}

#[derive(Default)]
pub(super) struct Processes {
    brokers: [Option<fixture::Running>; 3],
}

impl Processes {
    pub(super) fn signal(&mut self, index: usize, signal: &str) {
        self.brokers[index].as_mut().unwrap().signal(signal);
    }

    pub(super) async fn stop(&mut self, index: usize, signal: &str) {
        let mut broker = self.brokers[index].take().expect("broker is running");
        broker.signal(signal);
        broker.exited(signal == "KILL").await;
    }

    pub(super) fn restart(&mut self, fixture: &Cluster, index: usize, round: usize) {
        self.restart_recovering(fixture, index, round, &[]);
    }

    pub(super) fn restart_recovering(
        &mut self,
        fixture: &Cluster,
        index: usize,
        round: usize,
        selections: &[&str],
    ) {
        assert!(self.brokers[index].is_none());
        self.brokers[index] = Some(fixture.start_one(index, round, selections));
    }

    pub(super) async fn ready(&mut self, index: usize) {
        self.brokers[index].as_mut().unwrap().ready().await;
    }

    pub(super) async fn observe<T>(&mut self, stage: &str, future: impl Future<Output = T>) -> T {
        let mut brokers = self
            .brokers
            .iter_mut()
            .filter_map(Option::as_mut)
            .collect::<Vec<_>>();
        fixture::observe(&mut brokers, stage, future).await
    }

    pub(super) async fn shutdown(&mut self) {
        for index in 0..3 {
            if self.brokers[index].is_some() {
                self.stop(index, "TERM").await;
            }
        }
    }
}

pub(super) async fn orderly_restart(policy: Confirmation) {
    let cluster = Cluster::new(policy);
    cluster.provision();
    let checked = cluster.checked(0);
    let first_id = ozzy_proto::NodeId::from_bytes(*checked.identity.brokers["broker-0"].as_bytes());
    let mut processes = cluster.start();
    let mut client = processes
        .observe("open cluster SDK", Client::open(&checked))
        .await;
    assert_eq!(
        client.links.socket_count(),
        5,
        "two PEER and three SUB sockets"
    );
    let pending = client.queue(0).await;
    processes
        .observe("confirm initial records", client.confirm(pending))
        .await;
    processes
        .observe("replay initial records", client.replay())
        .await;
    let initial_session = client.links.session(first_id).unwrap();

    processes.stop(0, "TERM").await;
    let pending = client.queue(1).await;
    processes
        .observe("confirm with two brokers", client.confirm(pending))
        .await;
    processes.restart(&cluster, 0, 1);
    processes.stop(1, "INT").await;
    let pending = client.queue(2).await;
    processes
        .observe(
            "confirmation needs restarted broker",
            client.confirm(pending),
        )
        .await;
    assert_ne!(client.links.session(first_id), Some(initial_session));
    assert_eq!(client.links.socket_count(), 5);
    processes
        .observe("replay all exact records", client.replay())
        .await;
    processes.observe("close cluster SDK", client.close()).await;
    processes.shutdown().await;
}

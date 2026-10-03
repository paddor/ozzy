//! Real worker processes share broker transport with their OMQ control context.

use ozzy_bench::{BenchResult, control};
use serde_json::json;
use std::{path::Path, process::Child, time::Duration};

#[tokio::test(flavor = "current_thread")]
async fn production_process_control_initializes_serves_and_drains_each_policy() {
    tokio::time::timeout(Duration::from_secs(60), async {
        control::run(&control::Args::default(), 1, async {
            for policy in ["local-durable", "disk-quorum", "replicated-persisting"] {
                for io_threads in [1, 2] {
                    lifecycle(policy, io_threads).await.map_err(|cause| {
                        std::io::Error::other(format!("{policy}, OMQ I/O={io_threads}: {cause}"))
                    })?;
                }
            }
            Ok(())
        })
        .await
    })
    .await
    .unwrap()
    .unwrap();
}

async fn lifecycle(policy: &str, io_threads: usize) -> BenchResult<()> {
    let directory = super::support::storage("native-process-");
    let (source, reservations) = super::document(directory.path(), policy);
    let mut source = source.parse::<toml::Value>()?;
    drop(reservations);
    let mut children = prepare(directory.path(), policy, io_threads, &mut source).await?;
    let source = toml::to_string(&source)?;
    let deployment = super::DeploymentArtifact::initialize(directory.path(), &source)?;
    let shared_identity = std::fs::read_to_string(&deployment.identity)?;
    for (index, (_, connection, local)) in children.iter_mut().enumerate() {
        connection.send(
            &json!({"command": "initialize", "configuration": source, "identity": shared_identity}),
        )?;
        let initialized = connection.receive().await?;
        assert_eq!(initialized["event"], "initialized");
        assert_eq!(initialized["index"], index);
        assert!(local.join(format!("broker-{index}.identity")).is_file());
        assert_eq!(
            std::fs::read_to_string(local.join("deployment.toml"))?,
            source
        );
        assert_eq!(
            std::fs::read_to_string(local.join("deployment.identity"))?,
            shared_identity
        );
    }
    for (_, connection, _) in &mut children {
        connection.send(&json!({"command": "serve"}))?;
    }
    for (index, (worker, connection, _)) in children.iter_mut().enumerate() {
        let ready = connection.receive().await?;
        assert_eq!(ready["event"], "ready");
        assert_eq!(ready["pid"], worker.0.id());
        assert_eq!(ready["index"], index);
        assert_eq!(ready["topology"]["application_threads"], index + 1);
        assert_eq!(ready["topology"]["dispatcher_threads"], 1);
        assert_eq!(ready["topology"]["omq_io_threads"], io_threads);
        let observed = &ready["topology"]["observed_threads"];
        assert_eq!(observed["application"], index + 1);
        assert_eq!(observed["dispatcher"], 1);
        assert_eq!(observed["omq_io"], io_threads);
        assert_eq!(observed["omq_control"], usize::from(io_threads > 1));
        assert_eq!(observed["backend"], 2);
        assert_eq!(ready["topology"]["partitions"].as_array().unwrap().len(), 4);
        connection.send(&json!({"command": "start"}))?;
        assert_eq!(connection.receive().await?["event"], "started");
    }
    for (_, connection, _) in &mut children {
        connection.send(&json!({"command": "drain"}))?;
    }
    for (worker, connection, local) in &mut children {
        let drained = connection.receive().await?;
        assert_eq!(drained["event"], "drained");
        let threads = drained["usage"]["execution"]["threads"].as_array().unwrap();
        assert!(
            !threads.iter().any(|thread| {
                thread["name"].as_str().is_some_and(|name| {
                    name.starts_with("ozzy_app-")
                        || name == "ozzy_dispatch"
                        || name.starts_with("ozzy_io-")
                })
            }),
            "production workers survived physical drain: {threads:?}"
        );
        connection.shutdown()?;
        worker.finish().await?;
        assert!(!Path::new(&format!("/proc/{}", worker.0.id())).exists());
        assert!(!local.exists(), "worker storage survived shutdown");
    }
    assert_eq!(
        std::fs::read_to_string(&deployment.identity)?,
        shared_identity
    );
    Ok(())
}

type ChildLink = (Worker, control::Connection, std::path::PathBuf);

async fn prepare(
    root: &Path,
    policy: &str,
    io_threads: usize,
    source: &mut toml::Value,
) -> BenchResult<Vec<ChildLink>> {
    let mut children = Vec::new();
    let brokers = source["brokers"].as_table_mut().unwrap();
    for (index, (_, broker)) in brokers.iter_mut().enumerate() {
        let (mut connection, args) = control::Connection::listen(None).await?;
        let worker = Worker::spawn(root, policy, io_threads, index, &args)?;
        connection.send(&json!({"command": "prepare"}))?;
        let prepared = connection.receive().await?;
        assert_eq!(prepared["event"], "prepared");
        assert_eq!(prepared["index"], index);
        assert_eq!(prepared["pid"], worker.0.id());
        assert_eq!(prepared["storage"]["requested_parent"], json!(root));
        let directory = std::path::PathBuf::from(prepared["directory"].as_str().unwrap());
        assert_eq!(
            std::fs::read_dir(&directory)?.count(),
            0,
            "preparation formatted a store"
        );
        let mut endpoints = prepared["endpoints"].as_object().unwrap().clone();
        endpoints.retain(|_, value| !value.is_null());
        for endpoint in endpoints.values() {
            let address = endpoint.as_str().unwrap().strip_prefix("tcp://").unwrap();
            assert!(
                std::net::TcpListener::bind(address).is_err(),
                "worker lost endpoint reservation"
            );
        }
        broker["endpoints"] = toml::Value::try_from(endpoints)?;
        broker["devices"]["ssd"]["root"] =
            toml::Value::String(prepared["root"].as_str().unwrap().into());
        broker["topology"].as_table_mut().unwrap().insert(
            "omq".into(),
            toml::Value::try_from(json!({"io_threads": io_threads}))?,
        );
        children.push((worker, connection, directory));
    }
    Ok(children)
}

struct Worker(Child);

impl Worker {
    fn spawn(
        directory: &Path,
        policy: &str,
        io_threads: usize,
        index: usize,
        control: &control::Args,
    ) -> BenchResult<Self> {
        let mut arguments = vec![
            "--processes".into(),
            "--network-ingress".into(),
            "--streaming".into(),
            "--duration".into(),
            "1".into(),
            "--partitions".into(),
            "4".into(),
            "--system".into(),
            if policy == "local-durable" {
                "single-durable"
            } else {
                policy
            }
            .into(),
            "--worker-index".into(),
            index.to_string(),
            "--io-threads".into(),
            io_threads.to_string(),
            "--storage-dir".into(),
            directory.to_str().unwrap().into(),
        ];
        control.append(&mut arguments);
        Ok(Self(
            std::process::Command::new(env!("CARGO_BIN_EXE_ozy_timed_bench"))
                .args(arguments)
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()?,
        ))
    }

    async fn finish(&mut self) -> BenchResult<()> {
        let status = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(status) = self.0.try_wait()? {
                    return Ok::<_, std::io::Error>(status);
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await??;
        let mut diagnostic = String::new();
        std::io::Read::read_to_string(self.0.stderr.as_mut().unwrap(), &mut diagnostic)?;
        assert!(
            status.success() && diagnostic.is_empty(),
            "worker {status}: {diagnostic}"
        );
        Ok(())
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

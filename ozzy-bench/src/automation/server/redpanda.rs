//! Native Redpanda brokers, pinned and audited like the Iggy fixture: one for
//! local modes, three sharing the broker CPU pool for group modes.
mod build;
mod warm;
use crate::automation::{Result, SSD, capture, check_canceled, isolation, json_file};
pub use build::prepare;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

#[derive(Debug)]
struct Node {
    root: PathBuf,
    admin: String,
    child: Child,
}

#[derive(Debug)]
/// Owned local Redpanda process and its fresh storage directory.
pub struct Redpanda {
    /// Fresh server storage and diagnostic artifact directory.
    pub root: PathBuf,
    /// Kafka-protocol client endpoint of this Redpanda deployment.
    pub endpoint: String,
    nodes: Vec<Node>,
    /// One CPU list per node.
    cpus: Vec<Vec<usize>>,
    replicas: usize,
    build: Value,
    stopped: bool,
}

impl Redpanda {
    /// Start fresh Redpanda with the requested persistence policy and CPU placement.
    pub fn start(root: &Path, mode: &str, cpus: &[Vec<usize>]) -> Result<Self> {
        let replicas = if crate::automation::cluster_mode(mode) {
            3
        } else {
            1
        };
        if !matches!(
            mode,
            "buffered" | "durable" | "disk-quorum" | "replicated-persisting"
        ) || cpus.len() != replicas
            || cpus.iter().any(Vec::is_empty)
        {
            return Err("Redpanda comparison requires a supported mode and CPU budget".into());
        }
        let build = build::verified_identity()?;
        fs::create_dir_all(root)?;
        let data = PathBuf::from(SSD)
            .join("ozzy-bench")
            .join(root.file_name().ok_or("missing run name")?);
        // Hold every selected port until all configurations have been written.
        let ports = (0..3 * replicas)
            .map(|_| TcpListener::bind("127.0.0.1:0"))
            .collect::<std::io::Result<Vec<_>>>()?;
        let port = |index: usize| -> Result<u16> { Ok(ports[index].local_addr()?.port()) };
        let address = |index: usize| -> Result<Value> {
            Ok(json!({"address":"127.0.0.1","port":port(index)?}))
        };
        let seeds = if replicas == 1 {
            vec![]
        } else {
            (0..replicas)
                .map(|node| Ok(json!({"host":address(3 * node + 1)?})))
                .collect::<Result<Vec<_>>>()?
        };
        let mut configs = vec![];
        for node in 0..replicas {
            let node_root = if replicas == 1 {
                root.to_path_buf()
            } else {
                root.join(node.to_string())
            };
            let node_data = if replicas == 1 {
                data.clone()
            } else {
                data.join(node.to_string())
            };
            fs::create_dir_all(&node_root)?;
            fs::create_dir_all(&node_data)?;
            let (kafka, rpc, admin) = (3 * node, 3 * node + 1, 3 * node + 2);
            let config = json!({"redpanda":{
                "data_directory":node_data,"developer_mode":false,"seed_servers":seeds,
                "empty_seed_starts_cluster":replicas == 1,
                "kafka_api":[address(kafka)?],"advertised_kafka_api":[address(kafka)?],
                "rpc_server":address(rpc)?,"advertised_rpc_api":address(rpc)?,"admin":[address(admin)?]
            }});
            // JSON is a YAML subset. Keep exact requested and effective settings.
            json_file(&node_root.join("redpanda.yaml"), &config)?;
            json_file(
                &node_root.join(".bootstrap.yaml"),
                &json!({
                    "enable_metrics_reporter":false,"auto_create_topics_enabled":false,
                    // The fixture does not scrape internal metrics. Redpanda's
                    // internal leadership counter also treats its own controller
                    // topic as missing during bootstrap. Disable that unused
                    // instrumentation, while retaining diagnostic logs.
                    "disable_metrics":true,
                    // Readers use explicit partition assignment and never
                    // commit offsets. Keep their required group topic small.
                    "group_topic_partitions":1,
                    "default_topic_replications":replicas,
                    "internal_topic_replication_factor":replicas,
                    "write_caching_default":"false","rpk_path":build::rpk()
                }),
            )?;
            configs.push((node_root, format!("http://127.0.0.1:{}", port(admin)?)));
        }
        let endpoint = format!("127.0.0.1:{}", port(0)?);
        drop(ports);
        // One reactor per CPU of a node's own list. Three nodes sharing one
        // pool get one reactor each, like the three Iggy brokers.
        let nodes = configs
            .into_iter()
            .enumerate()
            .map(|(index, (root, admin))| {
                let smp = crate::automation::cpus::shards(cpus, index);
                Ok(Node {
                    child: spawn(&root, &cpus[index], smp, &format!("{}G", 2 * smp))?,
                    root,
                    admin,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut server = Self {
            root: root.into(),
            endpoint,
            nodes,
            cpus: cpus.into(),
            replicas,
            build,
            stopped: false,
        };
        server.wait_ready()?;
        json_file(&root.join("inspect.json"), &server.identity()?)?;
        Ok(server)
    }

    fn admin(node: &Node, path: &str) -> Result<Value> {
        Ok(serde_json::from_str(&capture(Command::new("curl").args(
            [
                "--fail",
                "--silent",
                "--show-error",
                "--max-time",
                "2",
                &format!("{}{path}", node.admin),
            ],
        ))?)?)
    }

    fn ready(&self) -> bool {
        self.nodes.iter().all(|node| {
            Self::admin(node, "/v1/status/ready").is_ok_and(|value| value["status"] == "ready")
        }) && (self.replicas == 1
            || Self::admin(&self.nodes[0], "/v1/cluster/health_overview").is_ok_and(|health| {
                health["is_healthy"] == true
                    && health["all_nodes"]
                        .as_array()
                        .is_some_and(|nodes| nodes.len() == self.replicas)
            }))
    }

    fn wait_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            check_canceled()?;
            self.check()?;
            for node in &mut self.nodes {
                if let Some(status) = node.child.try_wait()? {
                    return Err(format!(
                        "Redpanda exited during startup: {status}; {}",
                        node.root.display()
                    )
                    .into());
                }
            }
            if self.ready() {
                self.check()?;
                break;
            }
            if Instant::now() >= deadline {
                return Err("Redpanda readiness deadline expired".into());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        for node in &self.nodes {
            let effective = Self::admin(node, "/v1/node_config")?;
            if effective["developer_mode"] != false {
                return Err("Redpanda developer mode would weaken durability".into());
            }
            json_file(&node.root.join("effective-node.json"), &effective)?;
        }
        let cluster = Self::admin(&self.nodes[0], "/v1/cluster_config?include_defaults=true")?;
        if cluster["write_caching_default"] != "false"
            || cluster["disable_metrics"] != true
            || cluster["internal_topic_replication_factor"] != self.replicas
            || cluster["group_topic_partitions"] != 1
            || cluster["default_topic_replications"] != self.replicas
            || cluster["rpk_path"] != build::rpk().to_string_lossy().as_ref()
        {
            return Err("Redpanda effective cluster configuration differs".into());
        }
        json_file(&self.root.join("effective-cluster.json"), &cluster)?;
        self.affinity()?;
        let started = Instant::now();
        let mut attempts = 0;
        loop {
            check_canceled()?;
            self.check()?;
            attempts += 1;
            let allocated = match warm::allocate(&self.endpoint) {
                // A new cluster's allocator group may still be electing a
                // leader; its request then outlives the socket timeout.
                Err(error)
                    if error.downcast_ref::<std::io::Error>().is_some_and(|error| {
                        matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                        )
                    }) =>
                {
                    None
                }
                result => result?,
            };
            if let Some(producer_id) = allocated {
                json_file(
                    &self.root.join("startup-producer-id.json"),
                    &json!({"phase":"before benchmark clients and warmup", "producer_id":producer_id,
                        "attempts":attempts,"elapsed_ms":started.elapsed().as_secs_f64()*1000.0}),
                )?;
                break;
            }
            if started.elapsed() >= Duration::from_secs(60) {
                return Err("Redpanda producer-ID startup deadline expired".into());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }

    /// Process IDs owned by this deployment.
    pub fn pids(&self) -> Vec<u32> {
        self.nodes.iter().map(|node| node.child.id()).collect()
    }

    /// Reject an exited process or fatal server diagnostics.
    pub fn check(&self) -> Result<()> {
        for node in &self.nodes {
            let path = node.root.join("server.log");
            if has_diagnostic(&fs::read(&path)?)? {
                return Err(
                    format!("external server diagnostics; inspect {}", path.display()).into(),
                );
            }
        }
        Ok(())
    }

    /// Capture allowed CPUs of the running Redpanda server.
    pub fn affinity(&self) -> Result<Value> {
        let mut threads = BTreeMap::new();
        for (pid, budget) in self.pids().into_iter().zip(&self.cpus) {
            for entry in fs::read_dir(format!("/proc/{pid}/task"))? {
                let path = entry?.path();
                let tid = path
                    .file_name()
                    .ok_or("missing task")?
                    .to_string_lossy()
                    .parse::<u32>()?;
                let result = (|| -> Result<Value> {
                    let cpus = isolation::cpus(Some(tid))?;
                    if cpus.iter().any(|cpu| !budget.contains(cpu)) {
                        return Err("Redpanda thread escaped CPU budget".into());
                    }
                    Ok(
                        json!({"pid":pid,"name":fs::read_to_string(path.join("comm"))?.trim(),"cpus":cpus}),
                    )
                })();
                match result {
                    Ok(value) => {
                        threads.insert(tid.to_string(), value);
                    }
                    Err(_) if !path.exists() => (),
                    Err(error) => return Err(error),
                }
            }
        }
        if threads.is_empty() {
            return Err("Redpanda has no live threads".into());
        }
        Ok(json!(threads))
    }

    /// Collect pinned source, package, and executable identity.
    pub fn identity(&self) -> Result<Value> {
        if self.build != build::verified_identity()? {
            return Err("Redpanda binary changed during case".into());
        }
        let nodes = self
            .nodes
            .iter()
            .map(|node| {
                Ok(json!({"pid":node.child.id(),
                    "config":fs::read_to_string(node.root.join("redpanda.yaml"))?,
                    "effective_node":crate::automation::read_json(&node.root.join("effective-node.json"))?,
                    "broker_log":node.root.join("server.log")}))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(
            json!({"build":self.build,"replicas":self.replicas,"cpu_budget":self.cpus,
            "nodes":nodes,
            "effective_cluster":crate::automation::read_json(&self.root.join("effective-cluster.json"))?,
            "unsafe_bypass_fsync":false,"client":"rust-rdkafka / compiled librdkafka",
            "diagnostic_policy":"WARN retained; ERROR/FATAL/PANIC abort"}),
        )
    }

    /// Stop the server and reap its process.
    pub fn stop(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        self.check()?;
        for node in &mut self.nodes {
            if node.child.try_wait()?.is_some() {
                return Err("Redpanda exited before controlled shutdown".into());
            }
        }
        // Disposable data, all readers already verified. As with Iggy, fixture
        // shutdown is outside timing and must not create a second workload.
        for node in &mut self.nodes {
            node.child.kill()?;
            node.child.wait()?;
        }
        self.stopped = true;
        self.check()?;
        json_file(
            &self.root.join("stopped.json"),
            &json!({"pids":self.pids(),"all_reaped":true,"graceful":false}),
        )
    }
}

fn spawn(root: &Path, cpus: &[usize], smp: usize, memory: &str) -> Result<Child> {
    let log = fs::File::create(root.join("server.log"))?;
    let mut command = Command::new("taskset");
    command
        .args([
            "-c",
            &cpus
                .iter()
                .map(usize::to_string)
                .collect::<Vec<_>>()
                .join(","),
        ])
        .arg(build::loader())
        .arg("--library-path")
        .arg(build::directory().join("lib"))
        .arg(build::binary())
        .arg("--redpanda-cfg")
        .arg(root.join("redpanda.yaml"))
        .args([
            "--smp",
            &smp.to_string(),
            "--memory",
            memory,
            "--reserve-memory",
            "0M",
            // The fixture has only its broker and benchmark-client sockets.
            // Avoid reserving the default 10,000 shared Linux AIO slots per
            // shard on a host whose `aio-max-nr` is 65,536.
            "--max-networking-io-control-blocks",
            "1024",
            "--overprovisioned",
            "--unsafe-bypass-fsync=false",
            "--default-log-level=info",
        ])
        .env("TMPDIR", SSD)
        .current_dir(root)
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log);
    json_file(
        &root.join("launch.json"),
        &json!({"command":format!("{command:?}")}),
    )?;
    Ok(command.spawn()?)
}

fn has_diagnostic(log: &[u8]) -> Result<bool> {
    let diagnostic = regex::bytes::Regex::new(r"\b(ERROR|FATAL|PANIC|panic)\b")?;
    Ok(log.split(|byte| *byte == b'\n').any(|line| {
        // Judge structured logs by severity, not words in their messages.
        // Startup property descriptions can contain ERROR. External warnings
        // stay in the log; errors and unstructured failures still abort.
        ![b"INFO ".as_slice(), b"DEBUG ", b"TRACE "]
            .iter()
            .any(|prefix| line.starts_with(prefix))
            && diagnostic.is_match(line)
    }))
}

impl Drop for Redpanda {
    fn drop(&mut self) {
        if !self.stopped {
            for node in &mut self.nodes {
                let _ = node.child.kill();
                let _ = node.child.wait();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn external_warnings_are_retained_but_error_severity_aborts() {
        for log in [
            b"INFO  main - property: failures appear as `ERROR` lines\n".as_slice(),
            b"INFO  cluster - property description mentions WARN\n",
            b"WARN  cluster - id_allocator_frontend.cc:265 - can't find {kafka_internal/id_allocator} in the metadata cache\n",
            b"WARN  cluster - allocator topic already created; retrying\n",
            b"WARN  cluster - leadership change for unknown topic {redpanda/controller}\n",
        ] {
            assert!(!has_diagnostic(log).unwrap());
        }
        for log in [
            b"ERROR storage - write failed\n".as_slice(),
            b"panic: unstructured failure\n",
            b"INFO main - ready\nFATAL storage - write failed\n",
            b"WARN startup retry\nERROR storage failed\n",
            b"INFO  ready\nWARN  kafka - transient ERROR; retrying\n",
        ] {
            assert!(has_diagnostic(log).unwrap());
        }
    }
}

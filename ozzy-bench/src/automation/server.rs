//! Fresh release brokers per case. All share the assigned CPU budget.
pub(super) mod build;
mod distributed;
mod external;
pub mod redpanda;
use super::{Result, SSD, check_canceled, isolation, json_file};
pub use build::prepare;
pub use external::External;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    fs,
    net::{TcpListener, TcpStream},
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};
pub const RELEASE: &str = "0.9.0";
pub const RELEASE_TAG: &str = "server-0.9.0";
pub const REVISION: &str = "71f29618ba04917fded0ef5cd085c206fef0ee49";

#[derive(Debug, Clone, Copy)]
pub enum Monitor<'a> {
    Distributed(&'a distributed::Iggy),
    Native(&'a Iggy),
    Redpanda(&'a redpanda::Redpanda),
    Container(&'a str, &'a Path),
}
impl Monitor<'_> {
    pub fn check(self) -> Result<()> {
        match self {
            Self::Distributed(server) => server.check(),
            Self::Native(server) => server.check(),
            Self::Redpanda(server) => server.check(),
            Self::Container(name, root) => {
                let output = Command::new("podman").args(["logs", name]).output()?;
                if !output.status.success() {
                    return Err("external log monitor failed".into());
                }
                let logs = [output.stdout, output.stderr].concat();
                fs::write(root.join("server.log"), &logs)?;
                if fatal_log(&logs)? {
                    return Err("external server diagnostics".into());
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug)]
struct Broker {
    child: Child,
    root: PathBuf,
    data: PathBuf,
    endpoint: String,
}
#[derive(Debug)]
pub struct Iggy {
    pub root: PathBuf,
    pub endpoint: String,
    brokers: Vec<Broker>,
    /// One CPU list per broker.
    cpus: Vec<Vec<usize>>,
    build: Value,
    stopped: bool,
}

impl Iggy {
    pub fn start(root: &Path, mode: &str, cpus: &[Vec<usize>], log_filter: &str) -> Result<Self> {
        fs::create_dir_all(root)?;
        let mut server = Self {
            root: root.into(),
            endpoint: String::new(),
            brokers: vec![],
            cpus: cpus.into(),
            build: build::verified_identity()?,
            stopped: false,
        };
        let cluster = super::cluster_mode(mode);
        let count = if cluster { 3 } else { 1 };
        if cpus.len() != count {
            return Err("expected one CPU list per Iggy broker".into());
        }
        // Hold every selected port until all configurations have been written.
        let ports = (0..count * 2)
            .map(|_| TcpListener::bind("127.0.0.1:0"))
            .collect::<std::io::Result<Vec<_>>>()?;
        let nodes = (0..count).map(|index| Ok(json!({"name":format!("broker-{index}"),"ip":"127.0.0.1","replica_id":index,"ports":{"tcp":ports[2*index].local_addr()?.port(),"tcp_replica":ports[2*index+1].local_addr()?.port()}}))).collect::<Result<Vec<Value>>>()?;
        let template = fs::read_to_string(build::checkout().join("core/server/config.toml"))?;
        let mut configs = vec![];
        for index in 0..count {
            let broker_root = root.join(index.to_string());
            fs::create_dir(&broker_root)?;
            let data = PathBuf::from(SSD)
                .join("ozzy-bench")
                .join(root.file_name().ok_or("missing run name")?)
                .join(index.to_string());
            fs::create_dir_all(&data)?;
            let mut config: Value =
                serde_json::to_value(toml::from_str::<toml::Value>(&template)?)?;
            config["path"] = json!(data);
            for transport in ["http", "quic", "websocket"] {
                config[transport]["enabled"] = json!(false);
            }
            config["heartbeat"]["enabled"] = json!(false);
            config["logging"]["file_enabled"] = json!(false);
            config["logging"]["level"] = json!(log_filter);
            // A shared CPU pool belongs to the entire deployment: do not
            // multiply a four-core allocation into twelve Iggy shards.
            config["sharding"]["cpu_allocation"] =
                json!(crate::automation::cpus::shards(cpus, index));
            config["sharding"]["pin_cores"] = json!(false);
            config["cluster"]["enabled"] = json!(cluster);
            config["cluster"]["name"] = json!(root.file_name().unwrap().to_string_lossy());
            config["cluster"]["nodes"] = json!(nodes);
            let endpoint = format!("127.0.0.1:{}", ports[index * 2].local_addr()?.port());
            config["tcp"]["address"] = json!(endpoint);
            fs::write(broker_root.join("config.toml"), toml::to_string(&config)?)?;
            fs::write(broker_root.join(".env"), "")?;
            configs.push((broker_root, data, endpoint));
        }
        drop(ports);
        for (index, (broker_root, data, endpoint)) in configs.into_iter().enumerate() {
            let log = fs::File::create(broker_root.join("server.log"))?;
            let mut command = Command::new("taskset");
            command
                .args([
                    "-c",
                    &cpus[index]
                        .iter()
                        .map(usize::to_string)
                        .collect::<Vec<_>>()
                        .join(","),
                ])
                .arg(build::binary());
            if cluster {
                command.args(["--replica-id", &index.to_string()]);
            }
            for (key, _) in
                std::env::vars().filter(|(key, _)| key.starts_with("IGGY_") || key == "RUST_LOG")
            {
                command.env_remove(key);
            }
            command
                .current_dir(&broker_root)
                .env("IGGY_CONFIG_PATH", broker_root.join("config.toml"))
                .env("IGGY_ROOT_USERNAME", "iggy")
                .env("IGGY_ROOT_PASSWORD", "iggy")
                .env("TMPDIR", SSD)
                .env("LD_LIBRARY_PATH", build::library_path())
                .stdin(Stdio::null())
                .stdout(log.try_clone()?)
                .stderr(log);
            server.brokers.push(Broker {
                child: command.spawn()?,
                root: broker_root,
                data,
                endpoint,
            });
        }
        server.wait_ready()?;
        server.endpoint.clone_from(&server.brokers[0].endpoint);
        json_file(&root.join("inspect.json"), &server.identity()?)?;
        Ok(server)
    }

    fn wait_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(60);
        let cluster = self.brokers.len() == 3;
        loop {
            check_canceled()?;
            self.check()?;
            let mut ready = true;
            for broker in &mut self.brokers {
                if let Some(status) = broker.child.try_wait()? {
                    return Err(format!(
                        "Iggy exited during startup: {status}; {}",
                        broker.root.display()
                    )
                    .into());
                }
                // Iggy creates the effective configuration before writing it.
                ready &=
                    fs::read_to_string(broker.data.join("runtime/current_config.toml")).is_ok_and(
                        |text| !text.is_empty() && toml::from_str::<toml::Value>(&text).is_ok(),
                    ) && (!cluster
                        || fs::read_to_string(broker.root.join("server.log"))?
                            .contains("replica mesh complete: all peer connections established"))
                        && TcpStream::connect_timeout(
                            &broker.endpoint.parse()?,
                            Duration::from_millis(20),
                        )
                        .is_ok();
            }
            if ready {
                break;
            }
            if Instant::now() >= deadline {
                return Err("Iggy readiness deadline expired".into());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        for broker in &self.brokers {
            fs::copy(
                broker.data.join("runtime/current_config.toml"),
                broker.root.join("effective-config.toml"),
            )?;
            let load = |name: &str| -> Result<Value> {
                Ok(serde_json::to_value(toml::from_str::<toml::Value>(
                    &fs::read_to_string(broker.root.join(name))?,
                )?)?)
            };
            let requested = load("config.toml")?;
            let effective = load("effective-config.toml")?;
            verify_configuration(&requested, &effective)?;
        }
        self.affinity(false)?;
        Ok(())
    }
    pub fn pids(&self) -> Vec<u32> {
        self.brokers.iter().map(|b| b.child.id()).collect()
    }
    pub fn check(&self) -> Result<()> {
        for broker in &self.brokers {
            let path = broker.root.join("server.log");
            if fatal_log(&fs::read(&path)?)? {
                return Err(
                    format!("external server diagnostics; inspect {}", path.display()).into(),
                );
            }
        }
        Ok(())
    }
    pub fn affinity(&self, _install: bool) -> Result<Value> {
        let mut observed = BTreeMap::new();
        for (broker, budget) in self.brokers.iter().zip(&self.cpus) {
            let mut threads = BTreeMap::new();
            for entry in fs::read_dir(format!("/proc/{}/task", broker.child.id()))? {
                let task = entry?.path();
                let tid = task.file_name().unwrap().to_string_lossy().parse::<u32>()?;
                let result = (|| -> Result<Value> {
                    let cpus = isolation::cpus(Some(tid))?;
                    if cpus.iter().any(|cpu| !budget.contains(cpu)) {
                        return Err("Iggy thread escaped CPU budget".into());
                    }
                    Ok(json!({"name":fs::read_to_string(task.join("comm"))?.trim(),"cpus":cpus}))
                })();
                match result {
                    Ok(value) => {
                        threads.insert(tid.to_string(), value);
                    }
                    Err(_) if !task.exists() => (),
                    Err(error) => return Err(error),
                }
            }
            if threads.is_empty() {
                return Err("Iggy has no live threads".into());
            }
            observed.insert(broker.child.id().to_string(), threads);
        }
        Ok(json!(observed))
    }
    pub fn identity(&self) -> Result<Value> {
        if self.build != build::verified_identity()? {
            return Err("Iggy binary changed during case".into());
        }
        let configs = self.brokers.iter().map(|b| -> Result<_> {Ok(json!({"pid":b.child.id(),"config":fs::read_to_string(b.root.join("config.toml"))?,"effective_config":fs::read_to_string(b.root.join("effective-config.toml"))?}))}).collect::<Result<Vec<_>>>()?;
        Ok(json!({"release":RELEASE,"build":self.build,"brokers":configs,"cpu_budget":self.cpus}))
    }
    pub fn stop(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        self.check()?;
        let mut signals = Command::new("kill");
        // These are disposable throughput fixtures. Freeze the complete group
        // before its final log audit, then terminate/reap all brokers. A broker
        // must not observe peers disappearing during this untimed teardown.
        signals.arg("-STOP");
        for broker in &mut self.brokers {
            if broker.child.try_wait()?.is_some() {
                return Err("Iggy exited before controlled shutdown".into());
            }
            signals.arg(broker.child.id().to_string());
        }
        super::capture(&mut signals)?;
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let mut stopped = true;
            for broker in &self.brokers {
                stopped &= fs::read_to_string(format!("/proc/{}/status", broker.child.id()))?
                    .lines()
                    .any(|line| line.starts_with("State:\tT"));
            }
            if stopped {
                break;
            }
            if Instant::now() >= deadline {
                return Err("Iggy fixture freeze deadline expired".into());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        self.check()?;
        let mut terminate = Command::new("kill");
        terminate.arg("-KILL");
        for broker in &self.brokers {
            terminate.arg(broker.child.id().to_string());
        }
        super::capture(&mut terminate)?;
        for broker in &mut self.brokers {
            let status = broker.child.wait()?;
            if status.signal() != Some(9) {
                return Err(format!("unexpected Iggy fixture exit: {status}").into());
            }
        }
        self.stopped = true;
        self.check()?;
        json_file(
            &self.root.join("stopped.json"),
            &json!({"pids":self.pids(),"all_reaped":true,"graceful":false,"termination":"SIGSTOP group, final log audit, SIGKILL group; outside workload"}),
        )
    }
}

fn verify_configuration(requested: &Value, effective: &Value) -> Result<()> {
    // Iggy normalizes durations/byte units, adds defaults and redacts secrets.
    // Compare the fields this runner controls, then retain both full configs.
    for pointer in [
        "/path",
        "/cluster/enabled",
        "/cluster/name",
        "/cluster/auth/enabled",
        "/cluster/tls/enabled",
        "/sharding/cpu_allocation",
        "/sharding/pin_cores",
        "/tcp/enabled",
        "/tcp/address",
        "/tcp/tls/enabled",
        "/http/enabled",
        "/quic/enabled",
        "/websocket/enabled",
        "/heartbeat/enabled",
        "/logging/file_enabled",
        "/logging/level",
    ] {
        if requested.pointer(pointer).is_none()
            || requested.pointer(pointer) != effective.pointer(pointer)
        {
            return Err(format!("Iggy effective {pointer} configuration differs").into());
        }
    }
    let expected = requested["cluster"]["nodes"]
        .as_array()
        .ok_or("missing requested nodes")?;
    let actual = effective["cluster"]["nodes"]
        .as_array()
        .ok_or("missing effective nodes")?;
    if expected.len() != actual.len() {
        return Err("Iggy effective node count differs".into());
    }
    for (expected, actual) in expected.iter().zip(actual) {
        for pointer in [
            "/name",
            "/ip",
            "/replica_id",
            "/ports/tcp",
            "/ports/tcp_replica",
        ] {
            if expected.pointer(pointer).is_none()
                || expected.pointer(pointer) != actual.pointer(pointer)
            {
                return Err(format!("Iggy effective node {pointer} differs").into());
            }
        }
    }
    Ok(())
}
impl Drop for Iggy {
    fn drop(&mut self) {
        if !self.stopped {
            // Failure paths must leave no competing broker behind.
            for broker in &mut self.brokers {
                let _ = broker.child.kill();
            }
            for broker in &mut self.brokers {
                let _ = broker.child.wait();
            }
        }
    }
}

fn fatal_log(log: &[u8]) -> Result<bool> {
    let plain = regex::bytes::Regex::new(r"\x1b\[[0-9;]*m")?.replace_all(log, &b""[..]);
    Ok(regex::bytes::Regex::new(r"\b(ERROR|FATAL|PANIC|panic|panicked)\b")?.is_match(&plain))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn external_warnings_are_retained_but_errors_are_fatal() {
        for line in [
            b"ERROR shard-0 repair failed".as_slice(),
            b"\x1b[2m2026-09-19\x1b[0m \x1b[31mERROR\x1b[0m shard-0 repair failed",
            b"\x1b[31mpanic\x1b[0m invalid repair range",
        ] {
            assert!(
                fatal_log(line).unwrap(),
                "{}",
                String::from_utf8_lossy(line)
            );
        }
        assert!(!fatal_log(b"INFO external server ready").unwrap());
        for line in [
            b"\x1b[33mWARN\x1b[0m received old prepare (<= commit_min), skipping replication"
                .as_slice(),
            b"WARNING external warning",
        ] {
            assert!(!fatal_log(line).unwrap());
            let mixed = [line, b"\nERROR storage failed"].concat();
            assert!(fatal_log(&mixed).unwrap());
        }
    }
}

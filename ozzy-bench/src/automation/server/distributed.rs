//! Iggy fixtures using the same per-broker hosts, CPU masks, and storage as Ozzy.
use super::{Result, build, verify_configuration};
use crate::{
    automation::{
        distributed::{self as remote, quote},
        json_file,
    },
    placement::Placement,
};
use serde_json::{Value, json};
use std::{
    fs,
    net::{SocketAddr, TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Stdio},
    time::{Duration, Instant},
};

#[derive(Debug)]
struct Broker {
    placement: Placement,
    child: Child,
    pid: u32,
    root: PathBuf,
    work: PathBuf,
    data: PathBuf,
    endpoint: String,
}
#[derive(Debug)]
pub struct Iggy {
    pub root: PathBuf,
    pub endpoint: String,
    brokers: Vec<Broker>,
    build: Value,
    stopped: bool,
}
impl Iggy {
    pub fn start(root: &Path, placements: &[Placement; 3]) -> Result<Self> {
        fs::create_dir_all(root)?;
        let mut server = Self {
            root: root.into(),
            endpoint: String::new(),
            brokers: vec![],
            build: build::verified_identity()?,
            stopped: false,
        };
        let sockets = (0..6)
            .map(|_| TcpListener::bind("127.0.0.1:0"))
            .collect::<std::io::Result<Vec<_>>>()?;
        let ports = sockets
            .iter()
            .map(|s| s.local_addr().map(|a| a.port()))
            .collect::<std::io::Result<Vec<_>>>()?;
        let nodes=placements.iter().enumerate().map(|(i,p)|json!({"name":format!("broker-{i}"),"ip":p.bind,"replica_id":i,"ports":{"tcp":ports[2*i],"tcp_replica":ports[2*i+1]}})).collect::<Vec<_>>();
        let template: Value = serde_json::to_value(toml::from_str::<toml::Value>(
            &fs::read_to_string(build::checkout().join("core/server/config.toml"))?,
        )?)?;
        drop(sockets);
        for (i, p) in placements.iter().enumerate() {
            let mut template = template.clone();
            template["sharding"]["cpu_allocation"] = json!(shard_count(placements, i)?);
            server.launch(i, p, &ports, &nodes, &template)?;
        }
        server.wait_ready()?;
        server.endpoint.clone_from(&server.brokers[0].endpoint);
        json_file(&root.join("inspect.json"), &server.identity()?)?;
        Ok(server)
    }
    fn launch(
        &mut self,
        index: usize,
        p: &Placement,
        ports: &[u16],
        nodes: &[Value],
        template: &Value,
    ) -> Result<()> {
        let root = self.root.join(index.to_string());
        fs::create_dir(&root)?;
        let work = p
            .storage_dir
            .as_ref()
            .ok_or("missing storage")?
            .join(format!(
                "iggy-{}-{index}",
                self.root
                    .file_name()
                    .ok_or("missing case name")?
                    .to_string_lossy()
            ));
        let data = work.join("data");
        remote::run(
            p,
            &format!(
                "mkdir {}; mkdir {}",
                quote(&work.to_string_lossy()),
                quote(&data.to_string_lossy())
            ),
        )?;
        let endpoint = SocketAddr::new(p.bind, ports[2 * index]).to_string();
        let mut config = template.clone();
        config["path"] = json!(data);
        for transport in ["http", "quic", "websocket"] {
            config[transport]["enabled"] = json!(false);
        }
        config["heartbeat"]["enabled"] = json!(false);
        config["logging"]["file_enabled"] = json!(false);
        config["logging"]["level"] = json!("info");
        config["sharding"]["pin_cores"] = json!(false);
        config["cluster"]["enabled"] = json!(true);
        config["cluster"]["name"] = json!(self.root.file_name().unwrap().to_string_lossy());
        config["cluster"]["nodes"] = json!(nodes);
        config["tcp"]["address"] = json!(endpoint);
        let config_path = root.join("config.toml");
        fs::write(&config_path, toml::to_string(&config)?)?;
        remote::copy(p, &config_path, &work.join("config.toml"))?;
        let (binary, libraries) = if p.remote.is_some() {
            let root = remote::remote_root(p)?;
            (root.join("iggy-server"), root)
        } else {
            (build::binary(), build::library_path())
        };
        let script = format!(
            "set -e; cd {}; : > .env; echo $$ > pid; exec env -i PATH=/usr/bin:/bin HOME={} TMPDIR={} LD_LIBRARY_PATH={} IGGY_CONFIG_PATH={} IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy taskset -c {} {} --replica-id {index}",
            quote(&work.to_string_lossy()),
            quote(&work.to_string_lossy()),
            quote(&work.to_string_lossy()),
            quote(&libraries.to_string_lossy()),
            quote(&work.join("config.toml").to_string_lossy()),
            quote(&p.cpu_list().ok_or("missing CPU mask")?),
            quote(&binary.to_string_lossy())
        );
        let script = if p.remote.is_some() {
            format!("exec timeout --signal=KILL 180s sh -c {}", quote(&script))
        } else {
            script
        };
        let log = fs::File::create(root.join("server.log"))?;
        let child = remote::command(p, &script)
            .stdin(Stdio::null())
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?;
        self.brokers.push(Broker {
            placement: p.clone(),
            child,
            pid: 0,
            root,
            work,
            data,
            endpoint,
        });
        Ok(())
    }
    fn wait_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            crate::automation::check_canceled()?;
            self.check()?;
            let mut ready = true;
            for broker in &mut self.brokers {
                if broker.child.try_wait()?.is_some() {
                    return Err(format!("Iggy exited: {}", broker.root.display()).into());
                }
                if broker.pid == 0 {
                    let pid = remote::run(
                        &broker.placement,
                        &format!(
                            "cat {} 2>/dev/null || true",
                            quote(&broker.work.join("pid").to_string_lossy())
                        ),
                    )?;
                    if !pid.is_empty() {
                        broker.pid = pid.parse()?;
                    }
                }
                ready &= broker.pid != 0
                    && fs::read_to_string(broker.root.join("server.log"))?
                        .contains("replica mesh complete: all peer connections established")
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
                return Err("distributed Iggy readiness expired".into());
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        for broker in &self.brokers {
            let effective = remote::run(
                &broker.placement,
                &format!(
                    "cat {}",
                    quote(
                        &broker
                            .data
                            .join("runtime/current_config.toml")
                            .to_string_lossy()
                    )
                ),
            )?;
            fs::write(broker.root.join("effective-config.toml"), &effective)?;
            let requested = serde_json::to_value(toml::from_str::<toml::Value>(
                &fs::read_to_string(broker.root.join("config.toml"))?,
            )?)?;
            let effective = serde_json::to_value(toml::from_str::<toml::Value>(&effective)?)?;
            verify_configuration(&requested, &effective)?;
        }
        self.affinity()?;
        Ok(())
    }
    pub fn pids(&self) -> Vec<u32> {
        self.brokers
            .iter()
            .filter(|b| b.placement.remote.is_none())
            .map(|b| b.pid)
            .collect()
    }
    pub fn check(&self) -> Result<()> {
        for b in &self.brokers {
            if super::fatal_log(&fs::read(b.root.join("server.log"))?)? {
                return Err(format!("Iggy diagnostics: {}", b.root.display()).into());
            }
        }
        Ok(())
    }
    pub fn affinity(&self) -> Result<Value> {
        self.brokers
            .iter()
            .map(|b| {
                let row = remote::inspect(&b.placement, Some(b.pid))?;
                b.placement.verify_execution(&row["execution"])?;
                if row["executable_sha256"] != self.build["executable_sha256"] {
                    return Err("remote Iggy build differs".into());
                }
                Ok(row)
            })
            .collect::<Result<Vec<_>>>()
            .map(|rows| json!(rows))
    }
    pub fn identity(&self) -> Result<Value> {
        Ok(
            json!({"release":super::RELEASE,"build":self.build,"brokers":self.brokers.iter().map(|b|Ok(json!({"pid":b.pid,"remote":b.placement.remote.is_some(),"config":fs::read_to_string(b.root.join("config.toml"))?,"effective_config":fs::read_to_string(b.root.join("effective-config.toml"))?}))).collect::<Result<Vec<_>>>()?}),
        )
    }
    pub fn stop(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        self.check()?;
        for b in &self.brokers {
            signal(b, "STOP")?;
        }
        self.check()?;
        for b in &self.brokers {
            signal(b, "KILL")?;
        }
        for b in &mut self.brokers {
            let _ = b.child.wait()?;
        }
        self.stopped = true;
        self.check()?;
        for b in &self.brokers {
            remote::run(
                &b.placement,
                &format!("rm -r -- {}", quote(&b.work.to_string_lossy())),
            )?;
        }
        json_file(
            &self.root.join("stopped.json"),
            &json!({"all_reaped":true,"graceful":false}),
        )
    }
}
fn shard_count(placements: &[Placement; 3], index: usize) -> Result<usize> {
    let placement = &placements[index];
    let cpus = placement.cpus.as_ref().ok_or("missing CPUs")?;
    let sharing = placements
        .iter()
        .filter(|p| p.remote.is_none() && p.cpus.as_ref() == Some(cpus))
        .count();
    // A shared local mask is one deployment budget, not a budget per broker.
    Ok(if placement.remote.is_none() {
        (cpus.len() / sharing).max(1)
    } else {
        cpus.len()
    })
}

fn signal(b: &Broker, name: &str) -> Result<()> {
    if b.pid == 0 {
        return Err("missing Iggy PID".into());
    }
    let expected = if b.placement.remote.is_some() {
        remote::remote_root(&b.placement)?.join("iggy-server")
    } else {
        build::binary()
    };
    remote::run(
        &b.placement,
        &format!(
            "test \"$(readlink /proc/{}/exe)\" = {} && kill -{} {}",
            b.pid,
            quote(&expected.to_string_lossy()),
            name,
            b.pid
        ),
    )?;
    Ok(())
}
impl Drop for Iggy {
    fn drop(&mut self) {
        if !self.stopped {
            for b in &mut self.brokers {
                if b.pid == 0 {
                    b.pid = remote::run(
                        &b.placement,
                        &format!("cat {}", quote(&b.work.join("pid").to_string_lossy())),
                    )
                    .ok()
                    .and_then(|pid| pid.parse().ok())
                    .unwrap_or(0);
                }
                let _ = signal(b, "KILL");
                let _ = b.child.kill();
                let _ = b.child.wait();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_local_cpu_pool_does_not_multiply_iggy_shards() {
        let mut placements = Placement::load(None).unwrap();
        for p in &mut placements {
            p.cpus = Some(vec![0, 1, 2]);
        }
        for i in 0..3 {
            assert_eq!(shard_count(&placements, i).unwrap(), 1);
        }
        for (i, p) in placements.iter_mut().enumerate() {
            p.cpus = Some(vec![i]);
        }
        for i in 0..3 {
            assert_eq!(shard_count(&placements, i).unwrap(), 1);
        }
        placements[2].remote = Some(("remote".into(), "/bin/iggy-server".into()));
        placements[2].cpus = Some(vec![0, 1]);
        assert_eq!(shard_count(&placements, 2).unwrap(), 2);
    }
}

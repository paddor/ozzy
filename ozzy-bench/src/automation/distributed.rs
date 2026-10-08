//! Explicit three-device placement, SSH deployment, and remote process auditing.
use super::{Result, capture, json_file, source};
use crate::placement::Placement;
use serde_json::{Value, json};
use std::{
    fmt::Write as _,
    fs,
    io::Write as _,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// Quote a value as one POSIX shell argument.
pub fn quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
/// Construct local or SSH execution for the requested broker placement.
pub fn command(placement: &Placement, script: &str) -> Command {
    if let Some((host, _)) = &placement.remote {
        let mut command = Command::new("ssh");
        command.args([
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=5",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=2",
            "--",
            host,
        ]);
        command.arg(format!("sh -c {}", quote(script)));
        command
    } else {
        let mut command = Command::new("sh");
        command.args(["-c", script]);
        command
    }
}
/// Execute a placement command and return checked UTF-8 stdout.
pub fn run(placement: &Placement, script: &str) -> Result<String> {
    capture(&mut command(placement, script))
}
/// Resolve the configured source checkout on a remote broker host.
pub fn remote_root(placement: &Placement) -> Result<PathBuf> {
    Ok(placement
        .remote
        .as_ref()
        .ok_or("not remote")?
        .1
        .parent()
        .ok_or("missing executable parent")?
        .into())
}
/// Copy a local artifact to its placement destination, creating parent directories.
pub fn copy(placement: &Placement, local: &Path, destination: &Path) -> Result<()> {
    let bytes = fs::read(local)?;
    let mut child = command(
        placement,
        &format!("umask 077; cat > {}", quote(&destination.to_string_lossy())),
    )
    .stdin(Stdio::piped())
    .stdout(Stdio::null())
    .stderr(Stdio::piped())
    .spawn()?;
    child
        .stdin
        .take()
        .ok_or("missing upload input")?
        .write_all(&bytes)?;
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(format!("remote upload: {}", String::from_utf8_lossy(&output.stderr)).into());
    }
    Ok(())
}
/// Capture host, storage, process, and optional process-affinity evidence.
pub fn inspect(placement: &Placement, pid: Option<u32>) -> Result<Value> {
    let binary = if placement.remote.is_some() {
        remote_root(placement)?.join("ozzy_host")
    } else {
        super::build_target(&super::root()).join("release/ozzy_host")
    };
    let mut script = format!(
        "{} inspect --storage {}",
        quote(&binary.to_string_lossy()),
        quote(
            &placement
                .storage_dir
                .as_ref()
                .ok_or("missing placement storage")?
                .to_string_lossy()
        )
    );
    if let Some(pid) = pid {
        write!(script, " --pid {pid}")?;
    }
    Ok(serde_json::from_str(&run(placement, &script)?)?)
}
/// Require every placement to be free of competing benchmark servers.
pub fn idle(placements: &[Placement; 3]) -> Result<Value> {
    let mut observations = vec![];
    for placement in placements.iter().filter(|p| p.remote.is_some()) {
        let row = inspect(placement, None)?;
        if row["processes"]
            .as_array()
            .ok_or("missing remote processes")?
            .iter()
            .any(|p| p["executable"] != "ozzy_host")
        {
            return Err(format!("remote processes still resident: {}", row["processes"]).into());
        }
        observations.push(row);
    }
    Ok(json!(observations))
}
/// Install the native worker and optional Iggy binary on remote placements.
pub fn deploy(placements: &[Placement; 3], native: &Path, iggy: bool) -> Result<()> {
    let helper = super::build_target(&super::root()).join("release/ozzy_host");
    for p in placements.iter().filter(|p| p.remote.is_some()) {
        let root = remote_root(p)?;
        run(p, &format!("mkdir -p {}", quote(&root.to_string_lossy())))?;
        let mut files = vec![
            (native.to_path_buf(), p.remote.as_ref().unwrap().1.clone()),
            (helper.clone(), root.join("ozzy_host")),
        ];
        if iggy {
            files.push((super::server::build::binary(), root.join("iggy-server")));
            for name in ["libhwloc.so.15", "libudev.so.1"] {
                files.push((
                    super::server::build::library_path().join(name),
                    root.join(name),
                ));
            }
        }
        for (local, remote) in files {
            let hash = source::sha256(&local)?;
            let existing = run(
                p,
                &format!(
                    "sha256sum {} 2>/dev/null || true",
                    quote(&remote.to_string_lossy())
                ),
            )?;
            if existing.split_whitespace().next() != Some(hash.as_str()) {
                copy(p, &local, &remote)?;
            }
            run(
                p,
                &format!(
                    "chmod 700 {}; test \"$(sha256sum {} | cut -d' ' -f1)\" = {}",
                    quote(&remote.to_string_lossy()),
                    quote(&remote.to_string_lossy()),
                    quote(&hash)
                ),
            )?;
        }
    }
    Ok(())
}

#[derive(Debug)]
/// Periodic remote process isolation checks retained for one measured run.
pub struct Audit {
    placement: Placement,
    directory: PathBuf,
    child: Child,
    log: PathBuf,
    output: PathBuf,
    finished: bool,
}
impl Audit {
    /// Start remote isolation monitors and retain their evidence under the artifact directory.
    pub fn start(p: &Placement, implementation: &str, artifacts: &Path) -> Result<Self> {
        let directory = p
            .storage_dir
            .as_ref()
            .ok_or("missing storage")?
            .join(format!("audit-{}", super::run_id()?));
        let output = artifacts.join("remote-audit.json");
        let log = artifacts.join("remote-audit.stderr");
        let script = format!(
            "exec taskset -c 5 {} watch --implementation {} --directory {}",
            quote(&remote_root(p)?.join("ozzy_host").to_string_lossy()),
            quote(implementation),
            quote(&directory.to_string_lossy())
        );
        let child = command(p, &script)
            .stdin(Stdio::null())
            .stdout(fs::File::create(&output)?)
            .stderr(fs::File::create(&log)?)
            .spawn()?;
        let mut audit = Self {
            placement: p.clone(),
            directory,
            child,
            log,
            output,
            finished: false,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            audit.check()?;
            if run(
                p,
                &format!(
                    "test -f {} && echo ready || true",
                    quote(&audit.directory.join("ready").to_string_lossy())
                ),
            )? == "ready"
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err("remote audit readiness expired".into());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Ok(audit)
    }
    /// Fail when a remote isolation monitor reports interference or exits unexpectedly.
    pub fn check(&mut self) -> Result<()> {
        let log = fs::read_to_string(&self.log)?;
        if !log.is_empty() {
            return Err(format!("remote audit: {log}").into());
        }
        if self.child.try_wait()?.is_some() {
            return Err("remote audit exited early".into());
        }
        Ok(())
    }
    /// Stop remote monitors and collect their final isolation evidence.
    pub fn finish(mut self) -> Result<Value> {
        self.check()?;
        run(
            &self.placement,
            &format!(
                "touch {}",
                quote(&self.directory.join("stop").to_string_lossy())
            ),
        )?;
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait()? {
                if !status.success() {
                    return Err("remote audit failed".into());
                }
                break;
            }
            if Instant::now() >= deadline {
                return Err("remote audit stop expired".into());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        if fs::metadata(&self.log)?.len() != 0 {
            return Err("remote audit diagnostics".into());
        }
        self.finished = true;
        let row = super::read_json(&self.output)?;
        run(
            &self.placement,
            &format!("rm -r -- {}", quote(&self.directory.to_string_lossy())),
        )?;
        Ok(row)
    }
}
impl Drop for Audit {
    fn drop(&mut self) {
        if !self.finished {
            let _ = run(
                &self.placement,
                &format!(
                    "touch {}",
                    quote(&self.directory.join("stop").to_string_lossy())
                ),
            );
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Copy a source artifact into the run directory for immutable provenance.
pub fn freeze(path: &Path, directory: &Path) -> Result<PathBuf> {
    let placements = Placement::load(Some(path))?;
    let rows = placements
        .iter()
        .map(|p| {
            let mut row = json!({"bind":p.bind,"storage_dir":p.storage_dir,"cpus":p.cpus});
            if let Some((host, exe)) = &p.remote {
                row["ssh"] = json!(host);
                row["executable"] = json!(exe);
            }
            row
        })
        .collect::<Vec<_>>();
    let destination = directory.join("placements.json");
    json_file(&destination, &json!(rows))?;
    Ok(destination)
}

/// Collect host and device evidence for all three placements.
pub fn environment(placements: &[Placement; 3]) -> Result<Value> {
    let mut devices = std::collections::BTreeSet::new();
    let distributed = placements.iter().any(|p| p.remote.is_some());
    let mut rows = vec![];
    for placement in placements {
        let value = inspect(placement, None)?;
        let host = value["host"].as_str().ok_or("missing hostname")?;
        let device = value["storage"]["device"]
            .as_u64()
            .ok_or("missing storage device")?;
        let distinct = devices.insert((host.to_owned(), device));
        if value["storage"]["filesystem_type"] != 0x5846_5342u64 || (distributed && !distinct) {
            return Err("distributed comparison needs three distinct XFS devices".into());
        }
        let cpu = value["cpuinfo"]
            .as_str()
            .ok_or("missing CPU information")?
            .lines()
            .filter(|line| {
                line.split_once(':').is_some_and(|(key, _)| {
                    ["processor", "model name", "cpu cores", "siblings", "flags"]
                        .contains(&key.trim())
                })
            })
            .collect::<Vec<_>>();
        rows.push(json!({"host":host,"storage":value["storage"],"cpu":cpu,"memory":value["memory"].as_str().and_then(|m|m.lines().find(|line|line.starts_with("MemTotal:")))}));
    }
    Ok(json!(rows))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shell_arguments_remain_literal() {
        let input = "a'b $(false) `false`\n space";
        let local = Placement::load(None).unwrap();
        assert_eq!(
            run(&local[0], &format!("printf %s {}", quote(input))).unwrap(),
            input
        );
    }
}

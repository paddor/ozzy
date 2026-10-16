//! The serial case matrix: a fresh, isolated server per measured case.
use super::Args;
use crate::automation::{
    Result, artifact_root, cache, capture, check_canceled, distributed, isolation, json_field,
    json_file, records, root, server::External, source, supervise, validation,
};
use crate::placement::Placement;
use serde_json::{Value, json};
use std::{fs, path::Path, process::Command};

/// A ramp case becomes one ledger row per stage, keyed by its offered rate.
/// Stages the ramp could not sustain keep only their failure.
fn stage_rows(row: Value) -> Result<Vec<Value>> {
    let Some(stages) = row["measurements"].get("stages").cloned() else {
        return Ok(vec![row]);
    };
    stages
        .as_array()
        .ok_or("invalid ramp stages")?
        .iter()
        .map(|stage| {
            let mut row = row.clone();
            row["case"]["rate"] = stage["rate"].clone();
            if let Some(reason) = stage.get("failure") {
                row["failure"] = reason.clone();
                row["measurements"] = json!({});
            } else {
                row["measurements"] = stage["measurements"].clone();
            }
            Ok(row)
        })
        .collect()
}

/// What every case of one comparison run shares.
struct Matrix<'a> {
    args: &'a Args,
    binary: &'a Path,
    manifest: &'a Value,
    directory: &'a Path,
    placements: Option<[Placement; 3]>,
    id: &'a str,
}

/// Run every case once per repetition, reversing the order on even ones.
pub(super) async fn matrix(
    args: &Args,
    binary: &Path,
    manifest: &Value,
    directory: &Path,
) -> Result<Vec<Value>> {
    let cases = args.cases();
    let matrix = Matrix {
        args,
        binary,
        manifest,
        directory,
        placements: args
            .placements
            .as_deref()
            .map(|path| Placement::load(Some(path)))
            .transpose()?,
        id: json_field(manifest, "run_id", Value::as_str)?,
    };
    println!(
        "RUN {}: {} cases; results {}",
        matrix.id,
        cases.len() * args.repetitions as usize,
        directory.display()
    );
    let mut rows = vec![];
    for repetition in 1..=args.repetitions {
        let mut order: Vec<_> = (0..cases.len()).collect();
        if repetition % 2 == 0 {
            order.reverse();
        }
        for (index, case_index) in order.into_iter().enumerate() {
            rows.extend(matrix.case(repetition, index, &cases[case_index]).await?);
        }
    }
    source::require_unchanged(&root(), &manifest["fixture_source"])?;
    Ok(rows)
}

impl Matrix<'_> {
    /// Measure one case against its prestarted external `server`, if any.
    async fn measure(
        &self,
        case: &Value,
        directory: &Path,
        server: Option<&External>,
    ) -> Result<Value> {
        let implementation = json_field(case, "impl", Value::as_str)?;
        if let Some(server) = server {
            json_file(
                &directory.join("server-affinity-before.json"),
                &server.affinity(false)?,
            )?;
        }
        let cmd =
            self.args
                .case_command(self.binary, case, directory, server.map(External::endpoint))?;
        json_file(&directory.join("command.json"), &json!(cmd))?;
        let pids = server.map_or_else(Vec::new, External::pids);
        let storage = crate::automation::device::Snapshot::take(&artifact_root())?;
        let before = supervise::CpuSnapshot::take_servers(&pids)?;
        let mut guard = isolation::Guard::new(implementation, None).with_servers(&pids);
        if let Some(placements) = &self.placements
            && let Some(remote) = placements.iter().find(|p| p.remote.is_some())
        {
            guard.set_remote(distributed::Audit::start(
                remote,
                implementation,
                directory,
                self.args.budget().lifetime(),
            )?);
        }
        let raw = supervise::execute(
            &cmd,
            directory,
            self.args.budget().lifetime(),
            &std::collections::BTreeMap::new(),
            supervise::Monitors {
                server: server.map(External::monitor),
                isolation: Some(&mut guard),
            },
        )
        .await?;
        let remote_audit = guard.finish_remote()?;
        let storage = storage.finish()?;
        let cpu = before.elapsed(&supervise::CpuSnapshot::take_servers(&pids)?)?;
        if let Some(server) = server {
            json_file(
                &directory.join("server-affinity-after.json"),
                &server.affinity(false)?,
            )?;
        }
        Ok(
            json!({"case":case,"measurements":validation::comparison(case,&raw,&self.args.case_configuration(case)?)?,"execution_cpu":cpu,"device_io":storage,"raw":raw,"process_isolation":guard.report()?,"remote_process_isolation":remote_audit}),
        )
    }

    /// Measure one case on a fresh server and publish its ledger rows.
    async fn case(&self, repetition: u64, index: usize, selected: &Value) -> Result<Vec<Value>> {
        let settle = self.quiesce().await?;
        let implementation = json_field(selected, "impl", Value::as_str)?;
        let mode = json_field(selected, "mode", Value::as_str)?;
        let name = format!(
            "r{repetition}-{index}-{implementation}-{mode}-{}-{}",
            selected["size"],
            json_field(selected, "codec", Value::as_str)?
        );
        let case_dir = self.directory.join(&name);
        fs::create_dir(&case_dir)?;
        let server_dir = self
            .directory
            .join(format!("{implementation}-{repetition}-{index}-{}", self.id));
        let mut server = self.start_server(implementation, mode, &server_dir)?;
        self.sync_storage()?;
        println!("CASE {name}");
        let measured = self.measure(selected, &case_dir, server.as_ref()).await;
        let identity = server.as_ref().map(External::identity).transpose()?;
        let stopped = server.as_mut().map(External::stop).transpose();
        let mut row = measured?;
        stopped?;
        let after = self.verify_after(&mut row)?;
        self.annotate(&mut row, repetition, selected, &case_dir)?;
        row["device_settle"] = settle;
        row["server_identity"] = identity.unwrap_or(Value::Null);
        row["server_manifest"] = server
            .as_ref()
            .map_or(Value::Null, |s| json!(s.root().join("inspect.json")));
        row["process_isolation"]["after_shutdown"] = after;
        publish(row, implementation)
    }

    /// Before a case: no cancellation, an unchanged fixture and executable,
    /// idle hosts, and settled, synced storage.
    async fn quiesce(&self) -> Result<Value> {
        check_canceled()?;
        source::require_unchanged(&root(), &self.manifest["fixture_source"])?;
        if source::sha256(self.binary)? != self.manifest["executable_sha256"] {
            return Err("benchmark executable changed".into());
        }
        isolation::require_idle()?;
        if let Some(placements) = &self.placements {
            distributed::idle(placements)?;
        }
        capture(Command::new("sync").args(["-f"]).arg(artifact_root()))?;
        crate::automation::device::settle(&artifact_root()).await
    }

    /// A fresh external server for this case; Ozzy brokers run in the worker.
    fn start_server(
        &self,
        implementation: &str,
        mode: &str,
        root: &Path,
    ) -> Result<Option<External>> {
        if implementation == "ozzy" {
            return Ok(None);
        }
        if let Some(placements) = &self.placements {
            return Ok(Some(External::distributed(
                root,
                placements,
                self.args.budget().serving(),
            )?));
        }
        let brokers = if crate::automation::cluster_mode(mode) {
            3
        } else {
            1
        };
        Ok(Some(External::start(
            implementation,
            root,
            mode,
            &self.args.broker_cpus.brokers(brokers),
        )?))
    }

    /// Sync local and every placement's storage before measuring.
    fn sync_storage(&self) -> Result<()> {
        capture(Command::new("sync").args(["-f"]).arg(artifact_root()))?;
        for placement in self.placements.iter().flatten() {
            let storage = placement
                .storage_dir
                .as_ref()
                .ok_or("missing placement storage")?;
            distributed::run(
                placement,
                &format!("sync -f {}", distributed::quote(&storage.to_string_lossy())),
            )?;
        }
        Ok(())
    }

    /// After a case: idle hosts and the same fixture and executable. Returns
    /// the local isolation proof.
    fn verify_after(&self, row: &mut Value) -> Result<Value> {
        let after = isolation::require_idle()?;
        if let Some(placements) = &self.placements {
            row["remote_after_shutdown"] = distributed::idle(placements)?;
        }
        source::require_unchanged(&root(), &self.manifest["fixture_source"])?;
        if source::sha256(self.binary)? != self.manifest["executable_sha256"] {
            return Err("benchmark executable changed during measurement".into());
        }
        Ok(after)
    }

    /// The run provenance every measurement row carries.
    fn annotate(
        &self,
        row: &mut Value,
        repetition: u64,
        selected: &Value,
        case_dir: &Path,
    ) -> Result<()> {
        for key in [
            "source",
            "fixture_source",
            "executable_sha256",
            "host",
            "workload_sha256",
            "environment",
        ] {
            row[key] = self.manifest[key].clone();
        }
        row["kind"] = json!("measurement");
        row["run_id"] = json!(self.id);
        row["repetition"] = json!(repetition);
        row["configuration"] = self.args.case_configuration(selected)?;
        row["artifact_dir"] = json!(case_dir);
        row["manifest"] = json!(self.directory.join("manifest.json"));
        row["recorded_at"] = json!(capture(
            Command::new("date").args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        )?);
        Ok(())
    }
}

/// Append a case's ledger rows, one per ramp stage, and report each.
fn publish(row: Value, implementation: &str) -> Result<Vec<Value>> {
    let mut rows = vec![];
    for row in stage_rows(row)? {
        records::append(&cache()?, implementation, &row)?;
        println!(
            "VALID {}",
            json!({"case":row["case"],"measurements":row["measurements"],"failure":row.get("failure")})
        );
        rows.push(row);
    }
    Ok(rows)
}

//! Worker qualification and run provenance.
use super::{Args, Checks};
use crate::automation::{
    Result, artifact_root, capture, check_canceled, json_file, read_json, root, source,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

/// Qualify and build the worker, or reuse a stamped build with `--no-build`.
pub(super) fn build(args: &Args, directory: &Path) -> Result<PathBuf> {
    let binary = crate::automation::worker_binary();
    let stamp = crate::automation::artifacts().join("comparison-build.json");
    let identity = source::identity(&root())?;
    if args.no_build {
        reuse_build(&stamp, &binary, &identity)?;
        return Ok(binary);
    }
    for (index, command) in gate_commands(args.checks).iter().enumerate() {
        check_canceled()?;
        run_gate(command, &directory.join(format!("build-{index}.log")))?;
    }
    source::require_unchanged(&root(), &identity)?;
    json_file(
        &stamp,
        &json!({"source":identity,"focused_checks":args.checks == Checks::Focused,"worker_inputs":source::worker_identity(&root())?,
            "workload_sha256":source::workload_fingerprint(&root())?,
            "executable_sha256":source::sha256(&binary)?}),
    )?;
    Ok(binary)
}

/// Reuse the stamped worker only while its inputs and executable match.
/// Fill in the input fingerprints an older stamp lacks.
fn reuse_build(stamp: &Path, binary: &Path, identity: &Value) -> Result<()> {
    let saved = read_json(stamp)?;
    let inputs_match = if saved["worker_inputs"].is_null() {
        saved["source"] == *identity
    } else {
        saved["worker_inputs"] == source::worker_identity(&root())?
    };
    if !inputs_match || saved["executable_sha256"] != source::sha256(binary)? {
        return Err("build stamp differs; rerun without --no-build".into());
    }
    if saved["worker_inputs"].is_null() {
        let mut saved = saved;
        saved["worker_inputs"] = source::worker_identity(&root())?;
        saved["workload_sha256"] = json!(source::workload_fingerprint(&root())?);
        json_file(stamp, &saved)?;
    }
    Ok(())
}

/// Cargo commands that qualify the worker: format, lint, tests, and the
/// release build. Focused checks keep only format and build.
fn gate_commands(checks: Checks) -> Vec<Vec<&'static str>> {
    let mut commands = vec![
        vec!["fmt", "--all", "--", "--check"],
        vec![
            "clippy",
            "--locked",
            "-p",
            "ozzy-bench",
            "--features",
            "comparisons",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
        vec![
            "test",
            "--locked",
            "-p",
            "ozzy-bench",
            "--features",
            "comparisons",
            "--lib",
            "--bins",
            "--test",
            "ozzy_automation",
        ],
        vec![
            "build",
            "--locked",
            "--release",
            "-p",
            "ozzy-bench",
            "--features",
            "comparisons",
            "--bin",
            "ozzy_timed_bench",
            "--bin",
            "ozzy_host",
        ],
    ];
    if checks == Checks::Focused {
        commands.drain(1..3);
    }
    commands
}

/// Run one gate through the checkout's cargo wrapper, logging and echoing its
/// output. A failure or any warning fails the build.
fn run_gate(args: &[&str], log: &Path) -> Result<()> {
    println!("CHECK cargo {}", args.join(" "));
    let output = Command::new(root().join("scripts/ozzy_cargo"))
        .args(args)
        .current_dir(root())
        .env("CARGO_TARGET_DIR", crate::automation::build_target(&root()))
        .env("TMPDIR", artifact_root())
        .output()?;
    let text = [output.stdout, output.stderr].concat();
    fs::write(log, &text)?;
    print!("{}", String::from_utf8_lossy(&text));
    if !output.status.success() || String::from_utf8_lossy(&text).contains("warning:") {
        return Err("build/format/lint gate failed".into());
    }
    Ok(())
}

fn host() -> Result<Value> {
    let mut host = serde_json::Map::new();
    for (name, flag) in [
        ("system", "-s"),
        ("node", "-n"),
        ("release", "-r"),
        ("version", "-v"),
        ("machine", "-m"),
        ("processor", "-p"),
    ] {
        host.insert(
            name.into(),
            json!(capture(Command::new("uname").arg(flag))?),
        );
    }
    Ok(json!(host))
}

fn manifest(args: &Args, id: &str, binary: &Path) -> Result<Value> {
    let fixture_source = source::identity(&root())?;
    let build = read_json(&crate::automation::artifacts().join("comparison-build.json"))?;
    let source = &build["source"];
    let host = host()?;
    let cpu = fs::read_to_string("/proc/cpuinfo")?;
    let memory = fs::read_to_string("/proc/meminfo")?;
    let dependencies = source["checkouts"]
        .as_array()
        .ok_or("missing checkouts")?
        .iter()
        .filter(|c| c["root"] != root().to_string_lossy().as_ref())
        .cloned()
        .collect::<Vec<_>>();
    let cpu_lines = cpu
        .lines()
        .filter(|line| {
            line.split_once(':').is_some_and(|(name, _)| {
                matches!(
                    name.trim(),
                    "processor" | "model name" | "cpu cores" | "siblings" | "flags"
                )
            })
        })
        .collect::<Vec<_>>();
    Ok(
        json!({"run_id":id,"source":source,"focused_checks":build.get("focused_checks").and_then(Value::as_bool).unwrap_or(false),"fixture_source":fixture_source,"executable":binary,"executable_sha256":source::sha256(binary)?,"host":host,"cpuinfo":cpu,"memory":memory,"filesystem":capture(Command::new("findmnt").args(["-T"]).arg(artifact_root()))?,"arguments":args.configuration()?,"workload_sha256":build["workload_sha256"],"environment":{"host":host,"cpu":cpu_lines,"memory":memory.lines().find(|l|l.starts_with("MemTotal:")).ok_or("missing MemTotal")?,"filesystem":capture(Command::new("findmnt").args(["-n","-o","SOURCE,FSTYPE,OPTIONS","-T"]).arg(artifact_root()))?,"dependencies":dependencies}}),
    )
}

/// Deploy the run and return its manifest. Placed runs first install the
/// binaries on every host, require those hosts idle, and record the remote
/// deployment.
pub(super) fn deploy_run(args: &Args, id: &str, binary: &Path) -> Result<Value> {
    let mut manifest = manifest(args, id, binary)?;
    if let Some(path) = &args.placements {
        let placements = crate::placement::Placement::load(Some(path))?;
        crate::automation::distributed::deploy(
            &placements,
            binary,
            args.cases().iter().any(|c| c["impl"] == "iggy"),
        )?;
        crate::automation::distributed::idle(&placements)?;
        manifest["environment"]["deployment"] =
            crate::automation::distributed::environment(&placements)?;
    }
    Ok(manifest)
}

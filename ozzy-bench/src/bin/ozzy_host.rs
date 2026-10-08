//! Untimed host inspection and bounded isolation monitoring for remote fixtures.
#![forbid(unsafe_code)]
use clap::{Parser, Subcommand};
use ozzy_bench::automation::{Result, isolation};
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Action,
}
#[derive(Subcommand)]
enum Action {
    Inspect {
        #[arg(long)]
        pid: Option<u32>,
        #[arg(long)]
        storage: Option<PathBuf>,
    },
    Watch {
        #[arg(long)]
        implementation: String,
        #[arg(long)]
        directory: PathBuf,
    },
}
fn main() -> Result<()> {
    match Args::parse().command {
        Action::Inspect { pid, storage } => println!("{}", inspect(pid, storage)?),
        Action::Watch {
            implementation,
            directory,
        } => watch(&implementation, &directory)?,
    }
    Ok(())
}
fn inspect(pid: Option<u32>, storage: Option<PathBuf>) -> Result<Value> {
    let mut row = json!({"host":fs::read_to_string("/proc/sys/kernel/hostname")?.trim(),
        "cpuinfo":fs::read_to_string("/proc/cpuinfo")?, "memory":fs::read_to_string("/proc/meminfo")?,
        "processes":isolation::processes()?, "stat":fs::read_to_string("/proc/stat")?});
    if let Some(storage) = storage {
        row["storage"] = ozzy_bench::placement::storage(&storage)?;
    }
    if let Some(pid) = pid {
        let root = PathBuf::from(format!("/proc/{pid}"));
        let mut threads = vec![];
        for task in fs::read_dir(root.join("task"))? {
            let task = task?;
            let tid: u32 = task.file_name().to_str().ok_or("invalid task")?.parse()?;
            let value = (|| -> Result<Value> {
                Ok(json!({"tid":tid,
                "name":fs::read_to_string(task.path().join("comm"))?.trim(), "cpus":isolation::cpus(Some(tid))?}))
            })();
            match value {
                Ok(row) => threads.push(row),
                Err(_) if !task.path().exists() => (),
                Err(e) => return Err(e),
            }
        }
        if threads.is_empty() {
            return Err("worker has no threads".into());
        }
        row["execution"] = json!({"threads":threads});
        row["pid_stat"] = json!(fs::read_to_string(root.join("stat"))?);
        row["executable_sha256"] =
            json!(ozzy_bench::automation::source::sha256(&root.join("exe"))?);
    }
    Ok(row)
}
fn watch(implementation: &str, directory: &std::path::Path) -> Result<()> {
    if !matches!(implementation, "ozzy" | "iggy") {
        return Err("invalid remote implementation".into());
    }
    fs::create_dir(directory)?;
    let started = Instant::now();
    let mut samples = 0u64;
    let mut seen = std::collections::BTreeMap::new();
    loop {
        for row in isolation::processes()? {
            if row["executable"] == "ozzy_host" {
                continue;
            }
            let args = row["arguments"]
                .as_array()
                .ok_or("missing process arguments")?;
            let valid = match implementation {
                "ozzy" => {
                    row["executable"] == "ozzy_timed_bench"
                        && args.iter().any(|v| v == "--worker-index")
                }
                "iggy" => row["executable"] == "iggy-server",
                _ => false,
            };
            if !valid {
                return Err(format!("remote isolation violated: {row}").into());
            }
            seen.insert(
                (
                    row["pid"].as_u64().unwrap(),
                    row["started"].as_u64().unwrap(),
                ),
                row,
            );
        }
        if seen.len() > 1 {
            return Err("remote host ran more than its one assigned broker".into());
        }
        samples += 1;
        if samples == 1 {
            fs::write(directory.join("ready"), "ready")?;
        }
        if directory.join("stop").exists() {
            println!(
                "{}",
                json!({"samples":samples,"processes":seen.values().collect::<Vec<_>>(),"elapsed_seconds":started.elapsed().as_secs_f64(),"poll_seconds":0.1})
            );
            return Ok(());
        }
        if started.elapsed() > Duration::from_secs(180) {
            return Err("remote audit deadline expired".into());
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

//! Continuous, deadline-driven native workloads. Brokers never reset between
//! warmup and measurement. The retained-history budget is a failure limit,
//! not a record count that can end or silently throttle a successful run.

mod config;
#[cfg(feature = "comparisons")]
pub(super) mod external;
mod measurement;
pub(super) mod native;
mod pacing;
mod producer;
mod reader;
mod results;

use super::{
    Args, Result, error, metrics,
    processes::launch::{self, Placement, Process},
};
use config::Config;
use futures::future::try_join_all;
use measurement::Window;
use ozzy_proto::GroupId;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::sync::mpsc;

pub(super) async fn run(args: Args) -> Result<()> {
    let config = configuration(args)?;
    #[cfg(feature = "comparisons")]
    if config.args.external_system.is_some() {
        external::validate(&config)?;
        external::diagnostics()?;
        if config.args.producer_worker.is_some() || config.args.reader_worker.is_some() {
            return external::worker(config).await;
        }
    }
    if config.args.worker_index.is_some() {
        return native::broker::run(config).await;
    }
    if config.args.producer_worker.is_some() {
        return producer::run(config).await;
    }
    if config.args.reader_worker.is_some() {
        return reader::run(config).await;
    }
    run_processes(config).await
}

async fn run_processes(config: Config) -> Result<()> {
    config.preflight_memory()?;
    let provenance = ozzy_bench::provenance::capture()?;
    let directory = tempfile::Builder::new().prefix("ram-timed-").tempdir()?;
    let group = GroupId::new();
    let placements = Placement::load(config.args.placements.as_deref())?;
    let Workers {
        mut brokers,
        mut writers,
        mut readers,
    } = spawn(&config, &placements, group, directory.path()).await?;
    let monitors: Vec<_> = brokers
        .iter()
        .chain(&writers)
        .chain(&readers)
        .map(Process::monitor)
        .collect();
    let watchdog = Duration::from_secs_f64(
        65.0 + config.args.warmup
            + config.args.duration.unwrap()
            + config.args.drain_timeout_secs as f64,
    );
    let result = tokio::select! {
        result = tokio::time::timeout(watchdog, async {
            #[cfg(feature = "comparisons")]
            if config.args.external_system.is_some() {
                return external::coordinate(&config, &mut writers, &mut readers).await;
            }
            let mut prepared = native::Prepared::new(&config, &mut brokers, directory.path()).await?;
            native::coordinate(&config, &mut prepared, &mut brokers, &mut writers, &mut readers).await
        }) => match result {
                Ok(result) => result,
                Err(error) => Err(error.into()),
            },
        diagnostic = launch::watch_diagnostics(&monitors) => Err(diagnostic),
    };
    let row = match result {
        Ok(row) => row,
        Err(error) => {
            // Close control and observe worker exit before Drop's kill fallback.
            // Prepared storage and port reservations belong to those workers.
            futures::future::join_all(
                brokers
                    .iter_mut()
                    .chain(&mut writers)
                    .chain(&mut readers)
                    .map(Process::stop),
            )
            .await;
            let path = directory.keep();
            return Err(super::error(format!(
                "{error}; failed worker artifacts: {}",
                path.display()
            )));
        }
    };
    finish(
        &config,
        row,
        provenance,
        directory,
        Workers {
            brokers,
            writers,
            readers,
        },
    )
    .await
}

async fn finish(
    config: &Config,
    mut row: Value,
    provenance: Value,
    directory: tempfile::TempDir,
    mut workers: Workers,
) -> Result<()> {
    for child in workers
        .writers
        .iter_mut()
        .chain(&mut workers.readers)
        .chain(&mut workers.brokers)
    {
        child.stop().await?;
    }
    #[cfg(feature = "comparisons")]
    if config.args.external_system.is_some() {
        external::cleanup(
            config,
            row["external_topic"].as_str().expect("run-scoped topic"),
        )
        .await?;
    }
    #[cfg(not(feature = "comparisons"))]
    let _ = config;
    row["provenance"] = provenance;
    row["client_logs"] = json!(preserve_logs([
        &workers.writers,
        &workers.readers,
        &workers.brokers
    ])?);
    if std::env::var_os("OZZY_BENCH_HEAPTRACK").is_some() {
        row["heaptrack_artifacts"] = json!(directory.keep());
    }
    println!("{row}");
    Ok(())
}

struct Workers {
    brokers: Vec<Process>,
    writers: Vec<Process>,
    readers: Vec<Process>,
}

async fn spawn(
    config: &Config,
    placements: &[Placement],
    group: GroupId,
    directory: &std::path::Path,
) -> Result<Workers> {
    let mut brokers = Vec::new();
    for (index, placement) in placements.iter().take(broker_processes(config)).enumerate() {
        brokers.push(Process::spawn(&config.args, index, group, placement, directory).await?);
    }
    let mut writers = Vec::new();
    for index in 0..config.workers {
        writers.push(Process::spawn_timed(&config.args, index, group, false, directory).await?);
    }
    let mut readers = Vec::new();
    for index in 0..config.args.reader_workers() {
        readers.push(Process::spawn_timed(&config.args, index, group, true, directory).await?);
    }
    Ok(Workers {
        brokers,
        writers,
        readers,
    })
}

fn configuration(args: Args) -> Result<Config> {
    if std::env::var_os("OZZY_BENCH_AFFINITY").is_some() {
        return Err(error("timed runner no longer supports affinity"));
    }
    if std::env::var("OZZY_BENCH_PROFILE").as_deref() == Ok("stages") {
        ozzy_runtime::profiling::enable();
    }
    #[cfg(feature = "comparisons")]
    if args.external_system.is_some() {
        return Config::new(args);
    }
    Config::production(args)
}

fn preserve_logs(children: [&[Process]; 3]) -> Result<Option<std::path::PathBuf>> {
    let logs: Vec<_> = children
        .into_iter()
        .flatten()
        .filter(|child| std::fs::metadata(&child.errors).is_ok_and(|meta| meta.len() > 0))
        .collect();
    if !logs.is_empty() {
        let cache = ozzy_bench::automation::cache().join("client-logs");
        std::fs::create_dir_all(&cache)?;
        let saved = tempfile::Builder::new()
            .prefix("timed-")
            .tempdir_in(cache)?;
        for child in logs {
            std::fs::copy(
                &child.errors,
                saved.path().join(child.errors.file_name().unwrap()),
            )?;
        }
        return Ok(Some(saved.keep()));
    }
    Ok(None)
}

fn broker_processes(config: &Config) -> usize {
    #[cfg(feature = "comparisons")]
    if config.args.external_system.is_some() {
        return 0;
    }
    config.args.system.brokers()
}

async fn measure(
    config: &Config,
    brokers: &mut [Process],
    writers: &mut [Process],
    readers: &mut [Process],
) -> Result<(Window, Vec<Value>, Vec<Value>)> {
    // Brokers only meter their local process. Acknowledge that before creating
    // the client window; never compare monotonic clocks from different hosts.
    send_all(brokers, &json!({"command":"start"}))?;
    receive_all(brokers, "started").await?;
    let window = Window::new(metrics::monotonic_ns() + 200_000_000, &config.args)?;
    let start = json!({"command":"start","at_ns":window.start});
    send_all(readers, &start)?;
    send_all(writers, &start)?;
    // Workers enforce the actual drain deadline. Allow bounded report encoding
    // and transfer afterward, without extending the measured or drained cohort.
    let reports_by = window.deadline(&config.args) + Duration::from_secs(5);
    let writer_rows = try_join_all(
        writers
            .iter_mut()
            .map(|w| w.receive_until("reported", reports_by)),
    )
    .await?;
    validate_reports(&writer_rows, writers)?;
    let totals = results::totals(&writer_rows, config.writers)?;
    send_all(readers, &json!({"command":"finish","counts":totals}))?;
    let reader_rows = try_join_all(
        readers
            .iter_mut()
            .map(|r| r.receive_until("reported", reports_by)),
    )
    .await?;
    validate_reports(&reader_rows, readers)?;
    for copy in 0..config.args.readers_per_partition {
        let rows = results::reader_copy(&reader_rows, copy)?;
        if totals != results::totals(&rows, config.writers)? {
            return Err(error(
                "reader/writer final identities do not cover the same prefix",
            ));
        }
        results::verify_digests(&writer_rows, &rows, config.writers)?;
    }
    Ok((window, writer_rows, reader_rows))
}

fn send_all(children: &mut [Process], message: &Value) -> Result<()> {
    for child in children {
        child.send(message)?;
    }
    Ok(())
}
async fn receive_all(children: &mut [Process], event: &str) -> Result<Vec<Value>> {
    try_join_all(children.iter_mut().map(|child| child.receive(event))).await
}
async fn command(input: &mut mpsc::Receiver<Result<Value>>, expected: &str) -> Result<Value> {
    command_with_timeout(input, expected, Duration::from_secs(60)).await
}

async fn command_with_timeout(
    input: &mut mpsc::Receiver<Result<Value>>,
    expected: &str,
    timeout: Duration,
) -> Result<Value> {
    let value = tokio::time::timeout(timeout, input.recv())
        .await
        .map_err(|cause| error(format!("waiting for {expected} command: {cause}")))?
        .ok_or_else(|| error("coordinator closed control"))??;
    if value["command"] != expected {
        return Err(error(format!("expected {expected} command")));
    }
    Ok(value)
}
fn window(value: &Value, args: &Args) -> Result<Window> {
    let start = value["at_ns"]
        .as_u64()
        .ok_or_else(|| error("missing start clock"))?;
    let now = metrics::monotonic_ns();
    if start < now || start > now + 2_000_000_000 {
        return Err(error("late or invalid timed start"));
    }
    Window::new(start, args)
}
fn report(_config: &Config, value: &Value) -> Result<()> {
    let mut value = value.clone();
    value["event"] = json!("reported");
    launch::reply(&value)
}

fn validate_reports(rows: &[Value], children: &[Process]) -> Result<()> {
    for (index, (row, child)) in rows.iter().zip(children).enumerate() {
        if row["pid"] != child.id() || row["index"] != index {
            return Err(error("invalid timed worker identity"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn cli_accepts_only_production_ozzy_modes() {
        for (system, supported) in [
            ("single-volatile", false),
            ("single-buffered", false),
            ("single-durable", true),
            ("disk-quorum", true),
            ("replicated-persisting", true),
        ] {
            let parsed = Args::try_parse_from([
                "bench",
                "--system",
                system,
                "--processes",
                "--network-ingress",
                "--streaming",
                "--duration",
                "1",
            ]);
            if supported {
                assert!(configuration(parsed.unwrap()).is_ok(), "{system}");
            } else {
                assert!(parsed.is_err(), "{system}");
            }
        }
    }
}

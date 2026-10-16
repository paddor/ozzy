use futures::future::try_join_all;
use ozzy_proto::GroupId;
use serde_json::Value;
use tokio::sync::watch;

use super::super::{Result, error, metrics, workload};
use super::{Config, Window, await_connect, command, config, launch, report};

mod groups;
pub(in crate::bench::timed) mod native;
pub(in crate::bench::timed) mod oracle;

pub(super) async fn run(config: Config) -> Result<()> {
    let index = config
        .args
        .reader_worker
        .ok_or_else(|| error("missing reader worker"))?;
    let mut input = launch::input()?;
    let connect = await_connect(&mut input).await?;
    let setup = connect
        .get("native")
        .ok_or_else(|| error("missing production reader setup"))?;
    if config.args.native.groups != 0 {
        groups::run(config, index, setup, input).await
    } else {
        native::run(config, index, setup, input).await
    }
}

async fn final_counts(
    input: &mut tokio::sync::mpsc::Receiver<Result<Value>>,
    finish: watch::Sender<Option<Vec<u64>>>,
    config: &Config,
    window: Window,
) -> Result<()> {
    // Control never shortens the configured workload and final-drain deadline.
    let message = tokio::time::timeout_at(window.deadline(&config.args), input.recv())
        .await
        .map_err(|cause| error(format!("reader waiting for final writer counts: {cause}")))?
        .ok_or_else(|| error("reader control closed"))??;
    if message["command"] != "finish" {
        return Err(error("expected final writer counts"));
    }
    let counts = message["counts"]
        .as_array()
        .ok_or_else(|| error("missing final writer counts"))?
        .iter()
        .map(|n| n.as_u64().ok_or_else(|| error("invalid writer count")))
        .collect::<Result<Vec<_>>>()?;
    if counts.len() != config.logical_writers() {
        return Err(error("invalid writer count length"));
    }
    finish
        .send(Some(counts))
        .map_err(|_| error("readers exited before final counts"))?;
    Ok(())
}

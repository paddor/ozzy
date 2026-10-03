//! Coordinate production workers without legacy topic-wide broker snapshots.

use super::{Config, Prepared, Result, error};
use crate::bench::timed::{launch::Process, measure, receive_all, results, send_all};
use serde_json::{Value, json};

pub(in crate::bench::timed) async fn coordinate(
    config: &Config,
    prepared: &mut Prepared,
    brokers: &mut [Process],
    writers: &mut [Process],
    readers: &mut [Process],
) -> Result<Value> {
    send_all(brokers, &prepared.initialize()?)?;
    let initialized = receive_all(brokers, "initialized").await?;
    let configuration = ozzy_bench::provenance::digest(&prepared.artifact.configuration)?;
    let identity = ozzy_bench::provenance::digest(&prepared.artifact.identity)?;
    for (index, row) in initialized.iter().enumerate() {
        if row["index"] != index
            || row["configuration_xxh3_128"] != configuration
            || row["identity_xxh3_128"] != identity
        {
            return Err(error("production broker initialization identity mismatch"));
        }
    }
    send_all(brokers, &json!({"command": "serve"}))?;
    let ready = receive_all(brokers, "ready").await?;
    verify_ready(config, prepared, brokers, &ready)?;
    let clients = config
        .native
        .as_ref()
        .and_then(|settings| settings.clients.as_ref())
        .ok_or_else(|| error("missing production SDK reservations"))?;
    if writers.len() != clients.writers.len() || readers.len() != clients.readers.len() {
        return Err(error(
            "production SDK worker count differs from reservations",
        ));
    }
    for (worker, profile) in writers
        .iter_mut()
        .zip(&clients.writers)
        .chain(readers.iter_mut().zip(&clients.readers))
    {
        worker.send(&prepared.connect(profile)?)?;
    }
    receive_all(writers, "connected").await?;
    receive_all(readers, "connected").await?;
    let (window, writer_rows, reader_rows) = measure(config, brokers, writers, readers).await?;
    send_all(brokers, &json!({"command": "drain"}))?;
    let states = receive_all(brokers, "drained").await?;
    for (worker, state) in brokers.iter().zip(&states) {
        if let Some(placement) = &worker.placement {
            placement.verify_execution(&state["usage"]["execution"])?;
        }
    }
    let mut row = results::summarize(config, window, writer_rows, reader_rows, &states)?;
    row["effective_deployment"] = prepared.configuration.clone();
    row["sdk_reservations"] = json!({"writers": clients.writers, "readers": clients.readers,
        "scope": "per SDK process; includes unused partition writers and reply aliases"});
    Ok(row)
}

fn verify_ready(
    config: &Config,
    prepared: &Prepared,
    brokers: &[Process],
    ready: &[Value],
) -> Result<()> {
    let digest = ozzy_bench::provenance::digest(&std::env::current_exe()?)?;
    if ready.len() != prepared.brokers.len() || brokers.len() != ready.len() {
        return Err(error("production broker count differs from deployment"));
    }
    for (index, ((child, row), broker)) in
        brokers.iter().zip(ready).zip(&prepared.brokers).enumerate()
    {
        if (!child.remote && row["pid"] != child.id())
            || row["pid"].as_u64().is_none_or(|pid| pid == 0)
            || row["index"] != index
            || row["broker"] != broker.name
            || row["node"] != prepared.identity.brokers[&broker.name].to_string()
            || row["endpoint"] != broker.endpoints.peer
            || row["reader_publication"] != broker.endpoints.reader_pub
            || row["follower_publication"] != json!(broker.endpoints.follower_pub)
            || row["executable_xxh3_128"] != digest
            || row["topology"]["application_threads"] != config.args.app_threads
            || row["topology"]["dispatcher_threads"] != 1
            || row["topology"]["partitions"]
                .as_array()
                .is_none_or(|partitions| {
                    partitions.len() != config.native.as_ref().unwrap().partitions
                })
        {
            return Err(error(
                "production broker identity, build, or topology mismatch",
            ));
        }
        if let Some(placement) = &child.placement {
            placement.verify_execution(&row["execution"])?;
        }
        if row["storage"]
            .as_array()
            .is_none_or(|devices| devices.len() != 1 || devices[0]["root"] != json!(broker.root))
        {
            return Err(error("production broker storage differs from deployment"));
        }
    }
    Ok(())
}

//! Process shutdown observes the same production drain used by embedded brokers.

use ozzy_broker::{Broker, CheckedConfig, RecoverySelection, StartupError};
use ozzy_config::BrokerIdentity;

pub(super) async fn run(
    checked: CheckedConfig,
    local: BrokerIdentity,
    selections: &[RecoverySelection],
) -> Result<(), StartupError> {
    // Register before startup so a signal arriving while journals open remains
    // observable. Shutdown still waits for established I/O ownership to settle.
    let stopping = stop_signal().map_err(failure)?;
    let name = checked.plan.name.clone();
    let broker = if selections.is_empty() {
        Broker::start_trusted(checked, local).await?
    } else {
        Broker::start_recovering_trusted(checked, local, selections).await?
    };
    println!("Serving broker {name}.");
    let stopped = tokio::select! {
        result = broker.closed() => return result,
        result = stopping => result.map_err(failure),
    };
    let drained = broker.shutdown().await;
    stopped.and(drained)
}

#[cfg(unix)]
fn stop_signal() -> std::io::Result<impl Future<Output = std::io::Result<()>>> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    Ok(async move {
        let observed = tokio::select! {
            observed = interrupt.recv() => observed,
            observed = terminate.recv() => observed,
        };
        observed.ok_or_else(|| std::io::Error::other("shutdown signal stream ended"))
    })
}

#[cfg(not(unix))]
fn stop_signal() -> std::io::Result<impl Future<Output = std::io::Result<()>>> {
    Ok(tokio::signal::ctrl_c())
}

fn failure(error: impl std::fmt::Display) -> StartupError {
    StartupError::Runtime(error.to_string())
}

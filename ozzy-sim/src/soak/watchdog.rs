//! Long maintenance bursts must verify records, not merely execute disk jobs.
use super::Event;
use futures::FutureExt;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

pub(super) async fn run(
    deadline: Duration,
    event: Event,
    verified: Arc<AtomicU64>,
    operation: impl Future<Output = ()>,
) -> Option<String> {
    let mut operation = std::pin::pin!(crate::client::progress_timeout(
        deadline,
        verified.clone(),
        std::panic::AssertUnwindSafe(operation).catch_unwind(),
    ));
    let started = Instant::now();
    let initial = verified.load(Ordering::Relaxed);
    let mut tick =
        tokio::time::interval_at(started + Duration::from_secs(60), Duration::from_secs(60));
    loop {
        tokio::select! {
            result = &mut operation => return match result {
                Ok(result) => result.err().map(|panic| {
                    panic.downcast_ref::<String>().cloned()
                    .or_else(|| panic.downcast_ref::<&str>().map(|value| (*value).to_owned()))
                    .unwrap_or_else(|| "simulation panicked".into())
                }),
                Err(_) => Some(format!("progress deadline at {event:?}; verified {} records in this boundary", verified.load(Ordering::Relaxed) - initial)),
            },
            _ = tick.tick() => {
                let now = Instant::now();
                let count = verified.load(Ordering::Relaxed);
                println!("{}", serde_json::json!({
                        "event": "boundary-progress", "wave": event.wave,
                        "action": event.action, "elapsed_secs": now.duration_since(started).as_secs(),
                        "verified_records": count - initial,
                    }));
            },
        }
    }
}

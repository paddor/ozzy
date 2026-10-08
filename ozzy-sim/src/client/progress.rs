//! The same verified-delivery deadline for memory and cross-host soaks.
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

/// No independent delivery verification arrived before the configured deadline.
#[derive(Debug, thiserror::Error)]
#[error("no verified delivery within the progress deadline")]
pub struct ProgressTimeout;

/// Bound time without independent record verification. Checking disk activity
/// alone would let a busy loop hide a stalled SDK. The observation tick is at
/// most 250 ms; the enclosing soak separately bounds its total wall duration.
pub async fn progress_timeout<T>(
    deadline: Duration,
    verified: Arc<AtomicU64>,
    operation: impl Future<Output = T>,
) -> Result<T, ProgressTimeout> {
    assert!(!deadline.is_zero());
    let mut operation = std::pin::pin!(operation);
    let mut last_progress = Instant::now();
    let mut observed = verified.load(Ordering::Relaxed);
    let mut tick = tokio::time::interval(deadline.min(Duration::from_millis(250)));
    loop {
        tokio::select! {
            result = &mut operation => return Ok(result),
            _ = tick.tick() => {
                let now = Instant::now();
                let count = verified.load(Ordering::Relaxed);
                if count != observed {
                    observed = count;
                    last_progress = now;
                } else if now.duration_since(last_progress) >= deadline {
                    return Err(ProgressTimeout);
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn verified_delivery_keeps_a_long_retention_burst_alive() {
        let progress = Arc::new(AtomicU64::new(0));
        let publisher = progress.clone();
        let operation = async move {
            for _ in 0..6 {
                tokio::time::sleep(Duration::from_secs(10)).await;
                publisher.fetch_add(1, Ordering::Relaxed);
            }
        };
        assert!(
            progress_timeout(Duration::from_secs(30), progress, operation)
                .await
                .is_ok()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn no_verified_delivery_reaches_the_progress_deadline() {
        let started = Instant::now();
        let result = progress_timeout(
            Duration::from_secs(30),
            Arc::default(),
            std::future::pending::<()>(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(started.elapsed(), Duration::from_secs(30));
    }
}

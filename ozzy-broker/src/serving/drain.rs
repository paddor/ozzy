//! Device ownership survives canceled startup and shutdown observations.

use crate::{DevicePools, Shutdown, StartupError, shards::lifecycle::State};
use std::{
    panic::AssertUnwindSafe,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

/// Device lifetime owned by a broker. Called once on the drain thread after
/// application and frontend owners stop. Wait for physical jobs and workers.
pub trait StorageOwner: std::fmt::Debug + Send + Sync + 'static {
    /// Wait for accepted physical work and stop backend workers after owners exit.
    fn drain(&self);
}

impl StorageOwner for DevicePools {
    fn drain(&self) {
        futures::executor::block_on(self.shutdown());
        self.join();
    }
}

#[derive(Debug)]
pub(super) struct DeviceDrain(pub(super) Arc<Drain>);

#[derive(Debug)]
pub(super) struct Drain {
    pub(super) application: Arc<State>,
    pub(super) frontend: Arc<State>,
    pools: Box<dyn StorageOwner>,
    started: AtomicBool,
    finished: Shutdown,
    error: OnceLock<String>,
}

impl DeviceDrain {
    pub(super) fn new(
        pools: impl StorageOwner,
        application: Arc<State>,
        frontend: Arc<State>,
    ) -> Self {
        Self(Arc::new(Drain {
            application,
            frontend,
            pools: Box::new(pools),
            started: AtomicBool::new(false),
            finished: Shutdown::default(),
            error: OnceLock::new(),
        }))
    }
}

impl Drain {
    pub(super) fn request(self: &Arc<Self>) {
        self.application.stop.request();
        self.frontend.stop.request();
        if !self.started.swap(true, Ordering::AcqRel) {
            let drain = self.clone();
            let spawned = std::thread::Builder::new()
                .name("ozzy_drain".into())
                .spawn(move || {
                    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                        futures::executor::block_on(async {
                            futures::future::join(
                                drain.application.finished.requested(),
                                drain.frontend.finished.requested(),
                            )
                            .await;
                        });
                        // Each owner reported completion as its last action.
                        // Completion here means its threads have exited.
                        drain.application.join();
                        drain.frontend.join();
                        drain.pools.drain();
                    }));
                    if result.is_err() {
                        let _ = drain.error.set("broker drain worker panicked".into());
                    }
                    drain.finished.request();
                });
            if let Err(error) = spawned {
                let _ = self.error.set(format!("broker drain worker: {error}"));
                self.finished.request();
            }
        }
    }

    pub(super) async fn closed(&self) -> Result<(), StartupError> {
        self.finished.requested().await;
        if let Some(error) = self.error.get() {
            return Err(StartupError::Runtime(error.clone()));
        }
        self.application.result().and(self.frontend.result())
    }
}

impl Drop for DeviceDrain {
    fn drop(&mut self) {
        self.0.request();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shards::lifecycle::Registration;
    use ozzy_config::{Deployment, HostResources, IoBackend};
    use std::{sync::atomic::AtomicUsize, time::Duration};

    /// Reports completion like a shard or dispatcher thread, then needs a
    /// moment to exit.
    fn owner(ended: &Arc<AtomicUsize>) -> Arc<State> {
        let state = Arc::new(State::new(1));
        let registration = Registration::new(state.clone());
        let stop = state.stop.clone();
        let ended = ended.clone();
        state.own(std::thread::spawn(move || {
            futures::executor::block_on(stop.requested());
            drop(registration);
            std::thread::sleep(Duration::from_millis(50));
            ended.fetch_add(1, Ordering::AcqRel);
        }));
        state
    }

    #[tokio::test]
    async fn drain_completes_after_the_owned_threads_have_exited() {
        let mut config = Deployment::parse(include_str!(
            "../../../ozzy-config/tests/fixtures/single.toml"
        ))
        .unwrap();
        let broker = config.brokers.get_mut("laptop").unwrap();
        broker.devices.get_mut("ssd").unwrap().workers.backend = IoBackend::Pool;
        let plan = config
            .validate()
            .unwrap()
            .broker_plan(
                "laptop",
                &HostResources {
                    cpus: [(0, Some(0))].into(),
                    memory_nodes: [0].into(),
                    linux_aio: true,
                },
            )
            .unwrap();
        let (pools, lanes) = DevicePools::start(&plan).unwrap();
        drop(lanes);
        let ended = Arc::new(AtomicUsize::new(0));
        let drain = DeviceDrain::new(pools, owner(&ended), owner(&ended));
        drain.0.request();
        drain.0.closed().await.unwrap();
        assert_eq!(ended.load(Ordering::Acquire), 2);
    }
}

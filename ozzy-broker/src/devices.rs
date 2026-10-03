use crate::StartupError;
use ozzy_config::{BrokerPlan, ControllerPlan, IoBackend};
use ozzy_io::{Admission, Limits, Quota};
use ozzy_io_pool::{Client, Config, Initializer, Pool, Worker};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::Arc,
};

#[cfg(test)]
mod tests;

/// Fixed shared pools. Dropping requests detached drain; shutdown observes it.
#[derive(Debug)]
pub struct DevicePools {
    pools: BTreeMap<String, Backend>,
}

/// Move onto its application thread before constructing `ozzy_io::Local`.
/// All partitions on that thread share this one submission lane.
#[derive(Debug)]
pub struct ShardIo<B = Client> {
    pub shard: u32,
    pub controller: String,
    pub client: B,
}

#[derive(Debug)]
enum Backend {
    Pool(Pool),
    #[cfg(target_os = "linux")]
    Aio(ozzy_io_aio::Aio),
}

impl DevicePools {
    /// Startup-only: waits for worker placement, without opening partition files.
    /// One pool per used controller, irrespective of topic or partition count.
    pub fn start(plan: &BrokerPlan) -> Result<(Self, Vec<ShardIo>), StartupError> {
        Self::start_with(plan, initializer)
    }

    fn start_with(
        plan: &BrokerPlan,
        initialize: impl Fn(&ControllerPlan) -> Initializer,
    ) -> Result<(Self, Vec<ShardIo>), StartupError> {
        let mut names = BTreeSet::new();
        let mut shards = BTreeSet::new();
        let mut settings = Vec::new();
        for controller in &plan.controllers {
            let error = |source| StartupError::Device {
                controller: controller.name.clone(),
                source,
            };
            if !names.insert(&controller.name)
                || controller.shards.iter().any(|id| !shards.insert(*id))
            {
                return Err(error(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "duplicate controller or shard",
                )));
            }
            settings.push(config(controller).map_err(error)?);
        }
        if shards != plan.shards.iter().map(|shard| shard.id).collect() {
            return Err(StartupError::Host(
                "device lanes do not cover application shards".into(),
            ));
        }
        let mut pools = BTreeMap::new();
        let mut lanes = Vec::with_capacity(shards.len());
        for (controller, config) in plan.controllers.iter().zip(settings) {
            let (backend, clients) = start(
                controller.workers.backend,
                config,
                controller.workers.aio_depth,
                initialize(controller),
            )
            .map_err(|source| StartupError::Device {
                controller: controller.name.clone(),
                source,
            })?;
            for (shard, client) in controller.shards.iter().copied().zip(clients) {
                lanes.push(ShardIo {
                    shard,
                    controller: controller.name.clone(),
                    client,
                });
            }
            pools.insert(controller.name.clone(), backend);
        }
        lanes.sort_by_key(|lane| lane.shard);
        Ok((Self { pools }, lanes))
    }

    pub fn admission(&self, controller: &str) -> Option<&Admission> {
        self.pools.get(controller).map(|backend| match backend {
            Backend::Pool(pool) => pool.admission(),
            #[cfg(target_os = "linux")]
            Backend::Aio(aio) => aio.admission(),
        })
    }

    /// Call after shard journals have drained their admitted work.
    pub async fn shutdown(&self) {
        for backend in self.pools.values() {
            match backend {
                Backend::Pool(pool) => pool.shutdown().await,
                #[cfg(target_os = "linux")]
                Backend::Aio(aio) => aio.shutdown().await,
            }
        }
    }

    /// Block until every worker thread has exited. Call after `shutdown`
    /// from a thread that may block, never from an application shard.
    pub fn join(&self) {
        for backend in self.pools.values() {
            match backend {
                Backend::Pool(pool) => pool.join(),
                #[cfg(target_os = "linux")]
                Backend::Aio(aio) => aio.join(),
            }
        }
    }
}

fn config(controller: &ControllerPlan) -> io::Result<Config> {
    let workers = &controller.workers;
    let limits = Limits {
        shards: controller.shards.len(),
        data: Quota {
            operations: workers.queued_jobs,
            bytes: usize::try_from(workers.queued_bytes)
                .map_err(|_| io::ErrorKind::InvalidInput)?,
        },
        progress: Quota {
            operations: workers.progress_jobs,
            bytes: usize::try_from(workers.progress_bytes)
                .map_err(|_| io::ErrorKind::InvalidInput)?,
        },
    }
    .validate()?;
    Ok(Config {
        threads: workers.write_threads,
        max_inflight: workers.max_inflight,
        handles: workers.open_handles,
        limits,
    })
}

fn initializer(controller: &ControllerPlan) -> Initializer {
    let workers = controller.workers.clone();
    Arc::new(move |role| {
        crate::placement::pin(match role {
            Worker::Data(index) => workers.cpus.get(index).copied(),
            Worker::Progress => workers.progress_cpu,
            Worker::Direct => workers.aio_cpu,
        })
    })
}

fn start(
    backend: IoBackend,
    config: Config,
    depth: usize,
    initialize: Initializer,
) -> io::Result<(Backend, Vec<Client>)> {
    match backend {
        IoBackend::Pool => Pool::with_initializer(config, initialize)
            .map(|(pool, clients)| (Backend::Pool(pool), clients)),
        #[cfg(target_os = "linux")]
        IoBackend::Aio => ozzy_io_aio::Aio::with_initializer(
            ozzy_io_aio::Config {
                pool: config,
                depth,
            },
            initialize,
        )
        .map(|(aio, clients)| (Backend::Aio(aio), clients)),
        #[cfg(not(target_os = "linux"))]
        IoBackend::Aio => {
            let _ = depth;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Linux AIO requested on unsupported host",
            ))
        }
    }
}

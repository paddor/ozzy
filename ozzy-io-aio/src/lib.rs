//! Linux AIO data writes with shared bounded blocking helpers. All kernel
//! context, eventfd and descriptor work stays on backend-owned threads.
#![warn(missing_docs)]
#![cfg(target_os = "linux")]
#![deny(unsafe_code)]

pub mod kernel;
mod worker;

pub use ozzy_io_pool::{Client, Config as PoolConfig};
use std::io;

/// Fixed helper-pool limits and direct-write kernel depth for one device.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// Shared helper, handle, and per-shard admission bounds.
    pub pool: PoolConfig,
    /// Aggregate ordinary direct writes in flight, 1..=64. Default policy is
    /// one. One additional slot is reserved for progress-class direct writes.
    pub depth: usize,
}

/// Backend owner combining one kernel-write worker with fixed blocking helpers.
#[derive(Debug)]
pub struct Aio {
    pool: ozzy_io_pool::Pool,
}

impl Aio {
    /// Spawns fixed workers; kernel setup happens on the direct-write worker.
    /// Setup failure fences admission and fails queued work. No implicit pool
    /// fallback changes the configured mechanism.
    pub fn new(config: Config) -> io::Result<(Self, Vec<Client>)> {
        Self::with_initializer(config, std::sync::Arc::new(|_| Ok(())))
    }

    /// Apply placement before helper execution and direct-driver kernel setup.
    pub fn with_initializer(
        config: Config,
        initialize: ozzy_io_pool::Initializer,
    ) -> io::Result<(Self, Vec<Client>)> {
        if !(1..=64).contains(&config.depth) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AIO depth must be 1..=64",
            ));
        }
        let (pool, clients) = ozzy_io_pool::Pool::with_direct_worker_and_initializer(
            config.pool,
            worker::Driver {
                depth: config.depth,
            },
            initialize,
        )?;
        Ok((Self { pool }, clients))
    }

    /// Shared device admission observations and capacity wakeups.
    pub fn admission(&self) -> &ozzy_io::Admission {
        self.pool.admission()
    }

    /// Includes kernel destruction and descriptor close, not just delivery of
    /// results. Dropping this owner requests the same drain without waiting.
    pub async fn shutdown(&self) {
        self.pool.shutdown().await;
    }

    /// Block until every worker thread has exited. Call after `shutdown`
    /// from a thread that may block.
    pub fn join(&self) {
        self.pool.join();
    }
}

//! Uniform lifecycle for fresh native comparison servers.
use super::{Iggy, Monitor, Result, redpanda::Redpanda};
use serde_json::Value;
use std::path::Path;

#[derive(Debug)]
pub enum External {
    Distributed(super::distributed::Iggy),
    Iggy(Iggy),
    Redpanda(Redpanda),
}

impl External {
    pub fn distributed(root: &Path, placements: &[crate::placement::Placement; 3]) -> Result<Self> {
        Ok(Self::Distributed(super::distributed::Iggy::start(
            root, placements,
        )?))
    }
    /// `cpus` holds one CPU list per broker.
    pub fn start(
        implementation: &str,
        root: &Path,
        mode: &str,
        cpus: &[Vec<usize>],
    ) -> Result<Self> {
        match implementation {
            "iggy" => Ok(Self::Iggy(Iggy::start(root, mode, cpus, "info")?)),
            "redpanda" => Ok(Self::Redpanda(Redpanda::start(root, mode, cpus)?)),
            _ => Err("unknown external implementation".into()),
        }
    }
    pub fn root(&self) -> &Path {
        match self {
            Self::Distributed(s) => &s.root,
            Self::Iggy(s) => &s.root,
            Self::Redpanda(s) => &s.root,
        }
    }
    pub fn endpoint(&self) -> &str {
        match self {
            Self::Distributed(s) => &s.endpoint,
            Self::Iggy(s) => &s.endpoint,
            Self::Redpanda(s) => &s.endpoint,
        }
    }
    pub fn pids(&self) -> Vec<u32> {
        match self {
            Self::Distributed(s) => s.pids(),
            Self::Iggy(s) => s.pids(),
            Self::Redpanda(s) => s.pids(),
        }
    }
    pub fn affinity(&self, install: bool) -> Result<Value> {
        match self {
            Self::Distributed(s) => s.affinity(),
            Self::Iggy(s) => s.affinity(install),
            Self::Redpanda(s) => s.affinity(),
        }
    }
    pub fn identity(&self) -> Result<Value> {
        match self {
            Self::Distributed(s) => s.identity(),
            Self::Iggy(s) => s.identity(),
            Self::Redpanda(s) => s.identity(),
        }
    }
    pub fn monitor(&self) -> Monitor<'_> {
        match self {
            Self::Distributed(s) => Monitor::Distributed(s),
            Self::Iggy(s) => Monitor::Native(s),
            Self::Redpanda(s) => Monitor::Redpanda(s),
        }
    }
    pub fn stop(&mut self) -> Result<()> {
        match self {
            Self::Distributed(s) => s.stop(),
            Self::Iggy(s) => s.stop(),
            Self::Redpanda(s) => s.stop(),
        }
    }
}

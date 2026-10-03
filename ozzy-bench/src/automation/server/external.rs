//! Uniform lifecycle for fresh native comparison servers.
use super::{Iggy, Monitor, Result, redpanda::Redpanda};
use serde_json::Value;
use std::path::Path;

#[derive(Debug)]
/// Owned external-server deployment selected for one fresh comparison case.
pub enum External {
    /// Iggy deployment spanning configured broker hosts.
    Distributed(super::distributed::Iggy),
    /// Locally managed Iggy deployment.
    Iggy(Iggy),
    /// Locally managed Redpanda deployment.
    Redpanda(Redpanda),
}

impl External {
    /// Start an Iggy deployment across the three configured placements.
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
    /// Directory holding this deployment artifact and storage state.
    pub fn root(&self) -> &Path {
        match self {
            Self::Distributed(s) => &s.root,
            Self::Iggy(s) => &s.root,
            Self::Redpanda(s) => &s.root,
        }
    }
    /// Producer and consumer endpoint of this deployment.
    pub fn endpoint(&self) -> &str {
        match self {
            Self::Distributed(s) => &s.endpoint,
            Self::Iggy(s) => &s.endpoint,
            Self::Redpanda(s) => &s.endpoint,
        }
    }
    /// Local server process IDs belonging to this deployment.
    pub fn pids(&self) -> Vec<u32> {
        match self {
            Self::Distributed(s) => s.pids(),
            Self::Iggy(s) => s.pids(),
            Self::Redpanda(s) => s.pids(),
        }
    }
    /// Capture or install the deployment CPU placement where supported.
    pub fn affinity(&self, install: bool) -> Result<Value> {
        match self {
            Self::Distributed(s) => s.affinity(),
            Self::Iggy(s) => s.affinity(install),
            Self::Redpanda(s) => s.affinity(),
        }
    }
    /// Collect server source and binary identity for measurement provenance.
    pub fn identity(&self) -> Result<Value> {
        match self {
            Self::Distributed(s) => s.identity(),
            Self::Iggy(s) => s.identity(),
            Self::Redpanda(s) => s.identity(),
        }
    }
    /// Borrow the matching liveness and diagnostic monitor.
    pub fn monitor(&self) -> Monitor<'_> {
        match self {
            Self::Distributed(s) => Monitor::Distributed(s),
            Self::Iggy(s) => Monitor::Native(s),
            Self::Redpanda(s) => Monitor::Redpanda(s),
        }
    }
    /// Stop the owned deployment and reap its local or remote processes.
    pub fn stop(&mut self) -> Result<()> {
        match self {
            Self::Distributed(s) => s.stop(),
            Self::Iggy(s) => s.stop(),
            Self::Redpanda(s) => s.stop(),
        }
    }
}

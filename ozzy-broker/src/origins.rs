//! The durable record of the stores behind each broker peer.
//!
//! It lives beside the volume marker of the broker's first device. The
//! dispatcher adds a peer to it before the first link to that peer, and
//! refuses any later link under another origin.

use std::{
    collections::BTreeMap,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use ozzy_config::{BrokerIdentity, PeerRecord};
use ozzy_proto::NodeId;
use ozzy_runtime::frontend::Service;
use uuid::Uuid;

use crate::{CheckedConfig, StartupError, files, io_error, provisioning};

const RECORD: &str = "OZZY_PEERS";
const MAX_RECORD_BYTES: u64 = 64 * 1024;

/// This broker's store origin and what it recorded of its peers.
#[derive(Clone, Debug)]
pub(crate) struct PeerOrigins {
    local: u128,
    /// Whether the local origin replaces a lost one.
    replacement: bool,
    path: PathBuf,
    record: Arc<Mutex<PeerRecord>>,
}

impl PeerOrigins {
    /// Read the record of a broker with peers; an absent one is empty. A
    /// damaged record fails: it may have named a peer that must be refused.
    pub(crate) fn load(
        checked: &CheckedConfig,
        local: &BrokerIdentity,
    ) -> Result<Option<Self>, StartupError> {
        if checked.identity.brokers.len() < 2 {
            return Ok(None);
        }
        let Some(root) = checked.first_device_root()? else {
            return Ok(None);
        };
        let path = root.join(RECORD);
        let empty = PeerRecord {
            cluster: local.cluster,
            broker: local.broker,
            origins: BTreeMap::new(),
        };
        let record = if files::exists(&path)? {
            let text = provisioning::read_bounded(&path, MAX_RECORD_BYTES)?;
            let record = PeerRecord::decode(&text)?;
            if (record.cluster, record.broker) != (empty.cluster, empty.broker) {
                return Err(io_error(
                    path,
                    io::Error::new(io::ErrorKind::InvalidData, "peer record of another broker"),
                ));
            }
            record
        } else {
            empty
        };
        Ok(Some(Self {
            local: local.origin().as_u128(),
            replacement: false,
            path,
            record: Arc::new(Mutex::new(record)),
        }))
    }

    /// Say beside the local origin that it replaces a lost one.
    pub(crate) const fn replacing(mut self, replacement: bool) -> Self {
        self.replacement = replacement;
        self
    }

    /// Name the local origin on `service` and check every broker peer's.
    pub(crate) fn install(&self, service: &mut Service) {
        let known: Vec<_> = ozzy_io::lock_unpoisoned(&self.record)
            .origins
            .iter()
            .map(|(peer, origin)| (NodeId::from_bytes(*peer.as_bytes()), origin.as_u128()))
            .collect();
        let origins = self.clone();
        service.install_origins(self.local, known, move |peer, origin| {
            origins.keep(peer, origin).is_ok()
        });
        if self.replacement {
            service.announce_replacement();
        }
    }

    /// Add `peer` to the record on disk. Blocks the caller, once per peer
    /// in the life of the local stores.
    fn keep(&self, peer: NodeId, origin: u128) -> Result<(), StartupError> {
        let mut record = ozzy_io::lock_unpoisoned(&self.record);
        let mut next = record.clone();
        next.origins
            .insert(Uuid::from_bytes(*peer.as_bytes()), Uuid::from_u128(origin));
        files::publish(&self.path, &next.encode()?, true)?;
        *record = next;
        Ok(())
    }
}

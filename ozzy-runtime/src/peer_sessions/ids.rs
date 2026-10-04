use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Error, Result};

/// Identity source for one connection-table incarnation. Deterministic runs
/// must use a distinct namespace for every startup, including crash/restart.
/// IDs are correlation fences and carry no application authority.
#[derive(Debug, Default)]
pub struct LinkIds {
    sequence: Option<(NonZeroU64, AtomicU64)>,
}

impl LinkIds {
    /// Production UUID source, independent of protocol timer observations.
    pub const fn random() -> Self {
        Self { sequence: None }
    }

    /// Repeatable IDs within one simulation startup namespace.
    pub const fn deterministic(namespace: NonZeroU64) -> Self {
        Self {
            sequence: Some((namespace, AtomicU64::new(0))),
        }
    }

    #[allow(
        deprecated,
        reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
    )]
    pub(crate) fn next(&self) -> Result<u128> {
        if let Some((namespace, sequence)) = &self.sequence {
            let prior = sequence
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |prior| {
                    prior.checked_add(1)
                })
                .map_err(|_| Error::NotConnected)?;
            Ok((u128::from(namespace.get()) << 64) | u128::from(prior + 1))
        } else {
            Ok(u128::from_be_bytes(
                *ozzy_proto::RequestId::new().as_bytes(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn namespaces_replay_and_exhaustion_never_reuses_a_link_id() {
        let namespace = NonZeroU64::new(1).unwrap();
        let ids = LinkIds::deterministic(namespace);
        let replay = LinkIds::deterministic(namespace);
        let other = LinkIds::deterministic(NonZeroU64::new(2).unwrap());
        for _ in 0..32 {
            let id = ids.next().unwrap();
            assert_eq!(id, replay.next().unwrap());
            assert_ne!(id, other.next().unwrap());
        }
        ids.sequence
            .as_ref()
            .unwrap()
            .1
            .store(u64::MAX, Ordering::Release);
        assert!(ids.next().is_err());
        assert!(ids.next().is_err());
    }

    #[test]
    fn concurrent_requests_keep_one_namespace_and_unique_ids() {
        let ids = Arc::new(LinkIds::deterministic(NonZeroU64::new(7).unwrap()));
        let mut observed = std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for _ in 0..8 {
                let ids = ids.clone();
                workers.push(
                    scope.spawn(move || (0..128).map(|_| ids.next().unwrap()).collect::<Vec<_>>()),
                );
            }
            workers
                .into_iter()
                .flat_map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        observed.sort_unstable();
        assert_eq!(observed.len(), 1024);
        for (index, id) in observed.into_iter().enumerate() {
            assert_eq!(id, (7_u128 << 64) | (index + 1) as u128);
        }
    }
}

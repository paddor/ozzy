//! Explicit identity source for protocol correlation and journal incarnations.

use super::ActorError;
use ozzy_proto::RequestId;
use ozzy_replication::{
    JournalGeneration, Scope,
    flow::{Channel, ReceiveEpoch},
};

/// Unique IDs for one actor incarnation. Production uses UUIDs. Deterministic
/// runs supply a distinct nonzero namespace for every actor startup, including
/// retries after a simulated crash. IDs carry no replication authority.
#[derive(Debug)]
pub struct ActorIds {
    sequence: Option<(std::num::NonZeroU64, u64)>,
    handles: ReceiptHandles,
}

/// Short receipt aliases of one broker. A receiving broker binds each alias
/// to one group of the sending broker, so all its partition actors share one
/// source, across shards too. Clones share it. Exhaustion fails instead of
/// recycling an alias.
#[derive(Debug, Clone, Default)]
pub struct ReceiptHandles(std::sync::Arc<std::sync::atomic::AtomicU32>);

impl ReceiptHandles {
    #[allow(
        deprecated,
        reason = "Atomic::try_update requires Rust 1.95; MSRV is 1.93"
    )]
    fn next(&self) -> Result<u32, ActorError> {
        let ordering = std::sync::atomic::Ordering::Relaxed;
        self.0
            .fetch_update(ordering, ordering, |last| last.checked_add(1))
            .map(|last| last + 1)
            .map_err(|_| ActorError::Limits)
    }
}

impl ActorIds {
    /// Use production randomness without consulting protocol time.
    pub const fn random(handles: ReceiptHandles) -> Self {
        Self {
            sequence: None,
            handles,
        }
    }

    /// Repeatable source; never reuse this namespace in the same modeled run.
    pub const fn deterministic(namespace: std::num::NonZeroU64, handles: ReceiptHandles) -> Self {
        Self {
            sequence: Some((namespace, 0)),
            handles,
        }
    }

    fn next(&mut self) -> Result<u128, ActorError> {
        if let Some((namespace, counter)) = &mut self.sequence {
            *counter = counter.checked_add(1).ok_or(ActorError::Limits)?;
            Ok((u128::from(namespace.get()) << 64) | u128::from(*counter))
        } else {
            Ok(uuid::Uuid::now_v7().as_u128())
        }
    }

    /// The next receipt alias of this actor's broker.
    pub(super) fn compact_handle(&self) -> Result<u32, ActorError> {
        self.handles.next()
    }

    pub(super) fn request(&mut self) -> Result<RequestId, ActorError> {
        Ok(RequestId::from_bytes(self.next()?.to_be_bytes()))
    }

    pub(super) fn generation(&mut self) -> Result<JournalGeneration, ActorError> {
        Ok(JournalGeneration(self.next()?))
    }

    pub(super) fn channel(&mut self, scope: Scope) -> Result<Channel, ActorError> {
        Ok(Channel {
            scope,
            epoch: ReceiveEpoch::new(self.next()?)
                .map_err(ozzy_replication::flow::TransmitError::from)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deterministic_actor_ids_replay_without_reusing_types_or_startups() {
        let namespace = |n| std::num::NonZeroU64::new(n).unwrap();
        let mut left = ActorIds::deterministic(namespace(7), ReceiptHandles::default());
        let mut replay = ActorIds::deterministic(namespace(7), ReceiptHandles::default());
        let mut other = ActorIds::deterministic(namespace(8), ReceiptHandles::default());
        for _ in 0..16 {
            let id = left.request().unwrap();
            assert_eq!(id, replay.request().unwrap());
            assert_ne!(id, other.request().unwrap());
            let generation = left.generation().unwrap();
            assert_eq!(generation, replay.generation().unwrap());
            assert_ne!(generation.0.to_be_bytes(), *id.as_bytes());
        }
        left.sequence.as_mut().unwrap().1 = u64::MAX;
        assert!(left.request().is_err());
        assert!(left.generation().is_err());
    }

    #[test]
    fn receipt_handles_are_shared_per_broker_and_never_recycle() {
        let namespace = |n| std::num::NonZeroU64::new(n).unwrap();
        let broker = ReceiptHandles::default();
        let first = ActorIds::deterministic(namespace(1), broker.clone());
        let second = ActorIds::deterministic(namespace(2), broker.clone());
        let other = ActorIds::deterministic(namespace(3), ReceiptHandles::default());
        assert_eq!(first.compact_handle().unwrap(), 1);
        assert_eq!(second.compact_handle().unwrap(), 2);
        assert_eq!(other.compact_handle().unwrap(), 1);
        broker
            .0
            .store(u32::MAX, std::sync::atomic::Ordering::Relaxed);
        assert!(first.compact_handle().is_err());
    }
}

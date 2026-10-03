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
#[derive(Debug, Default)]
pub struct ActorIds {
    sequence: Option<(std::num::NonZeroU64, u64)>,
}

impl ActorIds {
    /// Use production randomness without consulting protocol time.
    pub const fn random() -> Self {
        Self { sequence: None }
    }

    /// Repeatable source; never reuse this namespace in the same modeled run.
    pub const fn deterministic(namespace: std::num::NonZeroU64) -> Self {
        Self {
            sequence: Some((namespace, 0)),
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

    /// Process-unique short aliases. Exhaustion fails instead of recycling one.
    pub(super) fn compact_handle() -> Result<u32, ActorError> {
        static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
        NEXT.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |next| next.checked_add(1),
        )
        .map_err(|_| ActorError::Limits)
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
            epoch: ReceiveEpoch::new(self.next()?).expect("nonzero ID source"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deterministic_actor_ids_replay_without_reusing_types_or_startups() {
        let mut left = ActorIds::deterministic(std::num::NonZeroU64::new(7).unwrap());
        let mut replay = ActorIds::deterministic(std::num::NonZeroU64::new(7).unwrap());
        let mut other = ActorIds::deterministic(std::num::NonZeroU64::new(8).unwrap());
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
}

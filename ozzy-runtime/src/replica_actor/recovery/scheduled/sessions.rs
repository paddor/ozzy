//! Peer session changes, applied to the active attempt or its successor.

use super::{Phase, ScheduledRecovery};
use crate::{
    replica_actor::{ActorError, RecoveryActor, ScheduleError, ScheduledReplica},
    replica_journal::RecoveryStorage,
    replica_transport::SendClass,
};
use ozzy_proto::{LinkSessionId, NodeId};
use ozzy_replication::wire::PeerBinding;
use std::time::Duration;

impl<J: RecoveryStorage + 'static> ScheduledRecovery<J> {
    /// Session comes from the frontend's independently established link table.
    pub fn replace_session(
        &mut self,
        peer: NodeId,
        expected: LinkSessionId,
        replacement: LinkSessionId,
        now: Duration,
    ) -> Result<bool, ScheduleError> {
        if replacement.as_bytes() == &[0; 16] {
            return Err(ScheduleError::Binding);
        }
        self.change_session(peer, expected, replacement, now)
    }

    /// A late disconnect cannot fence a newer link. Admitted disk work remains.
    pub fn disconnect_session(
        &mut self,
        peer: NodeId,
        expected: LinkSessionId,
        now: Duration,
    ) -> Result<bool, ScheduleError> {
        self.change_session(peer, expected, LinkSessionId::from_bytes([0; 16]), now)
    }

    fn change_session(
        &mut self,
        peer: NodeId,
        expected: LinkSessionId,
        replacement: LinkSessionId,
        now: Duration,
    ) -> Result<bool, ScheduleError> {
        self.observe(now)?;
        let slot = self
            .configuration
            .voters()
            .iter()
            .position(|node| *node == peer && peer != self.local)
            .ok_or(ScheduleError::Binding)?;
        let current = self.sessions[slot];
        if current == replacement {
            return Ok(false);
        }
        if current != expected {
            return Err(ScheduleError::Binding);
        }
        self.sessions[slot] = replacement;
        if let Phase::Active(actor) = &mut self.phase {
            set_session(
                actor,
                slot,
                replacement,
                now.saturating_sub(self.origin.unwrap_or(now)),
            )?;
        } else if let Phase::Ready(actor) = &mut self.phase {
            switch_session(actor, peer, current, replacement, now)?;
        }
        Ok(true)
    }

    pub(super) fn session(&self, peer: NodeId) -> Option<LinkSessionId> {
        let slot = self
            .configuration
            .voters()
            .iter()
            .position(|node| *node == peer)?;
        (self.sessions[slot].as_bytes() != &[0; 16]).then_some(self.sessions[slot])
    }
}

/// Move a normal replica's `peer` link from `current` to `replacement`; an
/// all-zero replacement disconnects it.
pub(super) fn switch_session(
    actor: &mut ScheduledReplica,
    peer: NodeId,
    current: LinkSessionId,
    replacement: LinkSessionId,
    now: Duration,
) -> Result<bool, ScheduleError> {
    if replacement.as_bytes() == &[0; 16] {
        actor.disconnect_session(peer, current, now)
    } else {
        actor.replace_session(peer, current, replacement, now)
    }
}

pub(super) fn set_session<J: RecoveryStorage>(
    actor: &mut RecoveryActor<J>,
    slot: usize,
    session: LinkSessionId,
    now: Duration,
) -> Result<(), ActorError> {
    let peer = actor.configuration.voters()[slot];
    actor.config.sessions[slot] = session;
    actor.bindings[slot] = if session.as_bytes() == &[0; 16] {
        None
    } else {
        Some(PeerBinding::new(actor.configuration, peer, session)?)
    };
    for class in [
        SendClass::Control,
        SendClass::Data,
        SendClass::Receipt,
        SendClass::Exchange,
    ] {
        actor.outbox.discard(peer, class);
    }
    actor.retries[slot].at = now;
    if let Some((request, retry)) = &mut actor.checkpoint_fetch
        && request.source.voter == peer
    {
        retry.at = now;
    }
    if let Some((request, retry)) = &mut actor.fetch
        && request.source.voter == peer
    {
        retry.at = now;
    }
    Ok(())
}

//! Follower data intake: PEER flow PREPAREs and the primary's group publications.

use super::{
    ActorError, Duration, Message, PeerBinding, ReplicaActor, ReplicaMessage, Scope, routed, wire,
};

impl ReplicaActor {
    pub(in crate::replica_actor) fn receive_data(
        &mut self,
        message: &Message,
        now: Duration,
    ) -> Result<bool, ActorError> {
        let Some((voter, binding, frames)) = routed(self.configuration, &self.bindings, message)
        else {
            return Ok(true);
        };
        let scope = self.driver.scope();
        let Some((batch, publication)) = self.decode_data(&scope, frames, binding) else {
            return Ok(true);
        };
        // A missing predecessor cannot be supplied by waiting on this frame.
        // Drop the gap and let the next correlated probe request the prefix.
        if batch.predecessor().op > self.work.receive_report().received.op {
            if publication {
                crate::profiling::event(crate::profiling::Event::ReplicaPublicationGap);
                self.work.flow.observe_receipt(
                    self.work.receive_report().received.op,
                    Some(batch.end().op),
                    true,
                    now,
                );
            }
            return Ok(true);
        }
        let end = batch.end().op;
        let consumed = self.receive_prepare(self.configuration.voters()[voter], batch, now)?;
        if publication && !consumed {
            self.work.flow.pending_publication = Some(end);
        }
        self.work.flow.observe_receipt(
            self.work.receive_report().received.op,
            None,
            publication,
            now,
        );
        // PUB receipt can advance beyond PEER reservations. Keep its full digest
        // report so the leader verifies exact history instead of trusting a counter.
        if publication {
            self.work.flow.clear_compact_receipt();
        }
        Ok(consumed)
    }

    /// Decode follower data: a group publication from the primary or a PEER
    /// flow PREPARE. `true` marks a publication; `None` drops the packet.
    fn decode_data<'a>(
        &self,
        scope: &'a Scope,
        frames: [&'a [u8]; 3],
        binding: PeerBinding,
    ) -> Option<(wire::PrepareBatch<'a>, bool)> {
        let packet = ozzy_proto::decode_packet(&frames, self.wire_limits.envelope).ok()?;
        if packet.envelope.opcode != ozzy_proto::Opcode::PreparePub {
            let Ok(ReplicaMessage::Flow(wire::FlowMessage::Prepare { batch, .. })) =
                wire::decode(&frames, binding, self.wire_limits)
            else {
                return None;
            };
            return Some((batch, false));
        }
        let published = [
            scope.group_id.as_bytes().as_slice(),
            frames[0],
            frames[1],
            frames[2],
        ];
        let batch =
            wire::decode_publication(&published, binding, self.local, *scope, self.wire_limits)
                .ok()?;
        crate::profiling::event(crate::profiling::Event::ReplicaPublicationReceived);
        Some((batch, true))
    }
}

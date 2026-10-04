//! Before a retained-topic restart votes, ask current normal copies whether its
//! prefix has expired. Replies are hints only: withdrawal still persists the
//! nonvoting marker and uses the ordinary two-copy recovery gates.

use super::{ActorError, Duration, Message, Prefix, ReplicaActor, SendClass, wire};
use bytes::Bytes;
use ozzy_replication::{
    recovery::RecoveryResponse,
    wire::{RecoveryRequest, RecoveryState},
};

#[derive(Debug)]
pub(super) struct Probe {
    pub(super) request: RecoveryRequest,
    pub(super) accepted: Prefix,
    responses: [Option<RecoveryResponse>; 3],
    deadline: Option<Duration>,
    retry_at: Duration,
}

impl Probe {
    pub(super) fn new(request: RecoveryRequest, accepted: Prefix) -> Self {
        Self {
            request,
            accepted,
            responses: [None; 3],
            deadline: None,
            retry_at: Duration::ZERO,
        }
    }
}

impl ReplicaActor {
    pub(super) fn startup_response(&mut self, voter: usize, state: &RecoveryState) {
        let Some(probe) = &mut self.startup_probe else {
            return;
        };
        if state.validate_response(probe.request).is_ok() {
            probe.responses[voter] = Some(state.response);
        }
    }

    pub(super) fn startup_round(&mut self, now: Duration) -> Result<bool, ActorError> {
        let Some(probe) = &mut self.startup_probe else {
            return Ok(false);
        };
        let deadline = *probe
            .deadline
            .get_or_insert(now.saturating_add(Duration::from_millis(500)));
        let decision = probe.responses.iter().flatten().find_map(|primary| {
            let log = primary.primary?;
            let matching = probe
                .responses
                .iter()
                .flatten()
                .filter(|other| other.scope == primary.scope)
                .count();
            (matching >= 2).then_some(
                log.checkpoint
                    .is_some_and(|anchor| anchor.predecessor.op > probe.accepted.op),
            )
        });
        if let Some(expired) = decision {
            self.startup_probe = None;
            if expired {
                self.recovery_required = true;
                self.ingress.close();
            }
            return Ok(expired);
        }
        if now >= deadline {
            self.startup_probe = None;
            return Ok(false);
        }
        if now >= probe.retry_at {
            probe.retry_at = now.saturating_add(Duration::from_millis(100));
            let request = probe.request;
            for peer in *self.configuration.voters() {
                if peer == self.local {
                    continue;
                }
                let Some(session) = self.session(peer) else {
                    continue;
                };
                let encoded = wire::encode_recovery(
                    self.local,
                    session,
                    request,
                    &mut self.metadata,
                    self.wire_limits,
                )?;
                let message = Message::multipart([
                    Bytes::copy_from_slice(&encoded.header),
                    Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
                    Bytes::new(),
                ]);
                self.enqueue(peer, SendClass::Control, message)?;
            }
        }
        Ok(true)
    }
}

//! Opt-in, nonce-bound donors sharing the actor's bounded journal schedule.

use super::io::FetchPurpose;
use super::{ActorError, Bytes, Message, NodeId, PendingIo, ReplicaActor, SendClass, wire};
use crate::replica_actor::HistoryReason;
use crate::replica_journal::{FetchedHistory, PinnedRecovery};
use ozzy_replication::recovery::RecoveryResponse;
use ozzy_replication::wire::{FetchOps, RecoveryRequest, RecoveryState};

#[derive(Debug, Default)]
pub(super) struct Donors {
    slots: [Slot; 3],
    cursor: usize,
    yield_normal: bool,
}

#[derive(Debug, Default)]
struct Slot {
    request: Option<RecoveryRequest>,
    response: Option<RecoveryResponse>,
    pin: Pin,
    fetch: Option<FetchOps>,
    checkpoint: Option<wire::CheckpointRequest>,
    reply_pending: bool,
}

/// Exact admitted work survives replacement of the request that wanted it.
#[derive(Debug, Default, Clone, Copy)]
enum Pin {
    #[default]
    Empty,
    Pinning(RecoveryResponse),
    Held(PinnedRecovery),
    Releasing(PinnedRecovery),
}

impl Pin {
    fn held(self) -> Option<PinnedRecovery> {
        match self {
            Self::Held(pin) => Some(pin),
            Self::Empty | Self::Pinning(_) | Self::Releasing(_) => None,
        }
    }

    fn response(self) -> Option<RecoveryResponse> {
        match self {
            Self::Pinning(response) => Some(response),
            Self::Held(pin) => Some(pin.response()),
            Self::Empty | Self::Releasing(_) => None,
        }
    }
}

impl Slot {
    fn clear_request(&mut self) {
        self.request = None;
        self.fetch = None;
        self.checkpoint = None;
        self.reply_pending = false;
    }
}

impl Donors {
    pub(super) fn disconnect(&mut self, index: usize) {
        // Keep the snapshot identity after releasing its backing. A same-nonce
        // retry must wait for a fresh attempt rather than recapture a moving tail.
        self.slots[index].clear_request();
    }

    pub(super) fn evidence(&self) -> ([Option<RecoveryResponse>; 3], [Option<PinnedRecovery>; 3]) {
        (
            self.slots.each_ref().map(|slot| slot.response),
            self.slots.each_ref().map(|slot| match slot.pin {
                Pin::Held(pin) | Pin::Releasing(pin) => Some(pin),
                Pin::Empty | Pin::Pinning(_) => None,
            }),
        )
    }

    pub(super) fn clear_pins(&mut self) {
        for slot in &mut self.slots {
            *slot = Slot::default();
        }
    }
}

impl ReplicaActor {
    /// Enable fixed-voter full-WAL recovery service on the existing endpoint.
    /// Only activated normal replicas answer. Source snapshots are immutable per
    /// nonce/view and retained on the journal worker; replies are never votes.
    /// Configured peers must already be independently authenticated.
    pub fn enable_recovery(&mut self) {
        self.donors.get_or_insert_with(Box::default);
    }

    pub(super) fn receive_recovery(
        &mut self,
        index: usize,
        request: RecoveryRequest,
    ) -> Result<(), ActorError> {
        if self.donors.is_none() || !self.application_ready() {
            return Ok(());
        }
        let slot = &mut self.donors.as_mut().expect("enabled donors").slots[index];
        if let Some(response) = slot.response.filter(|response| {
            response.scope == self.driver.scope()
                && response.nonce == request.nonce
                && response.primary.is_some()
        }) {
            let expired = match slot.pin {
                Pin::Empty => slot.request.is_none(),
                Pin::Releasing(pin) => {
                    pin.response().scope == response.scope && pin.response().nonce == response.nonce
                }
                Pin::Pinning(_) | Pin::Held(_) => false,
            };
            if expired {
                return Ok(());
            }
        }
        if slot.response.is_none_or(|response| {
            response.nonce != request.nonce || response.scope != self.driver.scope()
        }) {
            let retained = slot.pin.response();
            let response = if let Some(response) = retained.filter(|response| {
                response.nonce == request.nonce && response.scope == self.driver.scope()
            }) {
                response
            } else {
                self.driver
                    .normal()
                    .expect("ready normal")
                    .recovery_response(request.nonce)
                    .map_err(crate::replica_journal::JournalError::from)?
            };
            slot.response = Some(response);
            slot.fetch = None;
            slot.checkpoint = None;
        }
        slot.request = Some(request);
        slot.reply_pending = true;
        Ok(())
    }

    pub(super) fn donor_round(&mut self) -> Result<(), ActorError> {
        let ready = self.application_ready();
        let Some(donors) = &mut self.donors else {
            return Ok(());
        };
        let mut replies = [None; 3];
        for (index, slot) in donors.slots.iter_mut().enumerate() {
            if !ready
                || slot
                    .response
                    .is_some_and(|response| response.scope != self.driver.scope())
            {
                slot.response = None;
                slot.clear_request();
                continue;
            }
            if slot.reply_pending
                && let (Some(request), Some(response)) = (slot.request, slot.response)
                && (response.primary.is_none()
                    || slot
                        .pin
                        .held()
                        .is_some_and(|pin| pin.response() == response))
            {
                replies[index] = Some(RecoveryState {
                    request_id: request.request_id,
                    response,
                });
                slot.reply_pending = false;
            }
        }
        for (index, reply) in replies.into_iter().enumerate() {
            if let Some(reply) = reply {
                let to = self.configuration.voters()[index];
                let Some(session) = self.session(to) else {
                    continue; // Recovery requester owns its retry schedule.
                };
                let encoded = wire::encode_recovery_state(
                    self.local,
                    session,
                    reply,
                    &mut self.metadata,
                    self.wire_limits,
                )?;
                let message = Message::multipart([
                    Bytes::copy_from_slice(&encoded.header),
                    Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
                    Bytes::new(),
                ]);
                self.enqueue(to, SendClass::Control, message)?;
            }
        }
        Ok(())
    }

    pub(super) fn queue_recovery_fetch(&mut self, from: NodeId, request: FetchOps) -> bool {
        if !self.application_ready() {
            return false;
        }
        let Some(donors) = &mut self.donors else {
            return false;
        };
        let index = self
            .configuration
            .voters()
            .iter()
            .position(|voter| *voter == from)
            .expect("configured sender");
        let slot = &mut donors.slots[index];
        let Some(pin) = slot.pin.held().filter(|pin| pin.source() == request.source) else {
            return false;
        };
        if slot.response == Some(pin.response())
            && request.scope == self.driver.scope()
            && request.max_operations as usize <= self.config.transfer.max_operations
            && request.max_body_bytes as usize <= self.config.transfer.max_body_bytes
        {
            slot.fetch = Some(request);
        }
        true
    }

    pub(super) fn schedule_donor(&mut self, idle: bool) -> Result<bool, ActorError> {
        if !self.application_ready() {
            return Ok(false);
        }
        let Some(donors) = &mut self.donors else {
            return Ok(false);
        };
        if !idle && donors.yield_normal {
            donors.yield_normal = false;
            return Ok(false);
        }
        for delta in 0..3 {
            let index = (donors.cursor + delta) % 3;
            let slot = &mut donors.slots[index];
            let action = if let Some(response) = slot.response.filter(|response| {
                response.primary.is_some()
                    && matches!(slot.pin, Pin::Empty)
                    && slot.request.is_some()
            }) {
                if !self.pending_persistence.is_empty()
                    || self.pending_sync.is_some()
                    || !self.journal.settled()
                {
                    // Drain writes and their independent barrier before pinning
                    // authoritative history; busy admission is backpressure.
                    return Ok(true);
                }
                let pending = self
                    .journal
                    .pin_recovery(self.configuration.voters()[index], response)?;
                slot.pin = Pin::Pinning(response);
                Some(PendingIo::RecoveryPin(pending))
            } else if let (Some(pin), Some(request)) = (slot.pin.held(), slot.checkpoint.take()) {
                Some(PendingIo::RecoveryCheckpoint(
                    self.journal.fetch_checkpoint(pin, request)?,
                    self.configuration.voters()[index],
                ))
            } else if let (Some(pin), Some(request)) = (slot.pin.held(), slot.fetch) {
                let buffer = self
                    .buffer
                    .take()
                    .ok_or_else(|| ActorError::history(HistoryReason::BufferUnavailable))?;
                slot.fetch = None;
                Some(PendingIo::Fetch(
                    self.journal
                        .fetch_recovery(pin, request, buffer)
                        .map_err(|rejected| rejected.reason)?,
                    FetchPurpose::Recovery(
                        self.configuration.voters()[index],
                        pin.response().nonce,
                    ),
                ))
            } else {
                None
            };
            if let Some(action) = action {
                self.pending = Some(action);
                donors.yield_normal = true;
                donors.cursor = (index + 1) % 3;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Release obsolete sources even while a view change prevents serving.
    /// At most one exact release is admitted per bounded actor round.
    pub(super) fn schedule_donor_release(&mut self) -> Result<bool, ActorError> {
        let Some(donors) = &mut self.donors else {
            return Ok(false);
        };
        for delta in 0..3 {
            let index = (donors.cursor + delta) % 3;
            let slot = &mut donors.slots[index];
            if let Some(pin) = slot
                .pin
                .held()
                .filter(|pin| slot.request.is_none() || slot.response != Some(pin.response()))
            {
                self.pending = Some(PendingIo::RecoveryRelease(
                    self.journal.release_recovery(pin)?,
                ));
                slot.pin = Pin::Releasing(pin);
                donors.yield_normal = true;
                donors.cursor = (index + 1) % 3;
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn complete_recovery_pin(
        &mut self,
        pin: &PinnedRecovery,
        release: bool,
    ) -> Result<(), ActorError> {
        let index = self
            .configuration
            .voters()
            .iter()
            .position(|voter| *voter == pin.requester())
            .ok_or_else(|| ActorError::donor_pin(pin))?;
        let slot = &mut self
            .donors
            .as_mut()
            .ok_or_else(|| ActorError::donor_pin(pin))?
            .slots[index];
        if release {
            if !matches!(slot.pin, Pin::Releasing(expected) if expected == *pin) {
                return Err(ActorError::donor_pin(pin));
            }
            slot.pin = Pin::Empty;
        } else {
            let Pin::Pinning(mut scheduled) = slot.pin else {
                return Err(ActorError::donor_pin(pin));
            };
            // Capturing history may enrich the admitted response with its exact
            // checkpoint. Every other authority field must match that work.
            if let Some(log) = scheduled.primary.as_mut() {
                log.checkpoint = pin.response().primary.and_then(|log| log.checkpoint);
            }
            if scheduled != pin.response() {
                return Err(ActorError::donor_pin(pin));
            }
            if slot.response == slot.pin.response() {
                slot.response = Some(pin.response());
            }
            slot.pin = Pin::Held(*pin);
        }
        Ok(())
    }

    pub(super) fn complete_recovery_fetch(
        &mut self,
        to: NodeId,
        nonce: ozzy_proto::RequestId,
        fetched: FetchedHistory,
    ) -> Result<(), ActorError> {
        let current = self.application_ready()
            && self.donors.as_ref().is_some_and(|donors| {
                donors.slots.iter().enumerate().any(|(index, slot)| {
                    self.configuration.voters()[index] == to
                        && slot.pin.held().is_some_and(|pin| {
                            pin.response().nonce == nonce
                                && pin.source() == fetched.request().source
                                && pin.response().scope == fetched.request().scope
                                && slot.response == Some(pin.response())
                        })
                })
            })
            && fetched.request().scope == self.driver.scope();
        if current {
            self.send_ops(to, fetched)
        } else {
            self.recycle(fetched.into_buffer());
            Ok(())
        }
    }
}

impl ReplicaActor {
    pub(super) fn queue_recovery_checkpoint(
        &mut self,
        from: NodeId,
        request: wire::CheckpointRequest,
    ) {
        if !self.application_ready() {
            return;
        }
        let Some(donors) = &mut self.donors else {
            return;
        };
        let Some(index) = self
            .configuration
            .voters()
            .iter()
            .position(|&voter| voter == from)
        else {
            return;
        };
        let slot = &mut donors.slots[index];
        if slot.pin.held().is_some_and(|pin| {
            request.source == pin.source()
                && request.nonce == pin.response().nonce
                && request.scope == pin.response().scope
                && slot.response == Some(pin.response())
                && pin
                    .response()
                    .primary
                    .and_then(|log| log.checkpoint)
                    .is_some_and(|anchor| request.offset < anchor.state_bytes)
        }) && request.max_bytes as usize <= self.config.transfer.max_body_bytes
        {
            slot.checkpoint = Some(request);
        }
    }

    pub(super) fn complete_recovery_checkpoint(
        &mut self,
        to: NodeId,
        chunk: crate::replica_journal::RecoveryCheckpointRead,
    ) -> Result<(), ActorError> {
        let Some(index) = self
            .configuration
            .voters()
            .iter()
            .position(|&voter| voter == to)
        else {
            return Ok(());
        };
        if !self.application_ready()
            || !self.donors.as_ref().is_some_and(|donors| {
                donors.slots[index].pin.held().is_some_and(|pin| {
                    pin.response().scope == self.driver.scope()
                        && chunk.request.scope == pin.response().scope
                        && chunk.request.source == pin.source()
                        && chunk.request.nonce == pin.response().nonce
                        && donors.slots[index].response == Some(pin.response())
                })
            })
        {
            return Ok(());
        }
        let Some(session) = self.session(to) else {
            return Ok(());
        };
        let encoded = wire::encode_checkpoint(
            self.local,
            session,
            wire::CheckpointMessage::Chunk {
                request: chunk.request,
                bytes: &chunk.bytes,
            },
            &mut self.metadata,
            self.wire_limits,
        )?;
        let message = Message::multipart([
            Bytes::copy_from_slice(&encoded.header),
            Bytes::copy_from_slice(&self.metadata[..encoded.metadata_bytes]),
            chunk.bytes,
        ]);
        self.enqueue(to, SendClass::Data, message)?;
        Ok(())
    }
}

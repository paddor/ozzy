//! Production recovery core over whole-action disk publication and real codecs.
//! Scheduling here is a deterministic adapter, not the Tokio recovery actor.

use ozzy_proto::RequestId;
use ozzy_replication::recovery::{Recovery, RecoveryError, RecoveryResponse, RecoveryTicket};
use ozzy_replication::wire::{
    FetchOps, RecoveryMessage, RecoveryRequest, RecoveryState, ReplicaMessage,
};

use super::disk::DiskImage;
use super::*;

const RETRY: Duration = Duration::from_millis(20);
const STALLED: Duration = Duration::from_millis(250);

#[derive(Debug)]
pub(super) struct LostState {
    core: Recovery,
    requests: [RecoveryRequest; 3],
    replies: [Option<RecoveryResponse>; 3],
    ticket: Option<RecoveryTicket>,
    fetch: Option<FetchOps>,
    retry_at: Duration,
    progress_at: Duration,
    abandon: bool,
}

#[derive(Debug)]
pub(super) struct Donor {
    request: RecoveryRequest,
    response: RecoveryResponse,
    history: History,
    captured: bool,
    reply: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum RecoveryDisk {
    Capture {
        requester: usize,
        response: RecoveryResponse,
        operations: Vec<Operation>,
    },
    Stage {
        ticket: RecoveryTicket,
        operations: Vec<Operation>,
    },
    Publish(RecoveryTicket),
}

pub(super) fn perform(
    action: &RecoveryDisk,
    stable: &mut DiskImage,
    buffered: &mut Vec<Operation>,
) -> Option<Prefix> {
    match action {
        RecoveryDisk::Capture {
            response,
            operations,
            ..
        } => {
            assert!(stable.admitted);
            assert_eq!(tail(operations), response.primary.unwrap().accepted);
            assert_eq!(
                buffered.get(..operations.len()),
                Some(operations.as_slice())
            );
            // A real donor capture syncs submitted writes, without completing a
            // normal core sync ticket or granting any quorum vote.
            stable.operations.clone_from(buffered);
        }
        RecoveryDisk::Stage { ticket, operations } => {
            assert!(!stable.admitted);
            assert!(!operations.is_empty());
            assert!(buffered.len() + operations.len() <= HISTORY_LIMIT);
            assert_eq!(operations[0].previous, tail(buffered).digest);
            assert_eq!(operations[0].number, tail(buffered).op.0 + 1);
            assert!(tail(operations).op <= ticket.source().accepted.op);
            buffered.extend_from_slice(operations);
        }
        RecoveryDisk::Publish(ticket) => {
            assert!(!stable.admitted);
            assert_eq!(tail(buffered), ticket.source().accepted);
            assert_eq!(
                prefix(buffered, ticket.committed().op.0 as usize),
                ticket.committed()
            );
            // Exercise actual canonical replay, not a fabricated validation bit.
            // It remains private until fenced election/activation after reopen.
            let _selected = private_replay(buffered);
            let committed = private_replay(&buffered[..ticket.committed().op.0 as usize]);
            let applied = prefix(buffered, committed.committed().revision() as usize);
            assert_eq!(applied, ticket.committed());
            *stable = DiskImage {
                admitted: true,
                promised: ticket.scope(),
                last_normal_view: ticket.scope().view,
                committed: ticket.committed(),
                operations: buffered.clone(),
            };
            return Some(applied);
        }
    }
    None
}

impl Replica {
    pub(super) fn begin_recovery(&mut self, now: Duration) {
        assert!(!self.stable.admitted);
        assert!(self.driver.is_none());
        assert!(self.io.is_empty());
        self.generation = self.fresh_generation();
        if let Some(storage) = &mut self.storage
            && let Err(error) = storage.begin_staging(self.generation)
        {
            self.storage_error = Some(error.to_string());
            self.power_cut();
            return;
        }
        self.next_request += 1;
        let nonce = RequestId::from_bytes(self.next_request.to_be_bytes());
        let requests = std::array::from_fn(|_| {
            self.next_request += 1;
            RecoveryRequest {
                scope: self.configuration.scope(),
                request_id: RequestId::from_bytes(self.next_request.to_be_bytes()),
                nonce,
            }
        });
        self.buffered.clear();
        self.recovering = Some(LostState {
            core: Recovery::new(
                self.configuration,
                node(self.id),
                self.generation,
                nonce,
                limits(),
            )
            .unwrap(),
            requests,
            replies: [None; 3],
            ticket: None,
            fetch: None,
            retry_at: now,
            progress_at: now,
            abandon: false,
        });
    }

    pub(super) fn receive_recovery_request(&mut self, from: usize, message: &RecoveryMessage) {
        let RecoveryMessage::Request(request) = *message else {
            return;
        };
        let Some(snapshot) = self
            .snapshot()
            .filter(|s| s.ready_for_appends && self.candidate.is_none())
        else {
            return;
        };
        if self.donors[from].as_ref().is_none_or(|donor| {
            donor.request.nonce != request.nonce || donor.response.scope != snapshot.scope
        }) {
            let response = self
                .driver
                .as_ref()
                .unwrap()
                .normal()
                .unwrap()
                .recovery_response(request.nonce)
                .unwrap();
            self.donors[from] = Some(Donor {
                request,
                response,
                history: History {
                    source: LogSource {
                        voter: node(self.id),
                        generation: self.generation,
                        accepted: snapshot.accepted,
                    },
                    operations: self.accepted.clone(),
                },
                captured: response.primary.is_none(),
                reply: true,
            });
        }
        let donor = self.donors[from].as_mut().unwrap();
        donor.request = request;
        donor.reply = true;
    }

    pub(super) fn donor_history(&self, requester: usize) -> Option<&History> {
        self.donors[requester]
            .as_ref()
            .filter(|donor| donor.captured && donor.response.primary.is_some())
            .map(|donor| &donor.history)
    }

    pub(super) fn pump_donors(&mut self, output: &mut Vec<Packet>) {
        let Some(snapshot) = self
            .snapshot()
            .filter(|s| s.ready_for_appends && self.candidate.is_none())
        else {
            return;
        };
        let mut capture = None;
        for (requester, donor) in self.donors.iter_mut().enumerate() {
            let Some(donor) = donor
                .as_mut()
                .filter(|donor| donor.response.scope == snapshot.scope)
            else {
                continue;
            };
            if donor.captured && donor.reply {
                output.push(Packet::recovery_state(
                    self.id,
                    requester,
                    &RecoveryState {
                        request_id: donor.request.request_id,
                        response: donor.response,
                    },
                ));
                donor.reply = false;
            } else if !donor.captured && self.io.is_empty() && capture.is_none() {
                capture = Some(RecoveryDisk::Capture {
                    requester,
                    response: donor.response,
                    operations: donor.history.operations.clone(),
                });
            }
        }
        if let Some(capture) = capture {
            self.enqueue(DiskAction::Recovery(capture));
        }
    }

    pub(super) fn receive_recovery(&mut self, packet: &Packet, now: Duration) {
        let Ok(message) = packet.decode_for(self.configuration, self.flow.epoch()) else {
            return;
        };
        let recovery = self.recovering.as_mut().unwrap();
        match message {
            ReplicaMessage::Recovery(RecoveryMessage::State(state)) => {
                if state
                    .validate_response(recovery.requests[packet.from])
                    .is_err()
                {
                    return;
                }
                recovery
                    .core
                    .receive(node(packet.from), state.response)
                    .unwrap();
                if recovery.replies[packet.from]
                    .is_none_or(|old| state.response.scope.view > old.scope.view)
                {
                    recovery.progress_at = now;
                    recovery.replies[packet.from] = Some(state.response);
                }
                if recovery
                    .ticket
                    .is_some_and(|ticket| state.response.scope.view > ticket.scope().view)
                {
                    recovery.abandon = true;
                }
            }
            ReplicaMessage::Ops(batch) => {
                let Some(fetch) = recovery.fetch else { return };
                if recovery.abandon
                    || !self.io.is_empty()
                    || batch.validate_response(fetch).is_err()
                {
                    return;
                }
                let action = RecoveryDisk::Stage {
                    ticket: recovery.ticket.unwrap(),
                    operations: batch
                        .operations()
                        .map(|operation| wire::own(operation, self.configuration))
                        .collect(),
                };
                recovery.fetch = None;
                self.enqueue(DiskAction::Recovery(action));
            }
            // Nonvoting core receives fresh recovery responses only in this
            // driver model. Actor authority-hint filtering is tested separately.
            _ => {}
        }
    }

    pub(super) fn pump_recovery(&mut self, now: Duration) -> Vec<Packet> {
        let recovery = self.recovering.as_mut().unwrap();
        recovery.abandon |= now.saturating_sub(recovery.progress_at) >= STALLED;
        if !self.io.is_empty() {
            return Vec::new();
        }
        if recovery.abandon {
            // A publication can have completed before its callback/view check.
            // Intact reopen still fences; incomplete state starts a fresh nonce.
            self.reopen(now);
            return Vec::new();
        }
        if recovery.ticket.is_none() {
            match recovery.core.begin_transfer() {
                Ok(ticket) => {
                    recovery.ticket = Some(ticket);
                    recovery.progress_at = now;
                }
                Err(RecoveryError::QuorumMissing | RecoveryError::PrimaryMissing) => {}
                Err(error) => panic!("recovery selection failed: {error:?}"),
            }
        }
        if let Some(ticket) = recovery.ticket {
            if tail(&self.buffered) == ticket.source().accepted {
                self.enqueue(DiskAction::Recovery(RecoveryDisk::Publish(ticket)));
                return Vec::new();
            }
            if recovery.fetch.is_none() {
                self.next_request += 1;
                recovery.fetch = Some(FetchOps {
                    scope: ticket.scope(),
                    request_id: RequestId::from_bytes(self.next_request.to_be_bytes()),
                    source: ticket.source(),
                    predecessor: tail(&self.buffered),
                    max_operations: PIPELINE as u32,
                    max_body_bytes: BODY_BYTES as u32,
                });
                recovery.retry_at = now;
            }
        }
        if now < recovery.retry_at {
            return Vec::new();
        }
        recovery.retry_at = now + RETRY;
        let mut packets = Vec::new();
        for to in 0..3 {
            if to != self.id {
                packets.push(Packet::recovery(self.id, to, recovery.requests[to]));
            }
        }
        if let Some(fetch) = recovery.fetch {
            packets.push(Packet::fetch(self.id, index(fetch.source.voter), fetch));
        }
        packets
    }

    pub(super) fn complete_recovery_disk(
        &mut self,
        action: &RecoveryDisk,
        applied: Option<Prefix>,
        now: Duration,
    ) {
        match action {
            RecoveryDisk::Capture {
                requester,
                response,
                ..
            } => {
                if let Some(donor) = self.donors[*requester]
                    .as_mut()
                    .filter(|donor| donor.response == *response)
                {
                    donor.captured = true;
                }
            }
            RecoveryDisk::Stage { ticket, operations } => {
                assert_eq!(ticket.generation(), self.generation);
                let recovery = self.recovering.as_mut().unwrap();
                let metadata: Vec<_> = operations.iter().map(Operation::metadata).collect();
                match recovery.core.validate_chunk(*ticket, &metadata) {
                    Ok(()) => recovery.progress_at = now,
                    Err(RecoveryError::StaleView) => recovery.abandon = true,
                    Err(error) => panic!("recovery chunk failed: {error:?}"),
                }
            }
            RecoveryDisk::Publish(ticket) => {
                assert_eq!(ticket.generation(), self.generation);
                let recovery = self.recovering.as_mut().unwrap();
                match recovery.core.complete(
                    *ticket,
                    tail(&self.stable.operations),
                    applied.expect("publication performed actual committed-state replay"),
                ) {
                    Ok(state) => {
                        assert_eq!(state.scope, self.stable.promised);
                        assert_eq!(state.log.accepted, tail(&self.stable.operations));
                        self.reopen(now);
                    }
                    Err(RecoveryError::StaleView) => recovery.abandon = true,
                    Err(error) => panic!("recovery publication failed: {error:?}"),
                }
            }
        }
    }
}

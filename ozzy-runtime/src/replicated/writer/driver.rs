use crate::signal::DataSignal;
use crate::transport::Frames;
use bytes::Bytes;
use omq_tokio::message::Payload;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::append::PayloadEncoding;
use ozzy_proto::append::{Authority, stream};
use ozzy_proto::nack::{self, AuthorityHint, RetryClass};
use ozzy_proto::{Envelope, NodeId, Opcode, decode_packet};

use super::batch::{Batch, PayloadView, RequestView};
use super::{Error, RetryPolicy, Shared};

mod shared;
mod window;
use crate::replicated::broker_links::{OwnerClock as Clock, append};
pub(super) use shared::{Target, run_shared};
use window::{Sent, Window};

const TURN_RECORDS: usize = 1024;
// Scheduling is independent of request size. A one-record request must not
// force a yield after every socket send. Large indivisible requests may exceed
// this byte quantum, but always yield before preparing another request.
const TURN_BYTES: usize = 2 * 1024 * 1024;

enum Failure {
    Retry(Option<AuthorityHint>),
    /// The broker asked to try again later. It stays the destination.
    Later,
    Fatal(Error),
}

struct Prepared {
    message: Message,
    sent: Sent,
    payload_bytes: usize,
    retained_bytes: usize,
}

/// Fixed authenticated link and authority for one routed producer attempt.
struct SessionBinding<'a> {
    connection: &'a mut append::Connection,
    remote: NodeId,
    local: NodeId,
    authority: Authority,
    retry: RetryPolicy,
}

struct Session {
    route: Bytes,
    frames: crate::native_frames::ControlFrames,
    window: Window,
    pending: Option<Prepared>,
    metadata: Vec<u8>,
    incoming: Vec<Message>,
    deadline: Option<std::time::Duration>,
    retry_at: Option<std::time::Duration>,
    refused: Option<ozzy_proto::RequestId>,
    request_capacity: usize,
    clock: Clock,
    batch: Batch,
    compressor: Option<super::compression::Compressor>,
}

impl Session {
    fn new(shared: &Shared, remote: NodeId, clock: Clock) -> Self {
        let first = shared.progress.confirmed();
        Self {
            route: Bytes::copy_from_slice(remote.as_bytes()),
            frames: crate::native_frames::ControlFrames::new(shared.config.inflight_appends),
            window: Window::new(first, shared.config.inflight_appends),
            pending: None,
            metadata: Vec::with_capacity(shared.config.limits.envelope.max_metadata_bytes),
            incoming: Vec::with_capacity(TURN_RECORDS),
            deadline: None,
            retry_at: None,
            refused: None,
            request_capacity: shared.config.inflight_appends,
            clock,
            batch: Batch::new(),
            compressor: shared
                .config
                .compress_payloads
                .then(|| super::compression::Compressor::new(shared.config.inflight_appends)),
        }
    }

    async fn run(
        mut self,
        shared: &mut super::state::Driver,
        mut binding: SessionBinding<'_>,
    ) -> Result<(), Failure> {
        loop {
            // A failed or closed writer leaves here. Failing marks its work, so
            // without this check every turn would find work again and never
            // yield the SDK thread to the stop or to other writers.
            if shared.stop.is_closed() {
                return Ok(());
            }
            if self.retry_at.is_some_and(|at| self.clock.now() >= at) && self.replay_ready() {
                self.restart(shared, &mut binding);
            }
            if self
                .deadline
                .is_some_and(|deadline| self.clock.now() >= deadline)
            {
                crate::profiling::event(crate::profiling::Event::SdkResponseTimeout);
                return Err(Failure::Retry(None));
            }
            let work = shared.work.clone();
            // Admitted records only matter while a transmission can be
            // prepared. Leaving the intake flag set otherwise means a full
            // window costs each push one atomic swap instead of a task wake.
            let (filled, receive_full) = if self.can_prepare() {
                work.drain(|| self.turn(shared, &mut binding))?
            } else {
                self.turn(shared, &mut binding)?
            };
            if filled || receive_full {
                // Bound executor work even when both sockets and application stay ready.
                tokio::task::yield_now().await;
                continue;
            }
            self.wait(&work, &binding).await?;
        }
    }

    /// Preserve the physical session and drain earlier confirmations before
    /// replaying the remaining prefix. Old request IDs cannot confirm the new
    /// window. Transport aliases retain their local backing charge.
    fn restart(&mut self, shared: &super::state::Driver, binding: &mut SessionBinding<'_>) {
        binding.connection.forget_requests();
        self.window = Window::new(shared.progress.confirmed(), shared.config.inflight_appends);
        self.pending = None;
        self.retry_at = None;
        self.refused = None;
        self.deadline = None;
    }

    /// Apply ready replies, then send what the window allows. Whether the
    /// send turn and the receive turn each filled their bound.
    fn turn(
        &mut self,
        shared: &mut super::state::Driver,
        binding: &mut SessionBinding<'_>,
    ) -> Result<(bool, bool), Failure> {
        match binding
            .connection
            .try_recv_many_into(TURN_RECORDS, &mut self.incoming)
        {
            Ok(_) | Err(omq_tokio::Error::WouldBlock) => {}
            Err(_) => return Err(Failure::Retry(None)),
        }
        let incoming = std::mem::take(&mut self.incoming);
        let receive_full = incoming.len() == TURN_RECORDS;
        for message in &incoming {
            self.receive(shared, binding, message)?;
        }
        self.incoming = incoming;
        self.incoming.clear();
        let filled = self.send_ready(shared, binding)?;
        Ok((filled, receive_full))
    }

    /// Wait for replies, admitted records while a request could be prepared,
    /// send progress of a blocked request, the response deadline, or a retry.
    async fn wait(&self, work: &DataSignal, binding: &SessionBinding<'_>) -> Result<(), Failure> {
        // Confirmations in this turn may have emptied the window. An intake
        // flag left set by a skipped drain then wakes the next turn at once.
        let wants_records = self.can_prepare();
        let retry_at = self.retry_at.filter(|_| self.replay_ready());
        tokio::select! {
            result = binding.connection.recv_ready() => {
                result.map_err(|_| Failure::Retry(None))?;
            }
            () = work.ready(), if wants_records => {}
            () = async {
                match &self.pending {
                    Some(pending) => binding.connection.wait_send_progress_for(&pending.message).await,
                    None => std::future::pending().await,
                }
            } => {}
            () = self.clock.until_due(self.deadline) => {
                crate::profiling::event(crate::profiling::Event::SdkResponseTimeout);
                return Err(Failure::Retry(None));
            },
            () = self.clock.until_due(retry_at) => {},
        }
        Ok(())
    }

    /// Whether a new request or record could be prepared this turn: nothing
    /// is blocked on transport and the negotiated window has room.
    fn can_prepare(&self) -> bool {
        self.retry_at.is_none()
            && self.pending.is_none()
            && self.window.requests() < self.request_capacity
    }

    fn replay_ready(&self) -> bool {
        self.refused.is_none_or(|id| !self.window.has_prior(id))
    }

    /// Replay after the initial backoff, unless a replay is already due.
    fn back_off(&mut self, retry: &RetryPolicy) {
        self.retry_at
            .get_or_insert_with(|| self.clock.now().saturating_add(retry.initial_backoff));
    }

    fn send_ready(
        &mut self,
        shared: &mut super::state::Driver,
        binding: &mut SessionBinding<'_>,
    ) -> Result<bool, Failure> {
        if self.retry_at.is_some() {
            return Ok(false);
        }
        // The session was negotiated before this owner started sending.
        let parameters = binding
            .connection
            .parameters()
            .ok_or(Failure::Retry(None))?;
        let limits = shared.config.limits.intersection(parameters.receive);
        let mut sent_records = 0;
        let mut sent_bytes = 0;
        while sent_records < TURN_RECORDS {
            let pending = match self.pending.take() {
                Some(pending) => Some(pending),
                None => self.next_request(shared, binding, limits)?,
            };
            let Some(mut pending) = pending else {
                return Ok(false);
            };
            let records = (pending.sent.end - self.window.next()) as usize;
            let outgoing = append::Outgoing {
                id: pending.sent.id,
                end: pending.sent.end,
                records,
                retained_bytes: pending.retained_bytes,
            };
            match binding.connection.try_send(pending.message, outgoing) {
                Ok(()) => {
                    pending.sent.admitted_at = crate::profiling::start();
                    shared.stats.record(
                        (pending.sent.end - self.window.next()) as usize,
                        pending.payload_bytes,
                        self.window.requests() + 1,
                    );
                    sent_records += (pending.sent.end - self.window.next()) as usize;
                    sent_bytes += pending.payload_bytes;
                    self.window.sent(pending.sent);
                    self.deadline.get_or_insert_with(|| {
                        self.clock
                            .now()
                            .saturating_add(binding.retry.response_timeout)
                    });
                    if sent_bytes >= TURN_BYTES {
                        return Ok(true);
                    }
                }
                Err(TrySendError::Full(message)) => {
                    pending.message = message;
                    self.pending = Some(pending);
                    return Ok(false);
                }
                Err(TrySendError::Error(error @ omq_tokio::Error::Config(_))) => {
                    return Err(Failure::Fatal(Error::Transport(error)));
                }
                Err(TrySendError::Error(_) | TrySendError::Closed) => {
                    return Err(Failure::Retry(None));
                }
            }
        }
        Ok(true)
    }

    /// The LZ4 form of a request body when compression is on and it is
    /// smaller; `None` sends the body raw.
    fn compress(&mut self, decoded: &Payload) -> Result<Option<Payload>, Failure> {
        self.compressor
            .as_mut()
            .map_or(Ok(None), |compressor| {
                compressor.prepare(decoded.as_slice())
            })
            .map_err(|_| Failure::Fatal(Error::AppendCompression))
    }

    /// Select, pack, and frame the next request, if the window and queue allow.
    fn next_request(
        &mut self,
        shared: &mut super::state::Driver,
        binding: &SessionBinding<'_>,
        limits: ozzy_proto::append::DataLimits,
    ) -> Result<Option<Prepared>, Failure> {
        // A partially confirmed request still occupies one slot. A
        // transport-blocked prepared request reserves the next slot;
        // neither queue admission nor partial confirmations release it.
        if self.window.requests() >= self.request_capacity {
            return Ok(None);
        }
        let next = self.window.next();
        let in_flight = self.window.requests() != 0;
        if !self
            .batch
            .select(next, limits, in_flight, shared)
            .map_err(Failure::Fatal)?
        {
            if self.batch.waiting_for_payload {
                // A revisited route can still own old packed frames.
                // Keep failure detection active while waiting for OMQ
                // to release them, so other brokers remain reachable.
                self.deadline.get_or_insert_with(|| {
                    self.clock
                        .now()
                        .saturating_add(binding.retry.response_timeout)
                });
            }
            return Ok(None);
        }
        let Some(decoded_payload) = self.batch.take_payload() else {
            return Ok(None);
        };
        self.prepare(shared, binding, limits, decoded_payload)
            .map(Some)
    }

    fn prepare(
        &mut self,
        shared: &mut super::state::Driver,
        binding: &SessionBinding<'_>,
        limits: ozzy_proto::append::DataLimits,
        decoded_payload: Payload,
    ) -> Result<Prepared, Failure> {
        let compressed = self.compress(&decoded_payload)?;
        let (payload_encoding, encoded_payload_bytes) = compressed
            .as_ref()
            .map_or((PayloadEncoding::Raw, decoded_payload.len()), |encoded| {
                (PayloadEncoding::Lz4, encoded.len())
            });
        let id = binding
            .connection
            .next_request()
            .map_err(|error| Failure::Fatal(error.into()))?;
        let request = RequestView {
            envelope: Envelope {
                opcode: Opcode::Append,
                response: false,
                request_id: Some(id),
                sender: binding.local,
                session: binding.connection.session(),
            },
            authority: binding.authority,
            payload: PayloadView {
                decoded: decoded_payload.as_slice(),
                encoding: payload_encoding,
                encoded_bytes: encoded_payload_bytes,
            },
        };
        let header = self
            .batch
            .encode(shared, request, &mut self.metadata, limits)
            .map_err(Failure::Fatal)?;
        let bytes = self.batch.bytes;
        let end = self.window.next() + self.batch.records.len() as u64;
        let payload = compressed.unwrap_or(decoded_payload);
        let prepared = Prepared {
            message: self
                .frames
                .message(&self.route, header, &self.metadata, payload),
            sent: Sent {
                id,
                start: self.window.next(),
                end,
                bytes,
                admitted_at: None,
            },
            payload_bytes: encoded_payload_bytes,
            // Packing and LZ4 vectors retain growth capacity. A pooled
            // control view can retain a whole 64 KiB chunk.
            retained_bytes: shared.config.transport_backing_bytes(),
        };
        shared.stage(end);
        self.batch.records.clear();
        self.deadline.get_or_insert_with(|| {
            self.clock
                .now()
                .saturating_add(binding.retry.response_timeout)
        });
        Ok(prepared)
    }

    fn receive(
        &mut self,
        shared: &mut super::state::Driver,
        binding: &SessionBinding<'_>,
        message: &Message,
    ) -> Result<(), Failure> {
        let Some(frames) = frames(message, binding.remote) else {
            return Ok(());
        };
        let Ok(packet) = decode_packet(
            &frames.each_ref().map(AsRef::as_ref),
            shared.config.limits.envelope,
        ) else {
            return Ok(());
        };
        if packet.envelope.sender != binding.remote
            || packet.envelope.session != binding.connection.session()
            || !packet.envelope.response
        {
            return Ok(());
        }
        let Some(id) = packet
            .envelope
            .request_id
            .filter(|&id| self.window.correlates(id))
        else {
            return Ok(());
        };
        if packet.envelope.opcode == Opcode::Nack {
            return self.receive_nack(shared, binding, packet, id);
        }
        self.receive_confirmation(shared, binding, packet, id)
    }

    /// A NACK of request `id`. A refusal to try later backs off and replays
    /// from the earliest refused request; any other fails or retries.
    fn receive_nack(
        &mut self,
        shared: &super::state::Driver,
        binding: &SessionBinding<'_>,
        packet: ozzy_proto::Packet<'_>,
        id: ozzy_proto::RequestId,
    ) -> Result<(), Failure> {
        let reply = nack::decode(packet, shared.config.limits.envelope)
            .map_err(|_| Failure::Fatal(Error::Response))?;
        if reply.retry == RetryClass::AfterBackoff {
            crate::profiling::event(crate::profiling::Event::SdkAdmissionRefusal);
            if self
                .refused
                .is_none_or(|earlier| self.window.precedes(id, earlier))
            {
                self.refused = Some(id);
            }
            self.back_off(&binding.retry);
            return Ok(());
        }
        reject_packet(packet, shared.config.limits.envelope)
    }

    /// The confirmation of request `id`, applied in sequence order only.
    fn receive_confirmation(
        &mut self,
        shared: &mut super::state::Driver,
        binding: &SessionBinding<'_>,
        packet: ozzy_proto::Packet<'_>,
        id: ozzy_proto::RequestId,
    ) -> Result<(), Failure> {
        let apply_at = crate::profiling::start();
        let reply = stream::decode_confirmed(packet, shared.config.limits.envelope)
            .map_err(|error| Failure::Fatal(error.into()))?;
        if !confirms_producer(&reply, binding, &shared.config) {
            return Err(Failure::Fatal(Error::Response));
        }
        let key = reply.key;
        let end_sequence = reply.end_sequence;
        let confirmed = shared.progress.confirmed();
        if self
            .window
            .skips(id, key.first_sequence, end_sequence, confirmed)
        {
            // The broker holds the records before this range from an earlier
            // attempt, and refused or has not yet answered their resent
            // request. Only their own confirmation carries their offsets.
            self.back_off(&binding.retry);
            return Ok(());
        }
        if !self
            .window
            .accepts(id, key.first_sequence, end_sequence, confirmed)
        {
            return Err(Failure::Fatal(Error::Response));
        }
        if end_sequence <= confirmed {
            return Ok(());
        }
        let confirmed_bytes = shared
            .confirm_outcomes(
                key.first_sequence,
                reply
                    .outcomes()
                    .map_err(|error| Failure::Fatal(error.into()))?,
            )
            .map_err(|_| Failure::Fatal(Error::Response))?;
        self.window.confirm(end_sequence, confirmed_bytes);
        binding.connection.confirm(end_sequence);
        self.deadline = (!self.window.is_empty() || self.pending.is_some()).then(|| {
            self.clock
                .now()
                .saturating_add(binding.retry.response_timeout)
        });
        crate::profiling::finish(crate::profiling::Stage::ConfirmationApply, apply_at);
        Ok(())
    }
}

/// Whether `reply` confirms this producer: its authority, partition, owner
/// and producer epochs, producer identity, and policy.
fn confirms_producer(
    reply: &stream::Confirmed,
    binding: &SessionBinding<'_>,
    config: &super::WriterConfig,
) -> bool {
    reply.authority == binding.authority
        && reply.partition == config.partition
        && reply.owner_epoch == config.owner_epoch
        && reply.key.producer_id == config.producer_id
        && reply.key.producer_epoch == config.producer_epoch
        && reply.policy == config.policy
}

fn reject_packet(
    packet: ozzy_proto::Packet<'_>,
    limits: ozzy_proto::EnvelopeLimits,
) -> Result<(), Failure> {
    let reply = nack::decode(packet, limits).map_err(|_| Failure::Fatal(Error::Response))?;
    let hint = if matches!(reply.code, 5 | 12 | 13) {
        Some(AuthorityHint::decode(reply.detail).map_err(|_| Failure::Fatal(Error::Response))?)
    } else {
        None
    };
    Err(if reply.retry == RetryClass::Permanent {
        Failure::Fatal(Error::Rejected {
            code: reply.code,
            retry: reply.retry,
            hint,
        })
    } else {
        Failure::Retry(hint)
    })
}

fn frames(message: &Message, remote: NodeId) -> Option<[Bytes; 3]> {
    let (route, frames) = message.routed_packet_bytes()?;
    (route.as_ref() == remote.as_bytes()).then_some(frames)
}

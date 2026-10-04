use bytes::Bytes;
use omq_tokio::message::Payload;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::append::PayloadEncoding;
use ozzy_proto::append::{Authority, stream};
use ozzy_proto::nack::{self, AuthorityHint, RetryClass};
use ozzy_proto::{Envelope, NodeId, Opcode, decode_packet};

use super::batch::{Batch, PayloadView};
use super::{Error, RetryPolicy, Shared};

mod shared;
mod window;
use crate::replicated::{SdkClock as Clock, broker_links::append};
pub(super) use shared::run_shared;
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
    authority: Option<Authority>,
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
            if self.retry_at.is_some_and(|at| self.clock.now() >= at) && self.replay_ready() {
                // Preserve the physical session and drain earlier confirmations
                // before replaying the remaining prefix. Old request IDs cannot
                // confirm the new window. Transport aliases retain their local backing charge.
                binding.connection.forget_requests();
                self.window =
                    Window::new(shared.progress.confirmed(), shared.config.inflight_appends);
                self.pending = None;
                self.retry_at = None;
                self.refused = None;
                self.deadline = None;
            }
            if self
                .deadline
                .is_some_and(|deadline| self.clock.now() >= deadline)
            {
                return Err(Failure::Retry(None));
            }
            let work = shared.work.clone();
            // Admitted records only matter while a transmission can be
            // prepared. Leaving the intake flag set otherwise means a full
            // window costs each push one atomic swap instead of a task wake.
            let wants_records = self.can_prepare();
            let mut turn = || {
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
                    self.receive(shared, &binding, message)?;
                }
                self.incoming = incoming;
                self.incoming.clear();
                let filled = self.send_ready(shared, &mut binding)?;
                Ok((filled, receive_full))
            };
            let (filled, receive_full) = if wants_records {
                work.drain(turn)?
            } else {
                turn()?
            };
            if filled || receive_full {
                // Bound executor work even when both sockets and application stay ready.
                tokio::task::yield_now().await;
                continue;
            }
            // Confirmations in this turn may have emptied the window. An intake
            // flag left set by a skipped drain then wakes the next turn at once.
            let wants_records = self.can_prepare();
            let deadline = self.deadline;
            let retry_at = self.retry_at.filter(|_| self.replay_ready());
            tokio::select! {
                message = binding.connection.recv() => {
                    let message = message.map_err(|_| Failure::Retry(None))?;
                    self.receive(shared, &binding, &message)?;
                }
                () = work.ready(), if wants_records => {}
                () = async {
                    match &self.pending {
                        Some(pending) => binding.connection.wait_send_progress_for(&pending.message).await,
                        None => std::future::pending().await,
                    }
                } => {}
                () = async {
                    match deadline {
                        Some(deadline) => self.clock.until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {
                    return Err(Failure::Retry(None));
                },
                () = async {
                    match retry_at {
                        Some(at) => self.clock.until(at).await,
                        None => std::future::pending().await,
                    }
                } => {},
            }
        }
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

    fn send_ready(
        &mut self,
        shared: &mut super::state::Driver,
        binding: &mut SessionBinding<'_>,
    ) -> Result<bool, Failure> {
        if self.retry_at.is_some() {
            return Ok(false);
        }
        let parameters = binding.connection.parameters().expect("negotiated session");
        let limits = shared.config.limits.intersection(parameters.receive);
        let mut sent_records = 0;
        let mut sent_bytes = 0;
        while sent_records < TURN_RECORDS {
            if self.pending.is_none() {
                // A partially confirmed request still occupies one slot. A
                // transport-blocked prepared request reserves the next slot;
                // neither queue admission nor partial confirmations release it.
                if self.window.requests() >= self.request_capacity {
                    return Ok(false);
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
                    return Ok(false);
                }
                let Some(decoded_payload) = self.batch.take_payload() else {
                    return Ok(false);
                };
                let compressed = match self.compressor.as_mut() {
                    Some(compressor) => compressor
                        .prepare(decoded_payload.as_slice())
                        .map_err(|_| Failure::Fatal(Error::AppendCompression))?,
                    None => None,
                };
                self.install_pending(shared, binding, limits, decoded_payload, compressed)?;
            }
            let mut pending = self.pending.take().expect("prepared request");
            let records = (pending.sent.end - self.window.next()) as usize;
            match binding.connection.try_send(
                pending.message,
                pending.sent.id,
                pending.sent.end,
                records,
                pending.retained_bytes,
            ) {
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

    fn install_pending(
        &mut self,
        shared: &mut super::state::Driver,
        binding: &SessionBinding<'_>,
        limits: ozzy_proto::append::DataLimits,
        decoded_payload: Payload,
        compressed: Option<Payload>,
    ) -> Result<(), Failure> {
        let (payload_encoding, encoded_payload_bytes) = compressed
            .as_ref()
            .map_or((PayloadEncoding::Raw, decoded_payload.len()), |encoded| {
                (PayloadEncoding::Lz4, encoded.len())
            });
        let id = binding
            .connection
            .next_request()
            .map_err(|error| Failure::Fatal(error.into()))?;
        let envelope = Envelope {
            opcode: Opcode::Append,
            response: false,
            request_id: Some(id),
            sender: binding.local,
            session: binding.connection.session(),
        };
        let header = self
            .batch
            .encode(
                shared,
                binding.authority,
                envelope,
                PayloadView {
                    decoded: decoded_payload.as_slice(),
                    encoding: payload_encoding,
                    encoded_bytes: encoded_payload_bytes,
                },
                &mut self.metadata,
                limits,
            )
            .map_err(Failure::Fatal)?;
        let bytes = self.batch.bytes;
        let end = self.window.next() + self.batch.records.len() as u64;
        let payload = compressed.unwrap_or(decoded_payload);
        self.pending = Some(Prepared {
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
        });
        shared.stage(end);
        self.batch.records.clear();
        self.deadline.get_or_insert_with(|| {
            self.clock
                .now()
                .saturating_add(binding.retry.response_timeout)
        });
        Ok(())
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
            || !self.window.correlates(packet.envelope.request_id)
        {
            return Ok(());
        }
        if packet.envelope.opcode == Opcode::Nack {
            let reply = nack::decode(packet, shared.config.limits.envelope)
                .map_err(|_| Failure::Fatal(Error::Response))?;
            if reply.retry == RetryClass::AfterBackoff {
                crate::profiling::event(crate::profiling::Event::SdkAdmissionRefusal);
                let id = packet.envelope.request_id.expect("correlated request");
                if self
                    .refused
                    .is_none_or(|earlier| self.window.precedes(id, earlier))
                {
                    self.refused = Some(id);
                }
                self.retry_at.get_or_insert_with(|| {
                    self.clock
                        .now()
                        .saturating_add(binding.retry.initial_backoff)
                });
                return Ok(());
            }
            return reject_packet(
                packet,
                shared.config.limits.envelope,
                binding.authority.is_some(),
            );
        }
        let apply_at = crate::profiling::start();
        let reply = stream::decode_confirmed(packet, shared.config.limits.envelope)
            .map_err(|error| Failure::Fatal(error.into()))?;
        if Some(reply.authority) != binding.authority || reply.partition != shared.config.partition
        {
            return Err(Failure::Fatal(Error::Response));
        }
        let stream::Confirmed {
            owner_epoch,
            key,
            policy,
            end_sequence,
            first_offset,
            ..
        } = reply;
        let confirmed = shared.progress.confirmed();
        if owner_epoch != shared.config.owner_epoch
            || key.producer_id != shared.config.producer_id
            || key.producer_epoch != shared.config.producer_epoch
            || policy != shared.config.policy
        {
            return Err(Failure::Fatal(Error::Response));
        }
        let id = packet.envelope.request_id.expect("correlated request");
        if self
            .window
            .skips(id, key.first_sequence, end_sequence, confirmed)
        {
            // The broker holds the records before this range from an earlier
            // attempt, and refused or has not yet answered their resent
            // request. Only their own confirmation carries their offsets.
            self.retry_at.get_or_insert_with(|| {
                self.clock
                    .now()
                    .saturating_add(binding.retry.initial_backoff)
            });
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
            .confirm(key.first_sequence, end_sequence, first_offset)
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

fn reject_packet(
    packet: ozzy_proto::Packet<'_>,
    limits: ozzy_proto::EnvelopeLimits,
    routed: bool,
) -> Result<(), Failure> {
    let reply = nack::decode(packet, limits).map_err(|_| Failure::Fatal(Error::Response))?;
    let hint = if routed && matches!(reply.code, 5 | 12 | 13) {
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
    if message.len() != 4 || message.part_slice(0)? != remote.as_bytes() {
        return None;
    }
    Some(std::array::from_fn(|i| {
        message.part_bytes(i + 1).expect("checked frames")
    }))
}

use bytes::Bytes;
use omq_tokio::message::Payload;
use omq_tokio::{Message, TrySendError};
use ozzy_proto::append::PayloadEncoding;
use ozzy_proto::append::{Authority, stream};
use ozzy_proto::nack::{self, AuthorityHint, RetryClass};
use ozzy_proto::{Envelope, NodeId, Opcode, decode_packet};

use super::batch::{Batch, PayloadView};
use super::{DataLimits, Error, PartitionTarget, RetryPolicy, Shared};

mod link;
mod shared;
mod window;
use link::{Clock, Link};
pub(super) use shared::run_shared;
use window::{RequestLimit, Sent, Window};

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
    request_limit: RequestLimit,
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
            request_limit: RequestLimit::new(shared.config.inflight_appends),
            clock,
            batch: Batch::new(shared.config.limits.max_records),
            compressor: shared
                .config
                .compress_payloads
                .then(|| super::compression::Compressor::new(shared.config.inflight_appends)),
        }
    }

    async fn run(
        mut self,
        shared: &mut super::state::Driver,
        connection: Link<'_>,
        remote: NodeId,
        local: NodeId,
        authority: Option<Authority>,
        retry: RetryPolicy,
    ) -> Result<(), Failure> {
        loop {
            if self.retry_at.is_some_and(|at| self.clock.now() >= at) && self.replay_ready() {
                // Preserve the physical session and drain earlier confirmations
                // before replaying the remaining prefix. Old request IDs cannot
                // confirm the new window. Transport aliases retain their credit.
                connection.forget_requests();
                self.window =
                    Window::new(shared.progress.confirmed(), shared.config.inflight_appends);
                self.request_limit.replayed();
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
                match connection.try_recv_many_into(TURN_RECORDS, &mut self.incoming) {
                    Ok(_) | Err(omq_tokio::Error::WouldBlock) => {}
                    Err(_) => return Err(Failure::Retry(None)),
                }
                let incoming = std::mem::take(&mut self.incoming);
                let receive_full = incoming.len() == TURN_RECORDS;
                for message in &incoming {
                    self.receive(shared, connection, remote, authority, message, retry)?;
                }
                self.incoming = incoming;
                self.incoming.clear();
                let filled = self.send_ready(shared, connection, local, authority, retry)?;
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
            let linger = self.batch.deadline;
            tokio::select! {
                message = connection.recv() => {
                    let message = message.map_err(|_| Failure::Retry(None))?;
                    self.receive(shared, connection, remote, authority, &message, retry)?;
                }
                () = work.ready(), if wants_records => {}
                () = async {
                    match &self.pending {
                        Some(pending) => connection.wait_send_progress_for(&pending.message).await,
                        None => std::future::pending().await,
                    }
                } => {}
                () = async {
                    match deadline {
                        Some(deadline) => self.clock.until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => return Err(Failure::Retry(None)),
                () = async {
                    match retry_at {
                        Some(at) => self.clock.until(at).await,
                        None => std::future::pending().await,
                    }
                } => {},
                () = async {
                    match linger {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending().await,
                    }
                } => {}
            }
        }
    }

    /// Whether a new request or record could be prepared this turn: nothing
    /// is blocked on transport and the negotiated window has room.
    fn can_prepare(&self) -> bool {
        self.retry_at.is_none()
            && self.pending.is_none()
            && self.window.requests() < self.request_limit.current()
    }

    fn replay_ready(&self) -> bool {
        self.refused.is_none_or(|id| !self.window.has_prior(id))
    }

    fn send_ready(
        &mut self,
        shared: &mut super::state::Driver,
        connection: Link<'_>,
        local: NodeId,
        authority: Option<Authority>,
        retry: RetryPolicy,
    ) -> Result<bool, Failure> {
        if self.retry_at.is_some() {
            self.batch.deadline = None;
            return Ok(false);
        }
        let parameters = connection.parameters().expect("negotiated session");
        let limits = intersection(shared.config.limits, parameters.receive);
        let mut sent_records = 0;
        let mut sent_bytes = 0;
        self.batch.deadline = None;
        while sent_records < TURN_RECORDS {
            if self.pending.is_none() {
                // A partially confirmed request still occupies one slot. A
                // transport-blocked prepared request reserves the next slot;
                // neither queue admission nor partial confirmations release it.
                if self.window.requests() >= self.request_limit.current() {
                    return Ok(false);
                }
                let records = parameters
                    .inflight_records
                    .saturating_sub(self.window.next() - shared.progress.confirmed())
                    as usize;
                let bytes = parameters
                    .inflight_bytes
                    .saturating_sub(self.window.bytes() as u64)
                    as usize;
                if !self
                    .batch
                    .select(self.window.next(), limits, records, bytes, shared)
                    .map_err(Failure::Fatal)?
                {
                    if self.batch.waiting_for_payload {
                        // A revisited route can still own old packed frames.
                        // Keep failure detection active while waiting for OMQ
                        // to release them, so other brokers remain reachable.
                        self.deadline.get_or_insert_with(|| {
                            self.clock.now().saturating_add(retry.response_timeout)
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
                self.install_pending(
                    shared,
                    connection,
                    local,
                    authority,
                    limits,
                    decoded_payload,
                    compressed,
                    retry,
                )?;
            }
            let mut pending = self.pending.take().expect("prepared request");
            let records = (pending.sent.end - self.window.next()) as usize;
            match connection.try_send(
                pending.message,
                &pending.sent,
                records,
                pending.retained_bytes,
            ) {
                Ok(()) => {
                    if self.request_limit.current() == 1 && shared.config.inflight_appends > 1 {
                        crate::profiling::event(crate::profiling::Event::SdkSingleFlightAppend);
                    }
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
                        self.clock.now().saturating_add(retry.response_timeout)
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

    #[allow(clippy::too_many_arguments)]
    fn install_pending(
        &mut self,
        shared: &mut super::state::Driver,
        connection: Link<'_>,
        local: NodeId,
        authority: Option<Authority>,
        limits: ozzy_proto::append::DataLimits,
        decoded_payload: Payload,
        compressed: Option<Payload>,
        retry: RetryPolicy,
    ) -> Result<(), Failure> {
        let (payload_encoding, encoded_payload_bytes) = compressed
            .as_ref()
            .map_or((PayloadEncoding::Raw, decoded_payload.len()), |encoded| {
                (PayloadEncoding::Lz4, encoded.len())
            });
        let id = connection.next_request().map_err(Failure::Fatal)?;
        let envelope = Envelope {
            opcode: Opcode::Append,
            response: false,
            request_id: Some(id),
            sender: local,
            session: connection.session(),
        };
        let header = self
            .batch
            .encode(
                shared,
                authority,
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
        self.deadline
            .get_or_insert_with(|| self.clock.now().saturating_add(retry.response_timeout));
        Ok(())
    }

    fn receive(
        &mut self,
        shared: &mut super::state::Driver,
        connection: Link<'_>,
        remote: NodeId,
        authority: Option<Authority>,
        message: &Message,
        retry: RetryPolicy,
    ) -> Result<(), Failure> {
        let Some(frames) = frames(message, remote) else {
            return Ok(());
        };
        let Ok(packet) = decode_packet(
            &frames.each_ref().map(AsRef::as_ref),
            shared.config.limits.envelope,
        ) else {
            return Ok(());
        };
        if packet.envelope.sender != remote
            || packet.envelope.session != connection.session()
            || !packet.envelope.response
            || !self.window.correlates(packet.envelope.request_id)
        {
            return Ok(());
        }
        if packet.envelope.opcode == Opcode::Nack {
            let reply = nack::decode(packet, shared.config.limits.envelope)
                .map_err(|_| Failure::Fatal(Error::Response))?;
            if reply.retry == RetryClass::AfterCredit {
                crate::profiling::event(crate::profiling::Event::SdkCreditRefusal);
                let id = packet.envelope.request_id.expect("correlated request");
                if self.refused.is_none() {
                    self.request_limit.refused();
                }
                if self
                    .refused
                    .is_none_or(|earlier| self.window.precedes(id, earlier))
                {
                    self.refused = Some(id);
                }
                self.retry_at
                    .get_or_insert_with(|| self.clock.now().saturating_add(retry.initial_backoff));
                return Ok(());
            }
            return reject_packet(packet, shared.config.limits.envelope, authority.is_some());
        }
        let apply_at = crate::profiling::start();
        let (owner_epoch, key, policy, end_sequence, first_offset) = match &shared.config.partition
        {
            PartitionTarget::Group(partition) => {
                let reply = stream::decode_confirmed(packet, shared.config.limits.envelope)
                    .map_err(|error| Failure::Fatal(error.into()))?;
                if Some(reply.authority) != authority || reply.partition != *partition {
                    return Err(Failure::Fatal(Error::Response));
                }
                (
                    reply.owner_epoch,
                    reply.key,
                    reply.policy,
                    reply.end_sequence,
                    reply.first_offset,
                )
            }
        };
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
            self.retry_at
                .get_or_insert_with(|| self.clock.now().saturating_add(retry.initial_backoff));
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
        let pending_before = self.window.requests();
        self.window.confirm(end_sequence, confirmed_bytes);
        self.request_limit
            .confirmed(pending_before - self.window.requests());
        connection.confirm(end_sequence);
        self.deadline = (!self.window.is_empty() || self.pending.is_some())
            .then(|| self.clock.now().saturating_add(retry.response_timeout));
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

fn intersection(local: DataLimits, remote: DataLimits) -> DataLimits {
    DataLimits {
        envelope: ozzy_proto::EnvelopeLimits {
            max_metadata_bytes: local
                .envelope
                .max_metadata_bytes
                .min(remote.envelope.max_metadata_bytes),
            max_payload_bytes: local
                .envelope
                .max_payload_bytes
                .min(remote.envelope.max_payload_bytes),
        },
        max_records: local.max_records.min(remote.max_records),
        max_record_bytes: local.max_record_bytes.min(remote.max_record_bytes),
        max_parts: local.max_parts.min(remote.max_parts),
    }
}

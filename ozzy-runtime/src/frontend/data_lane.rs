//! Bounded dispatcher/shard handoff over OMQ inproc DEALER/ROUTER.
//! A binary metadata frame precedes the unchanged native frames. Replies return
//! dequeue accounting; one further maximum frame backs the shard's pending slot.

use super::{Binding, Kind, Placement, Routed};
use crate::{dispatch::Class, signal::CloseSignal};
use bytes::Bytes;
use omq_tokio::{Context, Message, Options, Socket, TrySendError};
use ozzy_proto::layout::Fixed;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

/// A routed frame whose authority is rechecked on the destination shard.
#[derive(Debug)]
pub struct DataInput {
    /// Established dispatch-time session.
    pub binding: Binding,
    /// Checked partition and broker-local placement.
    pub route: Routed,
    /// Intact native header, metadata and payload with identity prefix.
    pub message: Message,
}

#[derive(Debug, Default)]
struct Returned {
    bytes: AtomicUsize,
    slots: AtomicUsize,
    generation: AtomicU64,
}

/// Nonblocking dispatcher-owned sender for one kind/class queue.
#[derive(Debug)]
pub struct DataSender {
    socket: Socket,
    _context: Context,
    returned: Arc<Returned>,
    closed: CloseSignal,
    shard: u32,
    kind: Kind,
    class: Class,
    maximum_retained_bytes: usize,
    queued_bytes: usize,
    queued_slots: usize,
    slots: usize,
    queue_bytes: usize,
}

/// Shard-owned receiver. OMQ owns queueing and readiness.
#[derive(Debug)]
pub struct DataReceiver {
    socket: Socket,
    _context: Context,
    closed: CloseSignal,
}

/// Original input is returned unchanged when it cannot enter the queue.
#[derive(Debug)]
pub enum DataSendError {
    /// Queue count or retained bytes are full.
    Full(DataInput),
    /// Destination closed.
    Closed(DataInput),
    /// Backing exceeds the configured maximum.
    Oversized(DataInput),
    /// Wrong destination, kind or class.
    Invalid(DataInput),
}

/// Invalid bounds, local transport failure or ended destination.
#[derive(Debug, thiserror::Error)]
pub enum DataLaneError {
    /// Bounds cannot back both the queue and its pending frame.
    #[error("invalid shard queue capacity")]
    Capacity,
    /// Either owner ended.
    #[error("shard queue closed")]
    Closed,
    /// Invalid internal framing.
    #[error("invalid inproc shard message")]
    Frame,
    /// Local socket setup or operation failed.
    #[error(transparent)]
    Transport(#[from] omq_tokio::Error),
}

/// Queue bounds of one data or control lane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DataLaneLimits {
    /// Queued message count.
    pub slots: usize,
    /// Largest retained backing of one message.
    pub maximum_retained_bytes: usize,
    /// Retained bytes of the queue plus one pending message.
    pub retained_budget_bytes: usize,
}

/// Construct once at startup on the broker's existing owned OMQ context.
/// Fixed slots also bound the small routing metadata allocations. Payload bytes
/// remain charged by received backing, independent of native frame lengths.
pub fn data_channel(
    context: &Context,
    shard: u32,
    kind: Kind,
    class: Class,
    limits: DataLaneLimits,
) -> Result<(DataSender, DataReceiver), DataLaneError> {
    let DataLaneLimits {
        slots,
        maximum_retained_bytes,
        retained_budget_bytes,
    } = limits;
    if context.io_threads() == 0
        || slots == 0
        || slots > 65536
        || maximum_retained_bytes == 0
        || maximum_retained_bytes
            .checked_mul(2)
            .is_none_or(|bytes| bytes > retained_budget_bytes)
    {
        return Err(DataLaneError::Capacity);
    }
    let options = Options::default()
        .send_hwm(slots as u32)
        .recv_hwm(slots as u32)
        .max_message_size(maximum_retained_bytes.saturating_add(1024))
        .linger(Duration::ZERO);
    let (sender, receiver) = super::inproc::pair(context, options)?;
    let closed = CloseSignal::default();
    Ok((
        DataSender {
            socket: sender,
            _context: context.clone(),
            returned: Arc::new(Returned::default()),
            closed: closed.clone(),
            shard,
            kind,
            class,
            maximum_retained_bytes,
            queued_bytes: 0,
            queued_slots: 0,
            slots,
            queue_bytes: retained_budget_bytes - maximum_retained_bytes,
        },
        DataReceiver {
            socket: receiver,
            _context: context.clone(),
            closed,
        },
    ))
}

impl DataSender {
    pub(crate) fn shard(&self) -> u32 {
        self.shard
    }
    pub(crate) fn kind(&self) -> Kind {
        self.kind
    }
    pub(crate) fn class(&self) -> Class {
        self.class
    }
    /// Capture before trying admission. Only a dequeue or close makes room.
    pub fn space_generation(&self) -> u64 {
        self.returned.generation.load(Ordering::Acquire)
    }
    /// A dequeue receipt is consumed by this future and charged back to its
    /// sending owner. Cancellation before receipt consumption leaves it queued.
    pub fn space_changed_after(
        &self,
        generation: u64,
    ) -> impl std::future::Future<Output = ()> + use<> {
        let socket = self.socket.clone_shared();
        let returned = self.returned.clone();
        let closed = self.closed.clone();
        async move {
            if returned.generation.load(Ordering::Acquire) != generation
                || returned.slots.load(Ordering::Acquire) != 0
                || closed.is_closed()
            {
                return;
            }
            tokio::select! {
                reply = socket.recv() => {
                    if let Ok(reply) = reply && !returned.credit(&reply) {
                        closed.close();
                    }
                },
                () = closed.closed() => {},
            }
        }
    }
    /// Transfer one intact input or keep its ownership with the caller.
    #[expect(
        clippy::result_large_err,
        reason = "pressure returns the original input"
    )]
    pub fn try_send(
        &mut self,
        input: DataInput,
        retained_bytes: usize,
    ) -> Result<(), DataSendError> {
        if input.binding.kind != self.kind
            || input.route.class != self.class
            || input.route.placement.shard != self.shard
            || (self.class == Class::Data
                && self.kind == Kind::Client
                && input.route.writer.is_none())
            || (self.kind == Kind::Broker && input.route.writer.is_some())
        {
            return Err(DataSendError::Invalid(input));
        }
        if retained_bytes > self.maximum_retained_bytes
            || retained_bytes
                < input
                    .message
                    .max_message_size_len()
                    .saturating_add(std::mem::size_of::<Message>())
        {
            return Err(DataSendError::Oversized(input));
        }
        if self.closed.is_closed() {
            return Err(DataSendError::Closed(input));
        }
        while let Ok(reply) = self.socket.try_recv() {
            if !self.returned.credit(&reply) {
                self.closed.close();
                return Err(DataSendError::Closed(input));
            }
        }
        self.queued_bytes -= self.returned.bytes.swap(0, Ordering::AcqRel);
        self.queued_slots -= self.returned.slots.swap(0, Ordering::AcqRel);
        if self.queued_slots == self.slots || retained_bytes > self.queue_bytes - self.queued_bytes
        {
            return Err(DataSendError::Full(input));
        }
        let header = encode(&input, retained_bytes);
        let message = Message::with_prefix(Bytes::copy_from_slice(&header), input.message.clone());
        match self.socket.try_send(message) {
            Ok(()) => {
                self.queued_bytes += retained_bytes;
                self.queued_slots += 1;
                Ok(())
            }
            Err(TrySendError::Full(_)) => Err(DataSendError::Full(input)),
            Err(TrySendError::Closed) => Err(DataSendError::Closed(input)),
            Err(TrySendError::Error(_)) => Err(DataSendError::Invalid(input)),
        }
    }
}

impl DataReceiver {
    /// Dequeue one frame. Its pending slot remains separately backed.
    pub fn try_recv(&mut self) -> Result<Option<DataInput>, DataLaneError> {
        match self.socket.try_recv() {
            Ok(message) => decode(&self.socket, &self.closed, message).map(Some),
            Err(omq_tokio::Error::WouldBlock) if self.closed.is_closed() => {
                Err(DataLaneError::Closed)
            }
            Err(omq_tokio::Error::WouldBlock) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    /// Wait and consume one input. The caller installs it in the reserved
    /// pending slot before beginning another receive.
    pub fn ready(
        &self,
    ) -> impl std::future::Future<Output = Result<DataInput, DataLaneError>> + use<> {
        let socket = self.socket.clone_shared();
        let closed = self.closed.clone();
        async move {
            tokio::select! {
                message = socket.recv() => decode(&socket, &closed, message?),
                () = closed.closed() => Err(DataLaneError::Closed),
            }
        }
    }
}
impl Drop for DataSender {
    fn drop(&mut self) {
        self.closed.close();
    }
}
impl Drop for DataReceiver {
    fn drop(&mut self) {
        self.closed.close();
    }
}

impl Returned {
    /// Credit one private dequeue receipt. A malformed receipt credits nothing
    /// and returns false; the caller closes the lane.
    fn credit(&self, receipt: &Message) -> bool {
        let Some(bytes) = receipt
            .part_slice(0)
            .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
            .and_then(|bytes| usize::try_from(u64::from_be_bytes(bytes)).ok())
        else {
            return false;
        };
        self.bytes.fetch_add(bytes, Ordering::Release);
        self.slots.fetch_add(1, Ordering::Release);
        self.generation.fetch_add(1, Ordering::Release);
        true
    }
}

const HEADER_BYTES: usize = 95;
fn encode(input: &DataInput, retained: usize) -> [u8; HEADER_BYTES] {
    let mut bytes = [0u8; HEADER_BYTES];
    bytes[0] = u8::from(input.binding.kind == Kind::Broker);
    bytes[1] = u8::from(input.route.class == Class::Control);
    bytes[2] = u8::from(input.route.writer.is_some());
    bytes[3..19].copy_from_slice(input.binding.peer.as_bytes());
    bytes[19..35].copy_from_slice(input.binding.session.as_bytes());
    bytes[35..51].copy_from_slice(input.route.placement.group.as_bytes());
    bytes[51..67].copy_from_slice(input.route.placement.partition.as_bytes());
    bytes[67..71].copy_from_slice(&input.route.placement.shard.to_be_bytes());
    if let Some(writer) = input.route.writer {
        bytes[71..87].copy_from_slice(writer.as_bytes());
    }
    bytes[87..95].copy_from_slice(&(retained as u64).to_be_bytes());
    bytes
}
fn decode(
    socket: &Socket,
    closed: &CloseSignal,
    mut message: Message,
) -> Result<DataInput, DataLaneError> {
    let identity = message
        .pop_front_payload()
        .ok_or(DataLaneError::Frame)?
        .as_bytes();
    let header = message
        .pop_front_payload()
        .ok_or(DataLaneError::Frame)?
        .as_bytes();
    let bytes: [u8; HEADER_BYTES] = header
        .as_ref()
        .try_into()
        .map_err(|_| DataLaneError::Frame)?;
    if bytes[..3].iter().any(|&byte| byte > 1) {
        return Err(DataLaneError::Frame);
    }
    let input = DataInput {
        binding: Binding {
            kind: if bytes[0] == 1 {
                Kind::Broker
            } else {
                Kind::Client
            },
            peer: ozzy_proto::NodeId::from_bytes(bytes.array::<3, 16>()),
            session: ozzy_proto::LinkSessionId::from_bytes(bytes.array::<19, 16>()),
        },
        route: Routed {
            class: if bytes[1] == 1 {
                Class::Control
            } else {
                Class::Data
            },
            writer: (bytes[2] == 1)
                .then(|| ozzy_proto::ProducerId::from_bytes(bytes.array::<71, 16>())),
            placement: Placement {
                group: ozzy_proto::GroupId::from_bytes(bytes.array::<35, 16>()),
                partition: ozzy_proto::PartitionIncarnation::from_bytes(bytes.array::<51, 16>()),
                shard: bytes.u32_at::<67>(),
            },
        },
        message,
    };
    // Local closure can race an already-ready receive. The sender signals it
    // before dropping its socket; routing a receipt is no longer meaningful.
    if closed.is_closed() {
        return Err(DataLaneError::Closed);
    }
    let reply = Message::with_prefix(
        identity,
        Message::single(Bytes::copy_from_slice(&bytes[87..95])),
    );
    match socket.try_send(reply) {
        Ok(()) => Ok(input),
        Err(_) if closed.is_closed() => Err(DataLaneError::Closed),
        Err(TrySendError::Closed) => Err(DataLaneError::Closed),
        Err(TrySendError::Error(error)) => Err(DataLaneError::Transport(error)),
        Err(TrySendError::Full(_)) => Err(DataLaneError::Frame),
    }
}

#[cfg(test)]
mod tests;

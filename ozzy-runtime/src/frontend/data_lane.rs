//! Bounded dispatcher/shard handoff over OMQ inproc DEALER/ROUTER.
//! A binary metadata frame precedes the unchanged native frames. Replies return
//! dequeue accounting; one further maximum frame backs the shard's pending slot.

use super::{Binding, Kind, Placement, Routed};
use crate::{dispatch::Class, signal::CloseSignal};
use bytes::Bytes;
use omq_tokio::{Context, Message, Options, Socket, TrySendError};
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

/// Construct once at startup on the broker's existing owned OMQ context.
/// Fixed slots also bound the small routing metadata allocations. Payload bytes
/// remain charged by received backing, independent of native frame lengths.
pub fn data_channel(
    context: &Context,
    shard: u32,
    kind: Kind,
    class: Class,
    slots: usize,
    maximum_retained_bytes: usize,
    retained_budget_bytes: usize,
) -> Result<(DataSender, DataReceiver), DataLaneError> {
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
                reply = socket.recv() => { if let Ok(reply) = reply { return_bytes(&reply, &returned); } },
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
            return_bytes(&reply, &self.returned);
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

fn return_bytes(message: &Message, returned: &Returned) {
    let bytes = message
        .part_slice(0)
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
        .map(u64::from_be_bytes)
        .expect("private dequeue receipt");
    returned.bytes.fetch_add(
        usize::try_from(bytes).expect("local byte count"),
        Ordering::Release,
    );
    returned.slots.fetch_add(1, Ordering::Release);
    returned.generation.fetch_add(1, Ordering::Release);
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
            peer: ozzy_proto::NodeId::from_bytes(bytes[3..19].try_into().unwrap()),
            session: ozzy_proto::LinkSessionId::from_bytes(bytes[19..35].try_into().unwrap()),
        },
        route: Routed {
            class: if bytes[1] == 1 {
                Class::Control
            } else {
                Class::Data
            },
            writer: (bytes[2] == 1)
                .then(|| ozzy_proto::ProducerId::from_bytes(bytes[71..87].try_into().unwrap())),
            placement: Placement {
                group: ozzy_proto::GroupId::from_bytes(bytes[35..51].try_into().unwrap()),
                partition: ozzy_proto::PartitionIncarnation::from_bytes(
                    bytes[51..67].try_into().unwrap(),
                ),
                shard: u32::from_be_bytes(bytes[67..71].try_into().unwrap()),
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
mod tests {
    use super::*;
    use crate::dispatch::Class;
    use crate::frontend::{Kind, test_support};
    use ozzy_proto::ProducerId;

    fn input() -> DataInput {
        let binding = test_support::binding(Kind::Client);
        let placement = test_support::placement(0, 0);
        DataInput {
            binding,
            route: Routed {
                placement,
                class: Class::Data,
                writer: Some(ProducerId::from_bytes([4; 16])),
            },
            message: test_support::append(placement, binding),
        }
    }

    #[tokio::test]
    async fn received_input_observes_sender_close_before_its_dequeue_receipt() {
        let (mut sender, receiver) = data_channel(
            &omq_tokio::Context::new(),
            0,
            Kind::Client,
            Class::Data,
            1,
            4096,
            8192,
        )
        .unwrap();
        sender.try_send(input(), 4096).unwrap();
        let message = receiver.socket.recv().await.unwrap();
        // Drop signals closure before destroying the sender's socket. Hold that
        // exact interval so receipt routing cannot make this schedule flaky.
        sender.closed.close();
        assert!(matches!(
            decode(&receiver.socket, &receiver.closed, message),
            Err(DataLaneError::Closed)
        ));
    }

    #[tokio::test]
    async fn full_shard_lane_returns_frame_and_wakes_on_dequeue() {
        let (mut sender, mut receiver) = data_channel(
            &omq_tokio::Context::new(),
            0,
            Kind::Client,
            crate::dispatch::Class::Data,
            1,
            4096,
            8192,
        )
        .unwrap();
        sender.try_send(input(), 4096).unwrap();
        let held = match sender.try_send(input(), 4096).unwrap_err() {
            DataSendError::Full(input) => input,
            other => panic!("unexpected send result: {other:?}"),
        };
        let generation = sender.space_generation();
        let changed = sender.space_changed_after(generation);
        tokio::pin!(changed);
        assert!(futures::poll!(changed.as_mut()).is_pending());
        assert!(receiver.try_recv().unwrap().is_some());
        changed.await;
        sender.try_send(held, 4096).unwrap();
        assert!(receiver.try_recv().unwrap().is_some());
        assert!(receiver.try_recv().unwrap().is_none());
    }

    #[test]
    fn slot_count_and_held_frame_fit_fixed_retained_budget() {
        assert!(matches!(
            data_channel(
                &omq_tokio::Context::new(),
                0,
                Kind::Client,
                crate::dispatch::Class::Data,
                2,
                4096,
                8191
            ),
            Err(DataLaneError::Capacity)
        ));
        assert!(matches!(
            data_channel(
                &omq_tokio::Context::new(),
                0,
                Kind::Client,
                crate::dispatch::Class::Data,
                65537,
                4096,
                16384
            ),
            Err(DataLaneError::Capacity)
        ));
    }

    #[test]
    fn byte_pressure_returns_capacity_without_waiting_for_ring_full() {
        let (mut sender, mut receiver) = data_channel(
            &omq_tokio::Context::new(),
            0,
            Kind::Client,
            crate::dispatch::Class::Data,
            4,
            4096,
            12288,
        )
        .unwrap();
        sender.try_send(input(), 4096).unwrap();
        sender.try_send(input(), 4096).unwrap();
        let held = match sender.try_send(input(), 4096).unwrap_err() {
            DataSendError::Full(input) => input,
            other => panic!("unexpected byte pressure: {other:?}"),
        };
        assert_eq!(sender.queued_bytes, 8192);
        receiver.try_recv().unwrap().unwrap();
        sender.try_send(held, 4096).unwrap();
        assert_eq!(sender.queued_bytes, 8192);
    }

    #[test]
    fn small_frames_share_the_byte_budget_without_reserving_maximum_per_slot() {
        let (mut sender, mut receiver) = data_channel(
            &omq_tokio::Context::new(),
            0,
            Kind::Client,
            crate::dispatch::Class::Data,
            4,
            16384,
            32768,
        )
        .unwrap();
        for _ in 0..4 {
            sender.try_send(input(), 4096).unwrap();
        }
        assert_eq!(sender.queued_bytes, 16384);
        assert!(matches!(
            sender.try_send(input(), 4096),
            Err(DataSendError::Full(_))
        ));
        for _ in 0..4 {
            receiver.try_recv().unwrap().unwrap();
        }
        sender.try_send(input(), 16384).unwrap();
        assert_eq!(sender.queued_bytes, 16384);
        assert!(matches!(
            sender.try_send(input(), 4096),
            Err(DataSendError::Full(_))
        ));
    }

    #[test]
    fn concurrent_dequeues_and_refills_keep_byte_returns_bounded() {
        let (mut sender, mut receiver) = data_channel(
            &omq_tokio::Context::new(),
            0,
            Kind::Client,
            crate::dispatch::Class::Data,
            4,
            4096,
            24576,
        )
        .unwrap();
        let reader = std::thread::spawn(move || {
            for _ in 0..5000 {
                while receiver.try_recv().unwrap().is_none() {
                    std::thread::yield_now();
                }
            }
        });
        for _ in 0..5000 {
            let mut frame = input();
            loop {
                match sender.try_send(frame, 4096) {
                    Ok(()) => break,
                    Err(DataSendError::Full(returned)) => frame = returned,
                    error => panic!("unexpected queue outcome: {error:?}"),
                }
                std::thread::yield_now();
            }
            assert!(sender.queued_bytes <= sender.queue_bytes);
        }
        reader.join().unwrap();
    }

    #[test]
    fn wrong_shard_cannot_consume_a_producer_slot() {
        let (mut sender, mut receiver) = data_channel(
            &omq_tokio::Context::new(),
            1,
            Kind::Client,
            crate::dispatch::Class::Data,
            1,
            4096,
            8192,
        )
        .unwrap();
        assert!(matches!(
            sender.try_send(input(), 4096),
            Err(DataSendError::Invalid(_))
        ));
        assert!(receiver.try_recv().unwrap().is_none());
    }
}

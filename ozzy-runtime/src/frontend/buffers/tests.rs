use super::*;
use crate::frontend::Kind;
use crate::frontend::test_support::{append, binding, control, placement};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Tracked {
    bytes: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl AsRef<[u8]> for Tracked {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl Drop for Tracked {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn oversized(frame: &[u8], drops: &Arc<AtomicUsize>) -> Bytes {
    let mut bytes = vec![0; 1024 * 1024];
    bytes[..frame.len()].copy_from_slice(frame);
    Bytes::from_owner(Tracked {
        bytes,
        drops: drops.clone(),
    })
    .slice(..frame.len())
}

#[test]
fn compact_metadata_releases_hidden_owners_while_payload_stays_shared() {
    let source = append(placement(0, 0), binding(Kind::Client));
    let metadata_drops = Arc::new(AtomicUsize::new(0));
    let payload_drops = Arc::new(AtomicUsize::new(0));
    let payload = oversized(source.part_slice(3).unwrap(), &payload_drops);
    let pointer = payload.as_ptr();
    let message = Message::multipart_payloads([
        Payload::from_bytes(oversized(source.part_slice(0).unwrap(), &metadata_drops)),
        Payload::from_bytes(oversized(source.part_slice(1).unwrap(), &metadata_drops)),
        Payload::from_bytes(oversized(source.part_slice(2).unwrap(), &metadata_drops)),
        Payload::from_bytes(payload),
    ]);
    let buffers = ReceiveBuffers::new(
        EnvelopeLimits::default(),
        ReceiveStorage::Inproc {
            payload_backing_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let (message, charge) = buffers.prepare(message).unwrap();
    assert_eq!(metadata_drops.load(Ordering::SeqCst), 3);
    assert_eq!(message.part_slice(3).unwrap().as_ptr(), pointer);
    assert!(charge > 1024 * 1024);
    for index in 0..4 {
        assert_eq!(message.part_slice(index), source.part_slice(index));
    }
    let alias = message.part_bytes(3).unwrap().slice(..1);
    drop(message);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
    drop(alias);
    assert_eq!(payload_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn borrowed_prepare_keeps_original_frame_for_retry() {
    let source = append(placement(0, 0), binding(Kind::Client));
    let drops = Arc::new(AtomicUsize::new(0));
    let original =
        Message::multipart_payloads((0..4).map(|index| {
            Payload::from_bytes(oversized(source.part_slice(index).unwrap(), &drops))
        }));
    let original_payload = original.part_slice(3).unwrap().as_ptr();
    let buffers = ReceiveBuffers::new(
        EnvelopeLimits::default(),
        ReceiveStorage::Inproc {
            payload_backing_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let (prepared, _) = buffers.prepare_borrowed(&original).unwrap();
    for index in 0..4 {
        assert_eq!(prepared.part_slice(index), original.part_slice(index));
    }
    assert_eq!(prepared.part_slice(3).unwrap().as_ptr(), original_payload);
    drop(prepared);
    assert_eq!(original.part_slice(3).unwrap().as_ptr(), original_payload);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(original);
    assert_eq!(drops.load(Ordering::SeqCst), 4);
}

#[test]
fn empty_control_payload_cannot_retain_hidden_capacity() {
    let source = control(placement(0, 0), binding(Kind::Broker));
    let drops = Arc::new(AtomicUsize::new(0));
    let message =
        Message::multipart_payloads((0..4).map(|index| {
            Payload::from_bytes(oversized(source.part_slice(index).unwrap(), &drops))
        }));
    let buffers = ReceiveBuffers::new(
        EnvelopeLimits::default(),
        ReceiveStorage::Inproc {
            payload_backing_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    let (message, charge) = buffers.prepare(message).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 4);
    assert_eq!(message.part_slice(3).unwrap().len(), 0);
    assert!(charge < 4096);
}

#[test]
fn stream_small_payload_carries_read_chunk_charge_and_enforces_socket_limit() {
    let source = || append(placement(0, 0), binding(Kind::Client));
    let buffers = ReceiveBuffers::new(
        EnvelopeLimits::default(),
        ReceiveStorage::Stream {
            message_bytes: 1024,
        },
    )
    .unwrap();
    let (_, charge) = buffers.prepare(source()).unwrap();
    assert!(charge >= STREAM_READ_BACKING);
    let buffers = ReceiveBuffers::new(
        EnvelopeLimits::default(),
        ReceiveStorage::Inproc {
            payload_backing_bytes: 1,
        },
    )
    .unwrap();
    assert!(matches!(
        buffers.prepare(source()),
        Err(BufferError::Charge)
    ));
    assert!(matches!(
        buffers.prepare(Message::single("bad")),
        Err(BufferError::Frames)
    ));
}

#[test]
fn limits_reject_unbounded_metadata_copy_and_charge_overflow() {
    assert!(
        ReceiveBuffers::new(
            EnvelopeLimits {
                max_metadata_bytes: METADATA_LIMIT + 1,
                ..EnvelopeLimits::default()
            },
            ReceiveStorage::Inproc {
                payload_backing_bytes: 1
            }
        )
        .is_err()
    );
    assert!(
        ReceiveBuffers::new(
            EnvelopeLimits::default(),
            ReceiveStorage::Stream {
                message_bytes: usize::MAX,
            }
        )
        .is_err()
    );
}

#[cfg(feature = "lz4-transport")]
#[test]
fn lz4_passthrough_sentinel_crossing_pool_threshold_retains_full_backing_charge() {
    let source = append(placement(0, 0), binding(Kind::Client));
    let frames = std::array::from_fn::<_, 3, _>(|part| source.part_slice(part + 1).unwrap());
    let limits = EnvelopeLimits::default();
    let envelope = ozzy_proto::decode_packet(&frames, limits).unwrap().envelope;
    let message_bytes = crate::transport::message_size_limit(limits).unwrap();
    let buffers = ReceiveBuffers::new(limits, ReceiveStorage::Lz4Stream { message_bytes }).unwrap();
    buffers
        .check_transport(&"lz4+tcp://127.0.0.1:7000".parse().unwrap(), message_bytes)
        .unwrap();
    assert!(
        buffers
            .check_transport(&"tcp://127.0.0.1:7000".parse().unwrap(), message_bytes)
            .is_err()
    );
    let size = 128 * 1024 - 1;
    let header = envelope
        .encode_header(frames[1].len(), size, limits)
        .unwrap();
    let message = Message::multipart([
        source.part_bytes(0).unwrap(),
        Bytes::copy_from_slice(&header),
        Bytes::copy_from_slice(frames[1]),
        Bytes::from(vec![7; size]),
    ]);
    let (_, charged) = buffers.prepare(message).unwrap();
    assert!(charged >= STREAM_POOL_BACKING.min(message_bytes * 2));
    assert!(charged > STREAM_READ_BACKING);
    assert!(charged <= buffers.maximum_retained_bytes());
}

#[test]
fn allocation_profile_rejects_other_transport_paths_and_message_limits() {
    let stream = ReceiveBuffers::new(
        EnvelopeLimits::default(),
        ReceiveStorage::Stream {
            message_bytes: 4096,
        },
    )
    .unwrap();
    assert!(
        stream
            .check_transport(&"tcp://127.0.0.1:7000".parse().unwrap(), 4096)
            .is_ok()
    );
    assert!(
        stream
            .check_transport(&"ipc://@ozzy-buffers".parse().unwrap(), 4096)
            .is_ok()
    );
    assert_eq!(
        stream.check_transport(&"tcp://127.0.0.1:7000".parse().unwrap(), 8192),
        Err(BufferError::Transport)
    );
    assert_eq!(
        stream.check_transport(&"inproc://buffers".parse().unwrap(), 4096),
        Err(BufferError::Transport)
    );
    let inproc = ReceiveBuffers::new(
        EnvelopeLimits::default(),
        ReceiveStorage::Inproc {
            payload_backing_bytes: 4096,
        },
    )
    .unwrap();
    assert!(
        inproc
            .check_transport(&"inproc://buffers".parse().unwrap(), 4096)
            .is_ok()
    );
    assert_eq!(
        inproc.check_transport(&"tcp://127.0.0.1:7000".parse().unwrap(), 4096),
        Err(BufferError::Transport)
    );
}

#[test]
fn pooled_payload_charge_covers_larger_reused_allocations() {
    let original = append(placement(0, 0), binding(Kind::Client));
    let frames = std::array::from_fn::<_, 3, _>(|i| original.part_slice(i + 1).unwrap());
    let limits = EnvelopeLimits::default();
    let envelope = ozzy_proto::decode_packet(&frames, limits).unwrap().envelope;
    let message_bytes = crate::transport::message_size_limit(limits).unwrap();
    let buffers = ReceiveBuffers::new(limits, ReceiveStorage::Stream { message_bytes }).unwrap();
    for size in [128 * 1024, 900 * 1024] {
        let payload = Bytes::from(vec![7; size]);
        let pointer = payload.as_ptr();
        let header = envelope
            .encode_header(frames[1].len(), size, limits)
            .unwrap();
        // Only framing matters here. The shard, not ReceiveBuffers, validates
        // the command metadata against these deliberately opaque payload bytes.
        let message = Message::multipart([
            original.part_bytes(0).unwrap(),
            Bytes::copy_from_slice(&header),
            original.part_bytes(2).unwrap(),
            payload,
        ]);
        let (message, charge) = buffers.prepare(message).unwrap();
        assert_eq!(message.part_slice(3).unwrap().as_ptr(), pointer);
        assert!(
            charge > 2 * message_bytes,
            "pool capacity is independent of visible length"
        );
    }
}

#[test]
fn writer_credit_covers_every_frame_of_the_refused_allocation_class() {
    let buffers = ReceiveBuffers::new(
        EnvelopeLimits {
            max_metadata_bytes: 64 * 1024,
            max_payload_bytes: 512 * 1024,
        },
        ReceiveStorage::Stream {
            message_bytes: 600 * 1024,
        },
    )
    .unwrap();
    let (_, refused) = buffers
        .prepare(append(placement(0, 0), binding(Kind::Client)))
        .unwrap();
    let credit = buffers.class_retained_bytes(refused);
    // A retry with more records has more metadata and the same read chunk.
    assert!(credit >= refused + 64 * 1024 - 1024);
    assert_eq!(buffers.class_retained_bytes(credit), credit);
    assert!(credit < buffers.maximum_retained_bytes());
    // A frame from the large-frame pool needs the largest charge.
    assert_eq!(
        buffers.class_retained_bytes(credit + 1),
        buffers.maximum_retained_bytes()
    );
    assert_eq!(
        buffers.class_retained_bytes(buffers.maximum_retained_bytes()),
        buffers.maximum_retained_bytes()
    );
}

#[test]
fn measured_stream_backing_survives_normalization_without_pool_overreservation() {
    let original = append(placement(0, 0), binding(Kind::Client));
    let frames = std::array::from_fn::<_, 3, _>(|i| original.part_slice(i + 1).unwrap());
    let limits = EnvelopeLimits::default();
    let envelope = ozzy_proto::decode_packet(&frames, limits).unwrap().envelope;
    let message_bytes = crate::transport::message_size_limit(limits).unwrap();
    let buffers = ReceiveBuffers::new(limits, ReceiveStorage::Stream { message_bytes }).unwrap();
    let size = 256 * 1024;
    let capacity = 512 * 1024;
    let header = envelope
        .encode_header(frames[1].len(), size, limits)
        .unwrap();
    let bytes = Bytes::from(vec![7; capacity]).slice(..size);
    let pointer = bytes.as_ptr();
    let message = Message::multipart_payloads([
        Payload::from_slice(original.part_slice(0).unwrap()),
        Payload::from_slice(&header),
        Payload::from_slice(frames[1]),
        Payload::from_bytes_with_retained_size(bytes, capacity),
    ]);
    let (normalized, charged) = buffers.prepare_borrowed(&message).unwrap();
    assert_eq!(normalized.part_slice(3).unwrap().as_ptr(), pointer);
    assert!(charged >= capacity);
    assert!(charged < capacity + 4096);
    assert!(normalized.retained_size().unwrap() <= charged);
    assert_eq!(message.part_slice(3).unwrap().as_ptr(), pointer);
}

//! Owned control-frame storage shared by native clients and servers.

use bytes::{Bytes, BytesMut};
use omq_tokio::message::Payload;
use omq_tokio::{Message, MessagePool};

const CONTROL_CHUNK_BYTES: usize = 64 * 1024;
const CONTROL_CHUNKS: usize = 4;

/// Bounded reusable control storage for record-at-a-time writer sessions.
/// Sharing storage never delays a send or combines messages. Pinned or oversized
/// controls use the ordinary allocation path instead of growing the pool.
#[derive(Debug)]
pub(crate) struct ControlFrames {
    chunks: [BytesMut; CONTROL_CHUNKS],
    current: usize,
    messages: MessagePool,
}

impl ControlFrames {
    pub(crate) fn new(message_capacity: usize) -> Self {
        Self {
            chunks: std::array::from_fn(|_| BytesMut::with_capacity(CONTROL_CHUNK_BYTES)),
            current: 0,
            messages: MessagePool::new(message_capacity.min(8192), 4),
        }
    }

    pub(crate) fn message(
        &mut self,
        route: &[u8],
        header: [u8; 64],
        metadata: &[u8],
        payload: Payload,
    ) -> Message {
        let Some(index) = self.reserve(header.len().saturating_add(metadata.len())) else {
            return message(route, header, metadata, payload);
        };
        let chunk = &mut self.chunks[index];
        chunk.extend_from_slice(&header);
        chunk.extend_from_slice(metadata);
        let mut metadata = chunk.split().freeze();
        let header = metadata.split_to(header.len());
        self.messages.multipart_payloads([
            Payload::from_slice(route),
            Payload::from_bytes(header),
            Payload::from_bytes(metadata),
            payload,
        ])
    }

    fn reserve(&mut self, bytes: usize) -> Option<usize> {
        if bytes > CONTROL_CHUNK_BYTES {
            return None;
        }
        for distance in 0..CONTROL_CHUNKS {
            let index = (self.current + distance) % CONTROL_CHUNKS;
            let chunk = &mut self.chunks[index];
            // Each split leaves an empty mutable tail. Reclaiming cannot
            // overwrite any prefix still held by an immutable frame.
            if chunk.capacity() >= bytes || chunk.try_reclaim(CONTROL_CHUNK_BYTES) {
                self.current = index;
                return Some(index);
            }
        }
        None
    }
}

/// Keep the envelope and metadata in one backing, but separate wire frames.
/// Both frames share one reference count instead of promoting two allocations.
/// Payload ownership and OMQ's transport coalescing are unchanged.
pub(crate) fn message(
    route: &[u8],
    header: [u8; 64],
    metadata: &[u8],
    payload: impl Into<Payload>,
) -> Message {
    let mut control = Vec::with_capacity(header.len() + metadata.len());
    control.extend_from_slice(&header);
    control.extend_from_slice(metadata);
    let mut metadata = Bytes::from(control);
    let header = metadata.split_to(header.len());
    Message::multipart_payloads([
        Payload::from_slice(route),
        Payload::from_bytes(header),
        Payload::from_bytes(metadata),
        payload.into(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separate_frames_share_control_storage_without_copying_payload() {
        let route = Bytes::from_static(b"0123456789abcdef");
        let payload = Bytes::from(vec![7; 128]);
        let payload_pointer = payload.as_ptr();
        let packet = message(&route, [3; 64], &[5; 128], payload.clone());
        let frames: Vec<_> = packet.iter().collect();
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].as_ref(), route.as_ref());
        assert_eq!(frames[1].as_ref(), &[3; 64]);
        assert_eq!(frames[2].as_ref(), &[5; 128]);
        assert_eq!(frames[3].as_ref(), payload.as_ref());
        assert_eq!(frames[3].as_ref().as_ptr(), payload_pointer);
        assert_eq!(
            frames[2].as_ref().as_ptr() as usize,
            frames[1].as_ref().as_ptr() as usize + 64
        );
        let metadata = frames[2].clone();
        drop(packet);
        assert_eq!(metadata.as_ref(), &[5; 128]);
    }

    #[test]
    fn empty_and_large_metadata_preserve_wire_shape() {
        for size in [0, 1, 62, 192, 193, 4096] {
            let mut metadata = vec![9; size];
            let packet = message(&[], [4; 64], &metadata, Bytes::new());
            metadata.fill(0);
            let frames: Vec<_> = packet.iter().collect();
            assert_eq!(frames.len(), 4);
            assert!(frames[0].as_ref().is_empty());
            assert_eq!(frames[1].as_ref(), &[4; 64]);
            assert_eq!(frames[2].as_ref(), vec![9; size]);
            assert!(frames[3].as_ref().is_empty());
        }
    }

    #[test]
    fn pooled_controls_reclaim_only_after_every_frame_is_released() {
        let mut pool = ControlFrames::new(4);
        let payload = Bytes::from(vec![7; 128]);
        let mut frames = Vec::new();
        let mut pointers = Vec::new();
        for marker in 0..=CONTROL_CHUNKS {
            let frame = pool.message(
                &[],
                [marker as u8; 64],
                &vec![marker as u8; CONTROL_CHUNK_BYTES - 64],
                payload.clone().into(),
            );
            pointers.push(frame.part_slice(1).unwrap().as_ptr());
            frames.push(frame);
        }
        assert!(
            pointers[..CONTROL_CHUNKS]
                .iter()
                .all(|&ptr| ptr != pointers[CONTROL_CHUNKS])
        );
        for (marker, frame) in frames.iter().enumerate() {
            assert_eq!(frame.part_slice(1).unwrap(), &[marker as u8; 64]);
            assert!(
                frame
                    .part_slice(2)
                    .unwrap()
                    .iter()
                    .all(|&byte| byte == marker as u8)
            );
            assert_eq!(frame.part_slice(3).unwrap().as_ptr(), payload.as_ptr());
        }
        frames.clear();
        for _ in 0..CONTROL_CHUNKS {
            let frame = pool.message(
                &[],
                [9; 64],
                &vec![9; CONTROL_CHUNK_BYTES - 64],
                payload.clone().into(),
            );
            assert!(pointers[..CONTROL_CHUNKS].contains(&frame.part_slice(1).unwrap().as_ptr()));
            frames.push(frame);
        }
        drop(pool);
        for frame in frames {
            assert_eq!(frame.part_slice(1).unwrap(), &[9; 64]);
            assert!(frame.part_slice(2).unwrap().iter().all(|&byte| byte == 9));
        }
    }

    #[test]
    fn pooled_controls_handle_empty_and_oversized_metadata() {
        let mut pool = ControlFrames::new(4);
        for size in [0, 1, 62, 128, CONTROL_CHUNK_BYTES, CONTROL_CHUNK_BYTES + 1] {
            let metadata = vec![6; size];
            let expected = message(&[], [4; 64], &metadata, Bytes::new());
            let actual = pool.message(&[], [4; 64], &metadata, Bytes::new().into());
            assert_eq!(actual, expected);
        }
    }
}

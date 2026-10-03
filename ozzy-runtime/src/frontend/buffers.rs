//! Receive allocation accounting at the ordinary OMQ boundary. Metadata gets
//! compact owners; opaque payloads keep their original backing allocation.

use bytes::Bytes;
use omq_tokio::{Message, message::Payload};
use ozzy_proto::{ENVELOPE_BYTES, EnvelopeLimits};

// OMQ 0.23/0.24 ordinary stream receive uses an adaptive 128 KiB read buffer.
// BytesMut growth can retain the old prefix while doubling its allocation.
const STREAM_READ_BACKING: usize = 2 * 128 * 1024;
// The large-frame pool accepts allocations up to 8 MiB. A reused smaller
// allocation can double when grown. Frames larger than the pool use exact
// allocation. The socket's max_message_size also bounds every frame.
const STREAM_POOL_BACKING: usize = 2 * 8 * 1024 * 1024;
// Four freshly constructed payload descriptors, Bytes owners, Message, and
// destination retention adapters. Input descriptor vectors are not retained.
const DESCRIPTORS: usize = 2048;
const METADATA_LIMIT: usize = 64 * 1024;

/// Allocation contract of every connection feeding one ordinary receive socket.
/// These bounds concern backing capacity, independently of visible frame size.
#[derive(Clone, Copy, Debug)]
pub enum ReceiveStorage {
    /// Untransformed TCP/IPC with OMQ's default 128 KiB large-frame threshold
    /// and this exact local `max_message_size`. Do not use for inproc, WebSocket,
    /// or transport transforms. No dependency-specific allocation hooks needed.
    Stream {
        /// Socket-local message size ceiling, including frame descriptors.
        message_bytes: usize,
    },
    /// OMQ LZ4-over-TCP. Decoded frames have exact owned allocations; plaintext
    /// passthrough can retain the stream backing plus its four-byte sentinel.
    #[cfg(feature = "lz4-transport")]
    Lz4Stream {
        /// Socket-local decoded message ceiling, including descriptors.
        message_bytes: usize,
    },
    /// Controlled inproc senders guarantee this maximum payload backing size.
    /// A tiny slice of a large buffer needs the large buffer's full charge.
    /// Arbitrary external inproc senders cannot provide this guarantee.
    Inproc {
        /// Full allocation capacity even when a sender transmits a small slice.
        payload_backing_bytes: usize,
    },
}

/// Dispatcher-local preparation. It copies at most 64 KiB of metadata and never
/// copies or validates payload bytes. Memory charges survive payload aliases.
#[derive(Clone, Copy, Debug)]
pub struct ReceiveBuffers {
    limits: EnvelopeLimits,
    storage: ReceiveStorage,
}

impl ReceiveBuffers {
    /// Fix bounds before accepting traffic. Metadata copy work stays bounded.
    pub fn new(limits: EnvelopeLimits, storage: ReceiveStorage) -> Result<Self, BufferError> {
        if limits.max_metadata_bytes > METADATA_LIMIT
            || limits.max_payload_bytes > u32::MAX as usize
            || match storage {
                ReceiveStorage::Stream { message_bytes } => {
                    !(1024..=isize::MAX as usize / 2).contains(&message_bytes)
                }
                #[cfg(feature = "lz4-transport")]
                ReceiveStorage::Lz4Stream { message_bytes } => {
                    !(1024..=isize::MAX as usize / 2).contains(&message_bytes)
                }
                ReceiveStorage::Inproc {
                    payload_backing_bytes,
                } => payload_backing_bytes > isize::MAX as usize / 2,
            }
        {
            return Err(BufferError::Limits);
        }
        Ok(Self { limits, storage })
    }

    /// Refuse a socket/connection whose allocation path differs from this
    /// profile. Check the bound endpoint and every outbound connect endpoint.
    pub fn check_transport(
        &self,
        endpoint: &omq_tokio::Endpoint,
        message_bytes: usize,
    ) -> Result<(), BufferError> {
        use omq_tokio::Endpoint;
        match (self.storage, endpoint) {
            (
                ReceiveStorage::Stream {
                    message_bytes: bound,
                },
                Endpoint::Tcp { .. } | Endpoint::Ipc(_),
            ) if bound == message_bytes => Ok(()),
            #[cfg(feature = "lz4-transport")]
            (
                ReceiveStorage::Lz4Stream {
                    message_bytes: bound,
                },
                Endpoint::Lz4Tcp { .. },
            ) if bound == message_bytes => Ok(()),
            (ReceiveStorage::Inproc { .. }, Endpoint::Inproc { .. }) => Ok(()),
            _ => Err(BufferError::Transport),
        }
    }

    /// Largest retained charge for one legal packet under this allocation
    /// profile. Reserve at least this much when a lane must accept a singleton.
    pub fn maximum_retained_bytes(&self) -> usize {
        DESCRIPTORS
            + 16
            + ENVELOPE_BYTES
            + self.limits.max_metadata_bytes
            + self.payload_backing(self.limits.max_payload_bytes)
    }

    /// Conservative receive backing for a bounded normal replica packet.
    /// Large stream frames still reserve their possible full pooled backing.
    pub fn retained_bytes_for_payload(&self, payload_bytes: usize) -> usize {
        DESCRIPTORS
            + 16
            + ENVELOPE_BYTES
            + self.limits.max_metadata_bytes
            + self.payload_backing(payload_bytes.min(self.limits.max_payload_bytes))
    }

    /// Charge to reserve for a writer whose refused frame had this charge.
    /// Frames of one allocation class differ only in their metadata, and a
    /// retry that carries more records has more of it. Credit for the exact
    /// refused frame would refuse every larger retry again.
    pub fn class_retained_bytes(&self, observed: usize) -> usize {
        let small = DESCRIPTORS
            + 16
            + ENVELOPE_BYTES
            + self.limits.max_metadata_bytes
            + self.payload_backing(1);
        if observed <= small {
            small
        } else {
            self.maximum_retained_bytes().max(observed)
        }
    }

    fn payload_backing(&self, payload_bytes: usize) -> usize {
        if payload_bytes == 0 {
            return 0;
        }
        match self.storage {
            ReceiveStorage::Stream { message_bytes } => {
                stream_backing(payload_bytes, message_bytes)
            }
            #[cfg(feature = "lz4-transport")]
            ReceiveStorage::Lz4Stream { message_bytes } => {
                stream_backing(payload_bytes.saturating_add(4), message_bytes).max(payload_bytes)
            }
            ReceiveStorage::Inproc {
                payload_backing_bytes,
            } => payload_backing_bytes,
        }
    }

    /// Validate fixed framing before making any compact metadata allocation.
    /// Return the message and its conservative full retained-byte charge for
    /// `Service::receive`. Session and routing checks still belong to Service.
    pub fn prepare(&self, message: Message) -> Result<(Message, usize), BufferError> {
        if message.len() == 2 && message.part_slice(0).is_some_and(|id| id.len() == 16) {
            ozzy_replication::wire::CompactState::decode(message.part_slice(1).unwrap_or_default())
                .map_err(|_| BufferError::Frames)?;
            let normalized = Message::multipart_payloads([
                Payload::from_slice(message.part_slice(0).unwrap()),
                Payload::from_slice(message.part_slice(1).unwrap()),
            ]);
            return Ok((
                normalized,
                DESCRIPTORS + 16 + ozzy_replication::wire::COMPACT_STATE_BYTES,
            ));
        }
        if message.len() != 4 || message.part_slice(0).is_none_or(|id| id.len() != 16) {
            return Err(BufferError::Frames);
        }
        let frames: [&[u8]; 3] =
            std::array::from_fn(|index| message.part_slice(index + 1).unwrap());
        ozzy_proto::decode_packet(&frames, self.limits).map_err(|_| BufferError::Frames)?;
        let payload_bytes = frames[2].len();
        match self.storage {
            ReceiveStorage::Stream { message_bytes } => {
                if message.max_message_size_len() > message_bytes {
                    return Err(BufferError::Frames);
                }
            }
            #[cfg(feature = "lz4-transport")]
            ReceiveStorage::Lz4Stream { message_bytes } => {
                if message.max_message_size_len() > message_bytes {
                    return Err(BufferError::Frames);
                }
            }
            ReceiveStorage::Inproc {
                payload_backing_bytes,
            } => {
                if payload_bytes > payload_backing_bytes {
                    return Err(BufferError::Charge);
                }
            }
        }
        // Small stream frames can share an entire read chunk. Large frames can
        // reuse a larger pool slot. Charge the backing, not the visible slice.
        let backing = self.payload_backing(payload_bytes);
        // Discard even an empty Bytes owner: empty application frames must not
        // smuggle an unrelated allocation through the zero-payload charge.
        let payload = if payload_bytes == 0 {
            Bytes::new()
        } else {
            message.part_bytes(3).expect("validated payload")
        };
        let retained = DESCRIPTORS
            + 16
            + ENVELOPE_BYTES
            + frames[1].len()
            + if payload_bytes == 0 { 0 } else { backing };
        let normalized = Message::multipart_payloads([
            Payload::from_slice(message.part_slice(0).unwrap()),
            Payload::from_bytes(Bytes::copy_from_slice(frames[0])),
            Payload::from_bytes(Bytes::copy_from_slice(frames[1])),
            Payload::from_bytes(payload),
        ]);
        drop(message);
        Ok((normalized, retained))
    }
}

fn stream_backing(payload_bytes: usize, message_bytes: usize) -> usize {
    if payload_bytes < 128 * 1024 {
        STREAM_READ_BACKING
    } else {
        STREAM_READ_BACKING
            .max(STREAM_POOL_BACKING.min(message_bytes * 2))
            .max(payload_bytes)
    }
}

/// Framing or allocation configuration refused before destination admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum BufferError {
    /// Invalid local size or bounded metadata work limit.
    #[error("invalid receive allocation limits")]
    Limits,
    /// Malformed or oversized native packet.
    #[error("invalid or oversized receive frames")]
    Frames,
    /// Visible payload already exceeds the promised physical bound.
    #[error("payload exceeds its configured backing allocation bound")]
    Charge,
    /// The endpoint or socket limit does not match the allocation profile.
    #[error("transport differs from receive allocation profile")]
    Transport,
}

#[cfg(test)]
mod tests;

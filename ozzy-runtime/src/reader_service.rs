//! Shared reader cursor outcomes, bounded delivery storage, and response payloads.

use bytes::Bytes;
pub(crate) use ozzy_core::reader::ReadOutcome as ReadState;
pub(crate) use ozzy_core::reader::ReadScheduler;
use ozzy_proto::nack::RetryClass;

pub(crate) fn reader_frame(
    payload: crate::replicated::payload::Lease,
    shared: Option<Bytes>,
) -> Bytes {
    #[cfg(feature = "storage-metrics")]
    crate::storage_metrics::add(
        if shared.is_some() {
            &crate::storage_metrics::SHARED_READER_BYTES
        } else {
            &crate::storage_metrics::COPIED_READER_BYTES
        },
        shared.as_ref().map_or(payload.body.len(), Bytes::len),
    );
    match shared {
        Some(bytes) => payload.freeze_shared(bytes),
        None => payload.freeze(),
    }
}

/// Use the single PUB output lease so slow inproc subscribers cannot retain
/// writer allocations. Packed payload bytes remain unchanged without decoding.
pub(crate) fn publication_frame(
    mut payload: crate::replicated::payload::Lease,
    shared: Option<Bytes>,
) -> Bytes {
    if let Some(bytes) = shared {
        assert!(bytes.len() <= payload.body.capacity());
        payload.body.extend_from_slice(&bytes);
    }
    reader_frame(payload, None)
}

mod delivery;
pub(crate) use delivery::Delivery;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Failure {
    pub(crate) code: u16,
    pub(crate) retry: RetryClass,
    detail: [u8; 16],
    size: usize,
}

impl Failure {
    pub(crate) fn detail(&self) -> &[u8] {
        &self.detail[..self.size]
    }

    pub(crate) fn new(code: u16) -> Self {
        Self {
            code,
            retry: match code {
                5 | 12 => RetryClass::AfterAuthorityRefresh,
                10 => RetryClass::AfterBackoff,
                11 => RetryClass::UnknownOutcome,
                _ => RetryClass::Permanent,
            },
            detail: [0; 16],
            size: 0,
        }
    }

    pub(crate) fn position(code: u16, position: u64) -> Self {
        let mut failure = Self::new(code);
        failure.detail[..8].copy_from_slice(&position.to_be_bytes());
        failure.size = 8;
        failure
    }

    pub(crate) fn record_too_large(bytes: usize, parts: usize) -> Self {
        let mut failure = Self::position(17, bytes as u64);
        failure.detail[8..].copy_from_slice(&(parts as u64).to_be_bytes());
        failure.size = 16;
        failure
    }
    pub(crate) fn ambiguous(first: u64, last: u64) -> Self {
        let mut failure = Self::position(20, first);
        failure.detail[8..].copy_from_slice(&last.to_be_bytes());
        failure.size = 16;
        failure
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory, replicated::payload::Payload, signal::DataSignal};
    use std::sync::Arc;

    #[tokio::test(flavor = "current_thread")]
    async fn publication_releases_writer_backing_before_a_slow_subscriber_drops_it() {
        let owner = memory::Domain::new(None, 4096)
            .unwrap()
            .owner(memory::Limits {
                bytes: 4096,
                buffers: 1,
                cache_bytes: 0,
            })
            .unwrap();
        let mut source = owner.try_lease(4096).unwrap();
        source.as_mut().fill(7);
        let payload = Payload::notifying(4096, Arc::new(DataSignal::default()));
        let frame = publication_frame(payload.try_take().unwrap(), Some(source.freeze()));
        assert_eq!(frame.as_ref(), &[7; 4096]);
        assert!(payload.try_take().is_none(), "one outstanding PUB payload");
        owner.trim_cache();
        assert!(
            owner.try_lease(4096).is_ok(),
            "a retained PUB frame cannot consume writer admission"
        );
        drop(frame);
        assert!(
            payload.try_take().is_some(),
            "last drop returns the PUB buffer"
        );
    }
}

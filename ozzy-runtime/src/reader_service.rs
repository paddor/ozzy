//! Shared reader cursor outcomes, bounded delivery credit, and response payloads.

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
                10 => RetryClass::AfterCredit,
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
}

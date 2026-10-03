//! Session-local transmissions. Admission and retained retry payloads live elsewhere.

use ozzy_proto::RequestId;
use std::collections::VecDeque;

pub(super) struct Sent {
    pub(super) admitted_at: Option<std::time::Instant>,
    pub(super) id: RequestId,
    pub(super) start: u64,
    pub(super) end: u64,
    pub(super) bytes: usize,
}

pub(super) struct Window {
    first: u64,
    next: u64,
    bytes: usize,
    sent: VecDeque<Sent>,
}

impl Window {
    /// Reconnect starts at the shared confirmed prefix, with no old correlations.
    pub(super) fn new(first: u64, capacity: usize) -> Self {
        Self {
            first,
            next: first,
            bytes: 0,
            sent: VecDeque::with_capacity(capacity),
        }
    }

    pub(super) fn next(&self) -> u64 {
        self.next
    }

    pub(super) fn requests(&self) -> usize {
        self.sent.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.sent.is_empty()
    }

    /// Called only after transport accepts the complete message. Preparation
    /// and `WouldBlock` must not make records eligible for confirmation.
    pub(super) fn sent(&mut self, request: Sent) {
        debug_assert!(request.start == self.next && request.end > request.start);
        self.next = request.end;
        self.bytes += request.bytes;
        self.sent.push_back(request);
    }

    pub(super) fn correlates(&self, id: Option<RequestId>) -> bool {
        self.sent.iter().any(|sent| Some(sent.id) == id)
    }

    /// An earlier request may still confirm while this one is refused.
    pub(super) fn has_prior(&self, id: RequestId) -> bool {
        self.sent
            .iter()
            .position(|sent| sent.id == id)
            .is_some_and(|position| position > 0)
    }

    pub(super) fn precedes(&self, left: RequestId, right: RequestId) -> bool {
        let position = |id| self.sent.iter().position(|sent| sent.id == id);
        matches!((position(left), position(right)), (Some(left), Some(right)) if left < right)
    }

    fn contains(&self, id: RequestId, first: u64, end: u64) -> bool {
        self.sent
            .iter()
            .any(|sent| sent.id == id && first >= sent.start && end <= sent.end)
    }

    pub(super) fn accepts(&self, id: RequestId, first: u64, end: u64, confirmed: u64) -> bool {
        first >= self.first && first <= confirmed && self.contains(id, first, end)
    }

    /// A sent range that starts after the confirmed prefix. The records
    /// between them stay unconfirmed until their own confirmation arrives.
    pub(super) fn skips(&self, id: RequestId, first: u64, end: u64, confirmed: u64) -> bool {
        first > confirmed && first < end && self.contains(id, first, end)
    }

    /// Payload retirement supplies the exact newly confirmed byte count.
    /// Partial replies return bytes but retain their request's slot and ID.
    pub(super) fn confirm(&mut self, end: u64, bytes: usize) {
        debug_assert!(end <= self.next && bytes <= self.bytes);
        while self.sent.front().is_some_and(|sent| sent.end <= end) {
            let sent = self.sent.pop_front().expect("confirmed request");
            crate::profiling::finish(crate::profiling::Stage::RequestRoundtrip, sent.admitted_at);
        }
        self.bytes -= bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sent(id: u8, start: u64, end: u64, bytes: usize) -> Sent {
        Sent {
            admitted_at: None,
            id: RequestId::from_bytes([id; 16]),
            start,
            end,
            bytes,
        }
    }

    #[test]
    fn partial_confirmation_keeps_request_slot_and_correlation() {
        let mut window = Window::new(10, 2);
        window.sent(sent(1, 10, 14, 64));
        window.sent(sent(2, 14, 17, 48));
        window.confirm(12, 32);
        assert_eq!((window.requests(), window.bytes), (2, 80));
        assert!(window.correlates(Some(RequestId::from_bytes([1; 16]))));
        window.confirm(14, 32);
        assert_eq!((window.requests(), window.bytes), (1, 48));
        assert!(!window.correlates(Some(RequestId::from_bytes([1; 16]))));
        window.confirm(17, 48);
        assert!(window.is_empty());
        assert_eq!((window.next(), window.bytes), (17, 0));
    }

    #[test]
    fn session_fences_unsent_ranges_and_reconnect_discards_correlations() {
        let mut window = Window::new(10, 2);
        let id = RequestId::from_bytes([1; 16]);
        assert!(!window.accepts(id, 10, 11, 10));
        window.sent(sent(1, 10, 14, 64));
        assert!(!window.accepts(id, 9, 14, 10));
        assert!(!window.accepts(id, 11, 14, 10));
        assert!(!window.accepts(id, 10, 15, 10));
        assert!(window.accepts(id, 10, 12, 10));
        window.confirm(12, 32);
        let retry = Window::new(12, 1);
        assert_eq!(retry.next(), 12);
        assert_eq!(retry.bytes, 0);
        assert!(!retry.correlates(Some(RequestId::from_bytes([1; 16]))));
        assert!(!retry.accepts(id, 10, 14, 12));
    }

    #[test]
    fn range_past_the_confirmed_prefix_is_neither_accepted_nor_unsent() {
        let mut window = Window::new(10, 2);
        window.sent(sent(1, 10, 14, 64));
        window.sent(sent(2, 14, 17, 48));
        let first = RequestId::from_bytes([1; 16]);
        let second = RequestId::from_bytes([2; 16]);
        assert!(window.skips(second, 14, 17, 10));
        assert!(!window.accepts(second, 14, 17, 10));
        assert!(!window.skips(first, 10, 14, 10));
        assert!(!window.skips(second, 14, 18, 10));
        assert!(!window.skips(second, 14, 14, 10));
    }

    #[test]
    fn refused_request_waits_for_earlier_confirmation() {
        let mut window = Window::new(0, 3);
        window.sent(sent(1, 0, 1, 10));
        window.sent(sent(2, 1, 2, 10));
        window.sent(sent(3, 2, 3, 10));
        let id = |value| RequestId::from_bytes([value; 16]);
        assert!(window.has_prior(id(2)));
        assert!(window.precedes(id(2), id(3)));
        assert!(!window.precedes(id(3), id(2)));
        window.confirm(1, 10);
        assert!(!window.has_prior(id(2)));
    }

    #[test]
    fn one_request_cannot_confirm_another_requests_range() {
        let mut window = Window::new(10, 2);
        window.sent(sent(1, 10, 11, 10));
        window.sent(sent(2, 11, 12, 10));
        let first = RequestId::from_bytes([1; 16]);
        let second = RequestId::from_bytes([2; 16]);
        assert!(!window.accepts(second, 10, 11, 10));
        assert!(!window.accepts(first, 10, 12, 10));
        assert!(!window.skips(first, 11, 12, 10));
        assert!(window.accepts(first, 10, 11, 10));
    }
}

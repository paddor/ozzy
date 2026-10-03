//! Subscription state shared by socket-owned and multiplexed reader adapters.

use super::{Failure, ReadScheduler};
use ozzy_proto::{
    Envelope,
    reader::{self, Source, Subscribe},
};

#[derive(Debug)]
pub(crate) struct Delivery<C> {
    pub(crate) request: Envelope,
    pub(crate) subscribe: Subscribe,
    pub(crate) source: Source,
    pub(crate) cursor: C,
    pub(crate) next: u64,
    pub(crate) sent_records: u64,
    pub(crate) sent_bytes: u64,
    pub(crate) grant_records: u64,
    pub(crate) grant_bytes: u64,
    pub(crate) schedule: ReadScheduler,
    received: Option<u64>,
    processed: Option<u64>,
    full_window: (u64, u64),
}

impl<C> Delivery<C> {
    pub(crate) fn new(request: Envelope, subscribe: Subscribe, source: Source, cursor: C) -> Self {
        Self {
            request,
            next: subscribe.start,
            subscribe,
            source,
            cursor,
            sent_records: 0,
            sent_bytes: 0,
            grant_records: 0,
            grant_bytes: 0,
            schedule: ReadScheduler::new(),
            received: None,
            processed: None,
            full_window: (0, 0),
        }
    }

    pub(crate) fn credit(
        &mut self,
        credit: reader::Credit,
        window: (u64, u64),
    ) -> Result<(), Failure> {
        if self.subscribe.subscription != credit.subscription || self.source != credit.source {
            return Ok(());
        }
        if credit.records < self.grant_records || credit.bytes < self.grant_bytes {
            return Ok(());
        }
        if credit.records.saturating_sub(self.sent_records) > window.0
            || credit.bytes.saturating_sub(self.sent_bytes) > window.1
        {
            return Err(Failure::new(10));
        }
        if credit.records > self.grant_records || credit.bytes > self.grant_bytes {
            self.schedule.credit_advanced();
        }
        self.full_window.0 = self.full_window.0.max(credit.records - self.sent_records);
        self.full_window.1 = self.full_window.1.max(credit.bytes - self.sent_bytes);
        self.grant_records = credit.records;
        self.grant_bytes = credit.bytes;
        Ok(())
    }

    /// Largest count/byte balance this subscription has actually granted. A
    /// temporarily consumed balance must not split a block the full window fits.
    pub(crate) fn full_window(
        &self,
        mut limits: ozzy_proto::data::DataLimits,
    ) -> ozzy_proto::data::DataLimits {
        limits.max_records = (limits.max_records as u64).min(self.full_window.0) as usize;
        limits.envelope.max_payload_bytes =
            (limits.envelope.max_payload_bytes as u64).min(self.full_window.1) as usize;
        limits
    }

    pub(crate) fn observe(&mut self, ack: reader::Ack) -> Result<(), Failure> {
        if ack.subscription != self.subscribe.subscription || ack.source != self.source {
            return Ok(());
        }
        if ack.received.is_some_and(|offset| offset >= self.next) {
            return Err(Failure::new(15));
        }
        self.received = self.received.max(ack.received);
        self.processed = self.processed.max(ack.processed);
        Ok(())
    }
}

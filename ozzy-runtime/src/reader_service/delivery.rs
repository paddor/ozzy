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
    pub(crate) schedule: ReadScheduler,
    received: Option<u64>,
    processed: Option<u64>,
}

impl<C> Delivery<C> {
    pub(crate) fn new(request: Envelope, subscribe: Subscribe, source: Source, cursor: C) -> Self {
        Self {
            request,
            next: match subscribe.start {
                reader::Start::Offset(offset) => offset,
                _ => 0,
            },
            subscribe,
            source,
            cursor,
            schedule: ReadScheduler::new(),
            received: None,
            processed: None,
        }
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

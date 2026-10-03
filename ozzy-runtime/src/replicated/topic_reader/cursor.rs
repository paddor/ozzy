use super::super::broker_links::readers::Inbox;
use super::{
    BrokerLinkError, BrokerLinks, Bytes, Context, Decoder, Duration, Offset, Poll, RouteGeneration,
    TopicReaderError, TopicRecord, TopicRoutes,
};
use omq_tokio::Message;
use ozzy_core::live::{LiveCursor, Publication, Replayed};
use ozzy_proto::{LinkSessionId, NodeId, reader};
use std::{future::Future, pin::Pin, sync::Arc};

mod batch;
mod delivery;
mod subscription;
use batch::{Batch, FrameRecords};

type Selected = (NodeId, LinkSessionId, reader::Subscribed);
pub(super) struct Opening {
    broker: NodeId,
    session: LinkSessionId,
    source: reader::Source,
    request: Pin<Box<dyn Future<Output = Result<Selected, BrokerLinkError>> + Send>>,
}
pub(super) type Returning = Pin<Box<dyn Future<Output = Result<(), BrokerLinkError>> + Send>>;

pub(super) struct Cursor {
    pub number: u32,
    pub next: u64,
    inbox: Arc<Inbox>,
    selected: Option<Selected>,
    accepted: Option<Selected>,
    opening: Option<Opening>,
    returning: Option<Returning>,
    canceling: Option<Returning>,
    sent_credit: (u64, u64),
    pending: Option<Batch>,
    pending_live: bool,
    held: Option<FrameRecords>,
    ready: Option<Batch>,
    released: Option<(u64, u64)>,
    records: u64,
    bytes: u64,
    refresh: Duration,
    due: Duration,
    decode: bytes::BytesMut,
    live: LiveCursor,
    observed_input: Option<(u64, RouteGeneration)>,
    verified_source: Option<RouteGeneration>,
    /// Last route that named a leader, as view and broker.
    leader: Option<(u64, NodeId)>,
    pub live_records: u64,
    pub replayed_records: u64,
}

impl Cursor {
    pub(super) fn new(
        links: &BrokerLinks,
        partition: &ozzy_proto::directory::TopicPartition,
        from: u64,
        refresh: Duration,
    ) -> Result<Self, BrokerLinkError> {
        links.reader_window()?;
        let mut prefix = [0; 32];
        prefix[..16].copy_from_slice(partition.group.as_bytes());
        prefix[16..].copy_from_slice(partition.incarnation.as_bytes());
        Ok(Self {
            number: partition.number,
            next: from,
            inbox: links.reader_inbox(Bytes::copy_from_slice(&prefix))?,
            selected: None,
            accepted: None,
            opening: None,
            returning: None,
            canceling: None,
            sent_credit: (0, 0),
            pending: None,
            pending_live: false,
            held: None,
            ready: None,
            released: None,
            records: 0,
            bytes: 0,
            refresh,
            due: Duration::ZERO,
            decode: bytes::BytesMut::new(),
            live: LiveCursor::new(from, refresh),
            observed_input: None,
            verified_source: None,
            leader: None,
            live_records: 0,
            replayed_records: 0,
        })
    }

    pub(super) fn deadline(&self) -> Option<Duration> {
        if self.waiting_for_capacity() {
            return None;
        }
        Some(self.live.deadline().map_or(self.due, |at| at.min(self.due)))
    }

    fn waiting_for_capacity(&self) -> bool {
        // Application payload aliases retain frame backing. Sleep without a
        // timer or repeated subscriptions until their release signals capacity.
        self.live.replay().is_some()
            && self.selected.is_none()
            && self.opening.is_none()
            && !self.inbox.has_capacity()
    }

    fn waiting_for_input(&self, input: (u64, RouteGeneration), now: Duration) -> bool {
        // A quiet partition cannot gain records, credit, or a new source
        // without input, a pending future, or its existing repair timer.
        // Buffered records and observer futures remain independently runnable.
        self.observed_input == Some(input)
            && self.pending.is_none()
            && self.ready.is_none()
            && self.opening.is_none()
            && self.returning.is_none()
            && self.canceling.is_none()
            && self.deadline().is_none_or(|deadline| now < deadline)
    }

    pub(super) fn poll(
        &mut self,
        links: &BrokerLinks,
        routes: &TopicRoutes,
        decoder: &mut Decoder,
        now: Duration,
        input: (u64, RouteGeneration),
        cx: &mut Context<'_>,
    ) -> Poll<Result<TopicRecord, TopicReaderError>> {
        if self.waiting_for_input(input, now) {
            return Poll::Pending;
        }
        // Buffered records already passed the source check. Route and physical
        // session changes invalidate this generation before the next delivery.
        if self.pending.is_some()
            && self.verified_source == Some(input.1)
            && let Some(record) = self.deliver(links, routes, decoder)?
        {
            return Poll::Ready(Ok(record));
        }
        self.verified_source = None;
        self.observed_input = Some(input);
        let accepted = self.accepted;
        let route = routes.route(self.number)?;
        let source = self
            .accepted
            .map(|(broker, session, selected)| (broker, session, selected.source))
            .or_else(|| {
                self.opening
                    .as_ref()
                    .map(|opening| (opening.broker, opening.session, opening.source))
            });
        if let Some((broker, session, source)) = source
            && (links.session(broker) != Some(session)
                || route.as_ref().is_some_and(|route| {
                    route.leader != Some(broker) || source_view(source) != route.view
                }))
        {
            self.reset();
            // Dropping the old observer frees its SDK control slot when the
            // driver runs. Wake it even if retained payloads block a new open.
            routes.refresh(self.number)?;
        }
        // A partition that waits for a leader subscribes as soon as a route
        // names one, not at its next refresh.
        let leader = route
            .as_ref()
            .and_then(|route| route.leader.map(|leader| (route.view, leader)));
        if leader != self.leader {
            self.leader = leader;
            if leader.is_some() && self.selected.is_none() && self.opening.is_none() {
                self.due = now;
            }
        }
        self.poll_returns(links, routes, cx)?;
        self.poll_opening(links, routes, now, cx)?;
        if accepted.is_some() && self.accepted == accepted {
            self.verified_source = Some(input.1);
        }
        if let Some(record) = self.deliver(links, routes, decoder)? {
            return Poll::Ready(Ok(record));
        }
        self.release(links, cx)?;
        if let Some(ready) = self.ready.take() {
            self.pending = Some(ready);
            self.pending_live = true;
            cx.waker().wake_by_ref();
        } else {
            if self.accepted.is_some()
                && !self.live.is_paused()
                && let Some(message) = self.inbox.publication()
            {
                self.publication(links, &message, now)?;
                cx.waker().wake_by_ref();
            }
            if self.pending.is_none() && self.live.replay().is_some() && self.selected.is_some() {
                match self.inbox.pop() {
                    Ok(Some(message)) => {
                        self.replay(links, &message, now, cx)?;
                        cx.waker().wake_by_ref();
                    }
                    Err(_) => {
                        self.reset();
                        routes.refresh(self.number)?;
                    }
                    Ok(None) => {}
                }
            }
        }
        self.retire(links);
        if self.pending.is_none() && self.live.tick(now) {
            self.due = now;
        }
        if now >= self.due && !self.waiting_for_capacity() {
            self.due = now + self.refresh;
            routes.refresh(self.number)?;
            if self.live.replay().is_some()
                && self.selected.is_none()
                && self.opening.is_none()
                && self.canceling.is_none()
            {
                self.open(links, routes, route)?;
                if self.opening.is_some() {
                    cx.waker().wake_by_ref();
                }
            }
        }
        Poll::Pending
    }

    fn release(
        &mut self,
        links: &BrokerLinks,
        cx: &mut Context<'_>,
    ) -> Result<(), BrokerLinkError> {
        if self.pending.take().is_some()
            && let Some((records, bytes)) = self.released.take()
        {
            self.records = self
                .records
                .checked_add(records)
                .ok_or(BrokerLinkError::Response)?;
            self.bytes = self
                .bytes
                .checked_add(bytes)
                .ok_or(BrokerLinkError::Response)?;
            if self.selected.is_some() {
                self.credit(links)?;
            }
            cx.waker().wake_by_ref();
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.verified_source = None;
        self.opening = None;
        self.returning = None;
        self.canceling = None;
        self.pending = None;
        self.held = None;
        self.ready = None;
        self.released = None;
        self.selected = None;
        self.accepted = None;
        self.inbox.reset_delivery();
        self.live = LiveCursor::new(self.next, self.refresh);
        self.due = Duration::ZERO;
    }
}

fn source_view(source: reader::Source) -> u64 {
    match source {
        reader::Source::Group { authority, .. } => authority.view,
        reader::Source::Local { .. } => 0,
    }
}

fn retryable(error: &BrokerLinkError) -> bool {
    matches!(
        error,
        BrokerLinkError::Timeout
            | BrokerLinkError::Session
            | BrokerLinkError::Rejected {
                retry: ozzy_proto::nack::RetryClass::AfterAuthorityRefresh
                    | ozzy_proto::nack::RetryClass::AfterCredit
                    | ozzy_proto::nack::RetryClass::UnknownOutcome,
                ..
            }
    )
}

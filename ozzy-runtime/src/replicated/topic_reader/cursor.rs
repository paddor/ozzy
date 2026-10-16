use super::super::broker_links::{OwnerClock, readers::Inbox};
use super::{
    BrokerLinkError, BrokerLinks, Bytes, Context, Decoder, Duration, Offset, Poll, RouteGeneration,
    TopicReaderError, TopicRecord, TopicRoutes,
};
use omq_tokio::Message;
use ozzy_core::live::{LiveCursor, Publication, Replayed};
use ozzy_proto::{LinkSessionId, NodeId, directory::RouteState, reader};
use std::{future::Future, pin::Pin, sync::Arc};

mod batch;
mod delivery;
mod subscription;
use batch::{Batch, FrameRecords};

/// Reader and route generations a reader poll captured before visiting any
/// cursor, and the owner clock of this visit.
#[derive(Clone, Copy)]
pub(super) struct Observed<'a> {
    pub(super) input: (u64, RouteGeneration),
    pub(super) clock: &'a OwnerClock,
}

type Selected = (NodeId, LinkSessionId, reader::Subscribed);
pub(super) struct Opening {
    broker: NodeId,
    session: LinkSessionId,
    subscription: reader::Subscription,
    source: reader::Source,
    request: Pin<Box<dyn Future<Output = Result<Selected, BrokerLinkError>> + Send>>,
}
pub(super) type Returning = Pin<Box<dyn Future<Output = Result<(), BrokerLinkError>> + Send>>;

pub(super) struct Cursor {
    pub number: u32,
    pub next: u64,
    selector: Option<reader::Start>,
    inbox: Arc<Inbox>,
    selected: Option<Selected>,
    accepted: Option<Selected>,
    opening: Option<Opening>,
    /// The opening subscription's first replay input, taken from the inbox
    /// early so stale input behind it cannot hold the capacity it needs.
    early: Option<Message>,
    canceling: Option<Returning>,
    pending: Option<Batch>,
    pending_live: bool,
    held: Option<FrameRecords>,
    ready: Option<Batch>,
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
    pub live_gaps: u64,
}

impl Cursor {
    pub(super) fn new(
        links: &BrokerLinks,
        partition: &ozzy_proto::directory::TopicPartition,
        start: reader::Start,
        refresh: Duration,
    ) -> Result<Self, BrokerLinkError> {
        let from = match start {
            reader::Start::Offset(offset) => offset,
            _ => 0,
        };
        let mut prefix = [0; 32];
        prefix[..16].copy_from_slice(partition.group.as_bytes());
        prefix[16..].copy_from_slice(partition.incarnation.as_bytes());
        Ok(Self {
            number: partition.number,
            next: from,
            selector: Some(start),
            inbox: links.reader_inbox(Bytes::copy_from_slice(&prefix))?,
            selected: None,
            accepted: None,
            opening: None,
            early: None,
            canceling: None,
            pending: None,
            pending_live: false,
            held: None,
            ready: None,
            refresh,
            due: Duration::ZERO,
            decode: bytes::BytesMut::new(),
            live: LiveCursor::new(from, refresh),
            observed_input: None,
            verified_source: None,
            leader: None,
            live_records: 0,
            replayed_records: 0,
            live_gaps: 0,
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
            && !self.inbox.can_subscribe()
    }

    fn waiting_for_input(&self, input: (u64, RouteGeneration), now: Duration) -> bool {
        // A quiet partition cannot gain records or a new source
        // without input, a pending future, or its existing repair timer.
        // Buffered records and observer futures remain independently runnable.
        self.observed_input == Some(input)
            && self.pending.is_none()
            && self.ready.is_none()
            && self.opening.is_none()
            && self.canceling.is_none()
            && self.deadline().is_none_or(|deadline| now < deadline)
    }

    pub(super) fn poll(
        &mut self,
        routes: &TopicRoutes,
        decoder: &mut Decoder,
        observed: Observed<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<TopicRecord, TopicReaderError>> {
        let (links, input) = (routes.links(), observed.input);
        // Buffered records already passed the source check. Route and physical
        // session changes invalidate this generation before the next delivery.
        // This per-record path reads no clock.
        if self.pending.is_some()
            && self.verified_source == Some(input.1)
            && let Some(record) = self.deliver(links, routes, decoder)?
        {
            return Poll::Ready(Ok(record));
        }
        let now = observed.clock.now();
        if self.waiting_for_input(input, now) {
            return Poll::Pending;
        }
        self.verified_source = None;
        self.observed_input = Some(input);
        let accepted = self.accepted;
        let route = routes.route(self.number)?;
        self.check_source(routes, route.as_ref())?;
        self.follow_leader(route.as_ref(), now);
        self.poll_returns(routes, cx)?;
        self.poll_opening(links, routes, now, cx)?;
        if self.selected.is_none() && self.early.is_none() {
            // Without a selection, queued replay is stale, except the first
            // input of an opening subscription, held until it is accepted.
            self.early = self.next_replay();
        }
        if accepted.is_some() && self.accepted == accepted {
            self.verified_source = Some(input.1);
        }
        if let Some(record) = self.deliver(links, routes, decoder)? {
            return Poll::Ready(Ok(record));
        }
        self.pending = None;
        self.take_input(links, now, cx)?;
        self.retire(links, routes, cx)?;
        if self.pending.is_none() && self.live.tick(now) {
            self.due = now;
        }
        if now >= self.due && !self.waiting_for_capacity() {
            self.due = now.saturating_add(self.refresh);
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

    /// Reset when the accepted or opening source lost its link session, or
    /// `route` names another leader or view.
    fn check_source(
        &mut self,
        routes: &TopicRoutes,
        route: Option<&RouteState>,
    ) -> Result<(), TopicReaderError> {
        let source = self
            .accepted
            .map(|(broker, session, selected)| (broker, session, selected.source))
            .or_else(|| {
                self.opening
                    .as_ref()
                    .map(|opening| (opening.broker, opening.session, opening.source))
            });
        if let Some((broker, session, source)) = source
            && (routes.links().session(broker) != Some(session)
                || route.is_some_and(|route| {
                    route.leader != Some(broker) || source_view(source) != route.view
                }))
        {
            self.reset();
            // Dropping the old observer frees its SDK control slot when the
            // driver runs. Wake it even if retained payloads block a new open.
            routes.refresh(self.number)?;
        }
        Ok(())
    }

    /// A partition that waits for a leader subscribes as soon as a route
    /// names one, not at its next refresh.
    fn follow_leader(&mut self, route: Option<&RouteState>, now: Duration) {
        let leader = route.and_then(|route| route.leader.map(|leader| (route.view, leader)));
        if leader != self.leader {
            self.leader = leader;
            if leader.is_some() && self.selected.is_none() && self.opening.is_none() {
                self.due = now;
            }
        }
    }

    /// Stage the held live records, else apply one publication and one
    /// replay input, waking the reader after each.
    fn take_input(
        &mut self,
        links: &BrokerLinks,
        now: Duration,
        cx: &mut Context<'_>,
    ) -> Result<(), TopicReaderError> {
        if let Some(ready) = self.ready.take() {
            self.pending = Some(ready);
            self.pending_live = true;
            cx.waker().wake_by_ref();
            return Ok(());
        }
        if self.accepted.is_some()
            && !self.live.is_paused()
            && let Some(message) = self.inbox.publication()
        {
            self.publication(links, &message, now)?;
            cx.waker().wake_by_ref();
        }
        if self.pending.is_none()
            && self.live.replay().is_some()
            && self.selected.is_some()
            && let Some(message) = self.early.take().or_else(|| self.next_replay())
        {
            self.replay(links, &message, now, cx)?;
            cx.waker().wake_by_ref();
        }
        Ok(())
    }

    /// The next replay input of the subscription this cursor selected or is
    /// opening. The inbox drops every other generation's input.
    fn next_replay(&self) -> Option<Message> {
        let current = self
            .selected
            .map(|(_, _, selected)| selected.subscription)
            .or_else(|| self.opening.as_ref().map(|opening| opening.subscription));
        self.inbox.pop(current)
    }

    /// Drop queued replay, including an early frame.
    fn clear_replay(&mut self) {
        self.early = None;
        self.inbox.clear();
    }

    fn reset(&mut self) {
        self.verified_source = None;
        self.opening = None;
        self.canceling = None;
        self.pending = None;
        self.held = None;
        self.ready = None;
        self.selected = None;
        self.accepted = None;
        self.clear_replay();
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
                    | ozzy_proto::nack::RetryClass::AfterBackoff
                    | ozzy_proto::nack::RetryClass::UnknownOutcome,
                ..
            }
    )
}

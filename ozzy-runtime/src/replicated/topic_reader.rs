//! Topic-wide merged records over shared SUB delivery and authoritative PEER replay.

mod closing;
mod decoder;
mod error;
pub use error::ReaderError;
mod cursor;
use super::{BrokerLinkError, BrokerLinks, RouteGeneration, SdkClock, TopicRoutes};
use bytes::Bytes;
use closing::Closing;
use cursor::Cursor;
use decoder::Decoder;
use ozzy_proto::{MessageId, Offset, PartitionIncarnation, TopicId};
use std::{
    collections::BTreeSet,
    future::poll_fn,
    task::{Context, Poll},
    time::Duration,
};

/// Volatile receive positions. Persist them only after the application has
/// completed its own processing; this is not a consumer-group progress service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicCheckpoint {
    /// Exact topic incarnation. Recreating a topic invalidates its checkpoints.
    pub topic: TopicId,
    /// First undelivered offset of each subscribed numeric partition.
    pub positions: Vec<(u32, Offset)>,
}

/// Initial history selection. Reconnects use delivered offsets, never repeat a seek.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub enum ReaderStart {
    /// First retained record in each selected partition.
    #[default]
    Earliest,
    /// Current confirmed ends, followed by new records.
    Latest,
    /// Application-saved per-partition next offsets.
    Checkpoint(TopicCheckpoint),
    /// First retained offset whose broker append time is at least Unix milliseconds.
    Timestamp(u64),
    /// Select exactly one partition by application record ID.
    RecordId {
        /// Numeric topic partition.
        partition: u32,
        /// Application record identity, independent of producer retry identity.
        id: MessageId,
        /// Explicit duplicate selection; [`Self::record_id`] requires uniqueness.
        policy: ozzy_proto::reader::IdPolicy,
    },
}

impl ReaderStart {
    /// Select one partition by ID, rejecting multiple retained matches by default.
    pub const fn record_id(partition: u32, id: MessageId) -> Self {
        Self::RecordId {
            partition,
            id,
            policy: ozzy_proto::reader::IdPolicy::RequireUnique,
        }
    }
}

pub use ozzy_proto::reader::IdPolicy;

/// Topic reader configuration. Default starts at each partition's retained floor.
#[derive(Debug, Clone)]
pub struct TopicReaderConfig {
    /// Initial selector. Checkpoints require every selected partition.
    pub start: ReaderStart,
    /// Optional numeric partition filter. Routing remains automatic.
    pub partitions: Option<Vec<u32>>,
    /// Refresh after silence or an unavailable leader. Uses the SDK clock.
    pub refresh: Duration,
}

impl Default for TopicReaderConfig {
    fn default() -> Self {
        Self {
            start: ReaderStart::Earliest,
            partitions: None,
            refresh: Duration::from_millis(100),
        }
    }
}

/// One original confirmed record. Ordering is per partition, never topic-wide.
#[derive(Debug, Clone)]
pub struct TopicRecord {
    /// Topic incarnation.
    pub topic: TopicId,
    /// Numeric partition, useful for receipts and checkpoints.
    pub partition: u32,
    /// Persistent partition incarnation.
    pub incarnation: PartitionIncarnation,
    /// Global offset within this partition.
    pub offset: Offset,
    /// Original writer identity, independent of batching.
    pub message_id: MessageId,
    /// Original multipart bytes, decoded at this application boundary.
    pub payload: smallvec::SmallVec<[Bytes; 2]>,
}

/// Actual delivery paths of individual records returned by this reader.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TopicReaderStats {
    /// Records delivered through broker PUB and SDK SUB.
    pub live_records: u64,
    /// Records delivered through authoritative PEER replay.
    pub replayed_records: u64,
}

/// Bounded topic reader sharing the SDK's broker connections with other readers
/// and writers. Cancellation retains subscription, source, and delivery state.
pub struct TopicReader {
    links: BrokerLinks,
    routes: TopicRoutes,
    clock: SdkClock,
    cursors: Vec<Cursor>,
    next: usize,
    remaining: usize,
    decoder: Decoder,
    failed: bool,
    closed: bool,
    closing: Option<std::sync::Arc<Closing>>,
    closed_checkpoint: Option<TopicCheckpoint>,
    closed_stats: Option<TopicReaderStats>,
}

impl std::fmt::Debug for TopicReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicReader")
            .field("topic", &self.routes.metadata().name())
            .field("partitions", &self.cursors.len())
            .finish_non_exhaustive()
    }
}

impl TopicReader {
    /// Next owner deadline, including a refused subscription's backoff.
    pub(crate) fn deadline(&self) -> Option<Duration> {
        self.cursors.iter().filter_map(Cursor::deadline).min()
    }

    /// Look up the topic and register all selected interests without waiting for
    /// every broker. `next` establishes independent partition subscriptions.
    pub async fn open(
        links: BrokerLinks,
        topic: &str,
        config: TopicReaderConfig,
    ) -> Result<Self, TopicReaderError> {
        if config.refresh < Duration::from_millis(1) || config.refresh > Duration::from_hours(24) {
            return Err(TopicReaderError::Configuration);
        }
        let metadata = links.topic(topic).await?;
        let selected = config
            .partitions
            .clone()
            .unwrap_or_else(|| match config.start {
                ReaderStart::RecordId { partition, .. } => vec![partition],
                _ => (0..metadata.partition_count() as u32).collect(),
            });
        if let ReaderStart::RecordId { partition, .. } = config.start
            && selected != [partition]
        {
            return Err(TopicReaderError::Configuration);
        }
        let unique: BTreeSet<_> = selected.iter().copied().collect();
        if selected.is_empty()
            || unique.len() != selected.len()
            || selected.iter().any(|&n| metadata.partition(n).is_none())
        {
            return Err(TopicReaderError::Configuration);
        }
        let checkpoint = match &config.start {
            ReaderStart::Checkpoint(checkpoint) => Some(checkpoint),
            _ => None,
        };
        if let Some(checkpoint) = checkpoint
            && (checkpoint.topic != metadata.id()
                || checkpoint.positions.len() != selected.len()
                || checkpoint
                    .positions
                    .iter()
                    .map(|&(n, _)| n)
                    .collect::<BTreeSet<_>>()
                    != unique)
        {
            return Err(TopicReaderError::Configuration);
        }
        let clock = links.clock();
        let routes = links.routes(metadata)?;
        links.reader_publications(routes.metadata())?;
        let mut cursors = Vec::with_capacity(selected.len());
        for number in selected {
            routes.interest(number)?;
            let from = match &config.start {
                ReaderStart::Earliest => ozzy_proto::reader::Start::Earliest,
                ReaderStart::Latest => ozzy_proto::reader::Start::Latest,
                ReaderStart::Timestamp(timestamp) => {
                    ozzy_proto::reader::Start::Timestamp(*timestamp)
                }
                ReaderStart::RecordId { id, policy, .. } => ozzy_proto::reader::Start::RecordId {
                    id: *id,
                    policy: *policy,
                },
                ReaderStart::Checkpoint(checkpoint) => ozzy_proto::reader::Start::Offset(
                    checkpoint
                        .positions
                        .iter()
                        .find(|&&(n, _)| n == number)
                        .expect("checked checkpoint")
                        .1
                        .get(),
                ),
            };
            cursors.push(Cursor::new(
                &links,
                routes
                    .metadata()
                    .partition(number)
                    .expect("checked partition"),
                from,
                config.refresh,
            )?);
        }
        Ok(Self {
            links,
            routes,
            clock,
            cursors,
            next: 0,
            remaining: 0,
            decoder: Decoder::default(),
            failed: false,
            closed: false,
            closing: None,
            closed_checkpoint: None,
            closed_stats: None,
        })
    }

    /// Wait for one individual record. A slow or unreachable partition leaves
    /// other partitions runnable. Cancellation never advances a checkpoint.
    pub async fn next(&mut self) -> Result<TopicRecord, TopicReaderError> {
        if self.failed || self.closed {
            return Err(TopicReaderError::Closed);
        }
        loop {
            let seen = self.links.reader_generation();
            let routes_seen = self.routes.generation();
            // Received records still pass through the cursor's source fences.
            // Capture generations first so an empty poll cannot lose a wakeup.
            if let Poll::Ready(result) = poll_fn(|cx| Poll::Ready(self.poll(cx))).await {
                if result.is_err() {
                    self.failed = true;
                }
                return result;
            }
            let deadline = self.deadline();
            let links = self.links.clone();
            let routes = self.routes.clone();
            let clock = self.clock.clone();
            tokio::select! {
                result = poll_fn(|cx| self.poll(cx)) => {
                    if result.is_err() { self.failed = true; }
                    return result;
                }
                () = links.reader_changed_after(seen) => {},
                result = routes.changed_after(routes_seen) => result?,
                () = async {
                    match deadline {
                        Some(deadline) => clock.until(deadline).await,
                        // Retained payloads can consume every frame permit.
                        // Alias release wakes through reader_changed_after.
                        None => std::future::pending().await,
                    }
                } => {},
                () = links.closed() => return Err(links.closed_error().into()),
            }
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<TopicRecord, TopicReaderError>> {
        let count = self.cursors.len();
        let input = (self.links.reader_generation(), self.routes.generation());
        if self.remaining == 0 {
            self.remaining = count;
        }
        for _ in 0..self.remaining.min(16) {
            let index = self.next;
            self.next = (index + 1) % count;
            self.remaining -= 1;
            match self.cursors[index].poll(
                &self.links,
                &self.routes,
                &mut self.decoder,
                self.clock.now(),
                input,
                cx,
            ) {
                Poll::Ready(Ok(record)) => {
                    // This partition may hold more received records. Visit
                    // every partition again before waiting for input.
                    self.remaining = count;
                    return Poll::Ready(Ok(record));
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }
        if self.remaining > 0 {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    /// Positions advance only when individual records reach this caller.
    pub fn checkpoint(&self) -> TopicCheckpoint {
        if let Some(checkpoint) = &self.closed_checkpoint {
            return checkpoint.clone();
        }
        TopicCheckpoint {
            topic: self.routes.metadata().id(),
            positions: self
                .cursors
                .iter()
                .map(|cursor| (cursor.number, Offset::new(cursor.next)))
                .collect(),
        }
    }

    /// Count actual returned records by transport path, without inferring
    /// publication receipt from subscription setup or socket admission.
    pub fn stats(&self) -> TopicReaderStats {
        if let Some(stats) = self.closed_stats {
            return stats;
        }
        self.cursors
            .iter()
            .fold(TopicReaderStats::default(), |mut stats, cursor| {
                stats.live_records += cursor.live_records;
                stats.replayed_records += cursor.replayed_records;
                stats
            })
    }

    /// Validate application processing for one delivered partition and report it
    /// during active PEER replay. Live delivery has no broker subscription to
    /// observe. This never claims durable application processing.
    pub async fn acknowledge(
        &self,
        partition: u32,
        processed: Option<Offset>,
    ) -> Result<(), TopicReaderError> {
        let cursor = self
            .cursors
            .iter()
            .find(|cursor| cursor.number == partition)
            .ok_or(TopicReaderError::Configuration)?;
        cursor.acknowledge(&self.links, processed).await
    }

    /// Close only this reader's subscriptions. Shared writers and links remain usable.
    pub fn close(
        &mut self,
    ) -> impl std::future::Future<Output = Result<(), TopicReaderError>> + Send + 'static + use<>
    {
        self.closed = true;
        if self.closing.is_none() {
            self.closed_checkpoint = Some(self.checkpoint());
            self.closed_stats = Some(self.stats());
            self.closing = Some(Closing::start(
                &self.links,
                std::mem::take(&mut self.cursors),
            ));
        }
        let closing = self.closing.as_ref().expect("requested close").clone();
        async move { closing.closed().await }
    }
}

impl Drop for TopicReader {
    fn drop(&mut self) {
        if self.closing.is_none() {
            self.closing = Some(Closing::start(
                &self.links,
                std::mem::take(&mut self.cursors),
            ));
        }
    }
}

/// Topic lookup, source validation, or reader delivery failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TopicReaderError {
    /// No match survives retention, or the ID was never present.
    #[error("partition {partition}: record ID not found in retained history")]
    RecordNotFound {
        /// Numeric topic partition.
        partition: u32,
        /// Earliest currently retained offset.
        earliest: Offset,
    },
    /// Multiple retained records carry the ID; choose a duplicate policy explicitly.
    #[error("partition {partition}: ambiguous record ID at offsets {first:?}..{last:?}")]
    AmbiguousRecordId {
        /// Numeric topic partition.
        partition: u32,
        /// Oldest retained match.
        first: Offset,
        /// Newest retained match.
        last: Offset,
    },
    /// Invalid filter, checkpoint, or bounds.
    #[error("invalid topic reader configuration")]
    Configuration,
    /// Reader closed or previously failed.
    #[error("topic reader closed")]
    Closed,
    /// Requested records expired. Never skip them implicitly.
    #[error("partition {partition} records expired; earliest offset {earliest:?}")]
    RetentionGap {
        /// Numeric partition.
        partition: u32,
        /// First retained offset.
        earliest: Offset,
    },
    /// Broker link or protocol failed.
    #[error(transparent)]
    Broker(#[from] BrokerLinkError),
    /// Bounded record decoder rejected a delivery.
    #[error(transparent)]
    Reader(#[from] ReaderError),
}

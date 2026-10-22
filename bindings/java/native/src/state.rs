use ozzy::{
    BrokerLinks, ProducerIdentity, ReaderStart, RecordInput, RetryPolicy, SharedTopicPendingRecord,
    SharedTopicReceipt, SharedTopicWriter, SharedTopicWriterConfig, TopicReader, TopicReaderConfig,
    TopicRecord, WriterError, WriterRuntime,
};
use std::{future::Future, pin::Pin, sync::Arc};

/// A producer's captured flush prefix; it borrows no handle while waiting.
pub(crate) struct Flush(pub(crate) Pin<Box<dyn Future<Output = Result<(), WriterError>> + Send>>);
impl std::fmt::Debug for Flush {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Flush")
    }
}

#[derive(Debug)]
pub(crate) struct Client {
    runtime: Option<WriterRuntime>,
    links: Option<BrokerLinks>,
    config: ozzy_bindings::Config,
}
/// A client's shared links and producer configuration, cloned for one open
/// so the client stays available to other opens meanwhile.
#[derive(Debug)]
pub(crate) struct Open {
    links: BrokerLinks,
    writer: SharedTopicWriterConfig,
}
impl Client {
    pub(crate) fn open(&self) -> Result<Open, String> {
        Ok(Open {
            links: self.links.as_ref().ok_or("client is closed")?.clone(),
            writer: self.config.writer.clone(),
        })
    }
}
#[derive(Debug)]
pub(crate) struct Consumer {
    pub(crate) reader: TopicReader,
    pub(crate) last: Option<TopicRecord>,
    pub(crate) parts: Vec<[u64; 2]>,
}
#[derive(Debug)]
pub(crate) enum Handle {
    Client(Client),
    Producer(Option<SharedTopicWriter>),
    Consumer(Consumer),
}
#[derive(Debug)]
pub(crate) enum Request {
    Connect(Box<ozzy_bindings::Config>),
    Producer(Open, String, Option<ProducerIdentity>, bool),
    Consumer(Open, String, ReaderStart, Option<Vec<u32>>),
    Send(RecordInput, Option<Vec<u8>>, Option<u32>),
    Flush(Flush),
    Next,
    Confirm(Arc<SharedTopicPendingRecord>),
    Close,
}
#[derive(Debug)]
pub(crate) enum Value {
    Unit,
    Handle(Box<Handle>),
    /// Shared immutable observer; Java owns one strong count per pointer.
    Pending(Arc<SharedTopicPendingRecord>),
    Receipt(SharedTopicReceipt),
    Record(TopicRecord),
}

pub(crate) async fn run(
    handle: &mut Option<Box<Handle>>,
    request: Request,
) -> Result<Value, String> {
    match request {
        Request::Connect(config) => {
            let runtime = runtime().map_err(|e| e.to_string())?;
            let links = BrokerLinks::connect(&runtime, config.links.clone())
                .await
                .map_err(text)?;
            Ok(Value::Handle(Box::new(Handle::Client(Client {
                runtime: Some(runtime),
                links: Some(links),
                config: *config,
            }))))
        }
        Request::Confirm(pending) => pending.confirmed().await.map(Value::Receipt).map_err(text),
        Request::Flush(Flush(flushed)) => flushed.await.map(|()| Value::Unit).map_err(text),
        Request::Producer(open, topic, identity, takeover) => {
            producer(open, topic, identity, takeover).await
        }
        Request::Consumer(open, topic, start, partitions) => {
            consumer(open, topic, start, partitions).await
        }
        request => {
            operate(
                handle.as_deref_mut().ok_or("missing native handle")?,
                request,
            )
            .await
        }
    }
}
async fn operate(handle: &mut Handle, request: Request) -> Result<Value, String> {
    match (handle, request) {
        (Handle::Producer(writer), Request::Send(record, key, partition)) => send(
            writer.as_mut().ok_or("producer is closed")?,
            record,
            key.as_deref(),
            partition,
        )
        .await
        .map(|pending| Value::Pending(Arc::new(pending))),
        (Handle::Consumer(consumer), Request::Next) => {
            consumer.last = None;
            consumer
                .reader
                .next()
                .await
                .map(Value::Record)
                .map_err(text)
        }
        (handle, Request::Close) => close(handle).await.map(|()| Value::Unit),
        _ => Err("operation does not match native handle".into()),
    }
}
pub(crate) async fn send(
    writer: &mut SharedTopicWriter,
    record: RecordInput,
    key: Option<&[u8]>,
    partition: Option<u32>,
) -> Result<SharedTopicPendingRecord, String> {
    match partition {
        Some(partition) => writer.send_to_partition(record, partition).await,
        None => writer.send(record, key).await,
    }
    .map_err(text)
}
async fn close(handle: &mut Handle) -> Result<(), String> {
    match handle {
        Handle::Client(client) => {
            if let Some(links) = &client.links {
                links.shutdown().await.map_err(text)?;
            }
            // The last owners join SDK and transport threads. Drop them on a
            // blocking thread so the shared binding executor keeps running.
            let owners = (client.links.take(), client.runtime.take());
            crate::execution::blocking(move || drop(owners))?
                .await
                .map_err(text)?;
            Ok(())
        }
        Handle::Producer(writer) => {
            if let Some(writer) = writer.take() {
                writer.close().await.map_err(text)?;
            }
            Ok(())
        }
        Handle::Consumer(consumer) => {
            consumer.last = None;
            consumer.reader.close().await.map_err(text)
        }
    }
}
pub(crate) fn text(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn runtime() -> std::io::Result<WriterRuntime> {
    #[cfg(test)]
    if let Some(context) = crate::tests::context() {
        return WriterRuntime::with_context(context);
    }
    WriterRuntime::new()
}

async fn consumer(
    open: Open,
    topic: String,
    start: ReaderStart,
    partitions: Option<Vec<u32>>,
) -> Result<Value, String> {
    let config = TopicReaderConfig {
        start,
        partitions,
        ..Default::default()
    };
    let reader = TopicReader::open(open.links, &topic, config)
        .await
        .map_err(text)?;
    Ok(Value::Handle(Box::new(Handle::Consumer(Consumer {
        reader,
        last: None,
        parts: Vec::new(),
    }))))
}

async fn producer(
    open: Open,
    topic: String,
    identity: Option<ProducerIdentity>,
    takeover: bool,
) -> Result<Value, String> {
    let (links, config) = (&open.links, open.writer);
    let retry = RetryPolicy::default();
    let writer = match identity {
        Some(identity) if takeover => {
            SharedTopicWriter::takeover(links, &topic, identity, config, retry).await
        }
        Some(identity) => SharedTopicWriter::resume(links, &topic, identity, config, retry).await,
        None => SharedTopicWriter::open(links, &topic, config, retry).await,
    }
    .map_err(text)?;
    Ok(Value::Handle(Box::new(Handle::Producer(Some(writer)))))
}

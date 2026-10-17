use ozzy::{
    BrokerLinks, ProducerIdentity, ReaderStart, RecordInput, RetryPolicy, SharedTopicPendingRecord,
    SharedTopicReceipt, SharedTopicWriter, TopicReader, TopicReaderConfig, TopicRecord,
    WriterRuntime,
};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct Client {
    runtime: Option<WriterRuntime>,
    links: Option<BrokerLinks>,
    config: ozzy_bindings::Config,
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
    Pending(Arc<SharedTopicPendingRecord>),
}
#[derive(Debug)]
pub(crate) enum Request {
    Connect(Box<ozzy_bindings::Config>),
    Producer(String, Option<ProducerIdentity>, bool),
    Consumer(String, ReaderStart, Option<Vec<u32>>),
    Send(RecordInput, Option<Vec<u8>>, Option<u32>),
    Flush,
    Next,
    Confirm(Arc<SharedTopicPendingRecord>),
    Close,
}
#[derive(Debug)]
pub(crate) enum Value {
    Unit,
    Handle(Box<Handle>),
    Receipt(SharedTopicReceipt),
    Record(TopicRecord),
}
impl Value {
    pub(crate) fn pending(value: SharedTopicPendingRecord) -> Self {
        Self::Handle(Box::new(Handle::Pending(Arc::new(value))))
    }
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
        (Handle::Client(client), Request::Producer(topic, identity, takeover)) => {
            producer(client, topic, identity, takeover).await
        }
        (Handle::Client(client), Request::Consumer(topic, start, partitions)) => {
            let reader = TopicReader::open(
                client.links.as_ref().ok_or("client is closed")?.clone(),
                &topic,
                TopicReaderConfig {
                    start,
                    partitions,
                    ..Default::default()
                },
            )
            .await
            .map_err(text)?;
            Ok(Value::Handle(Box::new(Handle::Consumer(Consumer {
                reader,
                last: None,
                parts: Vec::new(),
            }))))
        }
        (Handle::Producer(writer), Request::Send(record, key, partition)) => send(
            writer.as_mut().ok_or("producer is closed")?,
            record,
            key.as_deref(),
            partition,
        )
        .await
        .map(Value::pending),
        (Handle::Producer(writer), Request::Flush) => writer
            .as_mut()
            .ok_or("producer is closed")?
            .flush()
            .await
            .map(|()| Value::Unit)
            .map_err(text),
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
            client.links = None;
            client.runtime = None;
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
        Handle::Pending(_) => Ok(()),
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

async fn producer(
    client: &Client,
    topic: String,
    identity: Option<ProducerIdentity>,
    takeover: bool,
) -> Result<Value, String> {
    let links = client.links.as_ref().ok_or("client is closed")?;
    let config = client.config.writer.clone();
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

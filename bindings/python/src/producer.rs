use crate::{
    awaitable::Awaitable,
    execution::{Lease, Owner, Slot, borrow, opened, probe, ready, timeout, wait},
    sdk_error,
    values::Receipt,
};
use bytes::Bytes;
use ozzy::{
    BrokerLinks, MessageId, ProducerIdentity, RecordInput, RetryPolicy, SharedTopicPendingRecord,
    SharedTopicWriter, SharedTopicWriterConfig,
};
use pyo3::{
    buffer::PyBuffer,
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    pybacked::PyBackedBytes,
    types::{PyBytes, PyMemoryView},
};
use std::{sync::Arc, task::Poll};
use sync_wrapper::SyncWrapper;

/// One payload part: a borrowed immutable `bytes` view, or one copy of any
/// other byte buffer taken before admission.
#[derive(Debug)]
pub(crate) enum Part {
    Shared(PyBackedBytes),
    Copied(Vec<u8>),
}

impl<'py> FromPyObject<'_, 'py> for Part {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        let py = obj.py();
        if let Ok(bytes) = obj.cast::<PyBytes>() {
            return Ok(Self::Shared(bytes.to_owned().into()));
        }
        if let Ok(buffer) = PyBuffer::<u8>::get(&obj) {
            let mut copy = vec![0; buffer.item_count()];
            buffer.copy_to_slice(py, &mut copy)?;
            return Ok(Self::Copied(copy));
        }
        if obj.is_instance_of::<PyMemoryView>() {
            // Typed views count bytes, not items; `bytes` flattens any format.
            let bytes = py
                .get_type::<PyBytes>()
                .call1((obj,))?
                .cast_into::<PyBytes>()?;
            return Ok(Self::Shared(bytes.into()));
        }
        Err(PyTypeError::new_err("payload parts must be bytes-like"))
    }
}

impl Part {
    fn bytes(self) -> Bytes {
        match self {
            Self::Shared(body) => Bytes::copy_from_slice(&body),
            Self::Copied(body) => Bytes::from(body),
        }
    }
    fn record(self, id: MessageId) -> RecordInput {
        match self {
            Self::Shared(body) => RecordInput::copy_from_slice(id, &body),
            // Unique storage moves into admission without another copy.
            Self::Copied(body) => RecordInput::single(id, Bytes::from(body)),
        }
    }
}

#[derive(Debug, FromPyObject)]
pub(crate) enum Payload {
    #[pyo3(transparent)]
    Single(Part),
    #[pyo3(transparent)]
    Multipart(Vec<Part>),
}
impl Payload {
    fn record(self, id: MessageId) -> RecordInput {
        match self {
            Self::Single(part) => part.record(id),
            Self::Multipart(mut parts) if parts.len() == 1 => parts.remove(0).record(id),
            Self::Multipart(parts) => {
                RecordInput::multipart(id, parts.into_iter().map(Part::bytes))
            }
        }
    }
}

#[pyclass(module = "ozzy._native")]
#[derive(Debug)]
pub(crate) struct Producer(SyncWrapper<Slot<Option<SharedTopicWriter>>>);
impl Owner for Producer {
    type State = Option<SharedTopicWriter>;
    fn slot(&mut self) -> &mut Slot<Self::State> {
        self.0.get_mut()
    }
}

impl Producer {
    pub(crate) async fn open(
        links: &BrokerLinks,
        topic: String,
        config: SharedTopicWriterConfig,
        identity: Option<Vec<u8>>,
        takeover: bool,
    ) -> PyResult<Self> {
        let retry = RetryPolicy::default();
        let writer = if let Some(identity) = identity {
            let bytes = identity
                .try_into()
                .map_err(|_| PyValueError::new_err("identity must be 32 bytes"))?;
            let identity = ProducerIdentity::from_bytes(bytes).map_err(sdk_error)?;
            if takeover {
                SharedTopicWriter::takeover(links, &topic, identity, config, retry).await
            } else {
                SharedTopicWriter::resume(links, &topic, identity, config, retry).await
            }
        } else {
            if takeover {
                return Err(PyValueError::new_err("takeover requires an identity"));
            }
            SharedTopicWriter::open(links, &topic, config, retry).await
        }
        .map_err(sdk_error)?;
        Ok(Self(SyncWrapper::new(Slot::new(Some(writer)))))
    }
}

type Destination = (Option<Vec<u8>>, Option<u32>);

/// Validate routing and identity, then build the record (one payload copy).
fn admission(
    parts: Payload,
    id: Option<Vec<u8>>,
    destination: Destination,
) -> PyResult<(RecordInput, Destination)> {
    if destination.0.is_some() && destination.1.is_some() {
        return Err(PyValueError::new_err(
            "key and partition are mutually exclusive",
        ));
    }
    let id = match id {
        Some(id) => {
            let bytes: [u8; 16] = id
                .try_into()
                .map_err(|_| PyValueError::new_err("message ID must be 16 bytes"))?;
            if bytes == [0; 16] {
                return Err(PyValueError::new_err("message ID must be nonzero"));
            }
            MessageId::from_bytes(bytes)
        }
        None => MessageId::new(),
    };
    Ok((parts.record(id), destination))
}

async fn send(
    writer: &mut SharedTopicWriter,
    record: RecordInput,
    destination: Destination,
) -> PyResult<PendingRecord> {
    let (key, partition) = destination;
    let pending = match partition {
        Some(partition) => writer.send_to_partition(record, partition).await,
        None => writer.send(record, key.as_deref()).await,
    }
    .map_err(sdk_error)?;
    Ok(PendingRecord(Slot::new(Arc::new(pending))))
}

#[pymethods]
impl Producer {
    #[getter]
    fn identity<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyBytes>> {
        let identity = {
            let mut this = borrow(slf)?;
            let writer = opened(this.slot().get_mut()?)?;
            writer.identity().to_bytes()
        };
        Ok(PyBytes::new(slf.py(), &identity))
    }
    #[getter]
    fn partition_count(slf: &Bound<'_, Self>) -> PyResult<usize> {
        let mut this = borrow(slf)?;
        Ok(opened(this.slot().get_mut()?)?.metadata().partition_count())
    }
    #[pyo3(signature = (parts, id, destination, timeout=None))]
    fn send(
        slf: Bound<'_, Self>,
        parts: Payload,
        id: Option<Vec<u8>>,
        destination: Destination,
        timeout: Option<f64>,
    ) -> PyResult<PendingRecord> {
        let timeout = self::timeout(timeout)?;
        let (record, destination) = admission(parts, id, destination)?;
        let py = slf.py();
        let mut lease = Lease::take(slf.unbind())?;
        ready(
            py,
            send(opened(lease.get_mut()?)?, record, destination),
            timeout,
        )?
    }
    fn try_send(
        slf: &Bound<'_, Self>,
        parts: Payload,
        id: Option<Vec<u8>>,
        destination: Destination,
    ) -> PyResult<Option<PendingRecord>> {
        // The Rust SDK assigns a sequence only on successful admission.
        // A pending probe is canceled before the asyncio fallback polls again.
        let (record, destination) = admission(parts, id, destination)?;
        let mut this = borrow(slf)?;
        let writer = opened(this.slot().get_mut()?)?;
        match probe(send(writer, record, destination)) {
            Poll::Ready(result) => result.map(Some),
            Poll::Pending => Ok(None),
        }
    }
    fn asend(
        slf: Py<Self>,
        parts: Payload,
        id: Option<Vec<u8>>,
        destination: Destination,
    ) -> PyResult<Awaitable> {
        let (record, destination) = admission(parts, id, destination)?;
        Ok(Awaitable::hot(async move {
            let mut lease = Lease::take(slf)?;
            let pending = send(opened(lease.get_mut()?)?, record, destination).await?;
            Python::attach(|py| Py::new(py, pending).map(Py::into_any))
        }))
    }
    /// Capture the admitted prefix, then wait without holding the handle,
    /// so other callers may keep sending meanwhile.
    #[pyo3(signature = (timeout=None))]
    fn flush(slf: &Bound<'_, Self>, timeout: Option<f64>) -> PyResult<()> {
        let timeout = self::timeout(timeout)?;
        let flushed = {
            let mut this = borrow(slf)?;
            opened(this.slot().get_mut()?)?.flush()
        };
        wait(slf.py(), flushed, timeout)?.map_err(sdk_error)
    }
    fn aflush(slf: &Bound<'_, Self>) -> PyResult<Awaitable> {
        let flushed = {
            let mut this = borrow(slf)?;
            opened(this.slot().get_mut()?)?.flush()
        };
        Ok(Awaitable::hot(async move {
            flushed.await.map_err(sdk_error)?;
            Ok(crate::awaitable::none())
        }))
    }
    fn close(slf: &Bound<'_, Self>) -> PyResult<()> {
        let writer = {
            let mut this = borrow(slf)?;
            this.slot().get_mut()?.take()
        };
        if let Some(writer) = writer {
            wait(slf.py(), writer.close(), None)?.map_err(sdk_error)?;
        }
        Ok(())
    }
    fn aclose(slf: Py<Self>) -> Awaitable {
        Awaitable::lifecycle(async move {
            // Seal on first poll, as an unawaited close leaves the writer open.
            let writer = Python::attach(|py| {
                let mut this = borrow(slf.bind(py))?;
                this.slot().get_mut().map(Option::take)
            })?;
            if let Some(writer) = writer {
                writer.close().await.map_err(sdk_error)?;
            }
            Ok(crate::awaitable::none())
        })
    }
}

/// Confirmation observer. Dropping it never cancels an admitted record.
#[pyclass(module = "ozzy._native", frozen)]
#[derive(Debug)]
pub(crate) struct PendingRecord(Slot<Arc<SharedTopicPendingRecord>, false>);

#[pymethods]
impl PendingRecord {
    /// Topic UUID, formatted on access.
    #[getter]
    fn topic_id(&self) -> PyResult<String> {
        Ok(self.0.get()?.topic().to_string())
    }
    /// Numeric destination partition selected before admission.
    #[getter]
    fn partition(&self) -> PyResult<u32> {
        Ok(self.0.get()?.partition())
    }
    /// Retry sequence within this producer and partition, independent of batching.
    #[getter]
    fn sequence(&self) -> PyResult<u64> {
        Ok(self.0.get()?.sequence())
    }
    /// Wait for a durable receipt, releasing the GIL while waiting.
    #[pyo3(signature = (timeout=None))]
    fn confirmed(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<Receipt> {
        let timeout = self::timeout(timeout)?;
        let pending = self.0.get()?;
        let result = match pending.try_confirmed() {
            Some(result) => result,
            None => wait(py, pending.confirmed(), timeout)?,
        };
        result.map(Receipt::from).map_err(sdk_error)
    }
    /// Await a durable receipt. Cancellation cancels only this observation.
    fn aconfirmed(&self) -> PyResult<Awaitable> {
        let pending = self.0.get()?.clone();
        Ok(Awaitable::hot(async move {
            let receipt = pending.confirmed().await.map_err(sdk_error)?;
            Python::attach(|py| Py::new(py, Receipt::from(receipt)).map(Py::into_any))
        }))
    }
    /// Return a receipt if ready, None if pending, or raise on failure.
    fn try_confirmed(&self) -> PyResult<Option<Receipt>> {
        self.0
            .get()?
            .try_confirmed()
            .map(|result| result.map(Receipt::from).map_err(sdk_error))
            .transpose()
    }
}

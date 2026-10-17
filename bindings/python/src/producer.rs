use crate::{
    execution::{Lease, Owner, Slot, drive, opened, ready, wait},
    sdk_error,
    values::Receipt,
};
use bytes::Bytes;
use ozzy::{
    BrokerLinks, MessageId, ProducerIdentity, RecordInput, RetryPolicy, SharedTopicPendingRecord,
    SharedTopicWriter, SharedTopicWriterConfig,
};
use pyo3::{exceptions::PyValueError, prelude::*, pybacked::PyBackedBytes, types::PyBytes};
use std::{
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};
use sync_wrapper::SyncWrapper;

#[derive(Debug, FromPyObject)]
pub(crate) enum Payload {
    #[pyo3(transparent)]
    Single(PyBackedBytes),
    #[pyo3(transparent)]
    Multipart(Vec<PyBackedBytes>),
}
impl Payload {
    fn record(self, id: MessageId) -> RecordInput {
        match self {
            Self::Single(body) => RecordInput::copy_from_slice(id, &body),
            Self::Multipart(parts) => match parts.as_slice() {
                [body] => RecordInput::copy_from_slice(id, body),
                _ => RecordInput::multipart(id, parts.iter().map(|p| Bytes::copy_from_slice(p))),
            },
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

async fn send(
    writer: &mut SharedTopicWriter,
    parts: Payload,
    id: Option<Vec<u8>>,
    destination: (Option<Vec<u8>>, Option<u32>),
) -> PyResult<PendingRecord> {
    let (key, partition) = destination;
    if key.is_some() && partition.is_some() {
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
    let record = parts.record(id);
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
    fn identity<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let writer = self
            .0
            .get_mut()
            .get()?
            .as_ref()
            .ok_or_else(|| crate::ClosedError::new_err("producer is closed"))?;
        Ok(PyBytes::new(py, &writer.identity().to_bytes()))
    }
    #[getter]
    fn partition_count(&mut self) -> PyResult<usize> {
        Ok(self
            .0
            .get_mut()
            .get()?
            .as_ref()
            .ok_or_else(|| crate::ClosedError::new_err("producer is closed"))?
            .metadata()
            .partition_count())
    }
    fn send(
        &mut self,
        py: Python<'_>,
        parts: Payload,
        id: Option<Vec<u8>>,
        destination: (Option<Vec<u8>>, Option<u32>),
    ) -> PyResult<PendingRecord> {
        ready(
            py,
            send(opened(self.slot().get_mut()?)?, parts, id, destination),
        )?
    }
    fn try_send(
        &mut self,
        parts: Payload,
        id: Option<Vec<u8>>,
        destination: (Option<Vec<u8>>, Option<u32>),
    ) -> PyResult<Option<PendingRecord>> {
        // The Rust SDK assigns a sequence only on successful admission.
        // A pending probe is canceled before the asyncio fallback polls again.
        let mut future = pin!(send(
            opened(self.slot().get_mut()?)?,
            parts,
            id,
            destination
        ));
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(result) => result.map(Some),
            Poll::Pending => Ok(None),
        }
    }
    async fn asend(
        slf: Py<Self>,
        parts: Payload,
        id: Option<Vec<u8>>,
        destination: (Option<Vec<u8>>, Option<u32>),
    ) -> PyResult<PendingRecord> {
        let mut lease = Lease::take(slf)?;
        send(opened(lease.get_mut()?)?, parts, id, destination).await
    }
    fn flush(&mut self, py: Python<'_>) -> PyResult<()> {
        wait(py, opened(self.slot().get_mut()?)?.flush())?.map_err(sdk_error)
    }
    async fn aflush(slf: Py<Self>) -> PyResult<()> {
        let mut lease = Lease::take(slf)?;
        drive(opened(lease.get_mut()?)?.flush())
            .await?
            .map_err(sdk_error)
    }
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(writer) = self.slot().get_mut()?.take() {
            wait(py, writer.close())?.map_err(sdk_error)?;
        }
        Ok(())
    }
    async fn aclose(slf: Py<Self>) -> PyResult<()> {
        let mut lease = Lease::take(slf)?;
        if let Some(writer) = lease.get_mut()?.take() {
            drive(writer.close()).await?.map_err(sdk_error)?;
        }
        Ok(())
    }
}

/// Confirmation observer. Dropping it never cancels an admitted record.
#[pyclass(module = "ozzy._native")]
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
    fn confirmed(&self, py: Python<'_>) -> PyResult<Receipt> {
        ready(py, self.0.get()?.confirmed())?
            .map(Receipt::from)
            .map_err(sdk_error)
    }
    /// Await a durable receipt. Cancellation cancels only this observation.
    async fn aconfirmed(slf: Py<Self>) -> PyResult<Receipt> {
        let pending = Python::attach(|py| {
            let pending = slf.try_borrow(py)?;
            Ok::<_, PyErr>(pending.0.get()?.clone())
        })?;
        pending
            .confirmed()
            .await
            .map(Receipt::from)
            .map_err(sdk_error)
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

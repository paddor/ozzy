use ozzy::{
    AppendDisposition, Offset, Policy, SharedTopicReceipt, TopicCheckpoint, TopicId, TopicRecord,
};
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyBytes, PyTuple},
};

/// Immutable durable confirmation. UUID strings are formatted only on access.
/// Confirmation does not prove application processing.
#[pyclass(module = "ozzy._native", frozen)]
#[derive(Debug)]
pub(crate) struct Receipt(SharedTopicReceipt);
impl From<SharedTopicReceipt> for Receipt {
    fn from(receipt: SharedTopicReceipt) -> Self {
        Self(receipt)
    }
}
#[pymethods]
impl Receipt {
    /// Topic UUID.
    #[getter]
    fn topic_id(&self) -> String {
        self.0.topic.to_string()
    }
    /// Numeric destination partition.
    #[getter]
    fn partition(&self) -> u32 {
        self.0.partition
    }
    /// Partition incarnation UUID, fencing older histories.
    #[getter]
    fn incarnation(&self) -> String {
        self.0.record.partition.to_string()
    }
    /// Partition-global record offset.
    #[getter]
    fn offset(&self) -> u64 {
        self.0.record.offset
    }
    /// Application event UUID used for record identity.
    #[getter]
    fn message_id(&self) -> String {
        self.0.record.message_id.to_string()
    }
    /// UUID of the producer that submitted this record.
    #[getter]
    fn producer_id(&self) -> String {
        self.0.record.key.producer_id.to_string()
    }
    /// Fencing epoch of the submitting producer.
    #[getter]
    fn producer_epoch(&self) -> u64 {
        self.0.record.key.producer_epoch
    }
    /// Partition owner epoch that confirmed the record.
    #[getter]
    fn owner_epoch(&self) -> u64 {
        self.0.record.owner_epoch
    }
    /// Retry sequence within this producer and partition, independent of batching.
    #[getter]
    fn sequence(&self) -> u64 {
        self.0.record.key.first_sequence
    }
    /// 'stored' for a new event or 'duplicate' for an existing deduplicated event.
    #[getter]
    fn disposition(&self) -> &'static str {
        match self.0.record.disposition {
            AppendDisposition::Stored => "stored",
            AppendDisposition::Duplicate => "duplicate",
            _ => "unknown",
        }
    }
    /// Durability boundary: `local_durable` or `quorum_durable`.
    #[getter]
    fn policy(&self) -> &'static str {
        match self.0.record.policy {
            Policy::LocalDurable => "local_durable",
            Policy::QuorumDurable => "quorum_durable",
        }
    }
}

/// Immutable delivered record with partition-global metadata and opaque payload.
/// Delivery advances the reader; it does not acknowledge application processing.
#[pyclass(module = "ozzy._native", frozen)]
#[derive(Debug)]
pub(crate) struct Record(TopicRecord);
impl From<TopicRecord> for Record {
    fn from(record: TopicRecord) -> Self {
        Self(record)
    }
}
#[pymethods]
impl Record {
    /// Topic UUID, formatted on access.
    #[getter]
    fn topic_id(&self) -> String {
        self.0.topic.to_string()
    }
    /// Numeric partition containing this record.
    #[getter]
    fn partition(&self) -> u32 {
        self.0.partition
    }
    /// Partition incarnation UUID, formatted on access.
    #[getter]
    fn incarnation(&self) -> String {
        self.0.incarnation.to_string()
    }
    /// Partition-global record offset.
    #[getter]
    fn offset(&self) -> u64 {
        self.0.offset.get()
    }
    /// Application event UUID, formatted on access.
    #[getter]
    fn message_id(&self) -> String {
        self.0.message_id.to_string()
    }
    /// Tuple of copied Python bytes. Read once when retaining the payload.
    #[getter]
    fn parts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(py, self.0.payload.iter().map(|part| PyBytes::new(py, part)))
    }
}

/// Native checkpoint: a topic UUID and first undelivered partition offsets.
#[pyclass(module = "ozzy._native", frozen, from_py_object)]
#[derive(Debug, Clone)]
pub(crate) struct Checkpoint(pub(crate) TopicCheckpoint);
#[pymethods]
impl Checkpoint {
    /// Construct from a 16-byte topic UUID and (partition, next offset) pairs.
    #[new]
    fn new(topic_id: Vec<u8>, positions: Vec<(u32, u64)>) -> PyResult<Self> {
        let topic_id = topic_id
            .try_into()
            .map_err(|_| PyValueError::new_err("topic ID must be 16 bytes"))?;
        Ok(Self(TopicCheckpoint {
            topic: TopicId::from_bytes(topic_id),
            positions: positions
                .into_iter()
                .map(|(p, o)| (p, Offset::new(o)))
                .collect(),
        }))
    }
    /// Topic UUID, formatted on access.
    #[getter]
    fn topic_id(&self) -> String {
        self.0.topic.to_string()
    }
    /// Numeric partitions and their first undelivered offsets.
    #[getter]
    fn positions(&self) -> Vec<(u32, u64)> {
        self.0
            .positions
            .iter()
            .map(|&(p, o)| (p, o.get()))
            .collect()
    }
}

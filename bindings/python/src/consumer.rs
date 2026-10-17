use crate::{
    execution::{Lease, Owner, Slot, drive, ready, wait},
    sdk_error,
    values::{Checkpoint, Record},
};
use ozzy::{BrokerLinks, ReaderStart, TopicReader, TopicReaderConfig};
use pyo3::{exceptions::PyValueError, prelude::*};
use sync_wrapper::SyncWrapper;

#[pyclass(module = "ozzy._native")]
#[derive(Debug)]
pub(crate) struct Consumer(SyncWrapper<Slot<TopicReader>>);
impl Owner for Consumer {
    type State = TopicReader;
    fn slot(&mut self) -> &mut Slot<TopicReader> {
        self.0.get_mut()
    }
}

impl Consumer {
    pub(crate) async fn open(
        links: BrokerLinks,
        topic: String,
        start: String,
        checkpoint: Option<Checkpoint>,
        partitions: Option<Vec<u32>>,
    ) -> PyResult<Self> {
        let start = if let Some(checkpoint) = checkpoint {
            if start != "earliest" {
                return Err(PyValueError::new_err(
                    "start and checkpoint are mutually exclusive",
                ));
            }
            ReaderStart::Checkpoint(checkpoint.0)
        } else {
            match start.as_str() {
                "earliest" => ReaderStart::Earliest,
                "latest" => ReaderStart::Latest,
                _ => {
                    return Err(PyValueError::new_err(
                        "start must be 'earliest' or 'latest'",
                    ));
                }
            }
        };
        let config = TopicReaderConfig {
            start,
            partitions,
            ..Default::default()
        };
        Ok(Self(SyncWrapper::new(Slot::new(
            TopicReader::open(links, &topic, config)
                .await
                .map_err(sdk_error)?,
        ))))
    }
}

#[pymethods]
impl Consumer {
    fn next(&mut self, py: Python<'_>) -> PyResult<Record> {
        ready(py, self.slot().get_mut()?.next())?
            .map(Record::from)
            .map_err(sdk_error)
    }
    async fn anext(slf: Py<Self>) -> PyResult<Record> {
        let mut lease = Lease::take(slf)?;
        lease
            .get_mut()?
            .next()
            .await
            .map(Record::from)
            .map_err(sdk_error)
    }
    fn try_next(&mut self) -> PyResult<Option<Record>> {
        self.slot()
            .get_mut()?
            .try_next()
            .map(|record| record.map(Record::from))
            .map_err(sdk_error)
    }
    fn checkpoint(&mut self) -> PyResult<Checkpoint> {
        Ok(Checkpoint(self.slot().get()?.checkpoint()))
    }
    fn stats(&mut self) -> PyResult<(u64, u64, u64)> {
        let stats = self.slot().get()?.stats();
        Ok((stats.live_records, stats.replayed_records, stats.live_gaps))
    }
    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        wait(py, self.slot().get_mut()?.close())?.map_err(sdk_error)
    }
    async fn aclose(slf: Py<Self>) -> PyResult<()> {
        let mut lease = Lease::take(slf)?;
        drive(lease.get_mut()?.close()).await?.map_err(sdk_error)
    }
}

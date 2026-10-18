use crate::{
    awaitable::Awaitable,
    execution::{Lease, Owner, Slot, borrow, ready, timeout, wait},
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
    #[pyo3(signature = (timeout=None))]
    fn next(slf: Py<Self>, py: Python<'_>, timeout: Option<f64>) -> PyResult<Record> {
        let timeout = self::timeout(timeout)?;
        let buffered = {
            let mut this = borrow(slf.bind(py))?;
            this.slot().get_mut()?.try_next().map_err(sdk_error)?
        };
        let record = if let Some(record) = buffered {
            record
        } else {
            let mut lease = Lease::take(slf)?;
            ready(py, lease.get_mut()?.next(), timeout)?.map_err(sdk_error)?
        };
        Record::new(py, record)
    }
    fn anext(slf: Py<Self>) -> Awaitable {
        Awaitable::hot(async move {
            let record = {
                let mut lease = Lease::take(slf)?;
                lease.get_mut()?.next().await.map_err(sdk_error)?
            };
            Python::attach(|py| Py::new(py, Record::new(py, record)?).map(Py::into_any))
        })
    }
    fn try_next(slf: &Bound<'_, Self>) -> PyResult<Option<Record>> {
        let record = {
            let mut this = borrow(slf)?;
            this.slot().get_mut()?.try_next().map_err(sdk_error)?
        };
        record
            .map(|record| Record::new(slf.py(), record))
            .transpose()
    }
    fn checkpoint(slf: &Bound<'_, Self>) -> PyResult<Checkpoint> {
        let mut this = borrow(slf)?;
        Ok(Checkpoint(this.slot().get()?.checkpoint()))
    }
    fn stats(slf: &Bound<'_, Self>) -> PyResult<(u64, u64, u64)> {
        let mut this = borrow(slf)?;
        let stats = this.slot().get()?.stats();
        Ok((stats.live_records, stats.replayed_records, stats.live_gaps))
    }
    fn close(slf: Py<Self>, py: Python<'_>) -> PyResult<()> {
        let mut lease = Lease::take(slf)?;
        wait(py, lease.get_mut()?.close(), None)?.map_err(sdk_error)
    }
    fn aclose(slf: Py<Self>) -> Awaitable {
        Awaitable::lifecycle(async move {
            let mut lease = Lease::take(slf)?;
            lease.get_mut()?.close().await.map_err(sdk_error)?;
            Ok(crate::awaitable::none())
        })
    }
}
